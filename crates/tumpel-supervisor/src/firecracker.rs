use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, Interest};
use tokio::net::unix::pipe;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::task::AbortHandle;
use tumpel_api::types::{
    EntityId, GuestHello, GuestInput, GuestMessage, SupervisorHandoverBox, SupervisorHandoverMachine, SupervisorRuntime,
};
use tumpel_guest::setup_output::{redacted, strip_ansi};
use tumpel_guest::{encode, Decoder, GUEST_CID, GUEST_PORT, READY_PORT};

use crate::boot::Boot;
use crate::bounded::bounded;
use crate::fd_store;
use crate::guest_drive;
use crate::lifecycle::{remove_dir, Attached, Guest, Machine, Stopped, Vmm};
use crate::pool::{Disk, Size, Tap, TapPool};
use crate::snapshot::Ext4Upper;

const BOOT_ARGS: &str = "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/sbin/tumpel-init";
const CONSOLE_TAIL: usize = 4096;
pub const OVERLAY_DRIVE: &str = "overlay.ext4";
const REPORT_INSPECTION: Duration = Duration::from_secs(1);
pub(crate) const TAKEOVER_INSPECTION_SECONDS: u32 = 5;
const TAKEOVER_INSPECTION: Duration = Duration::from_secs(TAKEOVER_INSPECTION_SECONDS as u64);
const SCOPE_STOP: Duration = Duration::from_secs(30);
const API_WAIT_STEP: Duration = Duration::from_millis(5);

struct Layout {
    rootfs: PathBuf,
    kernel: PathBuf,
    drives: Vec<(&'static str, PathBuf)>,
    args: String,
    restore_arg: &'static str,
}

fn present(path: PathBuf, fix: &str) -> Result<PathBuf, String> {
    if path.exists() {
        return Ok(path);
    }
    Err(format!("{} is missing from the image cache; {fix}", path.display()))
}

fn boot_word(value: &str, named: &str) -> Result<String, String> {
    if !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | '+'))
    {
        return Ok(value.to_string());
    }
    Err(format!(
        "the harness layer names {named} {value:?}, which the kernel command line cannot carry; rebuild the layer with a {named} of letters, digits, '.', '_', '-' and '+'"
    ))
}

pub struct VsockGuest {
    stream: UnixStream,
    decoder: Decoder,
}

impl VsockGuest {
    async fn read_message(&mut self) -> Result<GuestMessage, String> {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            if let Some(message) = self.decoder.take::<GuestMessage>().map_err(|error| error.to_string())? {
                if matches!(message, GuestMessage::Unrecognised(_)) {
                    tumpel_channel::update_required();
                    continue;
                }
                return Ok(message);
            }
            let read = self
                .stream
                .read(&mut buffer)
                .await
                .map_err(|error| format!("reading from the guest: {error}"))?;
            if read == 0 {
                return Err("the guest closed its vsock connection".to_string());
            }
            self.decoder.push(&buffer[..read]);
        }
    }

    async fn hello(mut self) -> Result<(Self, GuestHello), String> {
        match self.read_message().await? {
            GuestMessage::Hello(hello) => Ok((self, hello)),
            other => Err(format!("the guest opened with {other:?} instead of GuestHello")),
        }
    }
}

impl Guest for VsockGuest {
    async fn send(&mut self, input: &GuestInput) -> Result<(), String> {
        let frame = encode(input).map_err(|error| error.to_string())?;
        self.stream
            .write_all(&frame)
            .await
            .map_err(|error| format!("writing to the guest: {error}"))
    }

    async fn receive(&mut self) -> Result<GuestMessage, String> {
        self.read_message().await
    }
}

fn remove(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            Err(format!("removing {}: {error}", path.display()))
        }
        _ => Ok(()),
    }
}

fn entries(dir: &Path) -> Result<Vec<std::fs::DirEntry>, String> {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|error| format!("listing {}: {error}", dir.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(format!("listing {}: {error}", dir.display())),
    }
}

fn box_index(name: &std::ffi::OsStr) -> Option<u8> {
    name.to_str()?.strip_prefix("box")?.parse().ok()
}

pub fn remove_leftovers(data: &Path, boxes: &[u8], sessions: &[EntityId]) -> Result<(), String> {
    let kept_boxes: Vec<String> = boxes.iter().map(|index| format!("box{index}")).collect();
    for entry in entries(data)? {
        let name = entry.file_name().to_string_lossy().into_owned();
        if box_index(&entry.file_name()).is_some() && !kept_boxes.contains(&name) {
            remove_dir(&entry.path())?;
        }
    }
    for entry in entries(&data.join("sessions"))? {
        if !sessions.iter().any(|session| entry.file_name() == session.as_str()) {
            remove_dir(&entry.path())?;
        }
    }
    remove_dir(&data.join(crate::persona_files::DIR))
}

fn tail_bytes(console: &Mutex<VecDeque<u8>>) -> Vec<u8> {
    console
        .lock()
        .map_or_default(|tail| tail.iter().copied().collect::<Vec<u8>>())
}

fn console_text(tail: &[u8]) -> String {
    let text = strip_ansi(&String::from_utf8_lossy(tail));
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .collect();
    redacted(&lines.join("\n"))
}

fn keep_tail(stream: impl AsyncRead + Unpin + Send + 'static, tail: Arc<Mutex<VecDeque<u8>>>) -> AbortHandle {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(mut tail) = tail.lock() else { return };
            tail.extend(line.as_bytes());
            tail.push_back(b'\n');
            let excess = tail.len().saturating_sub(CONSOLE_TAIL);
            tail.drain(..excess);
        }
    })
    .abort_handle()
}

async fn api(socket: &Path, method: &str, path: &str, body: &Value) -> Result<(), String> {
    let body = body.to_string();
    let mut stream = UnixStream::connect(socket)
        .await
        .map_err(|error| format!("connecting to the Firecracker API at {}: {error}", socket.display()))?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("{method} {path}: {error}"))?;
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    reader
        .read_line(&mut status)
        .await
        .map_err(|error| format!("{method} {path}: {error}"))?;
    let mut length = 0usize;
    loop {
        let mut header = String::new();
        reader
            .read_line(&mut header)
            .await
            .map_err(|error| format!("{method} {path}: {error}"))?;
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut answer = vec![0u8; length];
    reader
        .read_exact(&mut answer)
        .await
        .map_err(|error| format!("{method} {path}: {error}"))?;
    answered(method, path, &status, &answer)
}

fn answered(method: &str, path: &str, status: &str, answer: &[u8]) -> Result<(), String> {
    if status
        .split_whitespace()
        .nth(1)
        .is_some_and(|code| code.starts_with('2'))
    {
        return Ok(());
    }
    Err(format!(
        "Firecracker refused {method} {path}: {} {}",
        status.trim(),
        String::from_utf8_lossy(answer)
    ))
}

#[derive(serde::Deserialize)]
struct MachineConfig {
    mem_size_mib: NonZeroU32,
}

struct ApiReply {
    stream: std::os::unix::net::UnixStream,
    deadline: std::time::Instant,
}

impl std::io::Read for ApiReply {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
        }
        self.stream.set_read_timeout(Some(remaining))?;
        self.stream.read(buffer)
    }
}

fn connect_local(path: &Path) -> std::io::Result<std::os::unix::net::UnixStream> {
    use rustix::net::{connect, socket_with, AddressFamily, SocketAddrUnix, SocketFlags, SocketType};
    let socket = socket_with(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
        None,
    )?;
    connect(&socket, &SocketAddrUnix::new(path)?)?;
    let stream = std::os::unix::net::UnixStream::from(socket);
    stream.set_nonblocking(false)?;
    Ok(stream)
}

fn connect_api(socket: &Path, pid: libc::pid_t) -> Result<std::os::unix::net::UnixStream, String> {
    let stream = connect_local(socket)
        .map_err(|error| format!("connecting to the Firecracker API at {}: {error}", socket.display()))?;
    let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
    let mut size = libc::socklen_t::try_from(std::mem::size_of::<libc::ucred>())
        .map_err(|error| format!("reading Firecracker peer credentials: {error}"))?;
    if unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut peer).cast(),
            &raw mut size,
        )
    } < 0
    {
        return Err(format!(
            "reading Firecracker peer credentials: {}",
            std::io::Error::last_os_error()
        ));
    }
    if peer.pid != pid {
        return Err(format!(
            "the Firecracker API socket belongs to process {}, expected {pid}",
            peer.pid
        ));
    }
    Ok(stream)
}

fn booted_memory(socket: &Path, pid: libc::pid_t, limit: Duration) -> Result<NonZeroU32, String> {
    use std::io::{BufRead, Read, Write};
    let deadline = std::time::Instant::now() + limit;
    let failed = |error: std::io::Error| {
        format!(
            "reading GET /machine-config from {} within {} s: {error}",
            socket.display(),
            limit.as_secs()
        )
    };
    let mut stream = connect_api(socket, pid)?;
    stream
        .set_write_timeout(Some(deadline.saturating_duration_since(std::time::Instant::now())))
        .map_err(failed)?;
    stream
        .write_all(b"GET /machine-config HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\r\n")
        .map_err(failed)?;
    let mut reader = std::io::BufReader::new(ApiReply { stream, deadline });
    let mut status = String::new();
    reader.read_line(&mut status).map_err(failed)?;
    let mut length = 0usize;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).map_err(failed)?;
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value
                .trim()
                .parse()
                .map_err(|_| "Firecracker sent an invalid Content-Length")?;
            if length > 64 * 1024 {
                return Err("Firecracker sent an oversized machine configuration".to_string());
            }
        }
    }
    let mut answer = vec![0u8; length];
    reader.read_exact(&mut answer).map_err(failed)?;
    if !status.starts_with("HTTP/1.1 200 ") && !status.starts_with("HTTP/1.0 200 ") {
        return Err("Firecracker refused GET /machine-config".to_string());
    }
    serde_json::from_slice::<MachineConfig>(&answer)
        .map(|config| config.mem_size_mib)
        .map_err(|error| {
            format!(
                "invalid Firecracker GET /machine-config response from {}: {error}",
                socket.display()
            )
        })
}

fn create_private_dir(dir: &Path) -> Result<(), String> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|error| format!("creating {}: {error}", dir.display()))
}

fn create_private_file(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
}

fn session_dir(data: &Path, session_id: &EntityId) -> Result<PathBuf, String> {
    crate::dir::validate_session_id(session_id)?;
    Ok(data.join("sessions").join(&**session_id))
}

fn scope_of(index: u8) -> String {
    format!("tumpel-box{index}.scope")
}

async fn stop_scope(scope: &str) -> Result<(), String> {
    let stopped = tokio::time::timeout(
        SCOPE_STOP,
        Command::new("systemctl")
            .args(["--user", "stop", scope])
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| format!("stopping {scope} took longer than {} s", SCOPE_STOP.as_secs()))?
    .map_err(|error| format!("starting systemctl to stop {scope}: {error}"))?;
    if stopped.status.success() {
        return Ok(());
    }
    Err(format!(
        "stopping {scope} failed with {}: {}; inspect the scope with systemctl --user status {scope}",
        stopped.status,
        String::from_utf8_lossy(&stopped.stderr).trim()
    ))
}

#[derive(Debug, PartialEq)]
enum Owner {
    Child,
    Scope(String),
}

struct Adopted {
    pid: libc::pid_t,
    owner: Owner,
}

impl Adopted {
    async fn end(self) -> Result<String, String> {
        match self.owner {
            Owner::Child => tokio::task::spawn_blocking(move || end_child(self.pid))
                .await
                .map_err(|error| format!("could not end Firecracker: {error}")),
            Owner::Scope(scope) => {
                stop_scope(&scope).await?;
                Ok(format!(
                    "was stopped with {scope}; systemd holds its exit status, as this supervisor did not start it"
                ))
            }
        }
    }
}

enum Process {
    Starting(Child),
    Spawned(Child),
    Adopted(Adopted),
}

impl Process {
    fn id(&self) -> Option<u32> {
        match self {
            Self::Starting(child) | Self::Spawned(child) => child.id(),
            Self::Adopted(adopted) => u32::try_from(adopted.pid).ok(),
        }
    }

    async fn kill(self) -> Result<String, String> {
        match self {
            Self::Starting(mut child) | Self::Spawned(mut child) => kill_child(&mut child).await,
            Self::Adopted(adopted) => adopted.end().await,
        }
    }
}

async fn kill_child(child: &mut Child) -> Result<String, String> {
    if let Some(status) = child
        .try_wait()
        .map_err(|error| format!("checking Firecracker's exit status: {error}"))?
    {
        return Ok(format!("had already ended with {}", exited(status)));
    }
    child
        .start_kill()
        .map_err(|error| format!("stopping Firecracker: {error}"))?;
    let status = tokio::time::timeout(SCOPE_STOP, child.wait())
        .await
        .map_err(|_| {
            format!(
                "waiting for Firecracker to stop took longer than {} s",
                SCOPE_STOP.as_secs()
            )
        })?
        .map_err(|error| format!("waiting for Firecracker after stopping it: {error}"))?;
    Ok(format!("was stopped and ended with {}", exited(status)))
}

fn exited(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit status {code}");
    }
    if let Some(signal) = status.signal() {
        return format!("signal {signal}");
    }
    status.to_string()
}

fn end_child(pid: libc::pid_t) -> String {
    let mut status = 0;
    match unsafe { libc::waitpid(pid, &raw mut status, libc::WNOHANG) } {
        0 => {}
        waited if waited == pid => return format!("had already ended with {}", exited(ExitStatus::from_raw(status))),
        _ => return format!("could not be waited for: {}", std::io::Error::last_os_error()),
    }
    unsafe { libc::kill(pid, libc::SIGKILL) };
    if unsafe { libc::waitpid(pid, &raw mut status, 0) } == pid {
        return format!("was stopped and ended with {}", exited(ExitStatus::from_raw(status)));
    }
    format!(
        "was killed and could not be waited for: {}",
        std::io::Error::last_os_error()
    )
}

fn peek_exit(pidfd: &OwnedFd) -> String {
    let Ok(id) = libc::id_t::try_from(pidfd.as_raw_fd()) else {
        return format!("has a pidfd {} no wait can name", pidfd.as_raw_fd());
    };
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    if unsafe { libc::waitid(libc::P_PIDFD, id, &raw mut info, libc::WEXITED | libc::WNOWAIT) } < 0 {
        return format!(
            "ended with a status this supervisor cannot read: {}",
            std::io::Error::last_os_error()
        );
    }
    let status = unsafe { info.si_status() };
    match info.si_code {
        libc::CLD_EXITED => format!("ended with exit status {status}"),
        libc::CLD_KILLED | libc::CLD_DUMPED => format!("ended with signal {status}"),
        code => format!("ended with wait code {code} and status {status}"),
    }
}

fn watch(
    index: u8,
    pid: libc::pid_t,
    session: String,
    console: &Arc<Mutex<VecDeque<u8>>>,
    scoped: bool,
) -> Result<AbortHandle, String> {
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if raw < 0 {
        return Err(format!(
            "watching Firecracker process {pid}: {}",
            std::io::Error::last_os_error()
        ));
    }
    let raw = i32::try_from(raw).map_err(|error| format!("watching Firecracker process {pid}: {error}"))?;
    let pidfd = unsafe { AsyncFd::register_with_interest(OwnedFd::from_raw_fd(raw), Interest::READABLE) }
        .map_err(|error| format!("watching Firecracker process {pid}: {error}"))?;
    let console = Arc::clone(console);
    let watcher = tokio::spawn(async move {
        let status = match pidfd.readable().await {
            Ok(_) if scoped => "ended; systemd holds its exit status, as this supervisor did not start it".to_string(),
            Ok(_) => peek_exit(pidfd.get_ref()),
            Err(error) => format!("could no longer be watched: {error}"),
        };
        eprintln!(
            "tumpel-supervisor: box {index} ({}) of session {session} ended on its own: Firecracker process {pid} {status}. Console: {}",
            scope_of(index),
            console_text(&tail_bytes(&console))
        );
    });
    Ok(watcher.abort_handle())
}

fn process_scope(pid: libc::pid_t, scope: &str) -> Result<bool, String> {
    std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .map(|cgroups| cgroups.lines().any(|line| line.ends_with(&format!("/{scope}"))))
        .map_err(|error| format!("reading the cgroup of process {pid}: {error}"))
}

fn in_scope(pid: libc::pid_t, scope: &str) -> bool {
    process_scope(pid, scope).is_ok_and(|inside| inside)
}

#[derive(PartialEq, Eq)]
struct ProcessIdentity {
    parent: libc::pid_t,
    start: u64,
}

fn process_identity(pid: libc::pid_t) -> Result<Option<ProcessIdentity>, String> {
    let text = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("reading the identity of process {pid}: {error}")),
    };
    let (_, fields) = text
        .rsplit_once(") ")
        .ok_or_else(|| format!("process {pid} has no readable identity"))?;
    let mut fields = fields.split_whitespace();
    if matches!(fields.next(), Some("Z" | "X" | "x")) {
        return Ok(None);
    }
    let parent = fields.next().and_then(|field| field.parse().ok());
    let start = fields.nth(17).and_then(|field| field.parse().ok());
    match (parent, start) {
        (Some(parent), Some(start)) => Ok(Some(ProcessIdentity { parent, start })),
        _ => Err(format!("process {pid} has no readable parent and start time")),
    }
}

fn process_start(pid: libc::pid_t) -> Result<u64, String> {
    process_identity(pid)?
        .map(|identity| identity.start)
        .ok_or_else(|| format!("process {pid} is no longer running"))
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    supervisor_pid: libc::pid_t,
    supervisor_start: u64,
    machine_start: u64,
    guest: Descriptor,
    stdout: Descriptor,
    stderr: Descriptor,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
struct Descriptor {
    fd: RawFd,
    device: u64,
    inode: u64,
}

impl Descriptor {
    fn read(pid: libc::pid_t, fd: RawFd, named: &str, socket: bool) -> Result<Self, String> {
        if fd < 3 {
            return Err(format!("process {pid} has no recorded descriptor for {named}"));
        }
        let metadata = std::fs::metadata(format!("/proc/{pid}/fd/{fd}"))
            .map_err(|error| format!("reading {named} from process {pid}: {error}"))?;
        descriptor_kind(&metadata, named, socket)?;
        let fdinfo = std::fs::read_to_string(format!("/proc/{pid}/fdinfo/{fd}"))
            .map_err(|error| format!("reading the access mode for {named} from process {pid}: {error}"))?;
        let flags = fdinfo
            .lines()
            .find_map(|line| line.strip_prefix("flags:"))
            .and_then(|flags| i32::from_str_radix(flags.trim(), 8).ok())
            .ok_or_else(|| format!("process {pid} has no readable access mode for {named}"))?;
        let access = if socket { libc::O_RDWR } else { libc::O_RDONLY };
        if flags & (libc::O_ACCMODE | libc::O_PATH) != access {
            return Err(format!("process {pid} has the wrong access mode for {named}"));
        }
        if flags & libc::O_NONBLOCK == 0 {
            return Err(format!("process {pid} has a blocking descriptor for {named}"));
        }
        Ok(Self {
            fd,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn same_file(&self, other: &Self) -> bool {
        self.device == other.device && self.inode == other.inode
    }
}

fn descriptor_kind(metadata: &std::fs::Metadata, named: &str, socket: bool) -> Result<(), String> {
    let kind = metadata.file_type();
    if (socket && kind.is_socket()) || (!socket && kind.is_fifo()) {
        return Ok(());
    }
    Err(format!(
        "the descriptor handed over as {named} is no {}",
        if socket { "socket" } else { "pipe" }
    ))
}

fn move_into_scope(pid: libc::pid_t, scope: &str) -> Result<(), String> {
    let moved = std::process::Command::new("systemd-run")
        .args(["--user", "--scope", "--quiet", "--collect", "--unit", scope, "--"])
        .args([
            "/bin/sh",
            "-c",
            r#"echo "$1" > "/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/cgroup.procs""#,
            "sh",
        ])
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .output()
        .map_err(|error| format!("starting systemd-run to move Firecracker process {pid} into {scope}: {error}"))?;
    if moved.status.success() && in_scope(pid, scope) {
        return Ok(());
    }
    Err(format!(
        "moving Firecracker process {pid} into {scope} failed with {}: {}; run the supervisor from its systemd user unit",
        moved.status,
        String::from_utf8_lossy(&moved.stderr).trim()
    ))
}

fn duplicate(fd: RawFd) -> Result<OwnedFd, String> {
    let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
    if copy < 0 {
        return Err(format!(
            "duplicating box descriptor {fd}: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(copy) })
}

const DESCRIPTORS: [(&str, bool); 3] = [
    ("the guest connection", true),
    ("the box's standard output", false),
    ("the box's standard error", false),
];

#[derive(Clone, Copy)]
enum InspectionSource<'a> {
    Report(Option<&'a Observation>),
    Daemon(Option<&'a Observation>, bool),
}

#[derive(Debug)]
struct Attachment {
    pid: libc::pid_t,
    owner: Owner,
    memory: NonZeroU32,
    guest_buffer: Vec<u8>,
    console_tail: Vec<u8>,
}

fn handed_pid(handed: &SupervisorHandoverMachine) -> Option<libc::pid_t> {
    libc::pid_t::try_from(handed.process_id.get()).ok()
}

fn recorded_buffers(handed: &SupervisorHandoverMachine) -> Result<(Vec<u8>, Vec<u8>), String> {
    let guest = STANDARD
        .decode(handed.guest_buffer.as_bytes())
        .map_err(|error| format!("guestBuffer is not base64: {error}"))?;
    let console = STANDARD
        .decode(handed.console_tail.as_bytes())
        .map_err(|error| format!("consoleTail is not base64: {error}"))?;
    Ok((guest, console))
}

fn tail_of(fd: &OwnedFd, console: &Arc<Mutex<VecDeque<u8>>>, named: &str) -> Result<AbortHandle, String> {
    let reader = fd
        .try_clone()
        .map_err(|error| format!("duplicating {named}: {error}"))
        .and_then(|copy| pipe::Receiver::from_owned_fd(copy).map_err(|error| format!("reading {named}: {error}")))?;
    Ok(keep_tail(reader, Arc::clone(console)))
}

fn process_disappeared(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

fn scope_processes() -> Result<Vec<(u8, PathBuf)>, String> {
    let uid = unsafe { libc::geteuid() };
    let mut scopes = Vec::new();
    for process in entries(Path::new("/proc"))? {
        if process
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
            .is_none()
        {
            continue;
        }
        let path = process.path();
        match std::fs::metadata(&path) {
            Ok(metadata) if metadata.uid() == uid => {}
            Ok(_) => continue,
            Err(error) if process_disappeared(&error) => continue,
            Err(error) => return Err(format!("reading process identity at {}: {error}", path.display())),
        }
        let cgroup = match std::fs::read_to_string(path.join("cgroup")) {
            Ok(cgroup) => cgroup,
            Err(error) if process_disappeared(&error) => continue,
            Err(error) => return Err(format!("reading a box scope at {}: {error}", path.display())),
        };
        let index = cgroup.lines().find_map(|line| {
            line.rsplit('/')
                .next()?
                .strip_prefix("tumpel-box")?
                .strip_suffix(".scope")?
                .parse::<u8>()
                .ok()
        });
        if let Some(index) = index.filter(|index| *index < crate::pool::TAP_POOL) {
            scopes.push((index, path));
        }
    }
    Ok(scopes)
}

fn opened_overlays(root: &Path, process: &Path) -> Result<Vec<EntityId>, String> {
    let mut sessions = Vec::new();
    for fd in entries(&process.join("fd"))? {
        let path = match std::fs::read_link(fd.path()) {
            Ok(path) => path,
            Err(error) if process_disappeared(&error) => continue,
            Err(error) => return Err(format!("inspecting the box disk of {}: {error}", process.display())),
        };
        let Some(session) = path
            .parent()
            .filter(|_| path.file_name().is_some_and(|name| name == OVERLAY_DRIVE))
        else {
            continue;
        };
        if session.parent() == Some(root) {
            if let Some(id) = session
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|id| EntityId::try_from(id).ok())
            {
                sessions.push(id);
            }
        }
    }
    Ok(sessions)
}

fn scope_reservations(
    root: &Path,
    processes: &[(u8, PathBuf)],
) -> Result<std::collections::BTreeMap<u8, crate::lifecycle::ScopeDisks>, String> {
    use std::collections::BTreeMap;
    let mut reserved: BTreeMap<u8, crate::lifecycle::ScopeDisks> = processes
        .iter()
        .map(|(index, _)| (*index, crate::lifecycle::ScopeDisks::default()))
        .collect();
    if reserved.is_empty() {
        return Ok(reserved);
    }
    let canonical = match root.canonicalize() {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(reserved),
        Err(error) => return Err(format!("locating the saved box disks at {}: {error}", root.display())),
    };
    for (index, process) in processes {
        match opened_overlays(&canonical, process) {
            Ok(opened) => reserved.entry(*index).or_default().identified.extend(opened),
            Err(error) => eprintln!("tumpel-supervisor: {error}; keeping possible box disks"),
        }
    }
    if reserved.values().any(|disks| disks.identified.is_empty()) {
        eprintln!(
            "tumpel-supervisor: an unclaimed scope has no identifiable disk; keeping existing session directories"
        );
        let possible: Vec<_> = entries(root)?
            .into_iter()
            .filter_map(|entry| EntityId::try_from(entry.file_name().to_str()?).ok())
            .collect();
        for sessions in reserved.values_mut().filter(|disks| disks.identified.is_empty()) {
            sessions.possible.clone_from(&possible);
        }
    }
    for sessions in reserved.values_mut() {
        sessions.identified.sort();
        sessions.identified.dedup();
        sessions.possible.sort();
    }
    Ok(reserved)
}

pub struct Firecracker {
    binary: PathBuf,
    images: PathBuf,
    data: PathBuf,
    vcpus: u32,
}

impl Firecracker {
    #[must_use]
    pub fn new(runtime: &SupervisorRuntime) -> Self {
        Self {
            binary: PathBuf::from(&*runtime.firecracker_path),
            images: PathBuf::from(&*runtime.image_cache_dir),
            data: PathBuf::from(&*runtime.data_dir),
            vcpus: runtime.vcpu_count.get(),
        }
    }
}

impl Firecracker {
    fn clear_box_files(&self, index: u8) -> Result<(), String> {
        fd_store::forget_box(index)?;
        remove_dir(&self.data.join(format!("box{index}")))
    }

    fn attachment_pid(&self, index: u8, handed: &SupervisorHandoverMachine) -> Result<libc::pid_t, String> {
        let dir = self.data.join(format!("box{index}"));
        if Path::new(&handed.api_socket) != dir.join("api.sock") || Path::new(&handed.vsock) != dir.join("v.sock") {
            return Err(format!(
                "box {index} has unexpected socket paths; check dataDir in runtime.json"
            ));
        }
        handed_pid(handed).ok_or_else(|| format!("process id {} is out of range", handed.process_id))
    }

    pub(crate) fn observe(&self, box_: &crate::inventory::RecordedBox) -> Result<(), String> {
        let record = &box_.record;
        let index = u8::try_from(record.tap_index).map_err(|_| "the box has an invalid TAP index")?;
        let source = InspectionSource::Report(box_.observation.as_ref());
        if self
            .inspect_attachment(index, &record.machine, source, &scope_of(index))?
            .is_none()
        {
            return Err("the recorded Firecracker process is no longer running".to_string());
        }
        self.booted_disk(&record.session_id)
            .map_err(|error| format!("{error}; check the recorded session and dataDir in runtime.json"))?;
        Ok(())
    }

    fn inspect_attachment(
        &self,
        index: u8,
        handed: &SupervisorHandoverMachine,
        source: InspectionSource<'_>,
        scope: &str,
    ) -> Result<Option<Attachment>, String> {
        let pid = self.attachment_pid(index, handed)?;
        let Some(identity) = process_identity(pid)? else {
            return Ok(None);
        };
        let own_pid = unsafe { libc::getpid() };
        let (supervisor_pid, fds, expected, reporting, inherited) = match source {
            InspectionSource::Report(observed) => {
                let observed = observed.ok_or("the box record has no process and descriptor identities; start the installed service to refresh the box records")?;
                (
                    observed.supervisor_pid,
                    [observed.guest.fd, observed.stdout.fd, observed.stderr.fd],
                    Some(observed),
                    true,
                    false,
                )
            }
            InspectionSource::Daemon(expected, inherited) => (
                own_pid,
                [handed.guest_fd, handed.stdout_fd, handed.stderr_fd],
                expected,
                false,
                inherited,
            ),
        };
        let scoped = process_scope(pid, scope)?;
        let child = identity.parent == own_pid;
        if !(scoped || (inherited && child)) {
            if !reporting {
                return Ok(None);
            }
            return Err(format!("Firecracker process {pid} is outside {scope}"));
        }
        let owner = if child {
            Owner::Child
        } else {
            Owner::Scope(scope.to_string())
        };
        let supervisor_start = process_start(supervisor_pid)?;
        if let Some(expected) = expected {
            if identity.start != expected.machine_start || (reporting && supervisor_start != expected.supervisor_start)
            {
                return Err(format!("box {index} has a different Firecracker or supervisor process"));
            }
        }
        if fds
            .iter()
            .enumerate()
            .any(|(index, fd)| *fd >= 3 && fds[..index].contains(fd))
        {
            return Err(format!("box {index} has duplicate descriptor numbers"));
        }
        let expected_fds = expected.map(|observed| [&observed.guest, &observed.stdout, &observed.stderr]);
        for (role, (fd, (named, socket))) in fds.into_iter().zip(DESCRIPTORS).enumerate() {
            let actual = Descriptor::read(supervisor_pid, fd, named, socket)?;
            if expected_fds.is_some_and(|fds| !actual.same_file(fds[role])) {
                return Err(format!("box {index} has a different descriptor for {named}"));
            }
        }
        let vsock = std::fs::symlink_metadata(&*handed.vsock)
            .map_err(|error| format!("reading the box's vsock path: {error}"))?;
        if !vsock.file_type().is_socket() {
            return Err("the recorded vsock path does not name a Unix socket".to_string());
        }
        let (guest_buffer, console_tail) = recorded_buffers(handed)?;
        let limit = if reporting {
            REPORT_INSPECTION
        } else {
            TAKEOVER_INSPECTION
        };
        let memory = booted_memory(Path::new(&handed.api_socket), pid, limit)?;
        if process_start(pid)? != identity.start || process_start(supervisor_pid)? != supervisor_start {
            return Err(format!("box {index} changed process during inspection"));
        }
        Ok(Some(Attachment {
            pid,
            owner,
            memory,
            guest_buffer,
            console_tail,
        }))
    }

    fn attach_box(
        &self,
        session_id: &EntityId,
        tap: &Tap,
        handed: &SupervisorHandoverMachine,
        attachment: Attachment,
    ) -> Result<(FirecrackerBox, VsockGuest), String> {
        let mut machine = self.machine(session_id, tap)?;
        let guest = duplicate(handed.guest_fd)?;
        let stdout = duplicate(handed.stdout_fd)?;
        let stderr = duplicate(handed.stderr_fd)?;
        let stream = UnixStream::from_std(std::os::unix::net::UnixStream::from(guest))
            .map_err(|error| format!("adopting the guest connection: {error}"))?;
        let mut decoder = Decoder::default();
        decoder.push(&attachment.guest_buffer);
        let guest = VsockGuest { stream, decoder };
        machine
            .console
            .lock()
            .map_err(|_| "the box console is unavailable")?
            .extend(attachment.console_tail);
        machine.console_readers = [
            (&stdout, "the box's standard output"),
            (&stderr, "the box's standard error"),
        ]
        .into_iter()
        .map(|(fd, named)| {
            let copy = fd
                .try_clone()
                .map_err(|error| format!("duplicating {named}: {error}"))?;
            pipe::Receiver::from_owned_fd_unchecked(copy).map_err(|error| format!("registering {named}: {error}"))
        })
        .collect::<Result<_, _>>()?;
        machine.stdout = Some(stdout);
        machine.stderr = Some(stderr);
        let watched_by_systemd = matches!(attachment.owner, Owner::Scope(_));
        machine.watcher = Some(watch(
            tap.index(),
            attachment.pid,
            machine.session_name(),
            &machine.console,
            watched_by_systemd,
        )?);
        machine.process = Some(Process::Adopted(Adopted {
            pid: attachment.pid,
            owner: attachment.owner,
        }));
        Ok((machine, guest))
    }

    fn booted_disk(&self, session_id: &EntityId) -> Result<NonZeroU32, String> {
        let drive = session_dir(&self.data, session_id)?.join(OVERLAY_DRIVE);
        let bytes = std::fs::metadata(&drive)
            .map_err(|error| format!("reading the size of the box disk {}: {error}", drive.display()))?
            .len();
        u32::try_from(bytes / (1024 * 1024))
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| {
                format!(
                    "the box disk {} holds {bytes} bytes, not a size in whole MiB a box boots with",
                    drive.display()
                )
            })
    }
}

impl Vmm for Firecracker {
    type Machine = FirecrackerBox;
    type Upper = Ext4Upper;

    fn machine(&self, session_id: &EntityId, tap: &Tap) -> Result<FirecrackerBox, String> {
        Ok(FirecrackerBox {
            binary: self.binary.clone(),
            images: self.images.clone(),
            dir: self.data.join(format!("box{}", tap.index())),
            index: tap.index(),
            session: session_dir(&self.data, session_id)?,
            device: tap.device(),
            mac: tap.guest_mac(),
            boot_args: format!("{BOOT_ARGS} {}", tap.kernel_ip()),
            vcpus: self.vcpus,
            memory_mib: tap.memory_mib(),
            overlay_bytes: u64::from(tap.disk_mib().get()) * 1024 * 1024,
            process: None,
            watcher: None,
            console_readers: Vec::new(),
            console_tasks: Vec::new(),
            stdout: None,
            stderr: None,
            console: Arc::new(Mutex::new(VecDeque::with_capacity(CONSOLE_TAIL))),
        })
    }

    fn reattach(
        &self,
        session_id: &EntityId,
        index: i32,
        handed: &SupervisorHandoverMachine,
        observation: Option<&Observation>,
        pool: &mut TapPool,
        inherited: bool,
    ) -> Result<Option<Attached<FirecrackerBox>>, String> {
        let tap_index = u8::try_from(index).map_err(|_| "the box has an invalid TAP index")?;
        let source = InspectionSource::Daemon(observation, inherited);
        let Some(attachment) = self.inspect_attachment(tap_index, handed, source, &scope_of(tap_index))? else {
            return Ok(None);
        };
        let size = Size {
            memory_mib: attachment.memory,
            disk_mib: self.booted_disk(session_id)?,
        };
        let tap = pool.claim_device(index, size)?;
        match self.attach_box(session_id, &tap, handed, attachment) {
            Ok((machine, guest)) => Ok(Some((machine, guest, tap))),
            Err(error) => {
                pool.release(tap);
                Err(error)
            }
        }
    }

    fn recorded_process(
        &self,
        index: u8,
        handed: &SupervisorHandoverMachine,
        observation: Option<&Observation>,
    ) -> Result<bool, String> {
        let Some(pid) = handed_pid(handed) else {
            return Ok(false);
        };
        match std::fs::metadata(format!("/proc/{pid}")) {
            Ok(metadata) if metadata.uid() == unsafe { libc::geteuid() } => {}
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(format!("reading the owner of Firecracker process {pid}: {error}")),
        }
        let Some(identity) = process_identity(pid)? else {
            return Ok(false);
        };
        if observation.is_some_and(|observed| observed.machine_start != identity.start) {
            return Ok(false);
        }
        Ok(identity.parent == unsafe { libc::getpid() } || process_scope(pid, &scope_of(index))?)
    }

    fn disk(&self, drives: &[String]) -> Result<Disk, String> {
        crate::usage::disk(&self.data, drives).map_err(|error| format!("{error:#}"))
    }

    fn unclaimed_scopes(&self, claimed: &[u8]) -> Result<Vec<u8>, String> {
        Ok(scope_processes()?
            .into_iter()
            .map(|(index, _)| index)
            .filter(|index| !claimed.contains(index))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    fn unclaimed_reservations(
        &self,
        claimed: &[u8],
    ) -> Result<std::collections::BTreeMap<u8, crate::lifecycle::ScopeDisks>, String> {
        let mut processes = scope_processes()?;
        processes.retain(|(index, _)| !claimed.contains(index));
        scope_reservations(&self.data.join("sessions"), &processes)
    }

    async fn discard_incomplete_boot(&self, index: u8) -> Result<(), String> {
        if self.unclaimed_scopes(&[])?.contains(&index) {
            stop_scope(&scope_of(index)).await?;
        }
        self.clear_box_files(index)
    }

    async fn discard_protected(
        &self,
        index: u8,
        saved: &SupervisorHandoverBox,
        observation: Option<&Observation>,
    ) -> Result<(), String> {
        let scope = scope_of(index);
        let handed = &saved.machine;
        if self.recorded_process(index, handed, observation)? {
            let pid = handed_pid(handed).ok_or("the recorded Firecracker PID is out of range")?;
            if observation.is_none() {
                let pid = self.attachment_pid(index, handed)?;
                connect_api(Path::new(&handed.api_socket), pid)?;
                let sessions = self
                    .data
                    .join("sessions")
                    .canonicalize()
                    .map_err(|error| format!("locating the saved sessions: {error}"))?;
                let disks = opened_overlays(&sessions, &PathBuf::from(format!("/proc/{pid}")))?;
                if !disks.contains(&saved.session_id) {
                    return Err(format!(
                        "Firecracker process {pid} does not hold the saved session disk"
                    ));
                }
            }
            if process_scope(pid, &scope)? {
                stop_scope(&scope).await?;
            } else {
                let ended = Adopted {
                    pid,
                    owner: Owner::Child,
                }
                .end()
                .await?;
                if process_identity(pid)?.is_some() {
                    return Err(format!("Firecracker process {pid} {ended}"));
                }
            }
        } else if self.unclaimed_scopes(&[])?.contains(&index) {
            return Err(format!(
                "the saved box does not own {scope}; its process identity changed"
            ));
        }
        self.clear_box_files(index)
    }

    fn remove_leftovers(&self, boxes: &[u8], sessions: &[EntityId]) -> Result<(), String> {
        remove_leftovers(&self.data, boxes, sessions)
    }

    fn open_upper(&self, drive: &Path) -> Result<Ext4Upper, String> {
        Ext4Upper::load(drive)
    }

    fn staging(&self, session_id: &EntityId) -> Result<PathBuf, String> {
        session_dir(&self.data, session_id)
    }

    fn image_cache(&self) -> PathBuf {
        self.images.clone()
    }
}

pub struct FirecrackerBox {
    binary: PathBuf,
    images: PathBuf,
    dir: PathBuf,
    index: u8,
    session: PathBuf,
    device: String,
    mac: String,
    boot_args: String,
    vcpus: u32,
    memory_mib: NonZeroU32,
    overlay_bytes: u64,
    process: Option<Process>,
    watcher: Option<AbortHandle>,
    console_readers: Vec<pipe::Receiver>,
    console_tasks: Vec<AbortHandle>,
    stdout: Option<OwnedFd>,
    stderr: Option<OwnedFd>,
    console: Arc<Mutex<VecDeque<u8>>>,
}

impl Drop for FirecrackerBox {
    fn drop(&mut self) {
        self.unwatch();
        for task in &self.console_tasks {
            task.abort();
        }
        if let Some(Process::Starting(mut child)) =
            self.process.take_if(|process| matches!(process, Process::Starting(_)))
        {
            if let Err(error) = child.start_kill() {
                eprintln!(
                    "tumpel-supervisor: stopping the unfinished launcher on TAP {} failed: {error}",
                    self.index
                );
            }
        } else if let Some(pid) = self.process.as_ref().and_then(Process::id) {
            eprintln!("tumpel-supervisor: released the handle for Firecracker {pid} on TAP {}; its scope keeps any running process", self.index);
        }
    }
}

impl FirecrackerBox {
    fn session_name(&self) -> String {
        self.session
            .file_name()
            .map_or_default(|name| name.to_string_lossy().into_owned())
    }

    fn unwatch(&mut self) {
        if let Some(watcher) = self.watcher.take() {
            watcher.abort();
        }
    }

    fn api_socket(&self) -> PathBuf {
        self.dir.join("api.sock")
    }

    fn vsock(&self) -> PathBuf {
        self.dir.join("v.sock")
    }

    fn ready_socket(&self) -> PathBuf {
        self.dir.join(format!("v.sock_{READY_PORT}"))
    }

    fn clear_sockets(&self) -> Result<(), String> {
        remove(&self.api_socket())?;
        remove(&self.vsock())?;
        remove(&self.ready_socket())
    }

    async fn call(&self, method: &str, path: &str, body: &Value) -> Result<(), String> {
        api(&self.api_socket(), method, path, body).await
    }

    async fn spawn(&mut self) -> Result<(), String> {
        create_private_dir(&self.dir)?;
        self.clear_sockets()?;
        let child = Command::new("systemd-run")
            .args(["--user", "--scope", "--quiet", "--collect", "--unit"])
            .arg(scope_of(self.index))
            .arg("--")
            .arg(&self.binary)
            .arg("--api-sock")
            .arg(self.api_socket())
            .args(["--level", "Warning"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                format!(
                    "starting {} in its own systemd user scope: {error}; install systemd-run and run the supervisor from its systemd user unit",
                    self.binary.display()
                )
            })?;
        self.follow_spawn(child).await
    }

    async fn follow_spawn(&mut self, child: Child) -> Result<(), String> {
        let socket = self.api_socket();
        self.process = Some(Process::Starting(child));
        let Some(Process::Starting(child)) = self.process.as_mut() else {
            return Err("Firecracker did not start".to_string());
        };
        if let Some(stdout) = child.stdout.take() {
            let stdout = stdout
                .into_owned_fd()
                .map_err(|error| format!("taking the box's standard output: {error}"))?;
            self.console_tasks
                .push(tail_of(&stdout, &self.console, "the box's standard output")?);
            fd_store::keep(self.index, fd_store::STDOUT, stdout.as_fd())?;
            self.stdout = Some(stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            let stderr = stderr
                .into_owned_fd()
                .map_err(|error| format!("taking the box's standard error: {error}"))?;
            self.console_tasks
                .push(tail_of(&stderr, &self.console, "the box's standard error")?);
            fd_store::keep(self.index, fd_store::STDERR, stderr.as_fd())?;
            self.stderr = Some(stderr);
        }
        while UnixStream::connect(&socket).await.is_err() {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(format!(
                    "Firecracker exited with {status} before opening its API socket; check that firecrackerPath in runtime.json names it"
                ));
            }
            tokio::time::sleep(API_WAIT_STEP).await;
        }
        let pid = child
            .id()
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .ok_or_else(|| "Firecracker ended right after opening its API socket".to_string())?;
        self.unwatch();
        self.watcher = Some(watch(self.index, pid, self.session_name(), &self.console, false)?);
        let Some(Process::Starting(child)) = self.process.take() else {
            return Err("the Firecracker launcher is missing".to_string());
        };
        self.process = Some(Process::Spawned(child));
        Ok(())
    }

    async fn connect(&self) -> Result<(VsockGuest, GuestHello), String> {
        let mut stream = UnixStream::connect(self.vsock())
            .await
            .map_err(|error| format!("connecting to the box's vsock at {}: {error}", self.vsock().display()))?;
        stream
            .write_all(format!("CONNECT {GUEST_PORT}\n").as_bytes())
            .await
            .map_err(|error| format!("asking for guest port {GUEST_PORT}: {error}"))?;
        let mut answer = Vec::new();
        loop {
            let byte = stream
                .read_u8()
                .await
                .map_err(|error| format!("guest port {GUEST_PORT} refused the connection: {error}"))?;
            if byte == b'\n' {
                break;
            }
            answer.push(byte);
        }
        if !answer.starts_with(b"OK ") {
            return Err(format!(
                "guest port {GUEST_PORT} answered {:?}",
                String::from_utf8_lossy(&answer)
            ));
        }
        VsockGuest {
            stream,
            decoder: Decoder::default(),
        }
        .hello()
        .await
    }

    fn layout(&self, boot: &Boot) -> Result<Layout, String> {
        match boot {
            Boot::Full(image) => {
                let fix = format!("build it with npm run guest:image -- {}", *image.harness.harness);
                Ok(Layout {
                    rootfs: present(self.images.join(format!("{}.ext4", *image.image_digest)), &fix)?,
                    kernel: present(self.images.join(format!("{}.vmlinux", *image.kernel_digest)), &fix)?,
                    drives: Vec::new(),
                    args: String::new(),
                    restore_arg: "",
                })
            }
            Boot::Layered(layered) => {
                let base = &layered.base;
                let fix = format!(
                    "delete {}.json from the image cache and the supervisor fetches the base again",
                    *base.image_digest
                );
                let harness = &layered.layer.manifest.harness;
                Ok(Layout {
                    rootfs: present(self.images.join(format!("{}.ext4", *base.image_digest)), &fix)?,
                    kernel: present(self.images.join(format!("{}.vmlinux", *base.kernel_digest)), &fix)?,
                    drives: vec![
                        (
                            "guest",
                            present(
                                guest_drive::path(&self.images)?,
                                "restart the supervisor, which writes its guest drive at start",
                            )?,
                        ),
                        (
                            "layer",
                            present(
                                layered.layer.path.clone(),
                                "the layer was deleted outside the supervisor; send the turn again to fetch it",
                            )?,
                        ),
                    ],
                    args: format!(
                        " tumpel.harness={}/{}",
                        boot_word(&harness.harness, "harness")?,
                        boot_word(&harness.harness_version, "harness version")?
                    ),
                    restore_arg: " tumpel.restore=vde",
                })
            }
        }
    }

    async fn attach(&self, id: &str, path: &Path, read_only: bool) -> Result<(), String> {
        self.call(
            "PUT",
            &format!("/drives/{id}"),
            &json!({ "drive_id": id, "path_on_host": path, "is_root_device": id == "rootfs", "is_read_only": read_only }),
        )
        .await
    }
}

impl Machine for FirecrackerBox {
    type Guest = VsockGuest;

    fn register_inherited(&self, guest: &VsockGuest) -> Result<(), String> {
        let pid = self
            .process
            .as_ref()
            .and_then(Process::id)
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .ok_or("the box has no running Firecracker process")?;
        let scope = scope_of(self.index);
        if process_scope(pid, &scope)? {
            return Ok(());
        }
        fd_store::store(self.index, fd_store::GUEST, guest.stream.as_fd())?;
        for (role, fd) in [(fd_store::STDOUT, &self.stdout), (fd_store::STDERR, &self.stderr)] {
            let fd = fd.as_ref().ok_or_else(|| format!("the box has no {role} descriptor"))?;
            fd_store::store(self.index, role, fd.as_fd())?;
        }
        move_into_scope(pid, &scope)?;
        Ok(())
    }

    fn activate(&mut self) {
        for reader in self.console_readers.drain(..) {
            self.console_tasks.push(keep_tail(reader, Arc::clone(&self.console)));
        }
    }

    async fn start(&mut self, boot: &Boot, restore: Option<&Path>) -> Result<(VsockGuest, GuestHello), String> {
        let layout = self.layout(boot)?;
        let boot_args = format!(
            "{}{}{}",
            self.boot_args,
            layout.args,
            if restore.is_some() { layout.restore_arg } else { "" }
        );
        create_private_dir(&self.session)?;
        let overlay = self.drive();
        remove(&overlay)?;
        create_private_file(&overlay)
            .and_then(|file| file.set_len(self.overlay_bytes))
            .map_err(|error| format!("creating the overlay drive {}: {error}", overlay.display()))?;
        self.clear_console();
        self.spawn().await?;
        let ready = UnixListener::bind(self.ready_socket())
            .map_err(|error| format!("listening on {}: {error}", self.ready_socket().display()))?;
        self.call(
            "PUT",
            "/machine-config",
            &json!({ "vcpu_count": self.vcpus, "mem_size_mib": self.memory_mib }),
        )
        .await?;
        self.call(
            "PUT",
            "/boot-source",
            &json!({ "kernel_image_path": layout.kernel, "boot_args": boot_args }),
        )
        .await?;
        self.attach("rootfs", &layout.rootfs, true).await?;
        self.attach("overlay", &overlay, false).await?;
        for (id, path) in &layout.drives {
            self.attach(id, path, true).await?;
        }
        if let Some(restore) = restore {
            self.attach("restore", restore, true).await?;
        }
        self.call(
            "PUT",
            "/network-interfaces/eth0",
            &json!({ "iface_id": "eth0", "host_dev_name": self.device, "guest_mac": self.mac }),
        )
        .await?;
        self.call(
            "PUT",
            "/vsock",
            &json!({ "guest_cid": GUEST_CID, "uds_path": self.vsock() }),
        )
        .await?;
        self.call("PUT", "/actions", &json!({ "action_type": "InstanceStart" }))
            .await?;
        let Some(Process::Spawned(process)) = self.process.as_mut() else {
            return Err("the box is not running".to_string());
        };
        let announcement = tokio::select! {
            accepted = ready.accept() => accepted.map_err(|error| format!("accepting the guest's ready call: {error}"))?.0,
            status = process.wait() => {
                return Err(format!("the box shut down while booting ({})", status.map_or_else(|error| error.to_string(), |status| status.to_string())));
            }
        };
        VsockGuest {
            stream: announcement,
            decoder: Decoder::default(),
        }
        .hello()
        .await?;
        drop(ready);
        remove(&self.ready_socket())?;
        let (guest, hello) = self.connect().await?;
        fd_store::keep(self.index, fd_store::GUEST, guest.stream.as_fd())?;
        Ok((guest, hello))
    }

    async fn pause(&mut self) -> Result<(), String> {
        self.call("PATCH", "/vm", &json!({ "state": "Paused" })).await
    }

    async fn resume(&mut self) -> Result<(), String> {
        self.call("PATCH", "/vm", &json!({ "state": "Resumed" })).await
    }

    fn drive(&self) -> PathBuf {
        self.session.join(OVERLAY_DRIVE)
    }

    async fn stop(mut self) -> Stopped {
        self.unwatch();
        let exit = match self.process.take() {
            Some(process) => process.kill().await,
            None => Ok("was not running".to_string()),
        };
        let exit = match exit {
            Ok(exit) => format!("process in {} {exit}", scope_of(self.index)),
            Err(error) => {
                return Stopped {
                    exit: error.clone(),
                    cleared: Err(error),
                }
            }
        };
        let cleared = fd_store::forget_box(self.index)
            .and_then(|()| remove_dir(&self.dir))
            .map_err(|error| format!("BoxNotCleared: tearing down box {}: {error}", self.index));
        Stopped { exit, cleared }
    }

    fn console_tail(&self) -> String {
        console_text(&tail_bytes(&self.console))
    }

    fn clear_console(&mut self) {
        if let Ok(mut tail) = self.console.lock() {
            tail.clear();
        }
    }

    fn record(&self) -> Result<SupervisorHandoverMachine, String> {
        let process_id = self
            .process
            .as_ref()
            .and_then(Process::id)
            .and_then(std::num::NonZeroU32::new)
            .ok_or_else(|| "the box has no running Firecracker process".to_string())?;
        Ok(SupervisorHandoverMachine {
            api_socket: bounded(
                "the Firecracker API socket path",
                self.api_socket().display().to_string(),
            )?,
            console_tail: bounded("the empty console tail", String::new())?,
            guest_buffer: bounded("the empty guest buffer", String::new())?,
            guest_fd: -1,
            process_id,
            stderr_fd: -1,
            stdout_fd: -1,
            vsock: bounded("the vsock path", self.vsock().display().to_string())?,
        })
    }

    fn observation(&self, guest: &VsockGuest) -> Result<Option<Observation>, String> {
        let supervisor_pid = i32::try_from(std::process::id()).map_err(|error| error.to_string())?;
        let machine_pid = self
            .process
            .as_ref()
            .and_then(Process::id)
            .and_then(|pid| i32::try_from(pid).ok())
            .ok_or("the box has no running Firecracker process")?;
        let fd = |pipe: &Option<OwnedFd>| pipe.as_ref().map_or(-1, AsRawFd::as_raw_fd);
        Ok(Some(Observation {
            supervisor_pid,
            supervisor_start: process_start(supervisor_pid)?,
            machine_start: process_start(machine_pid)?,
            guest: Descriptor::read(supervisor_pid, guest.stream.as_raw_fd(), "the guest connection", true)?,
            stdout: Descriptor::read(supervisor_pid, fd(&self.stdout), "the box's standard output", false)?,
            stderr: Descriptor::read(supervisor_pid, fd(&self.stderr), "the box's standard error", false)?,
        }))
    }

    async fn reconnect(&mut self, guest: &mut VsockGuest) -> Result<(), String> {
        if unsafe { libc::shutdown(guest.stream.as_raw_fd(), libc::SHUT_RDWR) } < 0 {
            return Err(format!(
                "closing the guest connection the stopped supervisor left: {}",
                std::io::Error::last_os_error()
            ));
        }
        fd_store::forget(self.index, fd_store::GUEST)?;
        let (fresh, _) = self.connect().await?;
        fd_store::keep(self.index, fd_store::GUEST, fresh.stream.as_fd())?;
        *guest = fresh;
        Ok(())
    }

    fn hand_over(mut self, guest: VsockGuest) -> Result<SupervisorHandoverMachine, String> {
        let machine = SupervisorHandoverMachine {
            console_tail: bounded("the console tail", STANDARD.encode(tail_bytes(&self.console)))?,
            guest_buffer: bounded(
                "the guest bytes buffered past the last frame",
                STANDARD.encode(guest.decoder.buffered()),
            )?,
            ..self.record()?
        };
        self.unwatch();
        self.process.take();
        Ok(machine)
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "host-tests"))]
mod host_tests;
