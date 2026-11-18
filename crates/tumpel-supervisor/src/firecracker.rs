use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::pipe;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tumpel_api::types::{EntityId, GuestHello, GuestInput, GuestMessage, SupervisorHandoverMachine, SupervisorRuntime};
use tumpel_guest::setup_output::{redacted, strip_ansi};
use tumpel_guest::{encode, Decoder, GUEST_CID, GUEST_PORT, READY_PORT};

use crate::boot::Boot;
use crate::fd_store;
use crate::guest_drive;
use crate::lifecycle::{Guest, Machine, Vmm};
use crate::pool::Tap;
use crate::snapshot::Ext4Upper;

const BOOT_ARGS: &str = "console=ttyS0 reboot=k panic=1 pci=off init=/usr/local/sbin/tumpel-init";
const CONSOLE_TAIL: usize = 4096;
const API_WAIT_STEP: Duration = Duration::from_millis(5);
const API_ANSWER: Duration = Duration::from_secs(5);

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

fn console_text(tail: &[u8]) -> String {
    let text = strip_ansi(&String::from_utf8_lossy(tail));
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .collect();
    redacted(&lines.join("\n"))
}

fn keep_tail(stream: impl AsyncRead + Unpin + Send + 'static, tail: Arc<Mutex<VecDeque<u8>>>) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(stream).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(mut tail) = tail.lock() else { return };
            tail.extend(line.as_bytes());
            tail.push_back(b'\n');
            let excess = tail.len().saturating_sub(CONSOLE_TAIL);
            tail.drain(..excess);
        }
    });
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

fn booted_memory(socket: &Path) -> Result<NonZeroU32, String> {
    use std::io::{BufRead, Read, Write};
    let failed = |error: std::io::Error| {
        format!(
            "Firecracker did not answer GET /machine-config on {} within {} s: {error}",
            socket.display(),
            API_ANSWER.as_secs()
        )
    };
    let mut stream = std::os::unix::net::UnixStream::connect(socket).map_err(failed)?;
    stream.set_read_timeout(Some(API_ANSWER)).map_err(failed)?;
    stream
        .write_all(b"GET /machine-config HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\r\n")
        .map_err(failed)?;
    let mut reader = std::io::BufReader::new(stream);
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
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut answer = vec![0u8; length];
    reader.read_exact(&mut answer).map_err(failed)?;
    answered("GET", "/machine-config", &status, &answer)?;
    serde_json::from_slice::<MachineConfig>(&answer)
        .map(|config| config.mem_size_mib)
        .map_err(|error| format!("Firecracker answered GET /machine-config without a positive mem_size_mib: {error}"))
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
    if !session_id
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
    {
        return Err(format!(
            "session id {:?} is not a safe directory name; the control plane mints ids of letters, digits, - and _",
            **session_id
        ));
    }
    Ok(data.join("sessions").join(&**session_id))
}

fn scope_of(index: u8) -> String {
    format!("tumpel-box{index}.scope")
}

fn stop_scopes(scopes: &[String]) -> Result<(), String> {
    let stopped = std::process::Command::new("systemctl")
        .args(["--user", "stop"])
        .args(scopes)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| format!("starting systemctl to stop {}: {error}", scopes.join(" ")))?;
    if stopped.status.success() {
        return Ok(());
    }
    Err(format!(
        "stopping {} failed with {}: {}; stop them with systemctl --user stop {}",
        scopes.join(" "),
        stopped.status,
        String::from_utf8_lossy(&stopped.stderr).trim(),
        scopes.join(" ")
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

impl Drop for Adopted {
    fn drop(&mut self) {
        match &self.owner {
            Owner::Child => end_child(self.pid),
            Owner::Scope(scope) => {
                if let Err(error) = stop_scopes(std::slice::from_ref(scope)) {
                    eprintln!("tumpel-supervisor: ending a box: {error}");
                }
            }
        }
    }
}

enum Process {
    Spawned(Child),
    Adopted(Adopted),
}

impl Process {
    fn id(&self) -> Option<u32> {
        match self {
            Self::Spawned(child) => child.id(),
            Self::Adopted(adopted) => u32::try_from(adopted.pid).ok(),
        }
    }

    fn keep(self) {
        std::mem::forget(self);
    }

    async fn kill(self) {
        match self {
            Self::Spawned(mut child) => {
                let _ = child.kill().await;
            }
            Self::Adopted(adopted) => {
                let _ = tokio::task::spawn_blocking(move || drop(adopted)).await;
            }
        }
    }
}

fn end_child(pid: libc::pid_t) {
    let mut status = 0;
    if unsafe { libc::waitpid(pid, &raw mut status, libc::WNOHANG) } != 0 {
        return;
    }
    unsafe { libc::kill(pid, libc::SIGKILL) };
    unsafe { libc::waitpid(pid, &raw mut status, 0) };
}

fn ended(status: i32) -> String {
    if libc::WIFEXITED(status) {
        return format!("exit status {}", libc::WEXITSTATUS(status));
    }
    if libc::WIFSIGNALED(status) {
        return format!("signal {}", libc::WTERMSIG(status));
    }
    format!("wait status {status}")
}

fn in_scope(pid: libc::pid_t, scope: &str) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/cgroup"))
        .is_ok_and(|cgroups| cgroups.lines().any(|line| line.ends_with(&format!("/{scope}"))))
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
        .env_remove("NOTIFY_SOCKET")
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

fn alive(pid: libc::pid_t, scope: &str) -> Result<Owner, String> {
    let mut status = 0;
    let waited = unsafe { libc::waitpid(pid, &raw mut status, libc::WNOHANG) };
    if waited == 0 {
        return Ok(Owner::Child);
    }
    if waited == pid {
        return Err(format!(
            "its Firecracker process {pid} ended during the handover with {}",
            ended(status)
        ));
    }
    if in_scope(pid, scope) {
        return Ok(Owner::Scope(scope.to_string()));
    }
    Err(format!(
        "its Firecracker process {pid} is gone: it is no child of this supervisor and runs in no {scope}"
    ))
}

fn take(fd: i32) -> Option<OwnedFd> {
    if fd < 3 || unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return None;
    }
    Some(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn adopt(fd: Option<OwnedFd>, named: &str, socket: bool) -> Result<OwnedFd, String> {
    let fd = fd.ok_or_else(|| format!("{named} was handed over as a descriptor this process does not hold open"))?;
    let file = std::fs::File::from(fd);
    let kind = file
        .metadata()
        .map_err(|error| format!("reading {named}: {error}"))?
        .file_type();
    if (socket && kind.is_socket()) || (!socket && kind.is_fifo()) {
        return Ok(OwnedFd::from(file));
    }
    Err(format!(
        "the descriptor handed over as {named} is no {}",
        if socket { "socket" } else { "pipe" }
    ))
}

struct Taken {
    guest: Option<OwnedFd>,
    stdout: Option<OwnedFd>,
    stderr: Option<OwnedFd>,
}

impl Taken {
    fn from(handed: &SupervisorHandoverMachine) -> Self {
        Self {
            guest: take(handed.guest_fd),
            stdout: take(handed.stdout_fd),
            stderr: take(handed.stderr_fd),
        }
    }
}

fn handed_pid(handed: &SupervisorHandoverMachine) -> Option<libc::pid_t> {
    libc::pid_t::try_from(handed.process_id.get()).ok()
}

fn tail_of(fd: &OwnedFd, console: &Arc<Mutex<VecDeque<u8>>>, named: &str) -> Result<(), String> {
    let reader = fd
        .try_clone()
        .map_err(|error| format!("duplicating {named}: {error}"))
        .and_then(|copy| pipe::Receiver::from_owned_fd(copy).map_err(|error| format!("reading {named}: {error}")))?;
    keep_tail(reader, Arc::clone(console));
    Ok(())
}

pub struct Firecracker {
    binary: PathBuf,
    images: PathBuf,
    data: PathBuf,
    vcpus: u32,
    overlay_bytes: u64,
}

impl Firecracker {
    #[must_use]
    pub fn new(runtime: &SupervisorRuntime) -> Self {
        Self {
            binary: PathBuf::from(&*runtime.firecracker_path),
            images: PathBuf::from(&*runtime.image_cache_dir),
            data: PathBuf::from(&*runtime.data_dir),
            vcpus: runtime.vcpu_count.get(),
            overlay_bytes: u64::try_from(runtime.overlay_mib).unwrap_or(0) * 1024 * 1024,
        }
    }
}

impl Firecracker {
    fn attach_box(
        &self,
        session_id: &EntityId,
        tap: &Tap,
        handed: &SupervisorHandoverMachine,
        taken: &mut Taken,
    ) -> Result<(FirecrackerBox, VsockGuest), String> {
        let mut machine = self.machine(session_id, tap)?;
        if Path::new(&handed.api_socket) != machine.api_socket() || Path::new(&handed.vsock) != machine.vsock() {
            return Err(format!(
                "it was handed over with API socket {} and vsock {}, and this supervisor runs box {} at {} and {}; set dataDir in runtime.json back to what the previous binary used",
                handed.api_socket,
                handed.vsock,
                tap.index(),
                machine.api_socket().display(),
                machine.vsock().display()
            ));
        }
        let pid = handed_pid(handed).ok_or_else(|| format!("process id {} is out of range", handed.process_id))?;
        let owner = alive(pid, &scope_of(tap.index()))?;
        let guest = adopt(taken.guest.take(), "the guest connection", true)?;
        let stdout = adopt(taken.stdout.take(), "the box's standard output", false)?;
        let stderr = adopt(taken.stderr.take(), "the box's standard error", false)?;
        let stream = std::os::unix::net::UnixStream::from(guest);
        stream
            .set_nonblocking(true)
            .map_err(|error| format!("making the guest connection non-blocking: {error}"))?;
        let stream = UnixStream::from_std(stream).map_err(|error| format!("adopting the guest connection: {error}"))?;
        let mut decoder = Decoder::default();
        decoder.push(
            &STANDARD
                .decode(&handed.guest_buffer)
                .map_err(|error| format!("guestBuffer is not base64: {error}"))?,
        );
        tail_of(&stdout, &machine.console, "the box's standard output")?;
        tail_of(&stderr, &machine.console, "the box's standard error")?;
        if let Ok(mut tail) = machine.console.lock() {
            tail.extend(STANDARD.decode(&handed.console_tail).unwrap_or_default());
        }
        machine.stdout = Some(stdout);
        machine.stderr = Some(stderr);
        machine.process = Some(Process::Adopted(Adopted { pid, owner }));
        Ok((machine, VsockGuest { stream, decoder }))
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
            overlay_bytes: self.overlay_bytes,
            process: None,
            stdout: None,
            stderr: None,
            console: Arc::new(Mutex::new(VecDeque::with_capacity(CONSOLE_TAIL))),
        })
    }

    fn reattach(
        &self,
        session_id: &EntityId,
        tap: &Tap,
        handed: &SupervisorHandoverMachine,
    ) -> Result<(FirecrackerBox, VsockGuest), String> {
        let mut taken = Taken::from(handed);
        let attached = self
            .attach_box(session_id, tap, handed, &mut taken)
            .and_then(|(machine, guest)| machine.home(&guest).map(|()| (machine, guest)));
        if attached.is_err() {
            if let Some(pid) = handed_pid(handed) {
                end_child(pid);
            }
        }
        drop(taken);
        attached
    }

    fn booted_memory(&self, handed: &SupervisorHandoverMachine) -> Result<NonZeroU32, String> {
        booted_memory(Path::new(&handed.api_socket))
    }

    fn discard(&self, handed: &SupervisorHandoverMachine) {
        let taken = Taken::from(handed);
        if let Some(pid) = handed_pid(handed) {
            end_child(pid);
        }
        drop(taken);
    }

    fn end_unclaimed(&self, claimed: &[u8]) -> Result<(), String> {
        let listed = std::process::Command::new("systemctl")
            .args([
                "--user",
                "list-units",
                "--all",
                "--plain",
                "--no-legend",
                "tumpel-box*.scope",
            ])
            .stdin(Stdio::null())
            .output()
            .map_err(|error| format!("starting systemctl to list the box scopes: {error}"))?;
        if !listed.status.success() {
            return Err(format!(
                "listing the box scopes failed with {}: {}; run the supervisor from its systemd user unit",
                listed.status,
                String::from_utf8_lossy(&listed.stderr).trim()
            ));
        }
        let kept: Vec<String> = claimed.iter().copied().map(scope_of).collect();
        let unclaimed: Vec<String> = String::from_utf8_lossy(&listed.stdout)
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|scope| !kept.iter().any(|kept| kept == scope))
            .map(str::to_string)
            .collect();
        if unclaimed.is_empty() {
            return Ok(());
        }
        eprintln!(
            "tumpel-supervisor: stopping {}, boxes this supervisor could not take over",
            unclaimed.join(" ")
        );
        stop_scopes(&unclaimed)
    }

    fn open_upper(&self, drive: &Path) -> Result<Ext4Upper, String> {
        Ext4Upper::load(drive)
    }

    fn staging(&self, session_id: &EntityId) -> Result<PathBuf, String> {
        session_dir(&self.data, session_id)
    }

    fn chunk_cache(&self) -> PathBuf {
        self.images.join("chunks")
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
    stdout: Option<OwnedFd>,
    stderr: Option<OwnedFd>,
    console: Arc<Mutex<VecDeque<u8>>>,
}

impl FirecrackerBox {
    fn home(&self, guest: &VsockGuest) -> Result<(), String> {
        let pid = self
            .process
            .as_ref()
            .and_then(Process::id)
            .and_then(|pid| libc::pid_t::try_from(pid).ok())
            .ok_or_else(|| "the box has no running Firecracker process".to_string())?;
        let scope = scope_of(self.index);
        if in_scope(pid, &scope) {
            return Ok(());
        }
        move_into_scope(pid, &scope)?;
        fd_store::keep(self.index, fd_store::GUEST, guest.stream.as_fd())?;
        for (role, fd) in [(fd_store::STDOUT, &self.stdout), (fd_store::STDERR, &self.stderr)] {
            if let Some(fd) = fd {
                fd_store::keep(self.index, role, fd.as_fd())?;
            }
        }
        Ok(())
    }

    fn console_bytes(&self) -> Vec<u8> {
        self.console
            .lock()
            .map_or_default(|tail| tail.iter().copied().collect::<Vec<u8>>())
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
        let mut child = Command::new("systemd-run")
            .args(["--user", "--scope", "--quiet", "--collect", "--unit"])
            .arg(scope_of(self.index))
            .env_remove("NOTIFY_SOCKET")
            .arg("--")
            .arg(&self.binary)
            .arg("--api-sock")
            .arg(self.api_socket())
            .args(["--level", "Warning"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                format!(
                    "starting {} in its own systemd user scope: {error}; install systemd-run and run the supervisor from its systemd user unit",
                    self.binary.display()
                )
            })?;
        if let Some(stdout) = child.stdout.take() {
            let stdout = stdout
                .into_owned_fd()
                .map_err(|error| format!("taking the box's standard output: {error}"))?;
            tail_of(&stdout, &self.console, "the box's standard output")?;
            fd_store::keep(self.index, fd_store::STDOUT, stdout.as_fd())?;
            self.stdout = Some(stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            let stderr = stderr
                .into_owned_fd()
                .map_err(|error| format!("taking the box's standard error: {error}"))?;
            tail_of(&stderr, &self.console, "the box's standard error")?;
            fd_store::keep(self.index, fd_store::STDERR, stderr.as_fd())?;
            self.stderr = Some(stderr);
        }
        let socket = self.api_socket();
        while UnixStream::connect(&socket).await.is_err() {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(format!(
                    "Firecracker exited with {status} before opening its API socket; check that firecrackerPath in runtime.json names it"
                ));
            }
            tokio::time::sleep(API_WAIT_STEP).await;
        }
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
        if let Ok(mut tail) = self.console.lock() {
            tail.clear();
        }
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
        self.session.join("overlay.ext4")
    }

    async fn stop(mut self) {
        if let Some(process) = self.process.take() {
            process.kill().await;
        }
        let cleared = fd_store::forget_box(self.index)
            .and_then(|()| self.clear_sockets())
            .and_then(|()| match std::fs::remove_dir_all(&self.session) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    Err(format!("deleting {}: {error}", self.session.display()))
                }
                _ => Ok(()),
            });
        if let Err(error) = cleared {
            eprintln!("tumpel-supervisor: tearing down a box: {error}; delete it by hand");
        }
    }

    fn console_tail(&self) -> String {
        console_text(&self.console_bytes())
    }

    fn record(&self) -> Result<SupervisorHandoverMachine, String> {
        let process_id = self
            .process
            .as_ref()
            .and_then(Process::id)
            .and_then(std::num::NonZeroU32::new)
            .ok_or_else(|| "the box has no running Firecracker process".to_string())?;
        Ok(SupervisorHandoverMachine {
            api_socket: self.api_socket().display().to_string(),
            console_tail: String::new(),
            guest_buffer: String::new(),
            guest_fd: -1,
            process_id,
            stderr_fd: -1,
            stdout_fd: -1,
            vsock: self.vsock().display().to_string(),
        })
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
            console_tail: STANDARD.encode(self.console_bytes()),
            guest_buffer: STANDARD.encode(guest.decoder.buffered()),
            ..self.record()?
        };
        if let Some(process) = self.process.take() {
            process.keep();
        }
        Ok(machine)
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "host-tests"))]
mod host_tests;
