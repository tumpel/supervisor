use std::io::{ErrorKind, Read, Write};
use std::num::NonZeroU32;
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::json;
use tumpel_api::types::{EntityId, GuestMessage, SupervisorHandoverMachine};
use tumpel_guest::encode;

use super::{adopt, alive, booted_memory, take, Firecracker, Owner, Taken};
use crate::lifecycle::{Guest, Machine, Vmm};
use crate::pool::{Disk, Size, TapPool};

const BOX: Size = Size {
    memory_mib: NonZeroU32::new(4096).expect("a positive size"),
    disk_mib: NonZeroU32::new(32_768).expect("a positive size"),
};

const ROOMY: Disk = Disk {
    total_mib: 1 << 30,
    free_mib: 1 << 29,
    written_mib: 0,
};

fn sleeper() -> libc::pid_t {
    let id = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .map(|child| child.id())
        .expect("sleep starts");
    libc::pid_t::try_from(id).expect("a pid")
}

fn zombie(pid: libc::pid_t) {
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let id = libc::id_t::try_from(pid).expect("a pid");
    let waited = unsafe { libc::waitid(libc::P_PID, id, &raw mut info, libc::WEXITED | libc::WNOWAIT) };
    assert_eq!(waited, 0, "the child ends and stays a zombie");
}

const SCOPE: &str = "tumpel-box0.scope";

fn reaped(pid: libc::pid_t) -> bool {
    alive(pid, SCOPE).is_err_and(|error| error.contains("is no child"))
}

fn own_cgroup() -> String {
    let cgroups = std::fs::read_to_string("/proc/self/cgroup").expect("this process's cgroup");
    cgroups
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .and_then(|path| path.rsplit('/').next())
        .expect("a unified cgroup")
        .to_string()
}

#[test]
fn alive_tells_a_running_child_from_a_zombie_and_a_stranger() {
    let pid = sleeper();
    assert_eq!(alive(pid, SCOPE), Ok(Owner::Child));
    zombie(pid);
    let ended = alive(pid, SCOPE).expect_err("a zombie is no live box");
    assert!(ended.contains("ended during the handover with signal 9"), "{ended}");
    assert!(reaped(pid), "alive reaps the zombie it reports");
    let stranger = alive(1, SCOPE).expect_err("init runs in no box scope");
    assert!(stranger.contains("runs in no tumpel-box0.scope"), "{stranger}");
}

#[test]
fn a_process_that_is_no_child_is_a_box_only_inside_the_scope_of_its_box() {
    let output = std::process::Command::new("sh")
        .args(["-c", "sleep 60 >/dev/null 2>&1 & echo $!"])
        .output()
        .expect("sh starts");
    let pid: libc::pid_t = String::from_utf8(output.stdout)
        .expect("a pid")
        .trim()
        .parse()
        .expect("a pid");
    let scope = own_cgroup();
    let adopted = alive(pid, &scope);
    let other = alive(pid, "tumpel-box9.scope");
    unsafe { libc::kill(pid, libc::SIGKILL) };
    assert_eq!(adopted, Ok(Owner::Scope(scope)));
    assert!(other.is_err_and(|error| error.contains("runs in no tumpel-box9.scope")));
}

#[test]
fn adopt_takes_only_the_descriptor_kind_it_was_handed() {
    let (socket, _peer) = UnixStream::pair().expect("a socket pair");
    let (reader, _writer) = std::io::pipe().expect("a pipe");
    let socket = socket.into_raw_fd();
    let reader = OwnedFd::from(reader).into_raw_fd();
    assert!(adopt(take(socket), "the guest connection", true).is_ok());
    let wrong = adopt(take(reader), "the guest connection", true).expect_err("a pipe is no socket");
    assert!(wrong.contains("is no socket"), "{wrong}");
    let (socket, _peer) = UnixStream::pair().expect("a socket pair");
    let wrong = adopt(take(socket.into_raw_fd()), "standard output", false).expect_err("a socket is no pipe");
    assert!(wrong.contains("is no pipe"), "{wrong}");
    assert!(take(2).is_none(), "standard error is never adopted");
    assert!(
        take(1_000_000).is_none(),
        "a descriptor this process does not hold is never adopted"
    );
}

struct Handed {
    machine: SupervisorHandoverMachine,
    peer: UnixStream,
    stdout: std::io::PipeWriter,
    stderr: std::io::PipeWriter,
    rest: Vec<u8>,
}

fn firecracker(name: &str) -> Firecracker {
    let data = std::env::temp_dir().join(format!("tumpel-firecracker-{}-{name}", std::process::id()));
    if data.exists() {
        std::fs::remove_dir_all(&data).expect("clearing the last run");
    }
    Firecracker {
        binary: PathBuf::from("firecracker"),
        images: data.join("images"),
        data,
        vcpus: 1,
    }
}

fn handed(vmm: &Firecracker, pid: libc::pid_t, guest_is_socket: bool) -> Handed {
    let (guest, peer) = UnixStream::pair().expect("a socket pair");
    let (out, stdout) = std::io::pipe().expect("a pipe");
    let (err, stderr) = std::io::pipe().expect("a pipe");
    let guest_fd = if guest_is_socket {
        guest.into_raw_fd()
    } else {
        let (reader, writer) = std::io::pipe().expect("a pipe");
        std::mem::forget(writer);
        OwnedFd::from(reader).into_raw_fd()
    };
    let message: GuestMessage =
        serde_json::from_value(json!({ "type": "TurnLine", "turnId": "turn-1", "lineSeq": 1, "text": "one" }))
            .expect("a guest message");
    let frame = encode(&message).expect("a frame");
    let (first, rest) = frame.split_at(3);
    let dir = vmm.data.join("box0");
    Handed {
        machine: SupervisorHandoverMachine {
            api_socket: dir
                .join("api.sock")
                .display()
                .to_string()
                .try_into()
                .expect("a socket path"),
            console_tail: STANDARD.encode("booted\n").try_into().expect("a console tail"),
            guest_buffer: STANDARD.encode(first).try_into().expect("a guest buffer"),
            guest_fd,
            process_id: NonZeroU32::new(u32::try_from(pid).expect("a pid")).expect("a pid"),
            stderr_fd: OwnedFd::from(err).into_raw_fd(),
            stdout_fd: OwnedFd::from(out).into_raw_fd(),
            vsock: dir
                .join("v.sock")
                .display()
                .to_string()
                .try_into()
                .expect("a vsock path"),
        },
        peer,
        stdout,
        stderr,
        rest: rest.to_vec(),
    }
}

fn released(pipe: &mut std::io::PipeWriter) -> bool {
    let capacity = unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_GETPIPE_SZ) };
    let overflow = vec![0u8; usize::try_from(capacity).expect("a pipe capacity") + 1];
    pipe.write_all(&overflow)
        .is_err_and(|error| error.kind() == ErrorKind::BrokenPipe)
}

fn closed(handed: &mut Handed) -> bool {
    let pipes = released(&mut handed.stdout) && released(&mut handed.stderr);
    let mut byte = [0u8; 1];
    let socket = handed.peer.read(&mut byte).is_ok_and(|read| read == 0);
    pipes && socket
}

#[tokio::test]
async fn reattach_adopts_a_live_child_and_its_guest_connection_where_the_old_binary_left_off() {
    let vmm = firecracker("reattach-live");
    let tap = TapPool::new(4096, BOX).claim(BOX, ROOMY).expect("tap");
    let pid = sleeper();
    let mut handed = handed(&vmm, pid, true);
    let session = "session-1".try_into().expect("session id");
    let (machine, mut guest) = vmm
        .attach_box(&session, &tap, &handed.machine, &mut Taken::from(&handed.machine))
        .expect("the box is adopted");
    handed
        .peer
        .write_all(&handed.rest)
        .expect("the guest writes the rest of its frame");
    let GuestMessage::TurnLine(line) = guest.receive().await.expect("a message") else {
        panic!("the guest sent its line");
    };
    assert_eq!(line.text.as_str(), "one");
    assert!(machine.console_tail().contains("booted"));
    let _ = machine.stop().await;
    assert!(reaped(pid), "stopping an adopted box kills and reaps its process");
}

#[tokio::test]
async fn reattach_of_a_box_whose_process_ended_leaves_the_incoming_descriptors_owned() {
    let vmm = firecracker("reattach-ended");
    let tap = TapPool::new(4096, BOX).claim(BOX, ROOMY).expect("tap");
    let pid = sleeper();
    zombie(pid);
    let mut handed = handed(&vmm, pid, true);
    let session = "session-1".try_into().expect("session id");
    let Err(error) = vmm.reattach(&session, &tap, &handed.machine) else {
        panic!("a box whose process ended is not adopted");
    };
    assert!(error.contains("ended during the handover"), "{error}");
    assert!(unsafe { libc::fcntl(handed.machine.guest_fd, libc::F_GETFD) } >= 0);
    drop(Taken::from(&handed.machine));
    assert!(closed(&mut handed));
    assert!(reaped(pid));
}

#[tokio::test]
async fn a_failed_attachment_preserves_the_live_child_and_console_descriptors() {
    let vmm = firecracker("reattach-wrong");
    let tap = TapPool::new(4096, BOX).claim(BOX, ROOMY).expect("tap");
    let pid = sleeper();
    let mut handed = handed(&vmm, pid, false);
    let session = "session-1".try_into().expect("session id");
    let Err(error) = vmm.reattach(&session, &tap, &handed.machine) else {
        panic!("a pipe handed as the guest connection is refused");
    };
    assert!(error.contains("is no socket"), "{error}");
    assert_eq!(alive(pid, SCOPE), Ok(Owner::Child));
    handed.stdout.write_all(b"out").expect("write to the preserved console");
    handed.stderr.write_all(b"err").expect("write to the preserved console");
    drop(Taken::from(&handed.machine));
    zombie(pid);
    assert!(alive(pid, SCOPE).is_err());
    assert!(reaped(pid));
}

#[test]
fn booted_memory_reads_mem_size_mib_from_the_firecracker_api_of_a_running_box() {
    let socket = std::env::temp_dir().join(format!("tumpel-machine-config-{}.sock", std::process::id()));
    if socket.exists() {
        std::fs::remove_file(&socket).expect("clearing the last run");
    }
    let listener = std::os::unix::net::UnixListener::bind(&socket).expect("the API socket");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("a connection");
        let mut request = [0u8; 256];
        let read = stream.read(&mut request).expect("the request");
        let body = r#"{"vcpu_count":2,"mem_size_mib":8192,"smt":false}"#;
        write!(stream, "HTTP/1.1 200 \r\nServer: Firecracker API\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len())
            .expect("the answer");
        String::from_utf8_lossy(&request[..read]).into_owned()
    });
    assert_eq!(
        booted_memory(
            &socket,
            i32::try_from(std::process::id()).expect("self pid"),
            super::REPORT_INSPECTION
        ),
        Ok(NonZeroU32::new(8192).expect("8192"))
    );
    assert!(server
        .join()
        .expect("the server")
        .starts_with("GET /machine-config HTTP/1.1\r\n"));
}

#[tokio::test]
async fn observation_checks_live_descriptors_and_uses_only_a_separate_api_connection() {
    use std::os::fd::FromRawFd;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let vmm = firecracker("observe-live");
    let data = vmm.data.clone();
    let box_dir = data.join("box0");
    std::fs::create_dir_all(&box_dir).expect("the box directory");
    let listener = tokio::net::UnixListener::bind(box_dir.join("api.sock")).expect("the fixture Firecracker API");
    let pid = i32::try_from(std::process::id()).expect("this process owns the fixture descriptors and API");
    let mut handed = handed(&vmm, pid, true);
    let machine = &handed.machine;
    let guest = UnixStream::from(unsafe { OwnedFd::from_raw_fd(machine.guest_fd) });
    let mut stdout = std::fs::File::from(unsafe { OwnedFd::from_raw_fd(machine.stdout_fd) });
    let mut stderr = std::fs::File::from(unsafe { OwnedFd::from_raw_fd(machine.stderr_fd) });
    let flags = |fd| unsafe { libc::fcntl(fd, libc::F_GETFL) };
    let guest_flags = flags(guest.as_raw_fd());
    let stdout_flags = flags(stdout.as_raw_fd());
    handed.peer.write_all(&handed.rest).expect("guest bytes waiting");
    handed.stdout.write_all(b"stdout").expect("stdout waiting");
    handed.stderr.write_all(b"stderr").expect("stderr waiting");
    let start = super::process_start(pid).expect("process start");
    let observed = super::Observation {
        supervisor_pid: pid,
        supervisor_start: start,
        machine_start: start,
        guest: super::Descriptor::read(pid, machine.guest_fd, "guest", true).expect("guest descriptor"),
        stdout: super::Descriptor::read(pid, machine.stdout_fd, "stdout", false).expect("stdout descriptor"),
        stderr: super::Descriptor::read(pid, machine.stderr_fd, "stderr", false).expect("stderr descriptor"),
    };
    let scope = own_cgroup();
    let mut stale = observed.clone();
    stale.machine_start += 1;
    assert!(vmm
        .inspect_attachment(0, machine, Some(&stale), &scope)
        .expect_err("stale PID identity")
        .contains("different Firecracker or supervisor process"));
    stale = observed.clone();
    stale.guest.fd = machine.stdout_fd;
    assert!(vmm
        .inspect_attachment(0, machine, Some(&stale), &scope)
        .expect_err("wrong descriptor")
        .contains("is no socket"));
    let machine = machine.clone();
    let observation = tokio::task::spawn_blocking(move || vmm.inspect_attachment(0, &machine, Some(&observed), &scope));
    let serve = async {
        let (mut connection, _) = listener.accept().await.expect("separate API connection");
        let mut request = vec![0; b"GET /machine-config HTTP/1.1\r\n".len()];
        connection.read_exact(&mut request).await.expect("request");
        assert_eq!(request, b"GET /machine-config HTTP/1.1\r\n");
        let body = r#"{"mem_size_mib":8192}"#;
        let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        connection.write_all(response.as_bytes()).await.expect("response");
    };
    let (observed, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(observation, serve)
    })
    .await
    .expect("inspection completes");
    observed.expect("observation worker").expect("valid observation");

    assert_eq!(flags(guest.as_raw_fd()), guest_flags);
    assert_eq!(flags(stdout.as_raw_fd()), stdout_flags);
    drop((handed.stdout, handed.stderr));
    let mut output = String::new();
    stdout.read_to_string(&mut output).expect("stdout read");
    assert_eq!(output, "stdout");
    output.clear();
    stderr.read_to_string(&mut output).expect("stderr read");
    assert_eq!(output, "stderr");
    guest
        .set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("bound the read");
    let mut remaining = vec![0; handed.rest.len()];
    (&guest).read_exact(&mut remaining).expect("guest bytes read");
    assert_eq!(remaining, handed.rest);
    handed.peer.set_nonblocking(true).expect("bound the peer read");
    assert_eq!(
        handed
            .peer
            .read(&mut [0; 1])
            .expect_err("no message to the guest")
            .kind(),
        ErrorKind::WouldBlock
    );
    std::fs::remove_dir_all(data).expect("removing the fixture directory");
}

#[test]
fn a_start_deletes_the_disk_of_a_box_it_did_not_take_over_and_keeps_the_one_it_did() {
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/firecracker-tests/leftovers");
    if data.exists() {
        std::fs::remove_dir_all(&data).expect("clearing the last run");
    }
    for session in ["kept", "lost"] {
        let dir = data.join("sessions").join(session);
        std::fs::create_dir_all(&dir).expect("a box disk directory");
        std::fs::write(dir.join("overlay.ext4"), b"disk").expect("a box disk");
    }
    let kept = EntityId::try_from("kept").expect("an id");

    super::remove_leftovers(&data, &[], &[kept]).expect("the leftovers are deleted");

    assert!(data.join("sessions/kept/overlay.ext4").exists());
    assert!(!data.join("sessions/lost").exists());
}

#[tokio::test(start_paused = true)]
async fn every_guest_family_survives_unknown_variants_and_extra_fields() {
    use tokio::io::AsyncWriteExt;
    use tracing_subscriber::layer::SubscriberExt;
    for known in [
        json!({"type":"GuestHello","protocolVersion":13,"harness":"Shell","harnessVersion":"1","lastLineSeq":0}),
        json!({"type":"TurnRepositoryReady","turnId":"turn-1"}),
        json!({"type":"TurnLine","turnId":"turn-1","lineSeq":1,"text":"known work"}),
        json!({"type":"TurnRepositoryCloneFailed","turnId":"turn-1","cause":"Other","detail":"clone failed"}),
        json!({"type":"TurnEnd","turnId":"turn-1","reason":"Completed","detail":"done","lastLineSeq":1}),
        json!({"type":"ClaudeSetupUrl","claudeSetupId":"setup-1","signInUrl":"https://example.test/login"}),
        json!({"type":"ClaudeSetupStored","claudeSetupId":"setup-1"}),
        json!({"type":"ClaudeSetupFailed","claudeSetupId":"setup-1","reason":"UrlNotPrinted","detail":"no URL"}),
    ] {
        let logs = crate::test_logs::UpdateLogs::default();
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.clone()));
        let (mut producer, stream) = tokio::net::UnixStream::pair().expect("guest socket");
        let mut guest = super::VsockGuest {
            stream,
            decoder: tumpel_guest::Decoder::default(),
        };
        let unknown = json!({"type":format!("Future{}",known["type"].as_str().expect("type")),"extra":true});
        producer
            .write_all(&encode(&unknown).expect("unknown frame"))
            .await
            .expect("send unknown");
        let field = if known.get("reason").is_some() {
            Some("reason")
        } else if known.get("cause").is_some() {
            Some("cause")
        } else {
            None
        };
        if let Some(field) = field {
            let mut unknown_reason = known.clone();
            unknown_reason[field] = json!("FutureFailureReason");
            producer
                .write_all(&encode(&unknown_reason).expect("future reason"))
                .await
                .expect("send reason");
        }
        if field.is_some() {
            let message = guest.read_message().await.expect("known family with a future reason");
            let value = serde_json::to_value(message).expect("message");
            assert_eq!(value[field.expect("reason field")], json!("FutureFailureReason"));
        }
        let mut extended = known.clone();
        extended["extra"] = json!(true);
        producer
            .write_all(&encode(&extended).expect("known frame"))
            .await
            .expect("send known");
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        let message = guest.read_message().await.expect("known work continues");
        assert_eq!(serde_json::to_value(message).expect("message"), known);
        assert_eq!(*logs.0.lock().expect("logs"), vec![tumpel_channel::UPDATE_REQUIRED]);
    }
}

#[test]
fn an_unclaimed_scope_keeps_its_open_box_disk_and_leaves_other_sessions_for_cleanup() {
    let vmm = firecracker("unclaimed-open-disk");
    let root = vmm.data.join("sessions");
    for session in ["live", "ended"] {
        std::fs::create_dir_all(root.join(session)).expect("session directory");
        std::fs::write(root.join(session).join(super::OVERLAY_DRIVE), b"box disk").expect("disk");
    }
    let open = std::fs::File::open(root.join("live").join(super::OVERLAY_DRIVE)).expect("open box disk");
    let processes = [(2, std::path::PathBuf::from("/proc/self"))];
    let held = super::sessions_in_scopes(&root, &[2], &processes).expect("the scope's open disk");
    assert_eq!(
        held.iter().map(tumpel_api::types::EntityId::as_str).collect::<Vec<_>>(),
        ["live"]
    );
    let ambiguous = super::sessions_in_scopes(&root, &[2, 3], &processes).expect("keep possible disks");
    assert_eq!(
        ambiguous
            .iter()
            .map(tumpel_api::types::EntityId::as_str)
            .collect::<Vec<_>>(),
        ["ended", "live"]
    );
    drop(open);
}
