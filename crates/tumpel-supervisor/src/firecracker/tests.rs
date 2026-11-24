use std::io::{ErrorKind, Read, Write};
use std::num::NonZeroU32;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::json;
use tumpel_api::types::{EntityId, GuestMessage, SupervisorHandoverMachine};
use tumpel_guest::encode;

use super::{booted_memory, Adopted, Firecracker, InspectionSource, Owner, Process};
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

struct VmChild(std::process::Child);

impl VmChild {
    fn new() -> Self {
        Self(
            std::process::Command::new("sleep")
                .arg("60")
                .spawn()
                .expect("fixture VM"),
        )
    }

    fn pid(&self) -> libc::pid_t {
        libc::pid_t::try_from(self.0.id()).expect("fixture PID")
    }
}

impl Drop for VmChild {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
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
fn inspecting_an_exited_process_leaves_its_exit_status_for_the_parent() {
    let mut child = VmChild::new();
    let pid = child.pid();
    child.0.kill().expect("end the fixture VM");
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let id = libc::id_t::try_from(pid).expect("a PID");
    let waited = unsafe { libc::waitid(libc::P_PID, id, &raw mut info, libc::WEXITED | libc::WNOWAIT) };
    assert_eq!(waited, 0);
    assert!(super::process_identity(pid).expect("process inspection").is_none());
    child.0.wait().expect("the parent still owns the child's exit status");
}

fn nonblocking(fd: &OwnedFd) {
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
}

struct Handed {
    machine: SupervisorHandoverMachine,
    guest: OwnedFd,
    out: OwnedFd,
    err: OwnedFd,
    peer: UnixStream,
    stdout: std::io::PipeWriter,
    stderr: std::io::PipeWriter,
    rest: Vec<u8>,
}

fn firecracker(name: &str) -> Firecracker {
    let data = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/firecracker-tests")
        .join(name);
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

fn handed(vmm: &Firecracker, pid: libc::pid_t) -> Handed {
    let (guest, peer) = UnixStream::pair().expect("a socket pair");
    let (out, stdout) = std::io::pipe().expect("a pipe");
    let (err, stderr) = std::io::pipe().expect("a pipe");
    let [guest, out, err] = [OwnedFd::from(guest), OwnedFd::from(out), OwnedFd::from(err)];
    for fd in [&guest, &out, &err] {
        nonblocking(fd);
    }
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
            guest_fd: guest.as_raw_fd(),
            process_id: NonZeroU32::new(u32::try_from(pid).expect("a pid")).expect("a pid"),
            stderr_fd: err.as_raw_fd(),
            stdout_fd: out.as_raw_fd(),
            vsock: dir
                .join("v.sock")
                .display()
                .to_string()
                .try_into()
                .expect("a vsock path"),
        },
        guest,
        out,
        err,
        peer,
        stdout,
        stderr,
        rest: rest.to_vec(),
    }
}

#[tokio::test]
async fn aborting_a_boot_stops_the_launcher_before_the_api_opens() {
    let vmm = firecracker("aborted-launch");
    let tap = TapPool::new(4096, BOX).claim(BOX, ROOMY).expect("tap");
    let session = "session-1".try_into().expect("session id");
    let mut machine = vmm.machine(&session, &tap).expect("machine");
    let launcher = tokio::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("launcher");
    let pid = i32::try_from(launcher.id().expect("launcher PID")).expect("valid PID");
    let mut boot = Box::pin(async move { machine.follow_spawn(launcher).await });
    assert!(tokio::time::timeout(std::time::Duration::ZERO, &mut boot)
        .await
        .is_err());
    drop(boot);

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while super::process_start(pid).is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the incomplete launcher is stopped");
}

#[tokio::test]
async fn failed_attachment_leaves_the_vm_and_all_streams_untouched() {
    let vmm = firecracker("reattach-wrong");
    let child = VmChild::new();
    let mut handed = handed(&vmm, child.pid());
    let (wrong, _peer) = UnixStream::pair().expect("an invalid console descriptor");
    let wrong = OwnedFd::from(wrong);
    nonblocking(&wrong);
    handed.machine.stderr_fd = wrong.as_raw_fd();
    handed.peer.write_all(b"guest").expect("guest bytes waiting");
    handed.stdout.write_all(b"stdout").expect("stdout bytes waiting");
    handed.stderr.write_all(b"stderr").expect("stderr bytes waiting");
    let fds = [&handed.guest, &handed.out, &handed.err];
    let flags = fds.map(|fd| unsafe {
        (
            libc::fcntl(fd.as_raw_fd(), libc::F_GETFD),
            libc::fcntl(fd.as_raw_fd(), libc::F_GETFL),
        )
    });
    let session = "session-1".try_into().expect("session id");
    let mut pool = TapPool::new(4096, BOX);
    let Err(error) = vmm.reattach(&session, 0, &handed.machine, None, &mut pool, true) else {
        panic!("a socket is not a console pipe");
    };
    assert!(
        error.contains("standard error") && error.contains("is no pipe"),
        "{error}"
    );
    assert!(super::process_identity(child.pid()).expect("VM state").is_some());
    let after = fds.map(|fd| unsafe {
        (
            libc::fcntl(fd.as_raw_fd(), libc::F_GETFD),
            libc::fcntl(fd.as_raw_fd(), libc::F_GETFL),
        )
    });
    assert_eq!(flags, after);
    let mut guest = UnixStream::from(handed.guest);
    let mut out = std::fs::File::from(handed.out);
    let mut err = std::fs::File::from(handed.err);
    let mut guest_bytes = [0; 5];
    let mut out_bytes = [0; 6];
    let mut err_bytes = [0; 6];
    guest.read_exact(&mut guest_bytes).expect("guest stream untouched");
    out.read_exact(&mut out_bytes).expect("stdout untouched");
    err.read_exact(&mut err_bytes).expect("stderr untouched");
    assert_eq!((&guest_bytes, &out_bytes, &err_bytes), (b"guest", b"stdout", b"stderr"));
}

#[tokio::test]
async fn dropping_an_adopted_handle_leaves_the_vm_for_explicit_stop() {
    let vmm = firecracker("drop-adopted");
    let tap = TapPool::new(4096, BOX).claim(BOX, ROOMY).expect("tap");
    let child = VmChild::new();
    let pid = child.pid();
    let session = "session-1".try_into().expect("session id");
    let mut machine = vmm.machine(&session, &tap).expect("machine handle");
    machine.process = Some(Process::Adopted(Adopted {
        pid,
        owner: Owner::Child,
    }));
    drop(machine);
    assert!(super::process_identity(pid).expect("VM state").is_some());
    let mut machine = vmm.machine(&session, &tap).expect("machine handle");
    machine.process = Some(Process::Adopted(Adopted {
        pid,
        owner: Owner::Child,
    }));
    let stopped = machine.stop().await;
    assert!(
        stopped.exit.contains("was stopped and ended with signal 9"),
        "{}",
        stopped.exit
    );
    assert!(super::process_identity(pid).expect("VM state").is_none());
}

#[tokio::test]
async fn handing_over_an_adopted_vm_releases_only_the_daemon_copies() {
    let vmm = firecracker("retire-adopted");
    let tap = TapPool::new(4096, BOX).claim(BOX, ROOMY).expect("tap");
    let child = VmChild::new();
    let mut handed = handed(&vmm, child.pid());
    let session = "session-1".try_into().expect("session id");
    let mut machine = vmm.machine(&session, &tap).expect("old machine handle");
    machine.process = Some(Process::Adopted(Adopted {
        pid: child.pid(),
        owner: Owner::Child,
    }));
    machine.stdout = Some(handed.out.try_clone().expect("old stdout"));
    machine.stderr = Some(handed.err.try_clone().expect("old stderr"));
    let old_guest = super::VsockGuest {
        stream: tokio::net::UnixStream::from_std(UnixStream::from(handed.guest.try_clone().expect("old guest")))
            .expect("the old guest stream"),
        decoder: tumpel_guest::Decoder::default(),
    };

    let _record = machine
        .hand_over(old_guest)
        .expect("retire the handle and save the machine");
    assert!(super::process_identity(child.pid()).expect("VM state").is_some());
    handed.peer.write_all(b"guest").expect("guest stream still connected");
    handed.stdout.write_all(b"out").expect("stdout still connected");
    handed.stderr.write_all(b"err").expect("stderr still connected");
    let mut guest = UnixStream::from(handed.guest);
    let mut out = std::fs::File::from(handed.out);
    let mut err = std::fs::File::from(handed.err);
    let mut guest_bytes = [0; 5];
    let mut out_bytes = [0; 3];
    let mut err_bytes = [0; 3];
    guest.read_exact(&mut guest_bytes).expect("guest bytes");
    out.read_exact(&mut out_bytes).expect("stdout bytes");
    err.read_exact(&mut err_bytes).expect("stderr bytes");
    assert_eq!((&guest_bytes, &out_bytes, &err_bytes), (b"guest", b"out", b"err"));
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

async fn answer_machine_config(listener: &tokio::net::UnixListener) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut connection, _) = listener.accept().await.expect("separate API connection");
    let mut request = vec![0; b"GET /machine-config HTTP/1.1\r\n".len()];
    connection.read_exact(&mut request).await.expect("request");
    assert_eq!(request, b"GET /machine-config HTTP/1.1\r\n");
    let body = r#"{"mem_size_mib":8192}"#;
    let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
    connection.write_all(response.as_bytes()).await.expect("response");
}

#[tokio::test]
async fn observation_checks_live_descriptors_and_uses_only_a_separate_api_connection() {
    let vmm = firecracker("observe-live");
    let box_dir = vmm.data.join("box0");
    std::fs::create_dir_all(&box_dir).expect("the box directory");
    let listener = tokio::net::UnixListener::bind(box_dir.join("api.sock")).expect("the fixture Firecracker API");
    let _vsock_path = std::os::unix::net::UnixListener::bind(box_dir.join("v.sock")).expect("the box vsock path");
    let pid = i32::try_from(std::process::id()).expect("this process owns the fixture descriptors and API");
    let mut handed = handed(&vmm, pid);
    let machine = &handed.machine;
    let flags = |fd| unsafe { libc::fcntl(fd, libc::F_GETFL) };
    let guest_flags = flags(handed.guest.as_raw_fd());
    handed.peer.write_all(&handed.rest).expect("guest bytes waiting");
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
        .inspect_attachment(0, machine, InspectionSource::Report(Some(&stale)), &scope)
        .expect_err("stale PID identity")
        .contains("different Firecracker or supervisor process"));
    stale = observed.clone();
    let wrong_guest = super::duplicate(machine.stdout_fd).expect("another descriptor for the console pipe");
    stale.guest.fd = wrong_guest.as_raw_fd();
    assert!(vmm
        .inspect_attachment(0, machine, InspectionSource::Report(Some(&stale)), &scope)
        .expect_err("wrong descriptor")
        .contains("is no socket"));
    let machine = machine.clone();
    let daemon_fds = [machine.guest_fd, machine.stdout_fd, machine.stderr_fd]
        .map(|fd| super::duplicate(fd).expect("the daemon receives a copy of each descriptor"));
    let mut daemon_machine = machine.clone();
    daemon_machine.guest_fd = daemon_fds[0].as_raw_fd();
    daemon_machine.stdout_fd = daemon_fds[1].as_raw_fd();
    daemon_machine.stderr_fd = daemon_fds[2].as_raw_fd();

    let observation = tokio::task::spawn_blocking(move || {
        let report = vmm
            .inspect_attachment(0, &machine, InspectionSource::Report(Some(&observed)), &scope)
            .expect("report validation")
            .expect("live VM");
        let daemon = vmm
            .inspect_attachment(
                0,
                &daemon_machine,
                InspectionSource::Daemon(Some(&observed), false),
                &scope,
            )
            .expect("daemon validation")
            .expect("live VM");
        assert_eq!(report.memory.get(), 8192);
        assert_eq!(daemon.memory, report.memory);
        (vmm, daemon_machine, daemon, daemon_fds)
    });
    let serve = async {
        answer_machine_config(&listener).await;
        answer_machine_config(&listener).await;
    };
    let (observed, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(observation, serve)
    })
    .await
    .expect("inspection completes");
    let (vmm, daemon_machine, attachment, daemon_fds) = observed.expect("observation worker");

    assert_eq!(flags(handed.guest.as_raw_fd()), guest_flags);
    let tap = TapPool::new(8192, BOX).claim(BOX, ROOMY).expect("tap");
    let session = "session-1".try_into().expect("session id");
    let (adopted, mut attached_guest) = vmm
        .attach_box(&session, &tap, &daemon_machine, attachment)
        .expect("attach the checked descriptors");
    drop(daemon_fds);
    let GuestMessage::TurnLine(line) = attached_guest.receive().await.expect("the rest of the guest frame") else {
        panic!("expected a turn line");
    };
    assert_eq!(line.text.as_str(), "one");
    assert!(adopted.console_tail().contains("booted"));
    drop((adopted, attached_guest));
    handed.peer.set_nonblocking(true).expect("bound the peer read");
    assert_eq!(
        handed
            .peer
            .read(&mut [0; 1])
            .expect_err("no message to the guest")
            .kind(),
        ErrorKind::WouldBlock
    );
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
    let held = super::scope_reservations(&root, &processes).expect("the scope's open disk");
    assert_eq!(
        held[&2]
            .iter()
            .map(tumpel_api::types::EntityId::as_str)
            .collect::<Vec<_>>(),
        ["live"]
    );
    let ambiguous_scope = (3, vmm.data.join("gone-process"));
    let ambiguous =
        super::scope_reservations(&root, &[processes[0].clone(), ambiguous_scope]).expect("keep possible disks");
    assert_eq!(ambiguous[&2], held[&2]);
    assert_eq!(
        ambiguous[&3]
            .iter()
            .map(tumpel_api::types::EntityId::as_str)
            .collect::<Vec<_>>(),
        ["ended", "live"]
    );
    drop(open);
}
