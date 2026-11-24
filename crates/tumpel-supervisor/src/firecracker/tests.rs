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

pub(super) fn nonblocking(fd: &OwnedFd) {
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
    let error = vmm
        .inspect_attachment(0, &handed.machine, InspectionSource::Daemon(None, false), &own_cgroup())
        .expect_err("a socket is not a console pipe");
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

pub(super) fn legacy_startup(machine: &SupervisorHandoverMachine, index: u8) -> crate::inventory::StartupBoxes {
    let handover = serde_json::from_value(json!({
        "handedAt": "2026-11-23T21:47:09Z",
        "boxes": [{
            "boxId": "box-1", "boxRetention": "Warm", "dirty": false, "idleMilliseconds": 0,
            "image": {"kind": "Full", "manifest": {
                "imageDigest": "a".repeat(64), "kernelDigest": "b".repeat(64), "baseRootfsDigest": "c".repeat(64),
                "kernelUrl": "https://example.test/kernel", "baseRootfsUrl": "https://example.test/rootfs.squashfs",
                "harness": {"harness": "Shell", "harnessVersion": "1", "protocol": "ShellLines",
                    "command": ["/bin/sh", "-s"], "workingDirectory": "/root", "environment": {"HOME": "/root", "LANG": "C.UTF-8", "PATH": "/usr/bin:/bin"},
                    "fixedVariables": [], "cloneSeconds": 120}, "components": []
            }}, "machine": machine, "queued": [], "sessionId": "session-1",
            "tapIndex": index, "state": {"kind": "Warm"}
        }]
    }))
    .expect("legacy handover record");
    let mut saved = crate::inventory::StartupBoxes::from_record(handover, crate::lifecycle::Taken::Handed);
    saved.inherited = true;
    saved
}

#[tokio::test]
async fn a_live_child_outside_its_box_scope_keeps_its_record_and_disk() {
    use crate::lifecycle::{
        tests::{settings, MemoryStore},
        Lifecycle,
    };
    let vmm = firecracker("unscoped-live");
    let child = VmChild::new();
    let handed = handed(&vmm, child.pid());
    let (out, _) = tokio::sync::mpsc::unbounded_channel();
    let path = vmm.data.join("boxes.json");
    let kept = vmm.data.join("sessions/session-1/overlay.ext4");
    let retired = vmm.data.join("sessions/old-session/overlay.ext4");
    for disk in [&kept, &retired] {
        std::fs::create_dir_all(disk.parent().expect("disk directory")).expect("session directory");
        std::fs::write(disk, b"fixture disk").expect("a session disk");
    }
    let mut saved = legacy_startup(&handed.machine, 0);
    saved.inherited = false;
    let mut lifecycle = Lifecycle::new(
        vmm,
        MemoryStore::default(),
        settings(),
        TapPool::new(4096, BOX),
        out,
        path.clone(),
    );
    lifecycle.start_inventory(Some(saved)).expect("startup keeps reporting");
    assert_eq!(
        lifecycle
            .sessions()
            .borrow()
            .iter()
            .map(EntityId::as_str)
            .collect::<Vec<_>>(),
        ["session-1"]
    );
    let inventory = crate::inventory::load(&path)
        .expect("saved inventory")
        .expect("a record");
    assert_eq!(inventory.boxes.len(), 1);
    assert!(kept.exists(), "the live disk survives startup");
    assert!(!retired.exists(), "ordinary startup still removes unrelated leftovers");
    assert!(super::process_identity(child.pid())
        .expect("the child is untouched")
        .is_some());
    std::fs::remove_dir_all(path.parent().expect("fixture directory")).expect("remove test files");
}

#[test]
fn a_legacy_child_and_streams_are_preserved_before_failed_attachment() {
    let vmm = firecracker("legacy-rejection");
    let child = VmChild::new();
    let mut record = handed(&vmm, child.pid());
    record.machine.guest_fd = record.guest.try_clone().expect("old guest FD").into_raw_fd();
    record.machine.stdout_fd = record.out.try_clone().expect("old stdout FD").into_raw_fd();
    record.machine.stderr_fd = record.err.try_clone().expect("old stderr FD").into_raw_fd();
    let guest_fd = record.machine.guest_fd;
    let flags = unsafe { libc::fcntl(guest_fd, libc::F_GETFL) };
    assert_eq!(
        unsafe { libc::fcntl(guest_fd, libc::F_SETFL, flags & !libc::O_NONBLOCK) },
        0
    );
    let moved = std::cell::Cell::new(false);
    let mut saved = legacy_startup(&record.machine, 0);
    let streams = Firecracker::prepare_legacy(&mut saved, |pid, scope| {
        assert_eq!(pid, child.pid());
        assert_eq!(scope, "tumpel-box0.scope");
        assert!(!vmm.data.join("box0/api.sock").exists());
        assert_ne!(unsafe { libc::fcntl(guest_fd, libc::F_GETFL) } & libc::O_NONBLOCK, 0);
        moved.set(true);
        Ok(())
    });
    assert!(moved.get());
    assert_eq!(
        streams.iter().map(|stream| stream.role).collect::<Vec<_>>(),
        ["guest", "stdout", "stderr"]
    );
    assert_ne!(unsafe { libc::fcntl(guest_fd, libc::F_GETFD) } & libc::FD_CLOEXEC, 0);
    assert!(super::process_identity(child.pid()).expect("the VM survives").is_some());
}

#[test]
fn duplicate_legacy_streams_survive_a_rejected_scope_move() {
    let vmm = firecracker("duplicate-legacy-streams");
    let child = VmChild::new();
    let second = VmChild::new();
    let first_hand = handed(&vmm, child.pid());
    let second_hand = handed(&vmm, second.pid());
    let duplicate = first_hand.guest.as_raw_fd();
    assert_eq!(unsafe { libc::fcntl(duplicate, libc::F_SETFD, 0) }, 0);
    let mut first = first_hand.machine;
    let mut other = second_hand.machine;
    other.guest_fd = duplicate;
    first.stdout_fd = first_hand.out.try_clone().expect("inherited stdout").into_raw_fd();
    first.stderr_fd = first_hand.err.try_clone().expect("inherited stderr").into_raw_fd();
    other.stdout_fd = second_hand.out.try_clone().expect("inherited stdout").into_raw_fd();
    other.stderr_fd = second_hand.err.try_clone().expect("inherited stderr").into_raw_fd();
    let mut saved = legacy_startup(&first, 0);
    let mut next = legacy_startup(&other, 1).inventory.boxes.remove(0);
    next.record.session_id = "session-2".try_into().expect("another session");
    next.record.box_id = "box-2".try_into().expect("another box");
    saved.inventory.boxes.push(next);
    saved.handed_count = 2;

    let scopes = std::cell::RefCell::new(Vec::new());
    let streams = Firecracker::prepare_legacy(&mut saved, |pid, scope| {
        scopes.borrow_mut().push((pid, scope.to_string()));
        Err("the service manager rejected the scope".to_string())
    });
    assert_eq!(
        scopes.into_inner(),
        [
            (child.pid(), "tumpel-box0.scope".into()),
            (second.pid(), "tumpel-box1.scope".into())
        ]
    );
    assert_eq!(streams.len(), 4);
    assert!(saved
        .inventory
        .boxes
        .iter()
        .all(|saved| saved.record.machine.guest_fd == -1));
    assert_eq!(unsafe { libc::fcntl(duplicate, libc::F_GETFD) }, libc::FD_CLOEXEC);
    assert!(super::process_identity(child.pid()).expect("first process").is_some());
    assert!(super::process_identity(second.pid()).expect("second process").is_some());
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
        .contains("different Firecracker process"));
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
        check_replaced_guest(&vmm, &daemon_machine, &observed, &scope);
        (vmm, daemon_machine, daemon, daemon_fds)
    });
    let serve = async {
        answer_machine_config(&listener).await;
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
    verify_materialized_attachment(&vmm, &daemon_machine, attachment, daemon_fds, &mut handed).await;
}

fn check_replaced_guest(
    vmm: &Firecracker,
    daemon_machine: &SupervisorHandoverMachine,
    observed: &super::Observation,
    scope: &str,
) {
    let mut replaced = daemon_machine.clone();
    let (guest, _peer) = UnixStream::pair().expect("the replaced guest connection");
    let guest = OwnedFd::from(guest);
    nonblocking(&guest);
    replaced.guest_fd = guest.as_raw_fd();
    assert!(vmm
        .inspect_attachment(0, &replaced, InspectionSource::Daemon(Some(observed), false), scope)
        .expect_err("a handed turn must keep its guest stream")
        .contains("different descriptor"));
    let recovery = vmm
        .inspect_attachment(0, &replaced, InspectionSource::Daemon(Some(observed), true), scope)
        .expect("crash recovery checks the stable console and VM identities")
        .expect("the VM still owns its box");
    assert!(
        recovery.guest_buffer.is_empty(),
        "a new guest stream inherits no buffered turn frame"
    );
}

async fn verify_materialized_attachment(
    vmm: &Firecracker,
    daemon_machine: &SupervisorHandoverMachine,
    attachment: super::Attachment,
    daemon_fds: [OwnedFd; 3],
    handed: &mut Handed,
) {
    let session = "session-1".try_into().expect("session id");
    let disk = vmm.data.join("sessions/session-1/overlay.ext4");
    std::fs::create_dir_all(disk.parent().expect("session directory")).expect("session directory");
    std::fs::File::create(disk)
        .expect("create the box disk")
        .set_len(32 * 1024 * 1024)
        .expect("size the disk");
    let mut pool = TapPool::new(8192, BOX);

    let mut exited = VmChild::new();
    let exited_pid = exited.pid();
    exited.0.kill().expect("end the process during inspection");
    exited.0.wait().expect("reap the exited process");
    let late = super::Attachment {
        pid: exited_pid,
        owner: Owner::Child,
        memory: attachment.memory,
        guest_buffer: Vec::new(),
        console_tail: Vec::new(),
    };
    let Err(error) = vmm.finish_attachment(&session, 0, daemon_machine, &mut pool, late) else {
        panic!("the watcher rejects a PID that disappeared");
    };
    assert!(error.contains("watching Firecracker process"), "{error}");

    let (mut adopted, mut attached_guest, tap) = vmm
        .finish_attachment(&session, 0, daemon_machine, &mut pool, attachment)
        .expect("the failed attachment released its TAP and left all streams available");
    handed.stdout.write_all(b"late stdout\n").expect("write the console");
    assert!(!adopted.console_tail().contains("late stdout"));
    adopted.activate();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !adopted.console_tail().contains("late stdout") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the console starts at activation");
    pool.release(tap);
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
            .identified
            .iter()
            .map(tumpel_api::types::EntityId::as_str)
            .collect::<Vec<_>>(),
        ["live"]
    );
    let ambiguous_scope = (3, vmm.data.join("gone-process"));
    let ambiguous =
        super::scope_reservations(&root, &[processes[0].clone(), ambiguous_scope]).expect("keep possible disks");
    assert_eq!(ambiguous[&2].identified, held[&2].identified);
    assert_eq!(
        ambiguous[&3]
            .possible
            .iter()
            .map(tumpel_api::types::EntityId::as_str)
            .collect::<Vec<_>>(),
        ["ended", "live"]
    );
    drop(open);
}
