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
async fn reattach_of_a_box_whose_process_ended_closes_every_handed_descriptor() {
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
    assert!(closed(&mut handed));
    assert!(reaped(pid));
}

#[tokio::test]
async fn reattach_with_the_wrong_descriptor_kills_and_reaps_the_live_child_and_closes_the_rest() {
    let vmm = firecracker("reattach-wrong");
    let tap = TapPool::new(4096, BOX).claim(BOX, ROOMY).expect("tap");
    let pid = sleeper();
    let mut handed = handed(&vmm, pid, false);
    let session = "session-1".try_into().expect("session id");
    let Err(error) = vmm.reattach(&session, &tap, &handed.machine) else {
        panic!("a pipe handed as the guest connection is refused");
    };
    assert!(error.contains("is no socket"), "{error}");
    assert!(reaped(pid), "the live child is killed and reaped");
    let pipes = released(&mut handed.stdout) && released(&mut handed.stderr);
    assert!(pipes, "the console pipes are closed");
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
    assert_eq!(booted_memory(&socket), Ok(NonZeroU32::new(8192).expect("8192")));
    assert!(server
        .join()
        .expect("the server")
        .starts_with("GET /machine-config HTTP/1.1\r\n"));
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

fn scan_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("tumpel-scan-{}-{name}", std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("clearing the last run");
    }
    std::fs::create_dir_all(&dir).expect("the scan directory");
    dir.canonicalize().expect("the scan directory")
}

fn holding(files: &[&Path]) -> std::process::Child {
    use std::io::BufRead;
    let mut child = std::process::Command::new("sh")
        .arg("-c")
        .arg(r#"exec 3<"$1" 4<"$2" 5<"$3"; echo ready; exec sleep 60"#)
        .arg("sh")
        .args(files)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("sh starts");
    let mut ready = String::new();
    std::io::BufReader::new(child.stdout.take().expect("its output"))
        .read_line(&mut ready)
        .expect("it opens the files");
    assert_eq!(ready, "ready\n");
    child
}

fn answer_machine_config(socket: &Path, mem_size_mib: u32) -> std::thread::JoinHandle<()> {
    std::fs::create_dir_all(socket.parent().expect("the box directory")).expect("the box directory");
    let listener = std::os::unix::net::UnixListener::bind(socket).expect("the API socket");
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("a connection");
        let mut request = [0u8; 256];
        let read = stream.read(&mut request).expect("the request");
        assert!(request[..read].starts_with(b"GET /machine-config "));
        let body = format!(r#"{{"vcpu_count":2,"mem_size_mib":{mem_size_mib},"smt":false}}"#);
        write!(
            stream,
            "HTTP/1.1 200 \r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .expect("the answer");
    })
}

fn overlay(data: &Path, session: &str, mib: u64) -> PathBuf {
    let dir = data.join("sessions").join(session);
    std::fs::create_dir_all(&dir).expect("the session directory");
    let drive = dir.join("overlay.ext4");
    std::fs::File::create(&drive)
        .and_then(|file| file.set_len(mib * 1024 * 1024))
        .expect("the box disk");
    drive
}

const HARNESS: &str = r#"{ "harness": "Shell", "harnessVersion": "1", "protocol": "ShellLines",
    "command": ["/bin/sh", "-s"], "workingDirectory": "/root",
    "environment": { "PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8" }, "fixedVariables": [], "cloneSeconds": 120 }"#;

#[test]
fn a_scan_tells_each_running_box_its_index_process_session_image_and_size() {
    let dir = scan_dir("running");
    let images = dir.join("images");
    let data = dir.join("data");
    let cgroups = dir.join("cgroups");
    std::fs::create_dir_all(images.join("layers")).expect("the image cache");
    let (full, base, layer) = ("a".repeat(64), "b".repeat(64), "c".repeat(64));
    std::fs::write(images.join(format!("{full}.ext4")), b"full").expect("the full image");
    std::fs::write(
        images.join(format!("{full}.json")),
        format!(
            r#"{{ "imageDigest": "{full}", "kernelUrl": "https://example.com/vmlinux", "kernelDigest": "{}",
            "baseRootfsUrl": "https://example.com/rootfs.squashfs", "baseRootfsDigest": "{}",
            "harness": {HARNESS}, "components": [] }}"#,
            "d".repeat(64),
            "e".repeat(64)
        ),
    )
    .expect("the full manifest");
    std::fs::write(images.join(format!("{base}.ext4")), b"base").expect("the base image");
    std::fs::write(
        images.join(format!("{base}.json")),
        format!(
            r#"{{ "imageDigest": "{base}", "kernelUrl": "https://example.com/vmlinux", "kernelDigest": "{}",
            "baseRootfsUrl": "https://example.com/rootfs.squashfs", "baseRootfsDigest": "{}",
            "baseLayoutVersion": 1, "components": [] }}"#,
            "d".repeat(64),
            "e".repeat(64)
        ),
    )
    .expect("the base manifest");
    std::fs::write(images.join("layers").join(format!("{layer}.squashfs")), b"layer").expect("the layer");
    std::fs::write(
        images.join("layers").join(format!("{layer}.json")),
        format!(
            r#"{{ "layerDigest": "{layer}", "recipeDigest": "{}", "architecture": "Amd64", "harness": {HARNESS} }}"#,
            "f".repeat(64)
        ),
    )
    .expect("the layer manifest");
    let full_box = holding(&[
        &images.join(format!("{full}.ext4")),
        &overlay(&data, "session-three", 64),
        Path::new("/dev/null"),
    ]);
    let layered_box = holding(&[
        &images.join(format!("{base}.ext4")),
        &images.join("layers").join(format!("{layer}.squashfs")),
        &overlay(&data, "session-five", 128),
    ]);
    for (group, child) in [("box3", &full_box), ("box5", &layered_box)] {
        std::fs::create_dir_all(cgroups.join(group)).expect("the scope cgroup");
        std::fs::write(cgroups.join(group).join("cgroup.procs"), format!("{}\n", child.id())).expect("cgroup.procs");
    }
    let servers = [
        answer_machine_config(&data.join("box3/api.sock"), 2048),
        answer_machine_config(&data.join("box5/api.sock"), 4096),
    ];
    let vmm = Firecracker {
        binary: PathBuf::from("firecracker"),
        images: images.clone(),
        data,
        vcpus: 1,
    };

    let scanned = vmm.scanned(
        "ControlGroup=/box3\nId=tumpel-box3.scope\n\nId=tumpel-box5.scope\nControlGroup=/box5\n",
        &cgroups,
    );

    let found: Vec<_> = scanned
        .into_iter()
        .map(|(index, vm)| {
            let vm = vm.expect("the box scans");
            (index, vm.pid, vm.session_id.to_string(), vm.image.named(), vm.size)
        })
        .collect();
    assert_eq!(
        found,
        vec![
            (
                3,
                libc::pid_t::try_from(full_box.id()).expect("a pid"),
                "session-three".to_string(),
                format!("image {full}"),
                Size {
                    memory_mib: NonZeroU32::new(2048).expect("2048"),
                    disk_mib: NonZeroU32::new(64).expect("64"),
                }
            ),
            (
                5,
                libc::pid_t::try_from(layered_box.id()).expect("a pid"),
                "session-five".to_string(),
                format!("base {base} with harness layer {layer}"),
                Size {
                    memory_mib: NonZeroU32::new(4096).expect("4096"),
                    disk_mib: NonZeroU32::new(128).expect("128"),
                }
            ),
        ]
    );
    for server in servers {
        server.join().expect("the API answers");
    }
    for mut child in [full_box, layered_box] {
        child.kill().expect("the box stops");
        child.wait().expect("the box is reaped");
    }
}

#[test]
fn a_scan_reports_the_step_that_fails_for_each_box_and_scans_the_rest() {
    let dir = scan_dir("failing");
    let images = dir.join("images");
    let data = dir.join("data");
    let cgroups = dir.join("cgroups");
    std::fs::create_dir_all(&images).expect("the image cache");
    let gone = "a".repeat(64);
    std::fs::write(images.join(format!("{gone}.ext4")), b"image").expect("an image without manifest");
    let disk = overlay(&data, "session-eight", 64);
    let unknown_image = holding(&[&images.join(format!("{gone}.ext4")), &disk, Path::new("/dev/null")]);
    let no_image = holding(&[&disk, Path::new("/dev/null"), Path::new("/dev/null")]);
    std::fs::create_dir_all(cgroups.join("box7")).expect("an empty scope cgroup");
    for (group, procs) in [
        ("box8", format!("{}\n", unknown_image.id())),
        ("box6", format!("{}\n", no_image.id())),
        ("box4", format!("{}\n{}\n", unknown_image.id(), no_image.id())),
    ] {
        std::fs::create_dir_all(cgroups.join(group)).expect("the scope cgroup");
        std::fs::write(cgroups.join(group).join("cgroup.procs"), procs).expect("cgroup.procs");
    }
    let vmm = Firecracker {
        binary: PathBuf::from("firecracker"),
        images,
        data,
        vcpus: 1,
    };

    let scanned = vmm.scanned(
        "Id=tumpel-box7.scope\nControlGroup=/box7\n\nId=tumpel-box8.scope\nControlGroup=/box8\n\nId=tumpel-box9.scope\nControlGroup=\n\nId=tumpel-box6.scope\nControlGroup=/box6\n\nId=tumpel-box4.scope\nControlGroup=/box4\n\nId=tumpel-box07.scope\nControlGroup=/box7\n",
        &cgroups,
    );

    let failed: Vec<(u8, String)> = scanned
        .into_iter()
        .map(|(index, vm)| (index, vm.err().expect("the scan step fails")))
        .collect();
    assert_eq!(
        failed.iter().map(|(index, _)| *index).collect::<Vec<_>>(),
        vec![7, 8, 9, 6, 4]
    );
    assert!(failed[0].1.contains("box7/cgroup.procs"), "{}", failed[0].1);
    assert!(
        failed[1]
            .1
            .contains(&format!("runs image {gone}, and its manifest is gone")),
        "{}",
        failed[1].1
    );
    assert_eq!(failed[2].1, "tumpel-box9.scope runs no process");
    assert!(
        failed[3].1.contains("holds no image from the image cache open"),
        "{}",
        failed[3].1
    );
    assert_eq!(
        failed[4].1,
        "tumpel-box4.scope runs 2 processes, and a box scope runs its Firecracker process alone"
    );
    for mut child in [unknown_image, no_image] {
        child.kill().expect("the box stops");
        child.wait().expect("the box is reaped");
    }
}
