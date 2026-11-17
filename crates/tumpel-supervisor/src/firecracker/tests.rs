use std::io::{ErrorKind, Read, Write};
use std::num::NonZeroU32;
use std::os::fd::{AsRawFd, IntoRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::json;
use tumpel_api::types::{GuestMessage, SupervisorHandoverMachine};
use tumpel_guest::encode;

use super::{adopt, alive, take, Firecracker};
use crate::lifecycle::{Guest, Machine, Vmm};
use crate::pool::TapPool;

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

fn reaped(pid: libc::pid_t) -> bool {
    alive(pid).is_err_and(|error| error.contains("is no child"))
}

#[test]
fn alive_tells_a_running_child_from_a_zombie_and_a_stranger() {
    let pid = sleeper();
    assert_eq!(alive(pid), Ok(()));
    zombie(pid);
    let ended = alive(pid).expect_err("a zombie is no live box");
    assert!(ended.contains("ended during the handover with signal 9"), "{ended}");
    assert!(reaped(pid), "alive reaps the zombie it reports");
    let stranger = alive(1).expect_err("init is no child");
    assert!(stranger.contains("is no child of this supervisor"), "{stranger}");
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
        memory_mib: 128,
        overlay_bytes: 0,
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
            api_socket: dir.join("api.sock").display().to_string(),
            console_tail: STANDARD.encode("booted\n"),
            guest_buffer: STANDARD.encode(first),
            guest_fd,
            process_id: NonZeroU32::new(u32::try_from(pid).expect("a pid")).expect("a pid"),
            stderr_fd: OwnedFd::from(err).into_raw_fd(),
            stdout_fd: OwnedFd::from(out).into_raw_fd(),
            vsock: dir.join("v.sock").display().to_string(),
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
    let tap = TapPool::new(1).expect("pool").claim().expect("tap");
    let pid = sleeper();
    let mut handed = handed(&vmm, pid, true);
    let session = "session-1".try_into().expect("session id");
    let (machine, mut guest) = vmm
        .reattach(&session, &tap, &handed.machine)
        .expect("the box is adopted");
    handed
        .peer
        .write_all(&handed.rest)
        .expect("the guest writes the rest of its frame");
    let GuestMessage::TurnLine(line) = guest.receive().await.expect("a message") else {
        panic!("the guest sent its line");
    };
    assert_eq!(line.text, "one");
    assert!(machine.console_tail().contains("booted"));
    machine.stop().await;
    assert!(reaped(pid), "stopping an adopted box kills and reaps its process");
}

#[tokio::test]
async fn reattach_of_a_box_whose_process_ended_closes_every_handed_descriptor() {
    let vmm = firecracker("reattach-ended");
    let tap = TapPool::new(1).expect("pool").claim().expect("tap");
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
    let tap = TapPool::new(1).expect("pool").claim().expect("tap");
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
