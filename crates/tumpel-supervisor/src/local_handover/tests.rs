use std::future::Future;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::{timeout, timeout_at, Instant};

use super::{read, receive, receive_handover, request, send, Endpoint, ACCEPT_LISTENER, NEGOTIATE};
use crate::local_handover::Descriptors;

const TAP: u8 = 7;
const TEST_TIME: Duration = Duration::from_secs(5);

struct Boxes {
    descriptors: Descriptors,
    guest_peer: std::os::unix::net::UnixStream,
    stdout: std::io::PipeWriter,
    stderr: std::io::PipeWriter,
}

impl Boxes {
    fn new() -> Self {
        let (guest, guest_peer) = std::os::unix::net::UnixStream::pair().expect("guest socket pair");
        let (output, stdout) = std::io::pipe().expect("stdout pipe");
        let (errors, stderr) = std::io::pipe().expect("stderr pipe");
        Self {
            descriptors: Descriptors::from([(TAP, std::sync::Arc::new([guest.into(), output.into(), errors.into()]))]),
            guest_peer,
            stdout,
            stderr,
        }
    }

    fn assert_usable(&mut self) {
        self.guest_peer.write_all(b"g").expect("write to the old guest socket");
        self.stdout.write_all(b"o").expect("write to the old stdout pipe");
        self.stderr.write_all(b"e").expect("write to the old stderr pipe");
        for (file, wanted) in self.descriptors[&TAP].iter().zip(*b"goe") {
            let mut byte = [0];
            std::fs::File::from(file.try_clone().expect("borrow a reader from the old owner"))
                .read_exact(&mut byte)
                .expect("read the old descriptor");
            assert_eq!(byte, [wanted]);
        }
    }
}

fn file_identity(fd: RawFd) -> String {
    let metadata = std::fs::metadata(format!("/proc/self/fd/{fd}")).expect("descriptor metadata");
    format!("{}:{}", metadata.dev(), metadata.ino())
}

struct State(PathBuf);

impl State {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("tumpel-handover-{}-{name}", std::process::id()));
        std::fs::create_dir(&path).expect("fixture state directory");
        Self(path)
    }

    fn listen(&self) -> Endpoint {
        Endpoint::bind(&self.0, &std::env::current_exe().expect("the staged executable")).expect("listen")
    }
}

impl Drop for State {
    fn drop(&mut self) {
        std::fs::remove_dir(&self.0).expect("remove the fixture directory");
    }
}

fn candidate(path: &Path, mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("the test executable"));
    command
        .args(["--exact", "local_handover::tests::candidate_process", "--nocapture"])
        .env_clear()
        .env("TUMPEL_TEST_HANDOVER_STATE", path)
        .env("TUMPEL_TEST_HANDOVER_MODE", mode)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

async fn with_server(endpoint: &Endpoint, descriptors: Descriptors, exercise: impl Future<Output = ()>) {
    tokio::select! {
        result = endpoint.serve(move || Ok(descriptors.clone())) => panic!("the handover server stopped: {result:?}"),
        () = exercise => {}
    }
}

#[test]
fn candidate_process() {
    let Some(state) = std::env::var_os("TUMPEL_TEST_HANDOVER_STATE").map(PathBuf::from) else {
        return;
    };
    let mode = std::env::var("TUMPEL_TEST_HANDOVER_MODE").expect("the fixture mode");
    let parent = unsafe { libc::getppid() };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("candidate runtime");
    runtime.block_on(async {
        if mode == "hold" {
            let offer = timeout(TEST_TIME, request(&state, parent))
                .await
                .expect("the listener arrives before the deadline")
                .expect("a listener offer");
            send(&offer.stream, ACCEPT_LISTENER, &[])
                .await
                .expect("accept the listener");
            let box_ = receive(&offer.stream, &offer.peer)
                .await
                .expect("receive the first box");
            assert_eq!(box_.kind, TAP);
            assert_eq!(box_.fds.len(), 3);
            println!("box descriptors received");
            std::future::pending::<()>().await;
        }
        if mode == "timeout" {
            let offer = timeout(TEST_TIME, request(&state, parent))
                .await
                .expect("the listener arrives before the deadline")
                .expect("a listener offer");
            println!("listener received");
            assert!(timeout_at(Instant::now() + NEGOTIATE, read(&offer.stream))
                .await
                .expect("the old owner closes the timed-out connection")
                .expect("read the local socket")
                .is_none());
            return;
        }
        let parent = u32::try_from(parent)
            .expect("parent PID")
            .try_into()
            .expect("nonzero PID");
        let received = receive_handover(&state, parent).await;
        match mode.as_str() {
            "busy" | "refused" => {
                let Err(error) = received else {
                    panic!("the successor must be refused");
                };
                let reason = if mode == "busy" {
                    "already has a handover successor"
                } else {
                    "refused the handover successor"
                };
                assert!(error.to_string().contains(reason), "{error:#}");
            }
            "complete" => {
                let transfer = received.expect("the candidate receives the transfer");
                assert_eq!(transfer.boxes.keys().copied().collect::<Vec<_>>(), [TAP]);
                let fds = std::iter::once(&transfer.listener).chain(transfer.boxes[&TAP].iter());
                let identities = fds
                    .map(|fd| {
                        let raw = fd.as_raw_fd();
                        assert_ne!(unsafe { libc::fcntl(raw, libc::F_GETFD) } & libc::FD_CLOEXEC, 0);
                        file_identity(raw)
                    })
                    .collect::<Vec<_>>();
                let expected = std::env::var("TUMPEL_TEST_HANDOVER_FDS").expect("the old owner's descriptors");
                assert_eq!(identities.join(" "), expected);
            }
            _ => panic!("unknown candidate fixture mode"),
        }
    });
}

#[tokio::test]
async fn the_candidate_receives_the_active_listener_and_matching_box_descriptors() {
    let state = State::new("complete");
    let endpoint = state.listen();
    let mut boxes = Boxes::new();
    let identities = std::iter::once(file_identity(endpoint.listener.as_raw_fd()))
        .chain(boxes.descriptors[&TAP].iter().map(|fd| file_identity(fd.as_raw_fd())))
        .collect::<Vec<_>>();
    with_server(&endpoint, boxes.descriptors.clone(), async {
        let output = timeout(
            TEST_TIME,
            candidate(&state.0, "complete")
                .env("TUMPEL_TEST_HANDOVER_FDS", identities.join(" "))
                .output(),
        )
        .await
        .expect("candidate exits")
        .expect("candidate starts");
        assert!(output.status.success(), "{output:?}");
    })
    .await;
    boxes.assert_usable();
}

#[tokio::test]
async fn a_successor_with_a_mismatched_user_is_refused() {
    let state = State::new("wrong-user");
    let mut endpoint = state.listen();
    endpoint.uid ^= 1;
    with_server(&endpoint, Descriptors::new(), async {
        let output = timeout(TEST_TIME, candidate(&state.0, "refused").output())
            .await
            .expect("candidate exits")
            .expect("candidate starts");
        assert!(output.status.success(), "{output:?}");
    })
    .await;
}

#[tokio::test]
async fn a_child_running_a_different_executable_is_refused() {
    let state = State::new("wrong-executable");
    let endpoint = Endpoint::bind(&state.0, Path::new("/proc/self/cwd")).expect("listen");
    with_server(&endpoint, Descriptors::new(), async {
        let output = timeout(TEST_TIME, candidate(&state.0, "refused").output())
            .await
            .expect("candidate exits")
            .expect("candidate starts");
        assert!(output.status.success(), "{output:?}");
    })
    .await;
}

async fn wait_for_transfer(lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>, expected: &str) {
    timeout(TEST_TIME, async {
        while let Some(line) = lines.next_line().await.expect("candidate output") {
            if line == expected {
                return;
            }
        }
        panic!("candidate exited before reporting: {expected}");
    })
    .await
    .expect("candidate receives its descriptors");
}

#[tokio::test]
async fn a_second_successor_is_refused_and_a_death_mid_transfer_keeps_the_originals() {
    let state = State::new("died");
    let endpoint = state.listen();
    let mut boxes = Boxes::new();
    with_server(&endpoint, boxes.descriptors.clone(), async {
        let mut first = candidate(&state.0, "hold").spawn().expect("first candidate starts");
        let mut lines = BufReader::new(first.stdout.take().expect("candidate output")).lines();
        wait_for_transfer(&mut lines, "box descriptors received").await;
        let second = timeout(TEST_TIME, candidate(&state.0, "busy").output())
            .await
            .expect("second candidate exits")
            .expect("second candidate starts");
        assert!(second.status.success(), "{second:?}");
        first
            .kill()
            .await
            .expect("kill and reap the first candidate mid-transfer");
        boxes.assert_usable();
    })
    .await;
    assert!(rustix::net::sockopt::socket_acceptconn(&*endpoint.listener).expect("old listener is still open"));
}

#[tokio::test]
async fn an_incomplete_transfer_expires_within_the_negotiation_deadline() {
    let state = State::new("deadline");
    let endpoint = state.listen();
    with_server(&endpoint, Descriptors::new(), async {
        let mut child = candidate(&state.0, "timeout").spawn().expect("candidate starts");
        let mut lines = BufReader::new(child.stdout.take().expect("candidate output")).lines();
        wait_for_transfer(&mut lines, "listener received").await;
        tokio::time::pause();
        tokio::time::advance(NEGOTIATE).await;
        tokio::time::resume();
        let output = timeout(TEST_TIME, child.wait_with_output())
            .await
            .expect("candidate observes EOF")
            .expect("candidate exits");
        assert!(output.status.success(), "{output:?}");
    })
    .await;
}
