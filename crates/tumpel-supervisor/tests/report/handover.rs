use std::fs::File;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::process::Stdio;

use anyhow::Context as _;
use ring::digest::{digest, SHA256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio_tungstenite::WebSocketStream;
use tumpel_api::types::GuestMessage;

use super::*;

const RELEASE: &str = "v999.1.0";
const TIME: Duration = Duration::from_secs(20);

fn sha256(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .flat_map(|byte| [HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 15)]])
        .map(char::from)
        .collect()
}

async fn frame(socket: &mut WebSocketStream<TcpStream>) -> anyhow::Result<Value> {
    let message = socket.next().await.context("expected a channel frame")??;
    Ok(serde_json::from_str(message.to_text()?)?)
}

async fn next_message(
    socket: &mut WebSocketStream<TcpStream>,
    matches: impl Fn(&Value) -> bool,
) -> anyhow::Result<Value> {
    loop {
        let mut value = frame(socket).await?;
        if matches(&value["message"]) {
            return Ok(value["message"].take());
        }
    }
}

async fn send(socket: &mut WebSocketStream<TcpStream>, message: Value) -> anyhow::Result<()> {
    Ok(socket.send(Message::text(message.to_string())).await?)
}

async fn hello(socket: &mut WebSocketStream<TcpStream>) -> anyhow::Result<Value> {
    send(socket, json!({"type":"Challenge", "nonce":"n".repeat(43)})).await?;
    let hello = frame(socket).await?;
    assert_eq!(hello["type"], "Hello");
    Ok(hello)
}

async fn welcome(socket: &mut WebSocketStream<TcpStream>) -> anyhow::Result<()> {
    send(
        socket,
        json!({"type":"Welcome", "hostId":"host-1", "protocolVersion":ChannelProtocol::VERSION}),
    )
    .await
}

async fn serve_release(listener: &TcpListener, candidate: &[u8], candidate_digest: &str) -> io::Result<()> {
    for (path, body) in [
        (
            "/v1/releases/current",
            json!({
                "releaseVersion": RELEASE,
                "channelProtocolVersion": ChannelProtocol::VERSION,
                "supervisors": [
                    {"architecture": "Amd64", "sha256": candidate_digest, "supervisorVersion": "999.0.0"},
                    {"architecture": "Arm64", "sha256": candidate_digest, "supervisorVersion": "999.0.0"}
                ]
            })
            .to_string()
            .into_bytes(),
        ),
        ("/v1/releases/supervisor/", Vec::new()),
    ] {
        let (mut connection, _) = listener.accept().await?;
        let mut request = BufReader::new(&mut connection);
        let mut line = String::new();
        request.read_line(&mut line).await?;
        assert!(line.starts_with(&format!("GET {path}")), "{line}");
        while line != "\r\n" {
            line.clear();
            assert_ne!(request.read_line(&mut line).await?, 0);
        }
        let body = if body.is_empty() { candidate } else { &body };
        connection
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
        connection.write_all(body).await?;
    }
    Ok(())
}

fn handover_record(host: &Host, pid: u32, guest_fd: i32, guest_buffer: &str) -> Value {
    json!({
        "handedAt": chrono::Utc::now().to_rfc3339(),
        "boxes": [{
            "boxId": "box-1", "sessionId":"session-1", "boxRetention":"Warm",
            "dirty":true, "idleMilliseconds":60000, "tapIndex":0, "queued":[],
            "state":{"kind":"Turn","turnId":"turn-1","turnMilliseconds":60000,"firstLineMilliseconds":60000,"repositoryReady":true,"seenLine":false},
            "machine":{
                "apiSocket":host.root.join("data/box0/api.sock"),
                "vsock":host.root.join("data/box0/v.sock"),
                "processId":pid, "guestFd":guest_fd, "stdoutFd":4, "stderrFd":5,
                "guestBuffer":guest_buffer, "consoleTail":""
            },
            "image":{"kind":"Full", "manifest":{
                "imageDigest":"a".repeat(64), "kernelUrl":"https://example.com/vmlinux",
                "kernelDigest":"b".repeat(64), "baseRootfsUrl":"https://example.com/rootfs",
                "baseRootfsDigest":"c".repeat(64), "components":[],
                "harness":{"harness":"Shell","harnessVersion":"1","protocol":"ShellLines",
                    "command":["/bin/sh","-s"],"workingDirectory":"/root",
                    "environment":{"HOME":"/root","PATH":"/usr/bin:/bin","LANG":"C.UTF-8"},"fixedVariables":[],"cloneSeconds":30}
            }}
        }]
    })
}

fn manager(host: &Host) -> io::Result<()> {
    fs::create_dir_all(host.root.join("config/systemd/user"))?;
    let manager = host.root.join("systemctl");
    fs::write(&manager, b"#!/bin/sh\nexit 0\n")?;
    fs::set_permissions(manager, fs::Permissions::from_mode(0o755))
}

async fn box_api(listener: UnixListener) -> io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let mut request = [0; 1024];
        let length = stream.read(&mut request).await?;
        assert!(request[..length].starts_with(b"GET /machine-config "));
        let body = r#"{"mem_size_mib":128}"#;
        stream
            .write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes())
            .await?;
    }
}

fn firecracker_api(host: &Host) -> io::Result<UnixListener> {
    fs::create_dir_all(host.root.join("data/sessions/session-1"))?;
    File::create(host.root.join("data/sessions/session-1/overlay.ext4"))?.set_len(64 * 1024 * 1024)?;
    fs::create_dir(host.root.join("data/box0"))?;
    UnixListener::bind(host.root.join("data/box0/api.sock"))
}

struct BoxPid(libc::pid_t);

impl Drop for BoxPid {
    fn drop(&mut self) {
        unsafe { libc::kill(self.0, libc::SIGKILL) };
    }
}

async fn launch_with_parked_turn(
    host: &Host,
    binary: &Path,
    guest_buffer: &str,
) -> anyhow::Result<(tokio::process::Child, BoxPid, UnixStream, [io::PipeWriter; 2])> {
    let (guest, peer) = StdUnixStream::pair()?;
    let (stdout, guest_stdout) = io::pipe()?;
    let (stderr, guest_stderr) = io::pipe()?;
    let source = [OwnedFd::from(guest), stdout.into(), stderr.into()].map(|fd| {
        use std::os::fd::FromRawFd as _;
        let raw = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10) };
        assert!(raw >= 10);
        unsafe { OwnedFd::from_raw_fd(raw) }
    });
    let mut command = host.command(Path::new("/bin/sh"));
    command
        .args([
            "-c",
            "/bin/sleep 60 </dev/null >/dev/null 2>&1 3<&- 4<&- 5<&- & echo $!; read start; exec \"$@\"",
            "fixture",
        ])
        .arg(binary)
        .args([
            "run",
            "--handover",
            "--from-sha256",
            &"a".repeat(64),
            "--from-version",
            "0.1.0",
            "--release-version",
            "v0.1.0-fixture",
            "--settle-started",
            "2026-11-24T15:00:00Z",
        ])
        .env("PATH", format!("{}:/usr/bin:/bin", host.root.display()))
        .env("XDG_RUNTIME_DIR", &host.root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    unsafe {
        command.pre_exec(move || {
            for (fd, target) in source.iter().zip([3, 4, 5]) {
                if libc::dup2(fd.as_raw_fd(), target) < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut child = tokio::process::Command::from(command).kill_on_drop(true).spawn()?;
    let mut stdout = BufReader::new(child.stdout.take().context("fixture PID output")?);
    let mut pid_line = String::new();
    tokio::time::timeout(TIME, stdout.read_line(&mut pid_line)).await??;
    let box_pid = BoxPid(pid_line.trim().parse().context("running box PID")?);
    let record = handover_record(host, u32::try_from(box_pid.0)?, 3, guest_buffer);
    let _: tumpel_api::types::SupervisorHandover = serde_json::from_value(record.clone())?;
    host.state("handover.json", &record)?;
    child.stdin.take().context("start pipe")?.write_all(b"start\n").await?;
    peer.set_nonblocking(true)?;
    Ok((
        child,
        box_pid,
        UnixStream::from_std(peer)?,
        [guest_stdout, guest_stderr],
    ))
}

#[tokio::test]
async fn exec_keeps_the_pid_and_descriptors_and_reports_online_after_welcome() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("control-plane socket");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target");
    fs::create_dir_all(&target).expect("test fixture parent");
    let target = target.canonicalize().expect("fixture parent path");
    let host = Host::in_dir(&target, "exec", &endpoint).expect("host");
    host.state(
        "runtime.json",
        &json!({
            "dataDir":host.root.join("data"), "imageCacheDir":host.root.join("images"),
            "firecrackerPath":host.root.join("firecracker"), "updateGraceSeconds":0
        }),
    )
    .expect("runtime settings");
    manager(&host).expect("fixture manager");
    let api = firecracker_api(&host).expect("Firecracker API socket");
    let binary = host.root.join("supervisor");
    fs::copy(BINARY, &binary).expect("old supervisor");
    let mut candidate = fs::read(BINARY).expect("candidate binary");
    let old_digest = sha256(&candidate);
    candidate.push(0);
    let new_digest = sha256(&candidate);

    let message: GuestMessage = serde_json::from_value(json!({
        "type":"TurnLine","turnId":"turn-1","lineSeq":1,"text":"after exec"
    }))
    .expect("guest line");
    let encoded = tumpel_guest::encode(&message).expect("guest frame");
    let (prefix, rest) = encoded.split_at(3);
    let (mut child, box_pid, mut peer, _console) = launch_with_parked_turn(&host, &binary, &STANDARD.encode(prefix))
        .await
        .expect("start the old supervisor with a parked turn");
    let pid = child.id().expect("supervisor PID");
    let api = tokio::spawn(box_api(api));
    let scenario = tokio::time::timeout(TIME, async {
        let (stream, _) = listener.accept().await.expect("old channel");
        let mut old = tokio_tungstenite::accept_async(stream).await.expect("old WebSocket");
        assert_eq!(
            hello(&mut old).await.expect("old Hello")["heldTurnIds"],
            json!(["turn-1"])
        );
        welcome(&mut old).await.expect("old Welcome");
        serve_release(&listener, &candidate, &new_digest)
            .await
            .expect("serve the release");
        let (stream, _) = listener.accept().await.expect("successor channel");
        let mut new = tokio_tungstenite::accept_async(stream)
            .await
            .expect("successor WebSocket");
        assert_eq!(
            hello(&mut new).await.expect("new Hello")["heldTurnIds"],
            json!(["turn-1"])
        );
        assert!(tokio::time::timeout(Duration::from_millis(75), new.next())
            .await
            .is_err());
        welcome(&mut new).await.expect("new Welcome");
        let online = next_message(&mut new, |message| {
            message["phase"] == "Online" && message["fromSha256"] == old_digest
        })
        .await
        .expect("successor Online");
        assert_eq!(online["toSha256"], new_digest);
        assert_eq!(online["releaseVersion"], RELEASE);
        assert_eq!(online["boxesRunning"], 1);
        assert_eq!(child.id(), Some(pid));
        assert_eq!(unsafe { libc::kill(box_pid.0, 0) }, 0);
        peer.write_all(rest).await.expect("finish the buffered guest frame");
        let lines = next_message(&mut new, |message| message["type"] == "TurnLines")
            .await
            .expect("resumed turn lines");
        assert_eq!(lines["turnId"], "turn-1");
        assert!(lines.to_string().contains("after exec"));
        let mut unexpected = [0u8; 1024];
        assert!(
            tokio::time::timeout(Duration::from_millis(75), peer.read(&mut unexpected))
                .await
                .is_err()
        );
        assert_eq!(
            unsafe { libc::kill(i32::try_from(pid).expect("PID"), libc::SIGTERM) },
            0
        );
    });
    let result = scenario.await;
    api.abort();
    if result.is_err() {
        child.start_kill().expect("stop stalled supervisor");
    }
    let output = tokio::time::timeout(TIME, child.wait_with_output())
        .await
        .expect("supervisor exits")
        .expect("supervisor logs");
    let log = String::from_utf8(output.stderr).expect("supervisor log");
    assert!(result.is_ok(), "{log}");
    assert!(output.status.success(), "{log}");
    assert_eq!(log.matches("adopted box box-1").count(), 2, "{log}");
    assert!(log.contains("parked turn turn-1"), "{log}");
    assert_eq!(unsafe { libc::kill(box_pid.0, 0) }, 0, "{log}");
    assert!(!host.root.join("state/handover.json").exists());
    assert_eq!(sha256(&fs::read(binary).expect("installed binary")), new_digest);
}

#[tokio::test]
async fn the_successor_crashes_with_the_old_version_and_box_when_a_descriptor_is_closed() {
    let host = Host::new("closed-descriptor", "http://127.0.0.1:9").expect("host");
    let binary = host.root.join("supervisor");
    fs::copy(BINARY, &binary).expect("new binary");
    manager(&host).expect("fixture manager");
    host.state("handover.json", &handover_record(&host, 1, 1_000_000, ""))
        .expect("record with a closed guest descriptor");
    let mut command = tokio::process::Command::from(host.command(&binary));
    command
        .env("PATH", &host.root)
        .env("XDG_RUNTIME_DIR", &host.root)
        .args([
            "run",
            "--handover",
            "--from-sha256",
            &"a".repeat(64),
            "--from-version",
            "0.1.0-old",
            "--release-version",
            RELEASE,
            "--settle-started",
            "2026-11-24T15:00:00Z",
        ]);
    let output = tokio::time::timeout(TIME, command.kill_on_drop(true).output())
        .await
        .expect("successor exits")
        .expect("successor output");
    let log = String::from_utf8(output.stderr).expect("supervisor log");
    assert_eq!(output.status.code(), Some(4), "{log}");
    for detail in [
        "taking the box descriptors",
        "box box-1 session session-1 tap 0",
        "parked turn turn-1",
        "guest fd 1000000",
        "os error 9",
        "0.1.0-old",
        "started 2026-11-24 15:00:00 +00:00",
        RELEASE,
        "Systemd restarts the supervisor",
    ] {
        assert!(log.contains(detail), "{detail} missing: {log}");
    }
}

#[tokio::test]
async fn an_exec_error_exits_with_the_step_versions_digests_and_errno() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("control-plane socket");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let host = Host::new("exec-error", &endpoint).expect("host");
    manager(&host).expect("fixture manager");
    let binary = host.root.join("supervisor");
    fs::copy(BINARY, &binary).expect("old supervisor");
    let old_digest = sha256(&fs::read(&binary).expect("old supervisor bytes"));
    let candidate = format!(
        "#!/bin/sh\nprintf 'exit 0\\n' > \"$0.invalid\"\nchmod a+x \"$0.invalid\"\nmv \"$0.invalid\" \"$0\"\necho 'PASS channel protocol {}'\n",
        ChannelProtocol::VERSION,
    );
    let new_digest = sha256(candidate.as_bytes());
    let mut command = tokio::process::Command::from(host.command(&binary));
    let child = command
        .arg("run")
        .env("PATH", format!("{}:/usr/bin:/bin", host.root.display()))
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("old supervisor");
    let peer = async {
        let (stream, _) = listener.accept().await.expect("old channel");
        let mut old = tokio_tungstenite::accept_async(stream).await.expect("old WebSocket");
        hello(&mut old).await.expect("old Hello");
        welcome(&mut old).await.expect("old Welcome");
        serve_release(&listener, candidate.as_bytes(), &new_digest)
            .await
            .expect("serve the release");
    };
    let ((), output) = tokio::time::timeout(TIME, async { tokio::join!(peer, child.wait_with_output()) })
        .await
        .expect("failed exec exits");
    let output = output.expect("supervisor output");
    let log = String::from_utf8(output.stderr).expect("supervisor log");
    assert_eq!(output.status.code(), Some(4), "{log}");
    for detail in [
        "executing the new supervisor",
        "settle bound (started",
        env!("TUMPEL_RELEASE_VERSION"),
        &old_digest,
        "999.0.0",
        &new_digest,
        "os error 8",
        "Systemd restarts the supervisor",
    ] {
        assert!(log.contains(detail), "{detail} missing: {log}");
    }
    assert_eq!(fs::read(binary).expect("candidate installed"), b"exit 0\n");
    assert!(!host.root.join("supervisor.candidate").exists());
    let record: tumpel_api::types::SupervisorHandover =
        serde_json::from_slice(&fs::read(host.root.join("state/handover.json")).expect("handover written"))
            .expect("unchanged handover record schema");
    assert_eq!(record.boxes, []);
}
