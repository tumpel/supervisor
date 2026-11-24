use std::collections::BTreeMap;
use std::fs;
use std::io::{self, ErrorKind};
use std::os::unix::{fs::PermissionsExt, process::CommandExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;
use tumpel_api::types::{ChannelProtocol, GuestProtocol, SupervisorCheckReport};

const BINARY: &str = env!("CARGO_BIN_EXE_tumpel-supervisor");
const CHECKS: [&str; 6] = [
    "startup",
    "boxes.json",
    "handover.json",
    "guest drive",
    "image cache",
    "Firecracker",
];

struct Host {
    root: PathBuf,
}

impl Host {
    fn new(name: &str, endpoint: &str) -> io::Result<Self> {
        let root = std::env::temp_dir().join(format!("tumpel-report-{}-{name}", std::process::id()));
        fs::create_dir(&root)?;
        let host = Self { root };
        for dir in ["state", "data", "images"] {
            fs::create_dir(host.root.join(dir))?;
        }
        host.state(
            "supervisor.json",
            &json!({"endpoint": endpoint, "hostId":"host-1", "tenantId":"tenant-1"}),
        )?;
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map_err(|error| io::Error::other(format!("generate fixture key: {error}")))?;
        let key = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())
            .map_err(|error| io::Error::other(format!("read fixture key: {error}")))?;
        host.state(
            "host-key.json",
            &json!({
                "privateKey": STANDARD.encode(pkcs8.as_ref()),
                "publicKey": STANDARD.encode(key.public_key().as_ref()),
            }),
        )?;
        host.state("channel.json", &json!({"received":0, "sent":0, "unacked":[]}))?;
        for name in ["boxes.json", "handover.json"] {
            host.state(name, &json!({"handedAt":"2026-11-16T09:30:00Z", "boxes":[]}))?;
        }
        let firecracker = host.root.join("firecracker");
        fs::write(&firecracker, b"#!/bin/sh\nexit 1\n")?;
        fs::set_permissions(&firecracker, fs::Permissions::from_mode(0o755))?;
        host.state(
            "runtime.json",
            &json!({
                "dataDir": host.root.join("data"),
                "imageCacheDir": host.root.join("images"),
                "firecrackerPath": firecracker,
            }),
        )?;
        Ok(host)
    }

    fn state(&self, name: &str, value: &Value) -> io::Result<()> {
        fs::write(self.root.join("state").join(name), value.to_string())
    }

    fn command(&self, binary: &Path) -> Command {
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("TUMPEL_SUPERVISOR_DIR", self.root.join("state"))
            .env("XDG_CONFIG_HOME", self.root.join("config"));
        command
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        let removed = fs::remove_dir_all(&self.root);
        assert!(removed.is_ok(), "remove the test host: {removed:?}");
    }
}

fn contents(path: &Path) -> io::Result<BTreeMap<PathBuf, Vec<u8>>> {
    let mut entries = BTreeMap::new();
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            entries.extend(contents(&path)?);
            entries.insert(path, Vec::new());
        } else {
            let bytes = fs::read(&path)?;
            entries.insert(path, bytes);
        }
    }
    Ok(entries)
}

#[test]
fn a_healthy_bare_report_passes_without_writing_or_connecting() {
    let control_plane = std::net::TcpListener::bind("127.0.0.1:0").expect("control-plane boundary");
    control_plane.set_nonblocking(true).expect("nonblocking listener");
    let host = Host::new(
        "healthy",
        &format!("http://{}", control_plane.local_addr().expect("listener address")),
    )
    .expect("healthy test host");
    fs::write(host.root.join("images/broken.json"), b"{").expect("unfinished cache manifest");
    fs::write(host.root.join("images/broken.ext4"), b"partial image").expect("unfinished cache image");
    let before = contents(&host.root).expect("read initial state");
    let output = host.command(Path::new(BINARY)).output().expect("report exits");
    let report = String::from_utf8(output.stdout).expect("report text");
    assert!(output.status.success(), "{report}");
    assert_eq!(report.lines().count(), CHECKS.len() + 1, "{report}");
    for check in CHECKS {
        assert!(report.contains(&format!("PASS {check}: ")), "{report}");
    }
    assert!(report.ends_with(&format!("PASS channel protocol {}\n", ChannelProtocol::VERSION)));
    assert_eq!(contents(&host.root).expect("read state after report"), before);
    assert_eq!(
        control_plane.accept().expect_err("no connection").kind(),
        ErrorKind::WouldBlock
    );
}

#[test]
fn a_failed_local_check_still_reports_the_other_checks() {
    for (file, failed) in [
        ("state/boxes.json", "boxes.json"),
        ("state/handover.json", "handover.json"),
        ("images", "image cache"),
        ("firecracker", "Firecracker"),
    ] {
        let host = Host::new("broken", "http://127.0.0.1:9").expect("test host");
        match file {
            "firecracker" => fs::remove_file(host.root.join(file)).expect("remove Firecracker"),
            "images" => fs::remove_dir(host.root.join(file)).expect("remove image cache"),
            _ => fs::write(host.root.join(file), b"{").expect("corrupt JSON"),
        }
        let before = contents(&host.root).expect("read initial state");
        let output = host.command(Path::new(BINARY)).output().expect("report exits");
        let report = String::from_utf8(output.stdout).expect("report text");
        assert_eq!(output.status.code(), Some(1), "{report}");
        for check in CHECKS {
            let status = if check == failed || (cfg!(guest_drive) && file == "images" && check == "guest drive") {
                "FAIL"
            } else {
                "PASS"
            };
            assert!(report.contains(&format!("{status} {check}: ")), "{report}");
        }
        assert!(report.ends_with(&format!("PASS channel protocol {}\n", ChannelProtocol::VERSION)));
        assert_eq!(contents(&host.root).expect("read state after report"), before);
    }
}

#[test]
fn a_readable_but_unwritable_image_cache_fails_the_report() {
    let host = Host::new("readonly-cache", "http://127.0.0.1:9").expect("test host");
    let binary = host.root.join("supervisor");
    fs::copy(BINARY, &binary).expect("fixture binary");
    let images = host.root.join("images");
    fs::set_permissions(&images, fs::Permissions::from_mode(0o555)).expect("make the image cache read-only");
    let before = contents(&host.root).expect("read initial state");
    let mut candidate = host.command(&binary);
    if unsafe { libc::geteuid() } == 0 {
        candidate.uid(65534);
    }

    let output = candidate.output().expect("report exits");
    let report = String::from_utf8(output.stdout).expect("report text");

    assert_eq!(output.status.code(), Some(1), "{report}");
    assert!(report.contains("PASS startup: "), "{report}");
    assert!(report.contains("FAIL image cache: "), "{report}");
    assert!(report.contains("Permission denied"), "{report}");
    assert_eq!(contents(&host.root).expect("read state after report"), before);
    fs::set_permissions(images, fs::Permissions::from_mode(0o755)).expect("allow fixture cleanup");
}

#[tokio::test]
async fn explicit_check_still_dials_for_a_challenge_and_prints_a_supervisor_check_report() {
    let control_plane = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("control-plane boundary");
    let host = Host::new(
        "check",
        &format!("http://{}", control_plane.local_addr().expect("listener address")),
    )
    .expect("test host");
    let server = async {
        let (stream, _) = control_plane.accept().await.expect("connection");
        let mut socket = tokio_tungstenite::accept_async(stream).await.expect("websocket");
        socket
            .send(Message::text(
                r#"{"type":"Challenge","nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}"#,
            ))
            .await
            .expect("challenge");
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    };
    let mut check = tokio::process::Command::from(host.command(Path::new(BINARY)));
    check.arg("check").kill_on_drop(true);
    let ((), output) = tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(server, check.output()) })
        .await
        .expect("check completes after the challenge");
    let output = output.expect("check exits");
    assert!(output.status.success(), "{output:?}");
    let report: SupervisorCheckReport = serde_json::from_slice(&output.stdout).expect("legacy check report");
    assert_eq!(report.supervisor_version.as_str(), env!("TUMPEL_RELEASE_VERSION"));
    assert_eq!(
        report.channel_protocol_version.get(),
        u32::from(ChannelProtocol::VERSION)
    );
    assert_eq!(report.guest_protocol_version.get(), u32::from(GuestProtocol::VERSION));
}

#[tokio::test]
async fn startup_io_failures_exit_for_systemd_restart_before_consuming_a_handover() {
    for (case, detail) in [
        ("candidate", "removing"),
        ("unit", "writing"),
        ("reload", "systemctl --user daemon-reload exited with exit status: 23"),
    ] {
        let host = Host::new(case, "http://127.0.0.1:9").expect("test host");
        let binary = host.root.join("supervisor");
        fs::copy(BINARY, &binary).expect("fixture binary");
        let unit_dir = host.root.join("config/systemd/user");
        fs::create_dir_all(&unit_dir).expect("fixture unit directory");
        fs::write(
            unit_dir.join("tumpel-supervisor.service"),
            "[Service]\nExecStartPre=/bin/true\n",
        )
        .expect("legacy unit");
        let systemctl = host.root.join("systemctl");
        fs::write(
            &systemctl,
            "#!/bin/sh\n[ \"$*\" = '--user daemon-reload' ] || exit 24\nexit 23\n",
        )
        .expect("fixture user manager");
        fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).expect("fixture user manager is executable");
        match case {
            "candidate" => fs::create_dir(host.root.join("supervisor.candidate")).expect("obstruct candidate cleanup"),
            "unit" => fs::create_dir(unit_dir.join("tumpel-supervisor.service.part")).expect("obstruct unit write"),
            _ => {}
        }
        let handover = host.root.join("state/handover.json");
        let before = fs::read(&handover).expect("handover record");
        let mut command = tokio::process::Command::from(host.command(&binary));
        command
            .args(["run", "--handover"])
            .env("PATH", &host.root)
            .env("XDG_RUNTIME_DIR", &host.root)
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(5), command.output())
            .await
            .expect("startup exits")
            .expect("supervisor output");
        let log = String::from_utf8(output.stderr).expect("supervisor log");
        assert_eq!(output.status.code(), Some(1), "{case}: {log}");
        assert!(log.contains(detail), "{case}: {log}");
        assert!(log.contains("systemd restarts the supervisor"), "{case}: {log}");
        assert_eq!(fs::read(handover).expect("handover remains for restart"), before);
    }
}
