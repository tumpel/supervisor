#![cfg(feature = "host-tests")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tumpel_api::types::{ChannelHostMessage, GuestImageManifest, Sequence, SupervisorRuntime};
use tumpel_supervisor::{
    restored_descriptors, BootOrder, BoxTurn, Budgets, Command, Ext4Upper, Firecracker, HttpStore, ImageOrder, Input,
    Lifecycle, Settings, Taken, TapPool, Upper,
};

#[cfg(test)]
fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME"))
}

#[cfg(test)]
fn turn(image: &GuestImageManifest, turn_id: &str, prompt: &str, last_line_seq: i64) -> Input {
    let expires_at = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    Input::Command(Command::Turn(Box::new(BoxTurn {
        turn_id: turn_id.try_into().expect("turn id"),
        session_id: "host-test-session".try_into().expect("session id"),
        box_id: "host-test-box".try_into().expect("box id"),
        image: BootOrder::Full(ImageOrder {
            download: serde_json::from_value(json!({
                "kind": "Full",
                "imageDigest": *image.image_digest,
                "kernelDigest": *image.kernel_digest,
                "manifestDigest": "0".repeat(64),
                "imageUrl": "https://r2.example.test/image",
                "kernelUrl": "https://r2.example.test/kernel",
                "manifestUrl": "https://r2.example.test/manifest",
                "downloadExpiresAt": expires_at,
                "fetchSeconds": 600
            }))
            .expect("download"),
            harness: image.harness.harness.to_string(),
            harness_version: image.harness.harness_version.to_string(),
        }),
        model: "none".try_into().expect("model"),
        prompt: prompt.to_string().try_into().expect("prompt"),
        credential: serde_json::from_value(json!({
            "credentialHandoverId": "host-test-credential",
            "expiresAt": expires_at,
            "variables": [{ "name": "TUMPEL_PROBE", "value": "in-box-memory" }]
        }))
        .expect("grant"),
        last_line_seq: Sequence(last_line_seq),
        snapshots: Vec::new(),
        persona_files: Vec::new(),
        repository: "https://github.com/octocat/Hello-World.git"
            .try_into()
            .expect("repository"),
        git_author_name: "Tumpel Agent".try_into().expect("author name"),
        git_author_email: "agent@tumpel.invalid".try_into().expect("author email"),
        files_cache: home().join(".cache/tumpel-host-test/persona-files"),
        prebuild_commit_sha: None,
        box_memory_mib: None,
    })))
}

#[cfg(test)]
async fn until_complete(received: &mut UnboundedReceiver<ChannelHostMessage>) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        let message = tokio::time::timeout(Duration::from_mins(1), received.recv())
            .await
            .expect("a message within 60 s")
            .expect("the channel stays open");
        let done = matches!(message, ChannelHostMessage::TurnComplete(_));
        seen.push(match message {
            ChannelHostMessage::TurnClaim(claim) => format!("claim {}", *claim.turn_id),
            ChannelHostMessage::TurnRepositoryReady(ready) => format!("ready {}", *ready.turn_id),
            ChannelHostMessage::TurnLine(line) => format!("line {} {}", line.line_seq.0, line.text),
            ChannelHostMessage::TurnComplete(done) => format!("complete {}", done.last_line_seq.0),
            other => panic!("unexpected {other:?}"),
        });
        if done {
            return seen;
        }
    }
}

#[tokio::test]
async fn a_shell_box_runs_a_turn_pauses_resumes_warm_and_its_drive_reads_back() {
    if std::env::var("TUMPEL_KVM").as_deref() != Ok("1") {
        eprintln!(
            "skipped: set TUMPEL_KVM=1 and TUMPEL_IMAGE_MANIFEST on a host with /dev/kvm and the TAP device tumpel0"
        );
        return;
    }
    let manifest_path = PathBuf::from(
        std::env::var("TUMPEL_IMAGE_MANIFEST").expect("TUMPEL_IMAGE_MANIFEST names a Shell image manifest"),
    );
    let image: GuestImageManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("manifest"))
        .expect("a GuestImageManifest");
    let data = home().join("development").join(".tmp").join("tumpel-host-test");
    if data.exists() {
        std::fs::remove_dir_all(&data).expect("clearing the last run");
    }
    std::fs::create_dir_all(&data).expect("the data directory");
    let _systemd = std::os::unix::net::UnixDatagram::bind(data.join("notify.sock")).expect("a stand-in for systemd");
    std::env::set_var("NOTIFY_SOCKET", data.join("notify.sock"));
    let runtime: SupervisorRuntime = serde_json::from_value(json!({
        "firecrackerPath": std::env::var("TUMPEL_FIRECRACKER").map_or_else(|_| home().join(".local/bin/firecracker"), PathBuf::from),
        "imageCacheDir": manifest_path.parent().expect("cache dir"),
        "dataDir": data,
        "boxCapacity": 1,
        "warmWindowSeconds": 900,
        "vcpuCount": 1,
        "memoryMib": 512,
        "overlayMib": 1024,
        "layerCacheMib": 1024,
        "budgets": { "bootSeconds": 30, "firstLineSeconds": 10, "turnSeconds": 30, "snapshotSeconds": 30 },
        "heartbeatIntervalSeconds": 10,
        "updateGraceSeconds": 600
    }))
    .expect("runtime");
    let settings = Settings {
        budgets: Budgets::from(&runtime.budgets),
        warm_window: Duration::from_mins(15),
        layer_cache_mib: runtime.layer_cache_mib,
        overlay_bytes: u64::try_from(runtime.overlay_mib).unwrap_or(0) * 1024 * 1024,
    };
    let (out, mut received) = unbounded_channel();
    let lifecycle = Lifecycle::new(
        Firecracker::new(&runtime),
        HttpStore::new().expect("store"),
        settings,
        TapPool::new(4096, std::num::NonZeroU32::new(4096).expect("4096")),
        out,
        data.join("boxes.json"),
    );
    let (commands, from_test) = unbounded_channel();
    let running = tokio::spawn(lifecycle.run(from_test, unbounded_channel().1));

    let started = Instant::now();
    commands
        .send(turn(
            &image,
            "turn-1",
            "echo one\necho probe > /root/probe\necho \"$TUMPEL_PROBE\"",
            0,
        ))
        .expect("send");
    assert_eq!(
        until_complete(&mut received).await,
        [
            "claim turn-1",
            "ready turn-1",
            "line 1 one",
            "line 2 in-box-memory",
            "complete 2"
        ]
    );
    let cold = started.elapsed();

    tokio::time::sleep(Duration::from_millis(200)).await;
    let drive = data.join("sessions").join("host-test-session").join("overlay.ext4");
    let upper = Ext4Upper::load(&drive).expect("the paused box's drive reads as ext4");
    assert!(upper.scan().expect("scan").contains_key(b"root/probe".as_slice()));

    let started = Instant::now();
    commands.send(turn(&image, "turn-2", "echo four", 2)).expect("send");
    assert_eq!(
        until_complete(&mut received).await,
        ["claim turn-2", "ready turn-2", "line 3 four", "complete 3"]
    );
    let warm = started.elapsed();

    println!(
        "cold boot + turn {} ms, warm resume + turn {} ms",
        cold.as_millis(),
        warm.as_millis()
    );
    drop(commands);
    running.await.expect("the lifecycle stops");
    std::fs::remove_dir_all(&data).expect("removing the test data");
}

#[cfg(test)]
fn stored(socket: &std::os::unix::net::UnixDatagram) -> std::collections::BTreeMap<String, std::os::fd::OwnedFd> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    socket.set_nonblocking(true).expect("non-blocking");
    let mut store = std::collections::BTreeMap::new();
    loop {
        let mut data = [0u8; 256];
        let mut control = [0u8; 64];
        let mut iov = libc::iovec {
            iov_base: data.as_mut_ptr().cast(),
            iov_len: data.len(),
        };
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &raw mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = control.len();
        let Ok(read) = usize::try_from(unsafe { libc::recvmsg(socket.as_raw_fd(), &raw mut message, 0) }) else {
            return store;
        };
        let state = String::from_utf8_lossy(&data[..read]).into_owned();
        let name = state
            .lines()
            .find_map(|line| line.strip_prefix("FDNAME="))
            .expect("a named descriptor")
            .to_string();
        let header = unsafe { libc::CMSG_FIRSTHDR(&raw const message) };
        if state.starts_with("FDSTOREREMOVE=1") || header.is_null() {
            store.remove(&name);
            continue;
        }
        let mut raw = [0u8; 4];
        unsafe { std::ptr::copy_nonoverlapping(libc::CMSG_DATA(header), raw.as_mut_ptr(), 4) };
        store.insert(name, unsafe { OwnedFd::from_raw_fd(i32::from_ne_bytes(raw)) });
    }
}

#[cfg(test)]
fn crash_runtime(data: &std::path::Path, manifest_path: &std::path::Path) -> (SupervisorRuntime, Settings) {
    let runtime: SupervisorRuntime = serde_json::from_value(json!({
        "firecrackerPath": std::env::var("TUMPEL_FIRECRACKER").map_or_else(|_| home().join(".local/bin/firecracker"), PathBuf::from),
        "imageCacheDir": manifest_path.parent().expect("cache dir"),
        "dataDir": data,
        "boxCapacity": 1,
        "warmWindowSeconds": 900,
        "vcpuCount": 1,
        "memoryMib": 512,
        "overlayMib": 1024,
        "layerCacheMib": 1024,
        "budgets": { "bootSeconds": 30, "firstLineSeconds": 90, "turnSeconds": 120, "snapshotSeconds": 30 },
        "heartbeatIntervalSeconds": 10,
        "updateGraceSeconds": 600
    }))
    .expect("runtime");
    let settings = Settings {
        budgets: Budgets::from(&runtime.budgets),
        warm_window: Duration::from_mins(15),
        layer_cache_mib: runtime.layer_cache_mib,
        overlay_bytes: u64::try_from(runtime.overlay_mib).unwrap_or(0) * 1024 * 1024,
    };
    (runtime, settings)
}

#[cfg(test)]
fn crash_paths() -> (PathBuf, PathBuf) {
    let manifest_path = PathBuf::from(
        std::env::var("TUMPEL_IMAGE_MANIFEST").expect("TUMPEL_IMAGE_MANIFEST names a Shell image manifest"),
    );
    (
        home().join("development").join(".tmp").join("tumpel-host-crash-test"),
        manifest_path,
    )
}

#[tokio::test]
#[ignore = "the crashing supervisor of the crash host test; that test starts it and kills it mid-turn"]
async fn crashing_supervisor() {
    let (data, manifest_path) = crash_paths();
    let image: GuestImageManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("manifest"))
        .expect("a GuestImageManifest");
    let (runtime, settings) = crash_runtime(&data, &manifest_path);
    let (out, mut received) = unbounded_channel();
    let mut lifecycle = Lifecycle::new(
        Firecracker::new(&runtime),
        HttpStore::new().expect("store"),
        settings,
        TapPool::new(4096, std::num::NonZeroU32::new(4096).expect("4096")),
        out,
        data.join("boxes.json"),
    );
    lifecycle.start(None).expect("the crashing supervisor starts");
    let (commands, from_test) = unbounded_channel();
    let _running = tokio::spawn(lifecycle.run(from_test, unbounded_channel().1));
    commands
        .send(turn(&image, "turn-1", "sleep 60\necho too late", 0))
        .expect("send");
    while let Some(message) = received.recv().await {
        if matches!(message, ChannelHostMessage::TurnRepositoryReady(_)) {
            println!("TURN RUNNING");
            std::future::pending::<()>().await;
        }
    }
}

#[tokio::test]
async fn a_supervisor_started_after_a_crash_takes_over_a_box_mid_turn_and_runs_its_next_turn() {
    if std::env::var("TUMPEL_KVM").as_deref() != Ok("1") {
        eprintln!(
            "skipped: set TUMPEL_KVM=1 and TUMPEL_IMAGE_MANIFEST on a host with /dev/kvm, a systemd user manager and the TAP device tumpel0"
        );
        return;
    }
    let (data, manifest_path) = crash_paths();
    let image: GuestImageManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path).expect("manifest"))
        .expect("a GuestImageManifest");
    if data.exists() {
        std::fs::remove_dir_all(&data).expect("clearing the last run");
    }
    std::fs::create_dir_all(&data).expect("the data directory");
    let systemd = std::os::unix::net::UnixDatagram::bind(data.join("notify.sock")).expect("a stand-in for systemd");
    let mut crashing = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args(["--exact", "crashing_supervisor", "--ignored", "--nocapture"])
        .env("NOTIFY_SOCKET", data.join("notify.sock"))
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("the crashing supervisor starts");
    let output = crashing.stdout.take().expect("its output");
    let running = std::io::BufRead::lines(std::io::BufReader::new(output))
        .map_while(Result::ok)
        .any(|line| line == "TURN RUNNING");
    assert!(running, "the crashing supervisor reaches its sleeping turn");
    crashing.kill().expect("the supervisor crashes");
    crashing.wait().expect("the crashed supervisor is reaped");

    let standing = data.join("boxes.json");
    let record: tumpel_api::types::SupervisorHandover =
        serde_json::from_str(&std::fs::read_to_string(&standing).expect("boxes.json")).expect("a SupervisorHandover");
    let pid = record.boxes[0].machine.process_id;
    let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).expect("the box outlives its supervisor");
    assert!(
        cgroup.lines().any(|line| line.ends_with("/tumpel-box0.scope")),
        "the box runs in its own scope: {cgroup}"
    );

    std::env::set_var("NOTIFY_SOCKET", data.join("notify.sock"));
    let mut descriptors = stored(&systemd);
    let record = restored_descriptors(record, &mut descriptors);
    let (runtime, settings) = crash_runtime(&data, &manifest_path);
    let (out, mut received) = unbounded_channel();
    let mut taker = Lifecycle::new(
        Firecracker::new(&runtime),
        HttpStore::new().expect("store"),
        settings,
        TapPool::new(4096, std::num::NonZeroU32::new(4096).expect("4096")),
        out,
        standing.clone(),
    );
    taker
        .start(Some((record, Taken::Crashed)))
        .expect("the next supervisor takes the box over");
    let failed = received.recv().await.expect("the cut turn is reported");
    assert!(
        matches!(&failed, ChannelHostMessage::TurnFailed(failed) if *failed.turn_id == "turn-1"),
        "{failed:?}"
    );
    let (commands, from_test) = unbounded_channel();
    let running = tokio::spawn(taker.run(from_test, unbounded_channel().1));
    let started = Instant::now();
    commands.send(turn(&image, "turn-2", "echo two", 0)).expect("send");
    let seen = until_complete(&mut received).await;
    assert_eq!(seen.first().map(String::as_str), Some("claim turn-2"));
    assert!(seen.iter().any(|line| line.ends_with(" two")), "{seen:?}");
    assert!(
        started.elapsed() < Duration::from_secs(25),
        "the next turn did not wait for the cut turn's sleep"
    );
    let after: tumpel_api::types::SupervisorHandover =
        serde_json::from_str(&std::fs::read_to_string(&standing).expect("boxes.json")).expect("a SupervisorHandover");
    assert_eq!(
        after.boxes[0].machine.process_id, pid,
        "the next turn ran on the same Firecracker"
    );
    drop(commands);
    running.await.expect("the lifecycle stops");
    std::fs::remove_dir_all(&data).expect("removing the test data");
}

#[cfg(test)]
const RESTART_UNIT: &str = "tumpel-restart-test.service";

#[cfg(test)]
fn restart_unit_path() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map_or_else(|| home().join(".config"), PathBuf::from)
        .join("systemd")
        .join("user")
        .join(RESTART_UNIT)
}

#[cfg(test)]
fn systemctl(args: &[&str]) {
    let status = std::process::Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .expect("systemctl starts");
    assert!(
        status.success(),
        "systemctl --user {} exited with {status}",
        args.join(" ")
    );
}

#[cfg(test)]
fn written(path: &std::path::Path) -> String {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if let Ok(text) = std::fs::read_to_string(path) {
            return text;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "the stand-in supervisor wrote no {} within 30 s; see journalctl --user -u {RESTART_UNIT}",
        path.display()
    );
}

#[tokio::test]
#[ignore = "the supervisor that a release older than the descriptor store exec'd into, for the restart host test; that test runs it as its systemd user unit"]
async fn exec_handed_supervisor() {
    use std::os::fd::{IntoRawFd, OwnedFd};
    use tumpel_supervisor::{received_descriptors, take_over_unit, Layout, Vmm};
    let data = PathBuf::from(std::env::var("TUMPEL_RESTART_TEST").expect("the restart test names its data directory"));
    let received = unsafe { received_descriptors() }.expect("the descriptors systemd passed");
    if !received.is_empty() {
        let names: Vec<&str> = received.keys().map(String::as_str).collect();
        std::fs::write(data.join("received"), names.join(" ")).expect("writing the received names");
        std::future::pending::<()>().await;
    }
    let (guest, peer) = std::os::unix::net::UnixStream::pair().expect("a guest connection");
    #[expect(
        clippy::zombie_processes,
        reason = "the box outlives this stand-in supervisor, and the restart test ends it through its scope"
    )]
    let mut child = std::process::Command::new("sleep")
        .arg("infinity")
        .stdin(OwnedFd::from(peer))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the box an old release started as its child");
    let stdout = OwnedFd::from(child.stdout.take().expect("its standard output"));
    let stderr = OwnedFd::from(child.stderr.take().expect("its standard error"));
    let handed = tumpel_api::types::SupervisorHandoverMachine {
        api_socket: data.join("box63").join("api.sock").display().to_string(),
        console_tail: String::new(),
        guest_buffer: String::new(),
        guest_fd: guest.into_raw_fd(),
        process_id: std::num::NonZeroU32::new(child.id()).expect("a pid"),
        stderr_fd: stderr.into_raw_fd(),
        stdout_fd: stdout.into_raw_fd(),
        vsock: data.join("box63").join("v.sock").display().to_string(),
    };
    let layout = Layout::new(data.join("tumpel-supervisor"), data.clone(), restart_unit_path());
    if let Err(failure) = take_over_unit(&layout).await {
        panic!("{}: {}", failure.kind, failure.detail);
    }
    let runtime: SupervisorRuntime = serde_json::from_value(json!({
        "firecrackerPath": "firecracker",
        "imageCacheDir": data,
        "dataDir": data,
        "boxCapacity": 1,
        "warmWindowSeconds": 900,
        "vcpuCount": 1,
        "memoryMib": 512,
        "overlayMib": 1024,
        "layerCacheMib": 1024,
        "budgets": { "bootSeconds": 30, "firstLineSeconds": 90, "turnSeconds": 120, "snapshotSeconds": 30 },
        "heartbeatIntervalSeconds": 10,
        "updateGraceSeconds": 600
    }))
    .expect("runtime");
    let one = std::num::NonZeroU32::new(512).expect("512");
    let tap = TapPool::new(512, one).claim_device(63, one).expect("tap 63");
    let session = "restart-test-session".try_into().expect("session id");
    let _box = Firecracker::new(&runtime)
        .reattach(&session, &tap, &handed)
        .expect("the supervisor takes the box over");
    std::fs::write(data.join("box"), child.id().to_string()).expect("writing the box pid");
    std::future::pending::<()>().await;
}

#[test]
fn a_restart_keeps_a_box_that_a_release_older_than_the_descriptor_store_handed_over_by_exec() {
    if std::env::var("TUMPEL_SYSTEMD_USER").as_deref() != Ok("1") {
        eprintln!("skipped: set TUMPEL_SYSTEMD_USER=1 on a host with a systemd user manager");
        return;
    }
    let data = home().join("development").join(".tmp").join("tumpel-host-restart-test");
    if data.exists() {
        std::fs::remove_dir_all(&data).expect("clearing the last run");
    }
    std::fs::create_dir_all(&data).expect("the data directory");
    let binary = data.join("tumpel-supervisor");
    std::fs::write(
        &binary,
        format!(
            "#!/bin/sh\nTUMPEL_RESTART_TEST={} exec {} --exact exec_handed_supervisor --ignored --nocapture\n",
            data.display(),
            std::env::current_exe().expect("this test binary").display()
        ),
    )
    .expect("the stand-in binary");
    std::fs::set_permissions(&binary, std::os::unix::fs::PermissionsExt::from_mode(0o755)).expect("executable");
    let unit = restart_unit_path();
    std::fs::create_dir_all(unit.parent().expect("the unit directory")).expect("the unit directory");
    std::fs::write(&unit, format!("[Service]\nExecStart={} run\n", binary.display())).expect("the old unit");
    systemctl(&["daemon-reload"]);
    systemctl(&["start", RESTART_UNIT]);
    let pid = written(&data.join("box"));

    systemctl(&["restart", RESTART_UNIT]);
    let received = written(&data.join("received"));
    let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup"));

    systemctl(&["stop", RESTART_UNIT, "tumpel-box63.scope"]);
    std::fs::remove_file(&unit).expect("removing the unit");
    systemctl(&["daemon-reload"]);
    std::fs::remove_dir_all(&data).expect("removing the test data");
    assert_eq!(received, "box63-guest box63-stderr box63-stdout");
    let cgroup = cgroup.expect("the box outlives the restart");
    assert!(
        cgroup.lines().any(|line| line.ends_with("/tumpel-box63.scope")),
        "the box runs in its own scope: {cgroup}"
    );
}
