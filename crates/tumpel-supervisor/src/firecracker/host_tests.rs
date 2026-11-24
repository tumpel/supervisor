use std::num::NonZeroU32;
use std::os::fd::{IntoRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tumpel_api::types::SupervisorHandoverMachine;

use super::{Firecracker, Process};
use crate::lifecycle::Vmm;
use crate::pool::{Size, TapPool};
use crate::update::Layout;
use crate::{fd_store, run};

const UNIT: &str = "tumpel-restart-test.service";

#[cfg(test)]
fn data() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME"))
        .join("development")
        .join(".tmp")
        .join("tumpel-host-restart-test")
}

#[cfg(test)]
fn unit_path() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map_or_else(
            || PathBuf::from(std::env::var("HOME").expect("HOME")).join(".config"),
            PathBuf::from,
        )
        .join("systemd")
        .join("user")
        .join(UNIT)
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
fn written(path: &Path) -> String {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if let Ok(text) = std::fs::read_to_string(path) {
            return text;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "the stand-in supervisor wrote no {} within 30 s; see journalctl --user -u {UNIT}",
        path.display()
    );
}

#[tokio::test]
#[ignore = "the supervisor that a release older than the descriptor store exec'd into, for the restart host test; that test runs it as its systemd user unit"]
async fn exec_handed_supervisor() {
    let data = data();
    let received = unsafe { fd_store::received() }.expect("the descriptors systemd passed");
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
    let handed = SupervisorHandoverMachine {
        api_socket: data
            .join("box63")
            .join("api.sock")
            .display()
            .to_string()
            .try_into()
            .expect("a path"),
        console_tail: String::new().try_into().expect("an empty tail"),
        guest_buffer: String::new().try_into().expect("an empty buffer"),
        guest_fd: guest.into_raw_fd(),
        process_id: NonZeroU32::new(child.id()).expect("a pid"),
        stderr_fd: stderr.into_raw_fd(),
        stdout_fd: stdout.into_raw_fd(),
        vsock: data
            .join("box63")
            .join("v.sock")
            .display()
            .to_string()
            .try_into()
            .expect("a path"),
    };
    unsafe { fd_store::take_notify_socket(true) }.expect("the user manager's notify socket");
    let layout = Layout::new(data.join("tumpel-supervisor"), data.clone(), unit_path());
    if let Err(failure) = run::refresh_unit(&layout, "the restart test fails").await {
        panic!("{}: {}", failure.kind, failure.detail);
    }
    let vmm = Firecracker {
        binary: data.join("firecracker"),
        images: data.clone(),
        data: data.clone(),
        vcpus: 1,
    };
    let mib = Size {
        memory_mib: NonZeroU32::new(512).expect("512"),
        disk_mib: NonZeroU32::new(64).expect("64"),
    };
    let mut pool = TapPool::new(1024, mib);
    let old_tap = pool.claim_device(63, mib).expect("tap 63");
    let old_session = "restart-test-old".try_into().expect("session id");
    let _old = vmm
        .reattach(&old_session, &old_tap, &handed)
        .expect("the supervisor takes the old box over");
    let new_tap = pool.claim_device(62, mib).expect("tap 62");
    let new_session = "restart-test-new".try_into().expect("session id");
    let mut new = vmm.machine(&new_session, &new_tap).expect("a new box");
    let api = new.api_socket();
    let started = data.join("firecracker-started");
    let (spawned, listener) = tokio::join!(new.spawn(), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        std::os::unix::net::UnixListener::bind(&api)
    });
    let _listener = listener.expect("the stand-in API socket");
    spawned.expect("the new box starts");
    let new_pid = new.process.as_ref().and_then(Process::id).expect("the new box's pid");
    std::fs::write(data.join("box"), format!("{} {new_pid}", child.id())).expect("writing the box pids");
    std::future::pending::<()>().await;
}

#[test]
fn a_restart_keeps_the_boxes_an_exec_handed_over_and_started_and_no_box_inherits_the_notify_socket() {
    let data = data();
    if data.exists() {
        std::fs::remove_dir_all(&data).expect("clearing the last run");
    }
    std::fs::create_dir_all(&data).expect("the data directory");
    let mode = <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755);
    let binary = data.join("tumpel-supervisor");
    std::fs::write(
        &binary,
        format!(
            "#!/bin/sh\nexec {} --exact firecracker::host_tests::exec_handed_supervisor --ignored --nocapture\n",
            std::env::current_exe().expect("this test binary").display()
        ),
    )
    .expect("the stand-in supervisor");
    std::fs::set_permissions(&binary, mode.clone()).expect("executable");
    let firecracker = data.join("firecracker");
    std::fs::write(
        &firecracker,
        format!(
            "#!/bin/sh\nprintf '%s' \"${{NOTIFY_SOCKET-unset}}\" > {data}/notify-socket-of-box\ntouch {data}/firecracker-started\nexec sleep infinity\n",
            data = data.display()
        ),
    )
    .expect("the stand-in Firecracker");
    std::fs::set_permissions(&firecracker, mode).expect("executable");
    let unit = unit_path();
    std::fs::create_dir_all(unit.parent().expect("the unit directory")).expect("the unit directory");
    std::fs::write(&unit, format!("[Service]\nExecStart={} run\n", binary.display())).expect("the old unit");
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_secs();
    systemctl(&["daemon-reload"]);
    systemctl(&["start", UNIT]);
    let pids = written(&data.join("box"));

    systemctl(&["restart", UNIT]);
    let received = written(&data.join("received"));
    let dropped = std::process::Command::new("journalctl")
        .args([
            "--user",
            "--no-pager",
            "--quiet",
            "--output=cat",
            "--grep=reception only permitted",
        ])
        .arg(format!("--user-unit={UNIT}"))
        .arg(format!("--since=@{since}"))
        .output()
        .expect("journalctl starts");
    let notify_socket = std::fs::read_to_string(data.join("notify-socket-of-box")).expect("the box's notify socket");
    let cgroups: Vec<String> = pids
        .split(' ')
        .map(|pid| std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap_or_default())
        .collect();

    systemctl(&["stop", UNIT, "tumpel-box62.scope", "tumpel-box63.scope"]);
    std::fs::remove_file(&unit).expect("removing the unit");
    systemctl(&["daemon-reload"]);
    std::fs::remove_dir_all(&data).expect("removing the test data");
    assert_eq!(notify_socket, "unset");
    assert_eq!(
        String::from_utf8_lossy(&dropped.stdout),
        "",
        "no child process notifies systemd for the unit"
    );
    assert_eq!(
        received,
        "box62-stderr box62-stdout box63-guest box63-stderr box63-stdout"
    );
    assert!(
        cgroups[0].lines().any(|line| line.ends_with("/tumpel-box63.scope")),
        "the handed box outlives the restart in its own scope: {}",
        cgroups[0]
    );
    assert!(
        cgroups[1].lines().any(|line| line.ends_with("/tumpel-box62.scope")),
        "the new box outlives the restart in its own scope: {}",
        cgroups[1]
    );
}
