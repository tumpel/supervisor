use super::{hello, http, next_frame, sha256, welcome, Host, BINARY};
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use tokio::net::TcpListener;
use tumpel_api::types::ChannelProtocol;

const UNIT: &str = "tumpel-supervisor.service";

fn systemctl(args: &[&str]) -> String {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .expect("systemctl starts");
    assert!(
        output.status.success(),
        "systemctl --user {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("systemctl output")
        .trim()
        .into()
}

fn property(name: &str) -> String {
    systemctl(&["show", "--value", "--property", name, UNIT])
}

struct TestUnit {
    file: PathBuf,
    drop_in: PathBuf,
}

impl TestUnit {
    fn install(host: &Host, binary: &Path) -> Self {
        assert_eq!(
            property("LoadState"),
            "not-found",
            "use a dedicated systemd user manager"
        );
        assert!(
            systemctl(&["list-units", "--no-legend", "--all", "tumpel-box*.scope"]).is_empty(),
            "use a systemd user manager with no boxes"
        );
        let config = std::env::var_os("XDG_CONFIG_HOME").map_or_else(
            || PathBuf::from(std::env::var_os("HOME").expect("home directory")).join(".config"),
            PathBuf::from,
        );
        let dir = config.join("systemd/user");
        fs::create_dir_all(&dir).expect("user unit directory");
        let unit = Self {
            file: dir.join(UNIT),
            drop_in: dir.join(format!("{UNIT}.d")),
        };
        fs::File::create_new(&unit.file)
            .expect("unused test unit")
            .write_all(format!("[Service]\nExecStart={} run\n", binary.display()).as_bytes())
            .expect("initial unit");
        fs::create_dir(&unit.drop_in).expect("unused test unit drop-ins");
        fs::write(
            unit.drop_in.join("environment.conf"),
            format!(
                "[Service]\nEnvironment=\"TUMPEL_SUPERVISOR_DIR={}/state\" \"XDG_CONFIG_HOME={}\"\n",
                host.root.display(),
                config.display()
            ),
        )
        .expect("fixture environment");
        systemctl(&["daemon-reload"]);
        systemctl(&["start", UNIT]);
        unit
    }
}

impl Drop for TestUnit {
    fn drop(&mut self) {
        systemctl(&["stop", UNIT]);
        fs::remove_dir_all(&self.drop_in).expect("remove fixture drop-ins");
        fs::remove_file(&self.file).expect("remove test unit");
        systemctl(&["daemon-reload"]);
    }
}

#[tokio::test]
async fn two_confirmed_updates_clear_restarts_and_startup_failures_back_off_to_the_limit() {
    let plane = TcpListener::bind("127.0.0.1:0").await.expect("control plane");
    let host = Host::new(
        "unit-counters",
        &format!("http://{}", plane.local_addr().expect("address")),
    )
    .expect("fixture host");
    let binary = host.root.join("supervisor");
    fs::copy(BINARY, &binary).expect("installed supervisor");
    let mut candidate = fs::read(&binary).expect("supervisor binary");
    let unit = TestUnit::install(&host, &binary);
    let architecture = if std::env::consts::ARCH == "aarch64" {
        "arm64"
    } else {
        "amd64"
    };
    tokio::time::timeout(Duration::from_secs(45), async {
        let (stream, _) = plane.accept().await.expect("first connection");
        let mut wire = hello(stream).await;
        welcome(&mut wire).await;
        for revision in [1u8, 2] {
            let old = sha256(&candidate);
            candidate.push(revision);
            let digest = sha256(&candidate);
            let release = json!({
                "releaseVersion": format!("999.0.{revision}"),
                "channelProtocolVersion": ChannelProtocol::VERSION,
                "supervisors": [{
                    "architecture": architecture, "sha256": digest, "supervisorVersion": format!("999.0.{revision}")
                }]
            });
            http(&plane, "/v1/releases/current", "200 OK", release.to_string().as_bytes()).await;
            http(
                &plane,
                &format!("/v1/releases/supervisor/{architecture}"),
                "200 OK",
                &candidate,
            )
            .await;
            let (stream, _) = plane.accept().await.expect("automatic restart connection");
            wire = hello(stream).await;
            assert_eq!(property("NRestarts"), "1");
            welcome(&mut wire).await;
            loop {
                let frame = next_frame(&mut wire).await;
                if frame["message"]["phase"] == "Online" {
                    assert_eq!(frame["message"]["fromSha256"], old);
                    assert_eq!(frame["message"]["toSha256"], digest);
                    break;
                }
            }
            while property("NRestarts") != "0" {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        http(&plane, "/v1/releases/current", "404 Not Found", b"").await;
    })
    .await
    .expect("two updates restart and clear the counters");

    assert_eq!(property("RestartSteps"), "10");
    assert_eq!(property("RestartMaxDelayUSec"), "30min");
    assert_eq!(property("StartLimitIntervalUSec"), "3h");
    assert_eq!(property("StartLimitBurst"), "11");
    systemctl(&["stop", UNIT]);
    systemctl(&["reset-failed", UNIT]);
    let starts = host.root.join("starts");
    fs::write(
        unit.drop_in.join("failure.conf"),
        format!(
            "[Service]\nExecStart=\nExecStart=/bin/sh -c 'date +%%s%%N >> {}; exit 1'\nRestartSec=50ms\nRestartMaxDelaySec=1s\n",
            starts.display()
        ),
    )
    .expect("startup failure and shorter delays");
    systemctl(&["daemon-reload"]);
    systemctl(&["start", "--no-block", UNIT]);
    tokio::time::timeout(Duration::from_secs(20), async {
        while property("Result") != "start-limit-hit" {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("startup failures hit the start limit");
    let starts: Vec<u64> = fs::read_to_string(starts)
        .expect("startup times")
        .lines()
        .map(|line| line.parse().expect("startup time in nanoseconds"))
        .collect();
    assert_eq!(starts.len(), 11);
    assert!(starts[10] - starts[9] >= 900_000_000);
    assert!(starts[10] - starts[9] > starts[1] - starts[0]);
}
