#![cfg(feature = "host-tests")]

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tumpel_api::types::{ChannelHostMessage, GuestImageManifest, Sequence, SupervisorRuntime};
use tumpel_supervisor::{
    BootOrder, BoxTurn, Budgets, Command, Ext4Upper, Firecracker, HttpStore, ImageOrder, Lifecycle, Settings, TapPool,
    Upper,
};

#[cfg(test)]
fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME"))
}

#[cfg(test)]
fn turn(image: &GuestImageManifest, turn_id: &str, prompt: &str, last_line_seq: i64) -> Command {
    let expires_at = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    Command::Turn(Box::new(BoxTurn {
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
    }))
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
        TapPool::new(1).expect("pool"),
        out,
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
