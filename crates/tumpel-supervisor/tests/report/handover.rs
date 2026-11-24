use super::*;
use std::collections::BTreeSet;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::{protocol::frame::coding::CloseCode, protocol::CloseFrame};
use tumpel_api::types::{ChannelCloseCode, GuestImageManifest, SupervisorHandover};

const UNIT: &str = "tumpel-handover-test.service";
type Wire = tokio_tungstenite::WebSocketStream<TcpStream>;

fn systemctl(args: &[&str]) -> bool {
    Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .unwrap()
        .success()
}

struct Unit;
impl Drop for Unit {
    fn drop(&mut self) {
        let _ = systemctl(&["stop", UNIT]);
    }
}

async fn open(plane: &TcpListener) -> Wire {
    let (stream, _) = plane.accept().await.unwrap();
    let mut wire = tokio_tungstenite::accept_async(stream).await.unwrap();
    send(&mut wire, json!({"type":"Challenge","nonce":"n".repeat(43)})).await;
    take(&mut wire, "Hello").await;
    send(
        &mut wire,
        json!({"type":"Welcome","hostId":"host-1","protocolVersion":ChannelProtocol::VERSION}),
    )
    .await;
    http(plane, "GET /v1/releases/current ", b"{}").await;
    wire
}

async fn send(wire: &mut Wire, value: Value) {
    wire.send(Message::text(value.to_string())).await.unwrap();
}

async fn take(wire: &mut Wire, kind: &str) -> Value {
    loop {
        let frame: Value = serde_json::from_str(wire.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        if frame["type"] == kind || frame["message"]["type"] == kind || frame["message"]["phase"] == kind {
            return frame;
        }
        assert_ne!(frame["message"]["type"], "TurnFailed", "{frame}");
        assert_ne!(frame["message"]["type"], "SnapshotFailed", "{frame}");
    }
}

async fn order(wire: &mut Wire, seq: &mut i64, message: Value) {
    *seq += 1;
    send(wire, json!({"type":"Frame","seq":seq,"ack":0,"message":message})).await;
}

async fn http(plane: &TcpListener, expected: &str, body: &[u8]) {
    let mut headers = BufReader::new(plane.accept().await.unwrap().0).lines();
    let request = headers.next_line().await.unwrap().unwrap();
    assert!(request.starts_with(expected), "{request}");
    let mut length = 0;
    while let Some(line) = headers.next_line().await.unwrap().filter(|line| !line.is_empty()) {
        if let Some(size) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
            length = size.parse().unwrap();
        }
    }
    let mut io = headers.into_inner();
    io.read_exact(&mut vec![0; length]).await.unwrap();
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    io.write_all(header.as_bytes()).await.unwrap();
    io.write_all(body).await.unwrap();
}

fn digest(bytes: &[u8]) -> String {
    let sum = ring::digest::digest(&ring::digest::SHA256, bytes);
    sum.as_ref().iter().map(|byte| format!("{byte:02x}")).collect()
}

fn pids(host: &Host) -> BTreeSet<u32> {
    let bytes = fs::read(host.root.join("state/boxes.json")).unwrap();
    let record: SupervisorHandover = serde_json::from_slice(&bytes).unwrap();
    let pids = record.boxes.into_iter().map(|box_| box_.machine.process_id.get());
    pids.filter(|pid| Path::new(&format!("/proc/{pid}")).exists()).collect()
}

async fn offer(wire: &mut Wire, seq: &mut i64, image: &GuestImageManifest, ids: (&str, &str), input: (&str, &str)) {
    let (turn, session) = ids;
    let (prompt, ends) = input;
    order(wire, seq, json!({
        "type":"TurnOffer","turnId":turn,"sessionId":session,"boxId":session,
        "repository":"https://github.com/octocat/Hello-World.git","harness":image.harness.harness,
        "harnessVersion":image.harness.harness_version,"model":"none","prompt":prompt,
        "credential":{"credentialHandoverId":"grant-1","expiresAt":ends,"variables":[],"connectors":[]},
        "image":{"kind":"Full","imageDigest":image.image_digest,"kernelDigest":image.kernel_digest,"manifestDigest":"0".repeat(64),
            "imageUrl":"https://example.test/image","kernelUrl":"https://example.test/kernel","manifestUrl":"https://example.test/manifest",
            "downloadExpiresAt":ends,"fetchSeconds":60},
        "lastLineSeq":0,"snapshots":[],"personaFiles":[],"gitAuthorName":"Agent","gitAuthorEmail":"agent@example.test",
        "boxRetention":"Auto","turnSeconds":120,"idleSeconds":600
    })).await;
    assert_eq!(take(wire, "TurnClaim").await["message"]["turnId"], turn);
    order(wire, seq, json!({"type":"TurnClaimed","turnId":turn})).await;
}

async fn request_snapshot(wire: &mut Wire, seq: &mut i64, ids: (&str, &str, &str), endpoint: &str, ends: &str) {
    let (id, session, turn) = ids;
    let request = json!({"type":"SnapshotRequest","snapshotId":id,"sessionId":session,
        "boxId":session,"turnId":turn,"snapshotSeq":1,"storageKey":format!("snapshots/{id}"),
        "uploadUrl":format!("{endpoint}/snapshot"),"uploadExpiresAt":ends});
    order(wire, seq, request).await;
    take(wire, "SnapshotChunks").await;
}

async fn finish_snapshot(plane: &TcpListener, wire: &mut Wire, seq: &mut i64, id: &str, ends: &str) -> Value {
    let held = json!({"type":"SnapshotChunksHeld","snapshotId":id,"part":1,"held":[]});
    order(wire, seq, held).await;
    let packs = take(wire, "SnapshotPacks").await;
    let urls = json!({"type":"SnapshotPackUploads","snapshotId":id,
        "part":packs["message"]["part"],"uploads":[],"uploadExpiresAt":ends});
    order(wire, seq, urls).await;
    http(plane, "PUT /snapshot ", b"").await;
    take(wire, "SnapshotDone").await
}

fn release(version: &str, sha: &str, protocol: u16) -> Value {
    json!({"releaseVersion":version,"channelProtocolVersion":protocol,"supervisors":[
        {"architecture":"Amd64","sha256":sha,"supervisorVersion":version},{"architecture":"Arm64","sha256":sha,"supervisorVersion":version}]})
}

#[tokio::test]
async fn the_user_service_keeps_firecracker_across_an_update_and_a_restart() {
    assert_eq!(std::env::var("TUMPEL_KVM").as_deref(), Ok("1"), "KVM host required");
    let manifest =
        PathBuf::from(std::env::var_os("TUMPEL_IMAGE_MANIFEST").expect("set the cached Shell image manifest"));
    let image = serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    let previous =
        std::env::var_os("TUMPEL_PREVIOUS_SUPERVISOR").expect("build this revision with a lower release version");
    let firecracker = std::env::var_os("TUMPEL_FIRECRACKER").expect("set the Firecracker executable");
    let plane = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", plane.local_addr().unwrap());
    let host = Host::new("kvm-handover", &endpoint).unwrap();
    host.state(
        "runtime.json",
        &json!({
            "firecrackerPath":firecracker, "imageCacheDir":manifest.parent().unwrap(), "dataDir":host.root.join("data"),
            "boxCapacity":3, "updateGraceSeconds":0, "heartbeatIntervalSeconds":1
        }),
    )
    .unwrap();
    fs::create_dir_all(host.root.join("config/systemd/user")).unwrap();
    let binary = host.root.join("supervisor");
    fs::copy(previous, &binary).unwrap();
    let old = digest(&fs::read(&binary).unwrap());
    let state = format!("--setenv=TUMPEL_SUPERVISOR_DIR={}", host.root.join("state").display());
    let config = format!("--setenv=XDG_CONFIG_HOME={}", host.root.join("config").display());
    assert!(Command::new("systemd-run")
        .args([
            "--user",
            "--collect",
            "--unit",
            UNIT,
            &state,
            &config,
            "--property=Restart=on-failure",
            "--property=RestartSec=1",
            "--property=NotifyAccess=main",
            "--property=FileDescriptorStoreMax=192",
            "--property=FileDescriptorStorePreserve=yes",
        ])
        .arg(&binary)
        .arg("run")
        .status()
        .unwrap()
        .success());
    let _unit = Unit;
    tokio::time::timeout(Duration::from_secs(180), async {
        let mut wire = open(&plane).await;
        let mut seq = 0;
        let ends = (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
        for id in ["warm", "snapshot"] {
            let prompt = "echo dirty >/root/handover; echo ready";
            offer(&mut wire, &mut seq, &image, (id, id), (prompt, &ends)).await;
            assert_eq!(take(&mut wire, "TurnComplete").await["message"]["turnId"], id);
        }
        let prompt = "echo dirty >/root/running; echo start; sleep 30; echo resumed";
        offer(&mut wire, &mut seq, &image, ("running", "running"), (prompt, &ends)).await;
        assert_eq!(take(&mut wire, "TurnLines").await["message"]["turnId"], "running");
        let before = pids(&host);
        assert_eq!(before.len(), 3);
        let version = env!("TUMPEL_RELEASE_VERSION");
        let candidate = fs::read(BINARY).unwrap();
        let new = digest(&candidate);
        let mut rejected = candidate.clone();
        rejected.push(0);
        for (bytes, sha, protocol) in [
            (&rejected, digest(&rejected), ChannelProtocol::VERSION + 1),
            (&candidate, new.clone(), ChannelProtocol::VERSION),
        ] {
            if sha == new {
                let ids = ("snap-1", "snapshot", "snapshot");
                request_snapshot(&mut wire, &mut seq, ids, &endpoint, &ends).await;
            }
            send(&mut wire, json!({"type":"ReleasePublished","releaseVersion":version})).await;
            let release = release(version, &sha, protocol);
            http(&plane, "GET /v1/releases/current ", release.to_string().as_bytes()).await;
            http(&plane, "GET /v1/releases/supervisor/", bytes).await;
            if sha != new {
                take(&mut wire, "SupervisorUpdateFailed").await;
                assert_eq!(pids(&host), before);
                assert_eq!(digest(&fs::read(&binary).unwrap()), old);
            }
        }
        take(&mut wire, "Restarting").await;
        let beat = take(&mut wire, "Heartbeat").await;
        assert_eq!(beat["heldSessionIds"].as_array().unwrap().len(), 3);
        let done = finish_snapshot(&plane, &mut wire, &mut seq, "snap-1", &ends).await;
        assert_eq!(digest(&fs::read(&binary).unwrap()), old);
        send(&mut wire, json!({"type":"Ack","seq":done["seq"]})).await;
        wire = open(&plane).await;
        let online = take(&mut wire, "Online").await;
        assert_eq!(online["message"]["releaseVersion"], version);
        assert_eq!(online["message"]["fromSha256"], old);
        assert_eq!(online["message"]["toSha256"], new);
        assert!(!host.root.join("state/update-pending.json").exists());
        assert_eq!(take(&mut wire, "TurnComplete").await["message"]["turnId"], "running");
        assert_eq!(pids(&host), before);
        let prompt = "echo start; sleep 5; echo resumed";
        offer(&mut wire, &mut seq, &image, ("restart", "warm"), (prompt, &ends)).await;
        take(&mut wire, "TurnLines").await;
        request_snapshot(&mut wire, &mut seq, ("snap-2", "running", "running"), &endpoint, &ends).await;
        assert!(systemctl(&["restart", "--no-block", UNIT]));
        let beat = take(&mut wire, "Heartbeat").await;
        assert_eq!(beat["heldSessionIds"].as_array().unwrap().len(), 3);
        let done = finish_snapshot(&plane, &mut wire, &mut seq, "snap-2", &ends).await;
        send(&mut wire, json!({"type":"Ack","seq":done["seq"]})).await;
        wire = open(&plane).await;
        assert_eq!(take(&mut wire, "TurnComplete").await["message"]["turnId"], "restart");
        assert_eq!(pids(&host), before);
        request_snapshot(&mut wire, &mut seq, ("snap-3", "warm", "restart"), &endpoint, &ends).await;
        assert!(systemctl(&["restart", "--no-block", UNIT]));
        take(&mut wire, "Heartbeat").await;
        wire.close(Some(CloseFrame {
            code: CloseCode::from(ChannelCloseCode::REVOKED),
            reason: "revoked".into(),
        }))
        .await
        .unwrap();
        while before.iter().any(|pid| Path::new(&format!("/proc/{pid}")).exists()) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the update, restart and revoke finish");
}
