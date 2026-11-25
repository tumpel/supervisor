use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ring::signature::Ed25519KeyPair;
use tumpel_api::types::HostUsage;
use tumpel_channel::{ChannelError, Event, HostChannel, Port, Signer, Usage};

use super::*;
use crate::dir::ChannelFile;

struct KeyPair(Ed25519KeyPair);

impl Signer for KeyPair {
    fn sign(&self, bytes: &[u8]) -> String {
        STANDARD.encode(self.0.sign(bytes).as_ref())
    }
}

#[derive(Default)]
struct Wire(Vec<serde_json::Value>);

impl Port for Wire {
    fn send(&mut self, text: String) -> Result<(), ChannelError> {
        self.0.push(serde_json::from_str(&text).expect("a wire envelope"));
        Ok(())
    }
}

fn receiver(dir: &Path, wire: &mut Wire) -> HostChannel<ChannelFile, KeyPair> {
    let mut channel = HostChannel::new(
        tumpel_channel::empty_state(),
        ChannelFile(dir.join("channel.json")),
        KeyPair(Ed25519KeyPair::from_seed_unchecked(&[7; 32]).expect("a signing key")),
        "host-1".try_into().expect("host id"),
        "0.1.0".to_string(),
    );
    channel.connect();
    let challenge = json!({"type": "Challenge", "nonce": "n".repeat(43)});
    let Event::Challenged(nonce) = channel.receive(&challenge.to_string()).expect("challenge") else {
        panic!("expected a challenge");
    };
    let usage = Usage {
        host: HostUsage {
            load_average: 0.0,
            memory_available_mib: 4096,
            disk_budget_mib: 0,
            disk_held_mib: 0,
            memory_budget_mib: 0,
            memory_held_mib: 0,
        },
    };
    channel
        .hello(&nonce, NonZeroU32::MIN, usage, Vec::new(), wire)
        .expect("hello");
    let welcome = json!({"type": "Welcome", "hostId": "host-1", "protocolVersion": ChannelProtocol::VERSION});
    assert!(matches!(channel.receive(&welcome.to_string()), Ok(Event::Welcomed)));
    wire.0.clear();
    channel
}

pub(super) fn refuse_claim(host: &Host, claim: ChannelHostMessage) -> serde_json::Value {
    let mut wire = Wire::default();
    let mut channel = receiver(&host.vmm.staging, &mut wire);
    channel.send(claim, &mut wire).expect("send the claim");
    let refusal = json!({"type": "FrameRefused", "seq": wire.0[0]["seq"], "reason": "the offer expired"});
    let Event::FrameRefused {
        claim: Some(refused), ..
    } = channel.receive(&refusal.to_string()).expect("receive the refusal")
    else {
        panic!("expected a refused claim");
    };
    host.answers.deliver(Answer::ClaimRefused(refused));
    channel.state().unacked[0].message.clone()
}

#[tokio::test(start_paused = true)]
async fn dropping_a_persona_download_removes_its_staging_dir() {
    let hold = Arc::new((Notify::new(), Notify::new()));
    let store = MemoryStore {
        5: Some(hold),
        ..MemoryStore::default()
    };
    let dir = staging("dropped-persona-download");
    let files = [serde_json::from_value(json!({
        "path": "README.md", "digest": "a".repeat(64), "sizeBytes": 1,
        "downloadUrl": PERSONA_FILE_URL, "downloadExpiresAt": in_seconds(300),
    }))
    .expect("a persona file")];
    {
        let download = crate::persona_files::fetch(&store, &dir, &files);
        tokio::pin!(download);
        tokio::select! {
            biased;
            _ = &mut download => panic!("the download must stay pending"),
            () = std::future::ready(()) => {}
        }
        assert!(dir.is_dir());
    }
    assert!(!dir.exists());
}

#[tokio::test(start_paused = true)]
async fn a_refused_warm_claim_cancels_the_files_stage_and_keeps_the_box() {
    let hold = Arc::new((Notify::new(), Notify::new()));
    let store = MemoryStore {
        5: Some(Arc::clone(&hold)),
        ..MemoryStore::default()
    };
    let mut host = host_on(
        "refused-warm-files",
        store,
        1,
        Harness::Echo,
        Boot::Works,
        settings(),
        120,
    );
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle_until("complete 1").await;
    host.log();

    let mut turn = host.box_turn("refused-claim", "session-1", "two", Vec::new(), 300);
    turn.files_dir = host.vmm.staging.join("persona-files");
    turn.persona_files = vec![serde_json::from_value(json!({
        "path": "README.md", "digest": "a".repeat(64), "sizeBytes": 1,
        "downloadUrl": PERSONA_FILE_URL, "downloadExpiresAt": in_seconds(300),
    }))
    .expect("a persona file")];
    let turn_id = turn.turn_id.clone();
    let staged = turn.files_dir.join(turn_id.as_str());
    host.send(Command::Turn(Box::new(turn)));
    let claim = tokio::time::timeout(SETTLE, host.received.recv())
        .await
        .expect("the claim goes out")
        .expect("the claim");
    assert!(matches!(&claim, ChannelHostMessage::TurnClaim(claim) if claim.turn_id == turn_id));
    tokio::time::timeout(SETTLE, hold.0.notified())
        .await
        .expect("the download starts");
    assert!(staged.is_dir());
    host.cancels.cancel(turn_id.clone());

    let refused_at = tokio::time::Instant::now();
    let terminal = refuse_claim(&host, claim);
    host.turn("turn-3", "session-1", "three", Vec::new());
    assert_eq!(
        host.settle_until("complete 1").await,
        ["claim turn-3", "line 1 three", "complete 1"]
    );
    assert_eq!(tokio::time::Instant::now(), refused_at);
    assert!(!host.cancels.contains(&turn_id));
    assert!(!staged.exists());
    assert_eq!(
        host.log(),
        ["resume session-1", "reconnect session-1", "pause session-1"]
    );
    assert_eq!(terminal["type"], "TurnFailed");
    assert_eq!(terminal["reason"], "Undelivered");
    assert_eq!(terminal["detail"], "the offer expired");
}
