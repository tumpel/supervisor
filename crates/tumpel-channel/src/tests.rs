use std::cell::RefCell;
use std::num::NonZeroU32;
use std::rc::Rc;

use serde_json::{json, Value};
use tumpel_api::types::{
    ChannelControlMessage, ChannelHostMessage, ChannelProtocol, ChannelSnapshotDone, ChannelSnapshotDoneType, EntityId,
    HostUsage, SupervisorChannelState,
};

use super::{empty_state, hello_bytes, ChannelError, Event, HostChannel, Phase, Port, Signer, Store};

#[derive(Default)]
struct Wire(Vec<String>);

impl Port for Wire {
    fn send(&mut self, text: String) -> Result<(), ChannelError> {
        self.0.push(text);
        Ok(())
    }
}

impl Wire {
    fn sent(&mut self) -> Vec<Value> {
        self.0
            .drain(..)
            .map(|text| serde_json::from_str(&text).expect("the channel sends JSON"))
            .collect()
    }
}

#[derive(Clone, Default)]
struct Disk(Rc<RefCell<Option<SupervisorChannelState>>>);

impl Store for Disk {
    fn save(&mut self, state: &SupervisorChannelState) -> Result<(), ChannelError> {
        *self.0.borrow_mut() = Some(state.clone());
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Recorder(Rc<RefCell<Vec<Vec<u8>>>>);

impl Signer for Recorder {
    fn sign(&self, bytes: &[u8]) -> String {
        self.0.borrow_mut().push(bytes.to_vec());
        "c2lnbmF0dXJl".to_string()
    }
}

const NONCE: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn usage() -> HostUsage {
    HostUsage {
        boxes_warm: 1,
        load_average: 0.5,
        memory_available_mib: 4096,
        disk_budget_mib: None,
        disk_held_mib: None,
    }
}

fn capacity() -> NonZeroU32 {
    NonZeroU32::new(8).expect("capacity")
}

fn channel(disk: &Disk, signer: &Recorder) -> HostChannel<Disk, Recorder> {
    HostChannel::new(
        empty_state(),
        disk.clone(),
        signer.clone(),
        "host-1".try_into().expect("host id"),
        "0.1.0".to_string(),
    )
}

fn done(snapshot: &str) -> ChannelHostMessage {
    ChannelSnapshotDone {
        snapshot_id: snapshot.try_into().expect("snapshot id"),
        uploaded_bytes: 10,
        type_: ChannelSnapshotDoneType::SnapshotDone,
    }
    .into()
}

fn frame(seq: i64, ack: i64, snapshot: &str) -> String {
    json!({
        "type": "Frame",
        "seq": seq,
        "ack": ack,
        "message": {
            "type": "SnapshotRequest",
            "snapshotId": snapshot,
            "sessionId": "session-1",
            "boxId": "box-1",
            "turnId": "turn-1",
            "snapshotSeq": 1,
            "storageKey": format!("snapshots/{snapshot}"),
            "uploadUrl": "https://storage.example.test/upload",
            "uploadExpiresAt": "2026-11-14T12:00:00+01:00"
        }
    })
    .to_string()
}

fn offer(seq: i64, credential: &str) -> String {
    json!({
        "type": "Frame",
        "seq": seq,
        "ack": 0,
        "message": {
            "type": "TurnOffer",
            "turnId": "turn-1",
            "sessionId": "session-1",
            "boxId": "box-1",
            "repository": "https://git.example.test/repo.git",
            "harness": "ClaudeCode",
            "harnessVersion": "1.0.0",
            "model": "claude-test",
            "prompt": "hello",
            "credential": {
                "credentialHandoverId": credential,
                "expiresAt": "2026-11-14T12:00:00+01:00",
                "variables": [{"name": "API_KEY", "value": format!("secret-{credential}")}],
                "connectors": []
            },
            "lastLineSeq": 0,
            "snapshots": [],
            "personaFiles": [],
            "image": {
                "kind": "Full",
                "imageDigest": "b".repeat(64),
                "kernelDigest": "c".repeat(64),
                "manifestDigest": "d".repeat(64),
                "imageUrl": "https://r2.example.test/image",
                "kernelUrl": "https://r2.example.test/kernel",
                "manifestUrl": "https://r2.example.test/manifest",
                "downloadExpiresAt": "2026-11-14T12:00:00+01:00",
                "fetchSeconds": 900
            },
            "gitAuthorName": "Tumpel Agent",
            "gitAuthorEmail": "agent@tumpel.invalid"
        }
    })
    .to_string()
}

fn open(channel: &mut HostChannel<Disk, Recorder>, wire: &mut Wire) {
    channel.connect();
    let challenge = json!({"type": "Challenge", "nonce": NONCE}).to_string();
    let Ok(Event::Challenged(nonce)) = channel.receive(&challenge, wire) else {
        panic!("the challenge asks for a Hello");
    };
    channel.hello(&nonce, capacity(), usage(), wire).expect("hello");
    let welcome =
        json!({"type": "Welcome", "hostId": "host-1", "protocolVersion": ChannelProtocol::VERSION}).to_string();
    assert!(matches!(
        channel.receive(&welcome, wire).expect("welcome"),
        Event::Welcomed
    ));
}

#[test]
fn hello_signs_the_documented_bytes() {
    let disk = Disk::default();
    let signer = Recorder::default();
    let mut channel = channel(&disk, &signer);
    let mut wire = Wire::default();
    channel.connect();
    let challenge = json!({"type": "Challenge", "nonce": NONCE}).to_string();
    let Ok(Event::Challenged(nonce)) = channel.receive(&challenge, &mut wire) else {
        panic!("the challenge asks for a Hello");
    };
    assert_eq!(nonce, NONCE);
    assert_eq!(channel.phase(), Phase::AwaitingHello);
    channel.hello(&nonce, capacity(), usage(), &mut wire).expect("hello");
    let mut expected = b"tumpel-channel-hello-v1\0".to_vec();
    expected.extend_from_slice(NONCE.as_bytes());
    expected.extend_from_slice(b"\0host-1");
    assert_eq!(hello_bytes(NONCE, "host-1"), expected);
    assert_eq!(signer.0.borrow().as_slice(), [expected]);
    assert_eq!(
        wire.sent(),
        [
            json!({"type": "Hello", "hostId": "host-1", "protocolVersion": ChannelProtocol::VERSION, "supervisorVersion": "0.1.0", "signature": "c2lnbmF0dXJl", "boxCapacity": 8, "usage": {"boxesWarm": 1, "loadAverage": 0.5, "memoryAvailableMib": 4096}})
        ]
    );
    assert_eq!(channel.phase(), Phase::AwaitingWelcome);
}

#[test]
fn a_frame_before_the_challenge_is_a_protocol_error() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    channel.connect();
    let outcome = channel.receive(&frame(1, 0, "snap-1"), &mut Wire::default());
    assert!(matches!(outcome, Err(ChannelError::Protocol(reason)) if reason.contains("expected Challenge")));
    assert_eq!(channel.phase(), Phase::Closed);
}

#[test]
fn duplicates_are_dropped_and_a_gap_closes_the_connection() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    assert!(matches!(
        channel.receive(&frame(1, 0, "snap-1"), &mut wire),
        Ok(Event::Message(_))
    ));
    assert!(matches!(
        channel.receive(&frame(1, 0, "snap-1"), &mut wire),
        Ok(Event::None)
    ));
    assert_eq!(disk.0.borrow().as_ref().map(|state| state.received.0), Some(1));
    let gap = channel.receive(&frame(4, 0, "snap-4"), &mut wire);
    assert!(matches!(gap, Err(ChannelError::Protocol(reason)) if reason == "frame gap: expected seq 2, got 4"));
    assert_eq!(channel.phase(), Phase::Closed);
}

#[test]
fn frames_sent_while_disconnected_are_saved_and_resent_after_welcome() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    channel.receive(&frame(1, 0, "snap-1"), &mut wire).expect("frame");
    channel.send(done("snap-1"), &mut wire).expect("send");
    assert_eq!(wire.sent().len(), 2);
    channel.disconnect();

    channel.send(done("snap-2"), &mut wire).expect("send while closed");
    assert_eq!(wire.sent(), Vec::<Value>::new());
    let saved = disk.0.borrow().clone().expect("saved state");
    assert_eq!((saved.received.0, saved.sent.0, saved.unacked.len()), (1, 2, 2));

    open(&mut channel, &mut wire);
    let resent = wire.sent();
    let resent: Vec<(i64, i64)> = resent[1..]
        .iter()
        .map(|frame| (frame["seq"].as_i64().unwrap_or(-1), frame["ack"].as_i64().unwrap_or(-1)))
        .collect();
    assert_eq!(resent, [(1, 1), (2, 1)]);

    channel
        .receive(&frame(2, 1, "snap-2"), &mut wire)
        .expect("ack of the first");
    let saved = disk.0.borrow().clone().expect("saved state");
    assert_eq!(saved.unacked.iter().map(|frame| frame.seq.0).collect::<Vec<_>>(), [2]);
}

#[test]
fn the_host_keeps_the_grant_of_the_offer_it_accepted_and_never_saves_one() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    let Ok(Event::Message(accepted)) = channel.receive(&offer(1, "grant-1"), &mut wire) else {
        panic!("the first offer is delivered");
    };
    let ChannelControlMessage::TurnOffer(accepted) = *accepted else {
        panic!("the delivered message is the offer");
    };
    assert_eq!(*accepted.credential.credential_handover_id, "grant-1");
    channel.disconnect();

    open(&mut channel, &mut wire);
    assert!(matches!(
        channel.receive(&offer(1, "grant-2"), &mut wire),
        Ok(Event::None)
    ));
    let saved = serde_json::to_string(&disk.0.borrow().clone().expect("saved state")).expect("state encodes");
    assert!(!saved.contains("grant-") && !saved.contains("secret-"), "{saved}");
}

#[test]
fn a_heartbeat_keeps_an_open_channel_open() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    let beat = json!({"type": "Heartbeat", "sentAt": "2026-11-14T10:00:00+01:00"}).to_string();
    assert!(matches!(channel.receive(&beat, &mut wire), Ok(Event::Heartbeat)));
    assert_eq!(channel.phase(), Phase::Open);
}

#[test]
fn a_release_notice_reaches_an_open_channel() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    let notice = json!({"type": "ReleasePublished", "releaseVersion": "v0.9.0"}).to_string();
    assert!(matches!(
        channel.receive(&notice, &mut wire),
        Ok(Event::ReleasePublished)
    ));
    assert_eq!(channel.phase(), Phase::Open);
}

#[test]
fn a_saved_frame_no_reader_knows_is_resent_unchanged_and_dropped_once_the_control_plane_refuses_it() {
    let step = json!({
        "type": "SupervisorUpdateStep",
        "fromSha256": "a".repeat(64),
        "toSha256": "b".repeat(64),
        "phase": "Swapping",
        "at": "2026-11-16T09:30:00+01:00"
    });
    let saved: SupervisorChannelState = serde_json::from_value(json!({
        "received": 4,
        "sent": 7,
        "unacked": [{"type": "Frame", "seq": 7, "ack": 3, "message": step}]
    }))
    .expect("channel.json with a message this binary does not read still loads");
    let disk = Disk::default();
    let mut channel = HostChannel::new(
        saved,
        disk.clone(),
        Recorder::default(),
        "host-1".try_into().expect("host id"),
        "0.1.0".to_string(),
    );
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    let sent = wire.sent();
    assert_eq!(sent[1], json!({"type": "Frame", "seq": 7, "ack": 4, "message": step}));

    let refused = json!({"type": "FrameRefused", "seq": 7, "reason": "message.phase: Invalid option"}).to_string();
    let Ok(Event::FrameRefused { seq, reason }) = channel.receive(&refused, &mut wire) else {
        panic!("the refusal is reported");
    };
    assert_eq!((seq, reason.as_str()), (7, "message.phase: Invalid option"));
    let state = disk.0.borrow().clone().expect("saved state");
    assert!(state.unacked.is_empty(), "the refused frame leaves channel.json");
    assert_eq!(channel.phase(), Phase::Open, "a refusal keeps the connection");

    channel.send(done("snap-8"), &mut wire).expect("send");
    let next = wire.sent();
    assert_eq!((next[0]["seq"].as_i64(), next[0]["ack"].as_i64()), (Some(8), Some(4)));
}

#[test]
fn a_frame_and_message_with_fields_this_supervisor_does_not_know_are_read_for_the_fields_it_knows() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    let mut extended: Value = serde_json::from_str(&frame(1, 0, "snap-1")).expect("frame");
    extended["priority"] = json!("High");
    extended["message"]["compression"] = json!("Zstd");
    let Ok(Event::Message(message)) = channel.receive(&extended.to_string(), &mut wire) else {
        panic!("the frame is read");
    };
    let ChannelControlMessage::SnapshotRequest(request) = *message else {
        panic!("the message is a SnapshotRequest");
    };
    assert_eq!(request.snapshot_id.as_str(), "snap-1");
    assert_eq!(channel.state().received.0, 1);
}

#[test]
fn a_heartbeat_names_every_held_session_and_an_empty_list_when_none() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    wire.sent();
    let sent_at = chrono::DateTime::parse_from_rfc3339("2026-11-18T10:00:00+01:00").expect("time");
    let held = vec![EntityId::try_from("session-1").expect("id")];
    channel.heartbeat(sent_at, usage(), held, &mut wire).expect("heartbeat");
    channel
        .heartbeat(sent_at, usage(), Vec::new(), &mut wire)
        .expect("heartbeat");
    let usage = json!({"boxesWarm": 1, "loadAverage": 0.5, "memoryAvailableMib": 4096});
    assert_eq!(
        wire.sent(),
        [
            json!({"type": "Heartbeat", "sentAt": "2026-11-18T10:00:00+01:00", "usage": usage, "heldSessionIds": ["session-1"]}),
            json!({"type": "Heartbeat", "sentAt": "2026-11-18T10:00:00+01:00", "usage": usage, "heldSessionIds": []}),
        ]
    );
}
