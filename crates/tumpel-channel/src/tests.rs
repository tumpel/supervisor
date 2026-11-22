use std::cell::RefCell;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::time::Duration;

use serde_json::{json, Value};
use tumpel_api::types::{
    ChannelControlMessage, ChannelHostMessage, ChannelProtocol, ChannelSnapshotDone, ChannelSnapshotDoneType, EntityId,
    HostReportedHealth, HostUsage, SupervisorChannelState,
};

use super::{
    empty_state, hello_bytes, Backoff, ChannelError, Event, HeldIds, HostChannel, Phase, Port, RateBackoff, RateLimit,
    Signer, Store,
};

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
        memory_budget_mib: None,
        memory_held_mib: None,
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
            "gitAuthorEmail": "agent@tumpel.invalid",
            "boxRetention": "Auto",
            "turnSeconds": 3600,
            "idleSeconds": 900
        }
    })
    .to_string()
}

fn welcome(channel: &mut HostChannel<Disk, Recorder>, wire: &mut Wire, welcome: &Value) -> Result<Event, ChannelError> {
    channel.connect();
    let challenge = json!({"type": "Challenge", "nonce": NONCE}).to_string();
    let Ok(Event::Challenged(nonce)) = channel.receive(&challenge, wire) else {
        panic!("the challenge asks for a Hello");
    };
    channel
        .hello(&nonce, capacity(), usage(), Vec::new(), wire)
        .expect("hello");
    wire.sent();
    channel.receive(&welcome.to_string(), wire)
}

fn open(channel: &mut HostChannel<Disk, Recorder>, wire: &mut Wire) {
    let quiet = json!({"type": "Welcome", "hostId": "host-1", "protocolVersion": ChannelProtocol::VERSION});
    let Ok(Event::Welcomed) = welcome(channel, wire, &quiet) else {
        panic!("the welcome opens the channel");
    };
    channel.release(usize::MAX, wire).expect("release");
}

struct ControlPlane {
    received: i64,
    taking: bool,
    taken: Vec<(i64, String)>,
}

impl ControlPlane {
    fn new(taking: bool) -> Self {
        Self {
            received: 0,
            taking,
            taken: Vec::new(),
        }
    }

    fn welcome(&self) -> Value {
        json!({"type": "Welcome", "hostId": "host-1", "protocolVersion": ChannelProtocol::VERSION, "ack": self.received})
    }

    fn read(&mut self, wire: &mut Wire) {
        for frame in wire.sent().into_iter().filter(|frame| frame["type"] == "Frame") {
            let seq = frame["seq"].as_i64().expect("seq");
            if seq <= self.received {
                continue;
            }
            assert_eq!(
                seq,
                self.received + 1,
                "the control plane closes the connection on a seq gap"
            );
            if !self.taking {
                return;
            }
            self.received = seq;
            self.taken
                .push((seq, frame["message"]["type"].as_str().unwrap_or_default().to_string()));
        }
    }

    fn reconnect(&mut self, channel: &mut HostChannel<Disk, Recorder>, wire: &mut Wire) -> Result<Event, ChannelError> {
        channel.disconnect();
        let event = welcome(channel, wire, &self.welcome())?;
        channel.release(usize::MAX, wire)?;
        Ok(event)
    }
}

fn first_unacked(wire: &mut Wire) -> Value {
    wire.0.first().map_or(Value::Null, |text| {
        serde_json::from_str(text).expect("the channel sends JSON")
    })
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
    channel
        .hello(&nonce, capacity(), usage(), Vec::new(), &mut wire)
        .expect("hello");
    let mut expected = b"tumpel-channel-hello-v1\0".to_vec();
    expected.extend_from_slice(NONCE.as_bytes());
    expected.extend_from_slice(b"\0host-1");
    assert_eq!(hello_bytes(NONCE, "host-1"), expected);
    assert_eq!(signer.0.borrow().as_slice(), [expected]);
    assert_eq!(
        wire.sent(),
        [
            json!({"type": "Hello", "hostId": "host-1", "protocolVersion": ChannelProtocol::VERSION, "supervisorVersion": "0.1.0", "signature": "c2lnbmF0dXJl", "boxCapacity": 8, "usage": {"boxesWarm": 1, "loadAverage": 0.5, "memoryAvailableMib": 4096}, "heldTurnIds": []})
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
    assert_eq!(wire.sent().len(), 1);
    channel.disconnect();

    channel.send(done("snap-2"), &mut wire).expect("send while closed");
    assert_eq!(wire.sent(), Vec::<Value>::new());
    let saved = disk.0.borrow().clone().expect("saved state");
    assert_eq!((saved.received.0, saved.sent.0, saved.unacked.len()), (1, 2, 2));

    open(&mut channel, &mut wire);
    let resent = wire.sent();
    let resent: Vec<(i64, i64)> = resent
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
    assert_eq!(sent[0], json!({"type": "Frame", "seq": 7, "ack": 4, "message": step}));

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
fn a_heartbeat_names_every_held_session_and_setup_and_an_empty_list_when_none() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    wire.sent();
    let sent_at = chrono::DateTime::parse_from_rfc3339("2026-11-18T10:00:00+01:00").expect("time");
    let held = HeldIds {
        session_ids: vec![EntityId::try_from("session-1").expect("id")],
        claude_setup_ids: vec![EntityId::try_from("setup-1").expect("id")],
        turn_ids: vec![EntityId::try_from("turn-1").expect("id")],
    };
    channel
        .heartbeat(sent_at, usage(), held, HostReportedHealth::Healthy, &mut wire)
        .expect("heartbeat");
    channel
        .heartbeat(
            sent_at,
            usage(),
            HeldIds::default(),
            HostReportedHealth::RateLimited,
            &mut wire,
        )
        .expect("heartbeat");
    let usage = json!({"boxesWarm": 1, "loadAverage": 0.5, "memoryAvailableMib": 4096});
    assert_eq!(
        wire.sent(),
        [
            json!({"type": "Heartbeat", "sentAt": "2026-11-18T10:00:00+01:00", "usage": usage, "heldSessionIds": ["session-1"], "heldClaudeSetupIds": ["setup-1"], "heldTurnIds": ["turn-1"], "health": "Healthy"}),
            json!({"type": "Heartbeat", "sentAt": "2026-11-18T10:00:00+01:00", "usage": usage, "heldSessionIds": [], "heldClaudeSetupIds": [], "heldTurnIds": [], "health": "RateLimited"}),
        ]
    );
}

#[test]
fn a_heartbeat_names_the_turn_of_every_unacked_frame_besides_the_held_turns_once_each() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    let claim = |turn: &str| -> ChannelHostMessage {
        serde_json::from_value(json!({"type": "TurnClaim", "turnId": turn, "boxId": "box-1"})).expect("claim")
    };
    channel.send(claim("turn-1"), &mut wire).expect("send");
    channel.send(claim("turn-2"), &mut wire).expect("send");
    channel.send(done("snap-1"), &mut wire).expect("send");
    wire.sent();
    let sent_at = chrono::DateTime::parse_from_rfc3339("2026-11-18T10:00:00+01:00").expect("time");
    let held = HeldIds {
        turn_ids: vec![EntityId::try_from("turn-1").expect("id")],
        ..HeldIds::default()
    };
    channel
        .heartbeat(sent_at, usage(), held, HostReportedHealth::Healthy, &mut wire)
        .expect("heartbeat");
    let [heartbeat] = wire.sent().try_into().expect("one heartbeat");
    assert_eq!(heartbeat["heldTurnIds"], json!(["turn-1", "turn-2"]));
}

#[test]
fn a_control_plane_that_takes_every_frame_but_sends_nothing_fails_none_over_twelve_reconnects() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    let mut control = ControlPlane::new(true);
    let mut backoff = Backoff::default();
    open(&mut channel, &mut wire);
    for snapshot in 1..=12 {
        channel
            .send(done(&format!("snap-{snapshot}")), &mut wire)
            .expect("send");
        control.read(&mut wire);
        assert!(matches!(
            control.reconnect(&mut channel, &mut wire),
            Ok(Event::Welcomed)
        ));
        assert_eq!(
            wire.sent(),
            Vec::<Value>::new(),
            "Welcome acked the frame, so nothing is resent"
        );
        assert_eq!(backoff.next(channel.progressed(), u16::MAX), Duration::from_secs(1));
    }
    assert_eq!(control.taken.len(), 12);
    let saved = disk.0.borrow().clone().expect("saved state");
    assert!(saved.unacked.is_empty() && saved.replays.is_empty(), "{saved:?}");
}

#[test]
fn a_control_plane_that_closes_after_every_welcome_is_reconnected_with_growing_gaps() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    let mut control = ControlPlane::new(true);
    let mut backoff = Backoff::default();
    open(&mut channel, &mut wire);
    let mut gaps = Vec::new();
    for _ in 0..10 {
        let wait = backoff.next(channel.progressed(), u16::MAX);
        gaps.push(wait.as_secs());
        assert!(matches!(
            control.reconnect(&mut channel, &mut wire),
            Ok(Event::Welcomed)
        ));
        control.read(&mut wire);
    }
    assert_eq!(gaps, [1, 2, 4, 8, 16, 30, 30, 30, 30, 30]);
    assert_eq!(control.taken, Vec::<(i64, String)>::new());
}

#[test]
fn a_frame_the_control_plane_never_acks_is_resent_unchanged_after_every_welcome_and_its_count_survives_a_restart() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    let mut control = ControlPlane::new(false);
    open(&mut channel, &mut wire);
    channel.send(done("snap-1"), &mut wire).expect("send");
    for _ in 0..6 {
        assert!(matches!(
            control.reconnect(&mut channel, &mut wire),
            Ok(Event::Welcomed)
        ));
        control.read(&mut wire);
    }
    assert_eq!(channel.stuck(), None);
    let saved = disk.0.borrow().clone().expect("saved state");
    assert_eq!(
        serde_json::to_value(&saved.replays).expect("replays encode"),
        json!([{"seq": 1, "replays": 6}])
    );

    let mut channel = HostChannel::new(
        saved,
        disk.clone(),
        Recorder::default(),
        "host-1".try_into().expect("host id"),
        "0.1.0".to_string(),
    );
    let mut resent = Vec::new();
    for _ in 0..24 {
        assert!(matches!(
            control.reconnect(&mut channel, &mut wire),
            Ok(Event::Welcomed)
        ));
        let frame = first_unacked(&mut wire);
        resent.push((frame["seq"].clone(), frame["message"]["type"].clone()));
        control.read(&mut wire);
    }
    assert_eq!(resent, vec![(json!(1), json!("SnapshotDone")); 24]);
    assert_eq!(channel.stuck(), Some((1, "SnapshotDone", 30)));

    control.taking = true;
    assert!(matches!(
        control.reconnect(&mut channel, &mut wire),
        Ok(Event::Welcomed)
    ));
    control.read(&mut wire);
    assert!(matches!(
        control.reconnect(&mut channel, &mut wire),
        Ok(Event::Welcomed)
    ));
    assert_eq!(control.taken, [(1, "SnapshotDone".to_string())]);
    assert_eq!(channel.stuck(), None);
    let saved = disk.0.borrow().clone().expect("saved state");
    assert!(saved.unacked.is_empty() && saved.replays.is_empty(), "{saved:?}");
}

#[test]
fn the_reconnect_wait_starts_over_only_once_a_frame_arrives() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    let mut backoff = Backoff::default();
    open(&mut channel, &mut wire);
    channel.send(done("snap-1"), &mut wire).expect("send");
    channel.disconnect();
    assert_eq!(backoff.next(channel.progressed(), u16::MAX), Duration::from_secs(1));

    open(&mut channel, &mut wire);
    channel.disconnect();
    assert_eq!(backoff.next(channel.progressed(), u16::MAX), Duration::from_secs(2));

    open(&mut channel, &mut wire);
    channel
        .receive(&frame(1, 1, "snap-2"), &mut wire)
        .expect("ack of the replay");
    channel.disconnect();
    assert_eq!(backoff.next(channel.progressed(), u16::MAX), Duration::from_secs(1));
}

#[test]
fn a_host_that_never_reaches_the_control_plane_keeps_reconnecting_every_thirty_seconds_and_progress_starts_the_wait_over(
) {
    let mut backoff = Backoff::default();
    let gaps: Vec<u64> = (0..100).map(|_| backoff.next(false, u16::MAX).as_secs()).collect();
    assert_eq!(gaps[..6], [1, 2, 4, 8, 16, 30]);
    assert!(gaps[6..].iter().all(|gap| *gap == 30), "{gaps:?}");
    assert_eq!(backoff.next(true, u16::MAX), Duration::from_secs(1));
}

#[test]
fn jitter_takes_up_to_half_off_each_reconnect_wait() {
    let mut backoff = Backoff::default();
    assert_eq!(backoff.next(false, 0), Duration::from_millis(500));
    assert_eq!(backoff.next(false, u16::MAX), Duration::from_secs(2));
    assert_eq!(backoff.next(false, 0), Duration::from_secs(2));
}

fn at(seconds: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(1_795_000_000 + seconds, 0).expect("time")
}

#[test]
fn every_rate_limit_pauses_until_its_retry_after_and_never_gives_up() {
    let mut limit = RateLimit::default();
    let mut now = 0;
    let mut waits = Vec::new();
    for _ in 0..50 {
        let RateBackoff::Until(until) = limit.limited(at(now), at(now + 20), 600, 0) else {
            panic!("a refusal after the pause ended pauses again");
        };
        waits.push((until - at(now)).num_seconds());
        assert_eq!(limit.limited(at(now + 1), at(now + 20), 600, 0), RateBackoff::Paused);
        now = (until - at(0)).num_seconds();
    }
    assert!(waits.iter().all(|wait| *wait == 20), "{waits:?}");
}

#[test]
fn jitter_only_lengthens_a_rate_limit_backoff_by_up_to_half() {
    let mut limit = RateLimit::default();
    let RateBackoff::Until(until) = limit.limited(at(0), at(20), 600, u16::MAX) else {
        panic!("the first refusal backs off");
    };
    assert_eq!((until - at(0)).num_seconds(), 30);
}

#[test]
fn a_rate_limited_host_reports_rate_limited_until_a_full_minute_passes_with_no_refusal_then_healthy() {
    let mut limit = RateLimit::default();
    assert_eq!(limit.health(at(0)), HostReportedHealth::Healthy);
    assert_eq!(limit.limited(at(0), at(20), 600, 0), RateBackoff::Until(at(20)));
    assert_eq!(limit.limited(at(30), at(60), 600, 0), RateBackoff::Until(at(60)));
    assert_eq!(limit.health(at(80)), HostReportedHealth::RateLimited);
    assert!(!limit.recovered(at(89)));
    assert_eq!(limit.health(at(90)), HostReportedHealth::Healthy);
    assert!(limit.recovered(at(90)));
    assert!(!limit.recovered(at(91)), "it turns Healthy once");
    assert_eq!(limit.limited(at(100), at(120), 600, 0), RateBackoff::Until(at(120)));
}

#[test]
fn a_frame_refused_over_the_rate_is_dropped_and_frames_sent_while_withheld_go_out_paced_after_the_pause() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    channel.send(done("snap-1"), &mut wire).expect("send");
    wire.sent();
    let limited = json!({"type": "FrameRateLimited", "seq": 1, "reason": "host host-1 sent more than 600 frames a minute", "framesPerMinute": 600, "retryAfter": "2026-11-21T11:01:00Z"});
    let Ok(Event::FrameRateLimited {
        seq: 1,
        frames_per_minute: 600,
        ..
    }) = channel.receive(&limited.to_string(), &mut wire)
    else {
        panic!("the refusal names the limit");
    };
    assert!(channel.state().unacked.is_empty(), "a refused frame is never resent");
    channel.withhold();
    for snapshot in ["snap-2", "snap-3", "snap-4"] {
        channel.send(done(snapshot), &mut wire).expect("send");
    }
    assert!(
        wire.sent().is_empty(),
        "nothing goes out while the host holds its frames"
    );
    channel.release(2, &mut wire).expect("release");
    let seqs = |wire: &mut Wire| {
        wire.sent()
            .iter()
            .map(|frame| frame["seq"].as_i64().expect("seq"))
            .collect::<Vec<_>>()
    };
    assert_eq!(seqs(&mut wire), [2, 3]);
    assert!(channel.withholding());
    channel.release(2, &mut wire).expect("release");
    assert_eq!(seqs(&mut wire), [4]);
    assert!(!channel.withholding());
    channel.send(done("snap-5"), &mut wire).expect("send");
    assert_eq!(seqs(&mut wire), [5]);
}

#[test]
fn a_backlog_saved_before_a_restart_is_held_at_the_welcome_and_goes_out_paced() {
    let saved: SupervisorChannelState = serde_json::from_value(json!({
        "received": 0,
        "sent": 3,
        "unacked": [
            {"type": "Frame", "seq": 1, "ack": 0, "message": {"type": "TurnDone", "turnId": "turn-1"}},
            {"type": "Frame", "seq": 2, "ack": 0, "message": {"type": "TurnDone", "turnId": "turn-2"}},
            {"type": "Frame", "seq": 3, "ack": 0, "message": {"type": "TurnDone", "turnId": "turn-3"}}
        ]
    }))
    .expect("channel.json");
    let mut channel = HostChannel::new(
        saved,
        Disk::default(),
        Recorder::default(),
        "host-1".try_into().expect("host id"),
        "0.1.0".to_string(),
    );
    assert!(channel.withholding());
    let mut wire = Wire::default();
    let quiet = json!({"type": "Welcome", "hostId": "host-1", "protocolVersion": ChannelProtocol::VERSION});
    let Ok(Event::Welcomed) = welcome(&mut channel, &mut wire, &quiet) else {
        panic!("the welcome opens the channel");
    };
    assert!(
        wire.sent().is_empty(),
        "the welcome replays nothing while the backlog is held"
    );
    channel.release(2, &mut wire).expect("release");
    let seqs = |wire: &mut Wire| {
        wire.sent()
            .iter()
            .map(|frame| frame["seq"].as_i64().expect("seq"))
            .collect::<Vec<_>>()
    };
    assert_eq!(seqs(&mut wire), [1, 2]);
    channel.release(2, &mut wire).expect("release");
    assert_eq!(seqs(&mut wire), [3]);
    assert!(!channel.withholding());
}

fn line(turn: &str, line_seq: i64) -> ChannelHostMessage {
    serde_json::from_value(
        json!({"type": "TurnLine", "turnId": turn, "lineSeq": line_seq, "text": format!("line {line_seq}")}),
    )
    .expect("line")
}

fn claim(turn: &str) -> ChannelHostMessage {
    serde_json::from_value(json!({"type": "TurnClaim", "turnId": turn, "boxId": "box-1"})).expect("claim")
}

fn unacked_seqs(disk: &Disk) -> Vec<i64> {
    disk.0
        .borrow()
        .as_ref()
        .map_or_default(|state| state.unacked.iter().map(|frame| frame.seq.0).collect())
}

#[test]
fn an_offer_is_held_from_its_receipt_until_the_box_or_a_frame_names_its_turn() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    channel.receive(&offer(1, "grant-1"), &mut wire).expect("offer");
    let sent_at = chrono::DateTime::parse_from_rfc3339("2026-11-18T10:00:00+01:00").expect("time");
    let beat = |channel: &mut HostChannel<Disk, Recorder>, wire: &mut Wire, turn_ids: Vec<EntityId>| {
        let held = HeldIds {
            turn_ids,
            ..HeldIds::default()
        };
        channel
            .heartbeat(sent_at, usage(), held, HostReportedHealth::Healthy, wire)
            .expect("heartbeat");
        let [heartbeat] = wire.sent().try_into().expect("one heartbeat");
        heartbeat["heldTurnIds"].clone()
    };
    assert_eq!(beat(&mut channel, &mut wire, Vec::new()), json!(["turn-1"]));
    let turn = EntityId::try_from("turn-1").expect("id");
    assert_eq!(beat(&mut channel, &mut wire, vec![turn]), json!(["turn-1"]));
    assert_eq!(
        beat(&mut channel, &mut wire, Vec::new()),
        json!([]),
        "once the box held the turn and let it go, the offer no longer holds it"
    );
}

#[test]
fn a_socket_closed_mid_turn_keeps_the_turn_held_and_resends_its_unacked_output_after_the_reconnect() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    let mut control = ControlPlane::new(true);
    open(&mut channel, &mut wire);
    channel.receive(&offer(1, "grant-1"), &mut wire).expect("offer");
    channel.send(claim("turn-1"), &mut wire).expect("send");
    control.read(&mut wire);
    channel.send(line("turn-1", 1), &mut wire).expect("send");
    wire.sent();
    channel.disconnect();
    channel.send(line("turn-1", 2), &mut wire).expect("send");
    channel.send(line("turn-1", 3), &mut wire).expect("send");
    assert!(wire.sent().is_empty(), "nothing goes out on a closed socket");
    assert_eq!(unacked_seqs(&disk), [1, 2, 3, 4]);

    channel.connect();
    let challenge = json!({"type": "Challenge", "nonce": NONCE}).to_string();
    let Ok(Event::Challenged(nonce)) = channel.receive(&challenge, &mut wire) else {
        panic!("the challenge asks for a Hello");
    };
    channel
        .hello(&nonce, capacity(), usage(), Vec::new(), &mut wire)
        .expect("hello");
    let [hello] = wire.sent().try_into().expect("one hello");
    assert_eq!(hello["heldTurnIds"], json!(["turn-1"]));
    let Ok(Event::Welcomed) = channel.receive(&control.welcome().to_string(), &mut wire) else {
        panic!("the welcome opens the channel");
    };
    let resent: Vec<(i64, i64)> = wire
        .sent()
        .iter()
        .map(|frame| {
            (
                frame["seq"].as_i64().expect("seq"),
                frame["message"]["lineSeq"].as_i64().expect("line seq"),
            )
        })
        .collect();
    assert_eq!(resent, [(2, 1), (3, 2), (4, 3)]);
    assert_eq!(unacked_seqs(&disk), [2, 3, 4]);
}

#[test]
fn twenty_failed_reconnects_wait_thirty_seconds_apart_and_keep_every_unacked_frame_for_the_next_welcome() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    let mut control = ControlPlane::new(true);
    open(&mut channel, &mut wire);
    channel.send(claim("turn-1"), &mut wire).expect("send");
    control.read(&mut wire);
    channel.disconnect();

    let mut backoff = Backoff::default();
    let mut waited = Duration::ZERO;
    for line_seq in 1..=20 {
        waited += backoff.next(channel.progressed(), u16::MAX);
        channel.connect();
        channel.send(line("turn-1", line_seq), &mut wire).expect("send");
        channel.disconnect();
    }
    assert_eq!(waited, Duration::from_secs(1 + 2 + 4 + 8 + 16 + 30 * 15));
    assert_eq!(wire.sent(), Vec::<Value>::new());
    assert_eq!(unacked_seqs(&disk), (1..=21).collect::<Vec<_>>());

    let Ok(Event::Welcomed) = control.reconnect(&mut channel, &mut wire) else {
        panic!("the twenty-first reconnect is welcomed");
    };
    let resent: Vec<i64> = wire
        .sent()
        .iter()
        .map(|frame| frame["message"]["lineSeq"].as_i64().expect("line seq"))
        .collect();
    assert_eq!(resent, (1..=20).collect::<Vec<_>>());
    let sent_at = chrono::DateTime::parse_from_rfc3339("2026-11-18T10:00:00+01:00").expect("time");
    channel
        .heartbeat(
            sent_at,
            usage(),
            HeldIds::default(),
            HostReportedHealth::Healthy,
            &mut wire,
        )
        .expect("heartbeat");
    let [heartbeat] = wire.sent().try_into().expect("one heartbeat");
    assert_eq!(heartbeat["heldTurnIds"], json!(["turn-1"]));
}

#[test]
fn a_host_rate_limited_for_five_windows_holds_its_output_until_each_retry_after_then_sends_it() {
    let disk = Disk::default();
    let mut channel = channel(&disk, &Recorder::default());
    let mut wire = Wire::default();
    open(&mut channel, &mut wire);
    let sent_at = chrono::DateTime::parse_from_rfc3339("2026-11-18T10:00:00+01:00").expect("time");
    let mut limit = RateLimit::default();
    let mut released = Vec::new();
    let mut now = 0;
    let mut line_seq = 0;
    for _ in 0..5 {
        line_seq += 1;
        channel.send(line("turn-1", line_seq), &mut wire).expect("send");
        let [refused] = wire.sent().try_into().expect("one frame");
        let limited = json!({"type": "FrameRateLimited", "seq": refused["seq"], "reason": "host host-1 sent more than 600 frames a minute", "framesPerMinute": 600, "retryAfter": at(now + 60).to_rfc3339()});
        let Ok(Event::FrameRateLimited { retry_after, .. }) = channel.receive(&limited.to_string(), &mut wire) else {
            panic!("the refusal names the limit");
        };
        assert_eq!(
            limit.limited(at(now), retry_after, 600, 0),
            RateBackoff::Until(at(now + 60))
        );
        channel.withhold();
        for _ in 0..3 {
            line_seq += 1;
            channel.send(line("turn-1", line_seq), &mut wire).expect("send");
        }
        let held = HeldIds {
            turn_ids: vec![EntityId::try_from("turn-1").expect("id")],
            ..HeldIds::default()
        };
        channel
            .heartbeat(sent_at, usage(), held, limit.health(at(now + 30)), &mut wire)
            .expect("heartbeat");
        let [heartbeat] = wire.sent().try_into().expect("only the heartbeat while paused");
        assert_eq!(heartbeat["health"], "RateLimited");
        assert_eq!(heartbeat["heldTurnIds"], json!(["turn-1"]));
        now += 60;
        assert!(!limit.paused(at(now)));
        channel.release(usize::MAX, &mut wire).expect("release");
        released.extend(
            wire.sent()
                .iter()
                .map(|frame| frame["message"]["lineSeq"].as_i64().expect("line seq")),
        );
    }
    assert_eq!(released, [2, 3, 4, 6, 7, 8, 10, 11, 12, 14, 15, 16, 18, 19, 20]);
    assert_eq!(
        unacked_seqs(&disk),
        [2, 3, 4, 6, 7, 8, 10, 11, 12, 14, 15, 16, 18, 19, 20]
    );
}
