use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tumpel_api::frozen::{v37, v38};
use tumpel_api::types::{
    ChannelControlEnvelope, ChannelControlFrame, ChannelControlMessage, ChannelFrameRateLimited, ChannelFrameRefused,
    ChannelHello, ChannelHelloType, ChannelHostHeartbeat, ChannelHostHeartbeatType, ChannelHostMessage,
    ChannelProtocol, ChannelSnapshotFailed, ChannelStoredHostFrame, ChannelStoredHostFrameType, ChannelTurnLines,
    ChannelTurnLinesType, ChannelWelcome, EntityId, HostReportedHealth, HostUsage, MetricBatch, Sequence,
    SupervisorChannelState,
};

pub const HEARTBEAT_DEADLINE: Duration = Duration::from_secs(30);
pub const FIRST_BACKOFF: Duration = Duration::from_secs(1);
pub const LAST_BACKOFF: Duration = Duration::from_secs(30);
pub const RATE_WINDOW: Duration = Duration::from_secs(60);
const ASSUMED_FRAMES_PER_MINUTE: u32 = 600;
pub const PACK_REUSE: u16 = 39;
pub const TURN_LINES_MAX_LINES: usize = 200;
pub const TURN_LINES_MAX_BYTES: usize = 256_000;
pub const TURN_LINES_WAIT: Duration = Duration::from_secs(2);

pub const UPDATE_REQUIRED: &str =
    "the control plane sent something this supervisor doesn't recognise; update the supervisor";

pub fn update_required() {
    if tracing::dispatcher::has_been_set() {
        tracing::warn!(target: "tumpel::compatibility", "{UPDATE_REQUIRED}");
        return;
    }
    eprintln!("{UPDATE_REQUIRED}");
}

const HELLO_CONTEXT: &[u8] = b"tumpel-channel-hello-v1";

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeldIds {
    pub session_ids: Vec<EntityId>,
    pub claude_setup_ids: Vec<EntityId>,
    pub turn_ids: Vec<EntityId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Usage {
    pub host: HostUsage,
}

/// # Errors
/// Fails when channel.json names a seq below zero.
pub fn resend_from(state: &SupervisorChannelState) -> Result<NonZeroU64, ChannelError> {
    let next = state.unacked.first().map_or(state.sent.0 + 1, |first| first.seq.0);
    u64::try_from(next)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| ChannelError::Store(format!("channel.json resends from seq {next}, below 1")))
}

fn legacy_line(message: &Value) -> Result<Option<Value>, ChannelError> {
    if message.get("type").and_then(Value::as_str) != Some("TurnLine") {
        return Ok(None);
    }
    let line: v37::types::ChannelTurnLine = serde_json::from_value(message.clone())
        .map_err(|error| ChannelError::Store(format!("channel.json holds a TurnLine that does not parse: {error}")))?;
    let first_line_seq = u64::try_from(line.line_seq.0)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| {
            ChannelError::Store(format!(
                "channel.json holds line {} of turn {} with no positive lineSeq",
                line.line_seq.0, *line.turn_id
            ))
        })?;
    let text = serde_json::to_value(&line.text)
        .and_then(serde_json::from_value)
        .map_err(|error| ChannelError::Store(format!("channel.json holds a TurnLine that does not parse: {error}")))?;
    let turn_id = EntityId::try_from(line.turn_id.as_str())
        .map_err(|error| ChannelError::Store(format!("channel.json holds a TurnLine that does not parse: {error}")))?;
    to_value(&ChannelHostMessage::TurnLines(ChannelTurnLines {
        first_line_seq,
        lines: vec![text],
        turn_id,
        type_: ChannelTurnLinesType::TurnLines,
    }))
    .map(Some)
}

fn batch_bytes(lines: &[Value]) -> usize {
    lines.iter().filter_map(Value::as_str).map(str::len).sum()
}

fn batch_full(batch: &Value) -> bool {
    batch
        .get("lines")
        .and_then(Value::as_array)
        .is_some_and(|lines| lines.len() >= TURN_LINES_MAX_LINES || batch_bytes(lines) >= TURN_LINES_MAX_BYTES)
}

fn append_lines(batch: &mut Value, added: &ChannelTurnLines) -> bool {
    if !is_turn_lines(batch) || batch.get("turnId").and_then(Value::as_str) != Some(added.turn_id.as_str()) {
        return false;
    }
    let Some(first) = batch.get("firstLineSeq").and_then(Value::as_u64) else {
        return false;
    };
    let Some(lines) = batch.get_mut("lines").and_then(Value::as_array_mut) else {
        return false;
    };
    let next = u64::try_from(lines.len()).map_or(u64::MAX, |count| first.saturating_add(count));
    let bytes: usize = added.lines.iter().map(|line| line.len()).sum();
    if next != added.first_line_seq.get()
        || lines.len() + added.lines.len() > TURN_LINES_MAX_LINES
        || batch_bytes(lines) + bytes > TURN_LINES_MAX_BYTES
    {
        return false;
    }
    lines.extend(added.lines.iter().map(|line| Value::String(line.to_string())));
    true
}

#[derive(Debug)]
pub enum Outgoing {
    Message(ChannelHostMessage),
    Snapshot(ChannelHostMessage, u16),
}

impl From<ChannelHostMessage> for Outgoing {
    fn from(message: ChannelHostMessage) -> Self {
        Self::Message(message)
    }
}

fn reuses_packs(version: u16) -> bool {
    version >= PACK_REUSE
}

fn built_for(message: &Value) -> Option<u16> {
    let field = match message.get("type").and_then(Value::as_str) {
        Some("SnapshotPacks") => "reusedPackDigests",
        Some("SnapshotDone") => "chunkCount",
        _ => return None,
    };
    Some(if message.get(field).is_some() {
        PACK_REUSE
    } else {
        PACK_REUSE - 1
    })
}

fn build_snapshot(message: &ChannelHostMessage, version: u16) -> Result<Value, ChannelError> {
    let what = format!("protocol {version} frame");
    match message {
        ChannelHostMessage::SnapshotPacks(_) | ChannelHostMessage::SnapshotDone(_) if reuses_packs(version) => {
            to_value(message)
        }
        ChannelHostMessage::SnapshotPacks(packs) if packs.reused_pack_digests.is_empty() => {
            frozen::<v38::types::ChannelSnapshotPacks>(to_value(message)?, &what)
        }
        ChannelHostMessage::SnapshotDone(_) => frozen::<v38::types::ChannelSnapshotDone>(to_value(message)?, &what),
        _ => Err(protocol(format!(
            "a {what} must be a SnapshotPacks or SnapshotDone, and below protocol {PACK_REUSE} it names no reused packs"
        ))),
    }
}

fn snapshot_id(message: &Value) -> Option<EntityId> {
    let snapshot_id = message.get("snapshotId").and_then(Value::as_str)?;
    EntityId::try_from(snapshot_id.to_string()).ok()
}

fn mismatched(message: &Value, built: u16, session: u16) -> Result<Value, ChannelError> {
    let kind = message.get("type").and_then(Value::as_str).unwrap_or_default();
    let failed = serde_json::json!({
        "type": "SnapshotFailed",
        "snapshotId": message.get("snapshotId").cloned().unwrap_or(Value::Null),
        "reason": "NotAnswered",
        "failedAt": Utc::now().fixed_offset(),
        "detail": format!("this host built the snapshot's {kind} for channel protocol {built}, but the session speaks {session}; ask for the snapshot again"),
    });
    let failed: ChannelSnapshotFailed = serde_json::from_value(failed)
        .map_err(|error| protocol(format!("failing a {kind} built for protocol {built}: {error}")))?;
    serde_json::to_value(&failed).map_err(|error| protocol(format!("encoding a SnapshotFailed: {error}")))
}

fn understood(message: &ChannelControlMessage) -> bool {
    use tumpel_api::types::{BoxRetention, ChannelBootImage, ClaudeSetupHarness, Harness};
    match message {
        ChannelControlMessage::Unrecognised(_) => false,
        ChannelControlMessage::TurnOffer(offer) => {
            !matches!(offer.harness, Harness::Unrecognised(_))
                && !matches!(offer.box_retention, BoxRetention::Unrecognised(_))
                && !matches!(offer.image, ChannelBootImage::Unrecognised(_))
        }
        ChannelControlMessage::ClaudeSetupBoxOffer(offer) => {
            !matches!(offer.harness, Some(ClaudeSetupHarness::Unrecognised(_)))
                && !matches!(offer.image, ChannelBootImage::Unrecognised(_))
        }
        _ => true,
    }
}

fn is_turn_lines(message: &Value) -> bool {
    message.get("type").and_then(Value::as_str) == Some("TurnLines")
}

fn to_value(message: &ChannelHostMessage) -> Result<Value, ChannelError> {
    serde_json::to_value(message).map_err(|error| protocol(format!("encoding a message for channel.json: {error}")))
}

#[must_use]
pub fn hello_bytes(nonce: &str, host_id: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HELLO_CONTEXT.len() + nonce.len() + host_id.len() + 2);
    bytes.extend_from_slice(HELLO_CONTEXT);
    bytes.push(0);
    bytes.extend_from_slice(nonce.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(host_id.as_bytes());
    bytes
}

#[must_use]
pub fn empty_state() -> SupervisorChannelState {
    SupervisorChannelState {
        received: Sequence(0),
        sent: Sequence(0),
        unacked: Vec::new(),
    }
}

#[derive(Debug)]
pub enum ChannelError {
    Protocol(String),
    Port(String),
    Store(String),
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(reason) => write!(f, "channel protocol error: {reason}"),
            Self::Port(reason) => write!(f, "sending on the channel failed: {reason}"),
            Self::Store(reason) => write!(f, "saving the channel state failed: {reason}"),
        }
    }
}

impl std::error::Error for ChannelError {}

pub trait Port {
    /// # Errors
    /// Fails when the transport cannot take the message.
    fn send(&mut self, text: String) -> Result<(), ChannelError>;
}

pub trait Store {
    /// # Errors
    /// Fails when the state cannot be written durably.
    fn save(&mut self, state: &SupervisorChannelState) -> Result<(), ChannelError>;
}

pub trait Signer {
    fn sign(&self, bytes: &[u8]) -> String;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    AwaitingChallenge,
    AwaitingHello,
    AwaitingWelcome,
    Open,
    Closed,
}

pub struct Backoff {
    wait: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self { wait: FIRST_BACKOFF }
    }
}

impl Backoff {
    pub fn next(&mut self, progressed: bool, jitter: u16) -> Duration {
        if progressed {
            self.wait = FIRST_BACKOFF;
        }
        let half = self.wait / 2;
        let wait = half + half.mul_f64(f64::from(jitter) / f64::from(u16::MAX));
        self.wait = (self.wait * 2).min(LAST_BACKOFF);
        wait
    }
}

#[derive(Default)]
pub struct RateLimit {
    paused_until: Option<DateTime<Utc>>,
    last_limited: Option<DateTime<Utc>>,
    frames_per_minute: u32,
}

#[derive(Debug, Eq, PartialEq)]
pub enum RateBackoff {
    Until(DateTime<Utc>),
    Paused,
}

impl RateLimit {
    pub fn limited(
        &mut self,
        now: DateTime<Utc>,
        retry_after: DateTime<Utc>,
        frames_per_minute: u32,
        jitter: u16,
    ) -> RateBackoff {
        self.last_limited = Some(now);
        self.frames_per_minute = frames_per_minute;
        let current = self.paused_until.filter(|_| self.paused(now));
        if current.is_some_and(|until| until >= retry_after) {
            return RateBackoff::Paused;
        }
        let wait = (retry_after - now).to_std().unwrap_or_default().max(FIRST_BACKOFF);
        let jittered = wait + (wait / 2).mul_f64(f64::from(jitter) / f64::from(u16::MAX));
        let until = now + chrono::Duration::from_std(jittered).unwrap_or(chrono::Duration::MAX);
        let until = current.map_or(until, |current| current.max(until));
        self.paused_until = Some(until);
        RateBackoff::Until(until)
    }

    #[must_use]
    pub fn paused(&self, now: DateTime<Utc>) -> bool {
        self.paused_until.is_some_and(|until| now < until)
    }

    #[must_use]
    pub fn release_budget(&self, tick: Duration) -> usize {
        let limit = if self.frames_per_minute == 0 {
            ASSUMED_FRAMES_PER_MINUTE
        } else {
            self.frames_per_minute
        };
        let per_tick = u128::from(limit / 2) * tick.as_millis() / RATE_WINDOW.as_millis();
        usize::try_from(per_tick).unwrap_or(usize::MAX).max(1)
    }

    #[must_use]
    pub fn health(&self, now: DateTime<Utc>) -> HostReportedHealth {
        let window = chrono::Duration::from_std(RATE_WINDOW).unwrap_or(chrono::Duration::MAX);
        let recent = self.last_limited.is_some_and(|at| now < at + window);
        if recent || self.paused(now) {
            return HostReportedHealth::RateLimited;
        }
        HostReportedHealth::Healthy
    }
}

#[derive(Debug)]
pub enum Event {
    None,
    Challenged(String),
    Welcomed,
    Heartbeat,
    ReleasePublished,
    FrameRefused {
        seq: i64,
        reason: String,
    },
    FrameRateLimited {
        seq: i64,
        reason: String,
        frames_per_minute: u32,
        retry_after: DateTime<Utc>,
    },
    Message(Box<ChannelControlMessage>),
}

pub struct HostChannel<S: Store, K: Signer> {
    state: SupervisorChannelState,
    store: S,
    signer: K,
    host_id: EntityId,
    supervisor_version: String,
    phase: Phase,
    progressed: bool,
    withheld: Option<i64>,
    offered: Vec<EntityId>,
    hello_version: u16,
    offering: u16,
    version: u16,
    sealed: i64,
    batch_started: Option<Instant>,
    accepted: Vec<EntityId>,
    failed: Vec<EntityId>,
}

fn snapshot_done(message: &Value) -> Option<EntityId> {
    v38::types::ChannelSnapshotDone::deserialize(message)
        .ok()
        .and_then(|done| EntityId::try_from(done.snapshot_id.to_string()).ok())
}

fn frozen<F: DeserializeOwned + Serialize>(value: Value, what: &str) -> Result<Value, ChannelError> {
    let frozen: F = serde_json::from_value(value).map_err(|error| protocol(format!("encoding a {what}: {error}")))?;
    serde_json::to_value(&frozen).map_err(|error| protocol(format!("encoding a {what}: {error}")))
}

fn encode(report: &impl Serialize, kind: &str) -> Result<String, ChannelError> {
    serde_json::to_string(report).map_err(|error| protocol(format!("encoding a {kind}: {error}")))
}

fn protocol(reason: impl Into<String>) -> ChannelError {
    ChannelError::Protocol(reason.into())
}

impl<S: Store, K: Signer> HostChannel<S, K> {
    pub fn new(
        state: SupervisorChannelState,
        store: S,
        signer: K,
        host_id: EntityId,
        supervisor_version: String,
    ) -> Self {
        let sealed = state.sent.0;
        Self {
            state,
            store,
            signer,
            host_id,
            supervisor_version,
            phase: Phase::Closed,
            progressed: false,
            withheld: None,
            accepted: Vec::new(),
            failed: Vec::new(),
            offered: Vec::new(),
            hello_version: ChannelProtocol::VERSION,
            offering: ChannelProtocol::VERSION,
            version: ChannelProtocol::VERSION,
            sealed,
            batch_started: None,
        }
    }

    #[must_use]
    pub fn phase(&self) -> Phase {
        self.phase
    }

    #[must_use]
    pub fn state(&self) -> &SupervisorChannelState {
        &self.state
    }

    #[must_use]
    pub fn progressed(&self) -> bool {
        self.progressed
    }

    pub fn connect(&mut self) {
        self.phase = Phase::AwaitingChallenge;
        self.progressed = false;
    }

    pub fn disconnect(&mut self) {
        self.phase = Phase::Closed;
    }

    #[must_use]
    pub fn version(&self) -> u16 {
        self.version
    }

    pub fn step_down(&mut self) -> Option<u16> {
        if self.offering <= ChannelProtocol::OLDEST {
            return None;
        }
        self.hello_version = self.offering - 1;
        Some(self.hello_version)
    }

    pub fn step_up(&mut self) {
        self.hello_version = ChannelProtocol::VERSION;
    }

    pub fn take_accepted(&mut self) -> Vec<EntityId> {
        std::mem::take(&mut self.accepted)
    }

    pub fn take_failed(&mut self) -> Vec<EntityId> {
        std::mem::take(&mut self.failed)
    }

    /// # Errors
    /// Fails when the state cannot be saved or the port refuses the frame.
    pub fn send(&mut self, outgoing: impl Into<Outgoing>, port: &mut impl Port) -> Result<(), ChannelError> {
        match outgoing.into() {
            Outgoing::Message(ChannelHostMessage::TurnLines(lines)) => self.store_lines(lines)?,
            Outgoing::Message(ChannelHostMessage::SnapshotPacks(_) | ChannelHostMessage::SnapshotDone(_)) => {
                return Err(protocol(
                    "a snapshot frame reached the channel built for no protocol version",
                ));
            }
            Outgoing::Message(message) => self.push(to_value(&message)?),
            Outgoing::Snapshot(message, built) => {
                let message = build_snapshot(&message, built)?;
                let message = if self.phase == Phase::Open && reuses_packs(built) != reuses_packs(self.version) {
                    self.failed.extend(snapshot_id(&message));
                    mismatched(&message, built, self.version)?
                } else {
                    message
                };
                self.push(message);
            }
        }
        self.store.save(&self.state)?;
        self.transmit_due(Instant::now(), port)
    }

    fn push(&mut self, message: Value) {
        if let Some(turn_id) = message.get("turnId").and_then(Value::as_str) {
            self.offered.retain(|offered| **offered != *turn_id);
        }
        let seq = self.state.sent.0 + 1;
        self.state.sent = Sequence(seq);
        self.state.unacked.push(ChannelStoredHostFrame {
            ack: Sequence(self.state.received.0),
            message,
            seq: Sequence(seq),
            type_: ChannelStoredHostFrameType::Frame,
        });
    }

    fn unsent(&self) -> impl DoubleEndedIterator<Item = &ChannelStoredHostFrame> {
        let sealed = self.sealed;
        self.state.unacked.iter().filter(move |frame| frame.seq.0 > sealed)
    }

    fn store_lines(&mut self, lines: ChannelTurnLines) -> Result<(), ChannelError> {
        let sealed = self.sealed;
        let tail_empty = self.unsent().next().is_none();
        let appended = self
            .state
            .unacked
            .iter_mut()
            .rev()
            .take_while(|frame| frame.seq.0 > sealed && is_turn_lines(&frame.message))
            .find(|frame| frame.message.get("turnId").and_then(Value::as_str) == Some(lines.turn_id.as_str()))
            .is_some_and(|frame| append_lines(&mut frame.message, &lines));
        if appended {
            return Ok(());
        }
        self.push(to_value(&ChannelHostMessage::TurnLines(lines))?);
        if tail_empty {
            self.batch_started = Some(Instant::now());
        }
        Ok(())
    }

    fn holding_lines(&self, now: Instant) -> bool {
        let mut turns: Vec<&str> = Vec::new();
        let mut tail = self.unsent().peekable();
        tail.peek().is_some()
            && tail.all(|frame| {
                let open = is_turn_lines(&frame.message) && !batch_full(&frame.message);
                let turn = frame.message.get("turnId").and_then(Value::as_str).unwrap_or_default();
                let first_of_turn = !turns.contains(&turn);
                turns.push(turn);
                open && first_of_turn
            })
            && self
                .batch_started
                .is_some_and(|started| now.saturating_duration_since(started) < TURN_LINES_WAIT)
    }

    fn wire(&self, frame: &ChannelStoredHostFrame) -> Result<String, ChannelError> {
        let sent = ChannelStoredHostFrame {
            ack: Sequence(self.state.received.0),
            ..frame.clone()
        };
        serde_json::to_string(&sent).map_err(|error| protocol(format!("encoding a stored frame: {error}")))
    }

    /// # Errors
    /// Fails when the port refuses a frame.
    pub fn transmit_due(&mut self, now: Instant, port: &mut impl Port) -> Result<(), ChannelError> {
        if self.phase != Phase::Open || self.withheld.is_some() {
            return Ok(());
        }
        if self.holding_lines(now) {
            return Ok(());
        }
        let due: Vec<(i64, String)> = self
            .unsent()
            .map(|frame| Ok((frame.seq.0, self.wire(frame)?)))
            .collect::<Result<_, ChannelError>>()?;
        for (seq, text) in due {
            port.send(text)?;
            self.sealed = self.sealed.max(seq);
        }
        Ok(())
    }

    #[must_use]
    pub fn withholding(&self) -> bool {
        self.withheld.is_some()
    }

    /// # Errors
    /// Fails when the port refuses a frame.
    pub fn release(&mut self, budget: usize, port: &mut impl Port) -> Result<(), ChannelError> {
        let Some(from) = self.withheld else {
            return Ok(());
        };
        if self.phase != Phase::Open {
            return Ok(());
        }
        let waiting: Vec<(i64, String)> = self
            .state
            .unacked
            .iter()
            .filter(|own| own.seq.0 >= from)
            .take(budget.saturating_add(1))
            .map(|frame| Ok((frame.seq.0, self.wire(frame)?)))
            .collect::<Result<_, ChannelError>>()?;
        for (seq, text) in waiting.iter().take(budget) {
            port.send(text.clone())?;
            self.sealed = self.sealed.max(*seq);
        }
        self.withheld = waiting.get(budget).map(|(seq, _)| *seq);
        Ok(())
    }

    fn unacked_turns(&self) -> Vec<EntityId> {
        self.state
            .unacked
            .iter()
            .filter_map(|frame| frame.message.get("turnId").and_then(Value::as_str))
            .filter_map(|turn_id| EntityId::try_from(turn_id).ok())
            .collect()
    }

    fn held_turns(&mut self, mut held: Vec<EntityId>) -> Vec<EntityId> {
        self.offered.retain(|offered| !held.contains(offered));
        for turn_id in self.unacked_turns().into_iter().chain(self.offered.iter().cloned()) {
            if !held.contains(&turn_id) {
                held.push(turn_id);
            }
        }
        held
    }

    /// # Errors
    /// Fails when the port refuses the heartbeat.
    pub fn heartbeat(
        &mut self,
        now: DateTime<FixedOffset>,
        usage: Usage,
        held: HeldIds,
        health: HostReportedHealth,
        telemetry: impl FnOnce() -> MetricBatch,
        port: &mut impl Port,
    ) -> Result<(), ChannelError> {
        let telemetry: MetricBatch = telemetry();
        let heartbeat = ChannelHostHeartbeat {
            ack: Sequence(self.state.received.0),
            health: Some(health),
            held_claude_setup_ids: held.claude_setup_ids,
            held_session_ids: held.session_ids,
            held_turn_ids: self.held_turns(held.turn_ids),
            sent_at: now,
            telemetry,
            type_: ChannelHostHeartbeatType::Heartbeat,
            usage: usage.host,
        };
        port.send(encode(&heartbeat, "Heartbeat")?)
    }

    /// # Errors
    /// Fails when no Challenge is waiting for its Hello, or the port refuses the Hello.
    pub fn hello(
        &mut self,
        nonce: &str,
        box_capacity: NonZeroU32,
        usage: Usage,
        held_turn_ids: Vec<EntityId>,
        port: &mut impl Port,
    ) -> Result<(), ChannelError> {
        if self.phase != Phase::AwaitingHello {
            return Err(protocol(format!(
                "a Hello is due after a Challenge, not in phase {:?}",
                self.phase
            )));
        }
        self.offering = self
            .state
            .unacked
            .iter()
            .filter_map(|frame| built_for(&frame.message))
            .fold(self.hello_version, u16::min);
        let hello = ChannelHello {
            box_capacity,
            channel_protocol_oldest: NonZeroU32::new(u32::from(ChannelProtocol::OLDEST)),
            guest_protocol: None,
            held_turn_ids: self.held_turns(held_turn_ids),
            host_id: self.host_id.clone(),
            protocol_version: NonZeroU32::new(u32::from(self.offering))
                .ok_or_else(|| protocol("protocol version 0"))?,
            resend_from: resend_from(&self.state)?,
            signature: self
                .signer
                .sign(&hello_bytes(nonce, &self.host_id))
                .try_into()
                .map_err(|error| protocol(format!("signature: {error}")))?,
            supervisor_version: self
                .supervisor_version
                .clone()
                .try_into()
                .map_err(|error| protocol(format!("supervisor version: {error}")))?,
            type_: ChannelHelloType::Hello,
            usage: usage.host,
        };
        port.send(encode(&hello, "Hello")?)?;
        self.phase = Phase::AwaitingWelcome;
        Ok(())
    }

    /// # Errors
    /// Fails on a message the protocol does not allow in the current phase, a frame gap, or when
    /// the state cannot be saved. The connection must be closed then.
    pub fn receive(&mut self, text: &str) -> Result<Event, ChannelError> {
        if self.phase == Phase::Closed {
            return Ok(Event::None);
        }
        let envelope: ChannelControlEnvelope = serde_json::from_str(text)
            .map_err(|error| protocol(format!("the message matches no channel envelope: {error}")))?;
        let outcome = self.handle(envelope);
        if outcome.is_err() {
            self.phase = Phase::Closed;
        }
        outcome
    }

    fn handle(&mut self, envelope: ChannelControlEnvelope) -> Result<Event, ChannelError> {
        match (self.phase, envelope) {
            (_, ChannelControlEnvelope::Unrecognised(_)) => {
                update_required();
                Ok(Event::None)
            }
            (Phase::AwaitingChallenge, ChannelControlEnvelope::Challenge(challenge)) => {
                self.phase = Phase::AwaitingHello;
                Ok(Event::Challenged(challenge.nonce.to_string()))
            }
            (Phase::AwaitingChallenge, other) => {
                Err(protocol(format!("expected Challenge first, got {}", kind(&other))))
            }
            (Phase::AwaitingWelcome, ChannelControlEnvelope::Welcome(welcome)) => self.welcomed(&welcome),
            (Phase::Open, ChannelControlEnvelope::ControlFrame(frame)) => self.accept(frame),
            (Phase::AwaitingWelcome | Phase::Open, ChannelControlEnvelope::Heartbeat(_)) => Ok(Event::Heartbeat),
            (Phase::Open, ChannelControlEnvelope::ReleasePublished(_)) => Ok(Event::ReleasePublished),
            (Phase::Open, ChannelControlEnvelope::FrameRefused(refused)) => self.refused(refused),
            (Phase::Open, ChannelControlEnvelope::FrameRateLimited(limited)) => Ok(self.rate_limited(limited)),
            (Phase::Open, ChannelControlEnvelope::Ack(ack)) => {
                self.drop_acked(ack.seq.0)?;
                self.store.save(&self.state)?;
                Ok(Event::None)
            }
            (phase, other) => Err(protocol(format!("{} arrived in phase {phase:?}", kind(&other)))),
        }
    }

    fn welcomed(&mut self, welcome: &ChannelWelcome) -> Result<Event, ChannelError> {
        let named = welcome.protocol_version.get();
        self.version = u16::try_from(named)
            .ok()
            .filter(|version| (ChannelProtocol::OLDEST..=self.offering).contains(version))
            .ok_or_else(|| {
                protocol(format!(
                    "the Welcome names protocol {named}, outside the {} to {} this Hello offered",
                    ChannelProtocol::OLDEST,
                    self.offering
                ))
            })?;
        self.reshape()?;
        self.store.save(&self.state)?;
        self.withheld = self.state.unacked.first().map(|first| first.seq.0);
        self.phase = Phase::Open;
        Ok(Event::Welcomed)
    }

    fn reshape(&mut self) -> Result<(), ChannelError> {
        for frame in &mut self.state.unacked {
            if let Some(batch) = legacy_line(&frame.message)? {
                frame.message = batch;
            }
            if let Some(built) =
                built_for(&frame.message).filter(|built| reuses_packs(*built) != reuses_packs(self.version))
            {
                self.failed.extend(snapshot_id(&frame.message));
                frame.message = mismatched(&frame.message, built, self.version)?;
            }
        }
        Ok(())
    }

    fn drop_acked(&mut self, ack: i64) -> Result<(), ChannelError> {
        if ack > self.sealed {
            return Err(protocol(format!(
                "an ack for seq {ack} arrived, but this host has sent only up to seq {}",
                self.sealed
            )));
        }
        if self.state.unacked.first().is_some_and(|first| first.seq.0 <= ack) {
            self.progressed = true;
        }
        let retired = self.state.unacked.iter().filter(|own| own.seq.0 <= ack);
        let accepted: Vec<EntityId> = retired.filter_map(|own| snapshot_done(&own.message)).collect();
        self.accepted.extend(accepted);
        self.state.unacked.retain(|own| own.seq.0 > ack);
        Ok(())
    }

    fn refused(&mut self, refused: ChannelFrameRefused) -> Result<Event, ChannelError> {
        self.progressed = true;
        self.state.unacked.retain(|own| own.seq.0 != refused.seq.0);
        self.store.save(&self.state)?;
        Ok(Event::FrameRefused {
            seq: refused.seq.0,
            reason: String::from(refused.reason),
        })
    }

    fn rate_limited(&mut self, limited: ChannelFrameRateLimited) -> Event {
        self.progressed = true;
        self.withheld = Some(self.withheld.map_or(limited.seq.0, |from| from.min(limited.seq.0)));
        Event::FrameRateLimited {
            seq: limited.seq.0,
            reason: String::from(limited.reason),
            frames_per_minute: limited.frames_per_minute.get(),
            retry_after: limited.retry_after.with_timezone(&Utc),
        }
    }

    fn accept(&mut self, frame: ChannelControlFrame) -> Result<Event, ChannelError> {
        let received = self.state.received.0;
        if frame.seq.0 <= received {
            return Ok(Event::None);
        }
        if frame.seq.0 != received + 1 {
            return Err(protocol(format!(
                "frame gap: expected seq {}, got {}",
                received + 1,
                frame.seq.0
            )));
        }
        self.drop_acked(frame.ack.0)?;
        self.state.received = Sequence(frame.seq.0);
        self.progressed = true;
        self.store.save(&self.state)?;
        if !understood(&frame.message) {
            update_required();
            return Ok(Event::None);
        }
        match &frame.message {
            ChannelControlMessage::TurnOffer(offer) if !self.offered.contains(&offer.turn_id) => {
                self.offered.push(offer.turn_id.clone());
            }
            ChannelControlMessage::TurnWithdrawn(withdrawn) => {
                self.offered.retain(|offered| *offered != withdrawn.turn_id);
            }
            _ => {}
        }
        Ok(Event::Message(Box::new(frame.message)))
    }
}

fn kind(envelope: &ChannelControlEnvelope) -> &'static str {
    match envelope {
        ChannelControlEnvelope::Challenge(_) => "Challenge",
        ChannelControlEnvelope::Welcome(_) => "Welcome",
        ChannelControlEnvelope::Heartbeat(_) => "Heartbeat",
        ChannelControlEnvelope::ReleasePublished(_) => "ReleasePublished",
        ChannelControlEnvelope::FrameRefused(_) => "FrameRefused",
        ChannelControlEnvelope::FrameRateLimited(_) => "FrameRateLimited",
        ChannelControlEnvelope::Ack(_) => "Ack",
        ChannelControlEnvelope::ControlFrame(_) => "Frame",
        ChannelControlEnvelope::Unrecognised(_) => "an unrecognised message",
    }
}

#[cfg(test)]
mod tests;
