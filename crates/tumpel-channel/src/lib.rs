use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, Utc};
use serde_json::Value;
use tumpel_api::types::{
    ChannelControlEnvelope, ChannelControlFrame, ChannelControlMessage, ChannelFrameRefused, ChannelHello,
    ChannelHelloType, ChannelHostEnvelope, ChannelHostFrame, ChannelHostFrameType, ChannelHostHeartbeat,
    ChannelHostHeartbeatType, ChannelHostMessage, ChannelProtocol, ChannelSnapshotDone, ChannelSnapshotFailed,
    ChannelSnapshotFailedType, ChannelSnapshotFailureReason, ChannelStoredHostFrame, ChannelStoredHostFrameType,
    EntityId, HostUsage, Sequence, SupervisorChannelReplay, SupervisorChannelState,
};

pub const HEARTBEAT_DEADLINE: Duration = Duration::from_secs(30);
pub const FIRST_BACKOFF: Duration = Duration::from_secs(1);
pub const LAST_BACKOFF: Duration = Duration::from_secs(30);
pub const MAX_REPLAYS: u32 = 10;
pub const MAX_RECONNECTS: u32 = 10;

const HELLO_CONTEXT: &[u8] = b"tumpel-channel-hello-v1";

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
        replays: Vec::new(),
    }
}

#[derive(Debug)]
pub enum ChannelError {
    Protocol(String),
    Port(String),
    Store(String),
    Undelivered(String),
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(reason) => write!(f, "channel protocol error: {reason}"),
            Self::Port(reason) => write!(f, "sending on the channel failed: {reason}"),
            Self::Store(reason) => write!(f, "saving the channel state failed: {reason}"),
            Self::Undelivered(reason) => f.write_str(reason),
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
    fruitless: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            wait: FIRST_BACKOFF,
            fruitless: 0,
        }
    }
}

impl Backoff {
    pub fn next(&mut self, progressed: bool, jitter: u16) -> Option<Duration> {
        if progressed {
            self.fruitless = 0;
            self.wait = FIRST_BACKOFF;
        }
        self.fruitless += 1;
        if self.fruitless > MAX_RECONNECTS {
            return None;
        }
        let half = self.wait / 2;
        let wait = half + half.mul_f64(f64::from(jitter) / f64::from(u16::MAX));
        self.wait = (self.wait * 2).min(LAST_BACKOFF);
        Some(wait)
    }
}

#[derive(Debug)]
pub enum Event {
    None,
    Challenged(String),
    Welcomed,
    Heartbeat,
    ReleasePublished,
    FrameRefused { seq: i64, reason: String },
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
}

fn encode(envelope: &ChannelHostEnvelope) -> Result<String, ChannelError> {
    serde_json::to_string(envelope).map_err(|error| ChannelError::Protocol(format!("encoding a message: {error}")))
}

fn protocol(reason: impl Into<String>) -> ChannelError {
    ChannelError::Protocol(reason.into())
}

fn message_type(frame: &ChannelStoredHostFrame) -> &str {
    frame.message.get("type").and_then(Value::as_str).unwrap_or("unknown")
}

fn snapshot_failed(frame: &ChannelStoredHostFrame) -> Result<Option<Value>, ChannelError> {
    if message_type(frame) != "SnapshotDone" {
        return Ok(None);
    }
    let done: ChannelSnapshotDone = serde_json::from_value(frame.message.clone())
        .map_err(|error| protocol(format!("reading saved SnapshotDone frame {}: {error}", frame.seq.0)))?;
    let failed: ChannelHostMessage = ChannelSnapshotFailed {
        detail: format!(
            "the control plane did not ack SnapshotDone for snapshot {} after the host resent it after {MAX_REPLAYS} Welcomes, so the host reports the snapshot failed in its place; check the control plane logs for this snapshot",
            *done.snapshot_id
        )
        .try_into()
        .map_err(|error| protocol(format!("the SnapshotFailed detail for snapshot {}: {error}", *done.snapshot_id)))?,
        failed_at: Utc::now().fixed_offset(),
        reason: ChannelSnapshotFailureReason::NotAnswered,
        snapshot_id: done.snapshot_id,
        type_: ChannelSnapshotFailedType::SnapshotFailed,
    }
    .into();
    serde_json::to_value(&failed)
        .map(Some)
        .map_err(|error| protocol(format!("encoding SnapshotFailed for channel.json: {error}")))
}

fn replays_of(replays: &[SupervisorChannelReplay], seq: i64) -> u32 {
    replays
        .iter()
        .find(|replay| replay.seq.0 == seq)
        .map_or(0, |replay| replay.replays.get())
}

impl<S: Store, K: Signer> HostChannel<S, K> {
    pub fn new(
        state: SupervisorChannelState,
        store: S,
        signer: K,
        host_id: EntityId,
        supervisor_version: String,
    ) -> Self {
        Self {
            state,
            store,
            signer,
            host_id,
            supervisor_version,
            phase: Phase::Closed,
            progressed: false,
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

    /// # Errors
    /// Fails when the state cannot be saved or the port refuses the frame.
    pub fn send(&mut self, message: ChannelHostMessage, port: &mut impl Port) -> Result<(), ChannelError> {
        let frame = ChannelHostFrame {
            ack: Sequence(self.state.received.0),
            message,
            seq: Sequence(self.state.sent.0 + 1),
            type_: ChannelHostFrameType::Frame,
        };
        let message = serde_json::to_value(&frame.message)
            .map_err(|error| protocol(format!("encoding a message for channel.json: {error}")))?;
        self.state.sent = Sequence(frame.seq.0);
        self.state.unacked.push(ChannelStoredHostFrame {
            ack: Sequence(frame.ack.0),
            message,
            seq: Sequence(frame.seq.0),
            type_: ChannelStoredHostFrameType::Frame,
        });
        self.store.save(&self.state)?;
        if self.phase == Phase::Open {
            port.send(encode(&frame.into())?)?;
        }
        Ok(())
    }

    /// # Errors
    /// Fails when the port refuses the heartbeat.
    pub fn heartbeat(
        &mut self,
        now: DateTime<FixedOffset>,
        usage: HostUsage,
        held_session_ids: Vec<EntityId>,
        port: &mut impl Port,
    ) -> Result<(), ChannelError> {
        port.send(encode(
            &ChannelHostHeartbeat {
                held_session_ids,
                sent_at: now,
                type_: ChannelHostHeartbeatType::Heartbeat,
                usage,
            }
            .into(),
        )?)
    }

    /// # Errors
    /// Fails when no Challenge is waiting for its Hello, or the port refuses the Hello.
    pub fn hello(
        &mut self,
        nonce: &str,
        box_capacity: NonZeroU32,
        usage: HostUsage,
        port: &mut impl Port,
    ) -> Result<(), ChannelError> {
        if self.phase != Phase::AwaitingHello {
            return Err(protocol(format!(
                "a Hello is due after a Challenge, not in phase {:?}",
                self.phase
            )));
        }
        let hello = ChannelHello {
            box_capacity,
            channel_protocol_oldest: None,
            guest_protocol: None,
            host_id: self.host_id.clone(),
            protocol_version: NonZeroU32::new(u32::from(ChannelProtocol::VERSION))
                .ok_or_else(|| protocol("protocol version 0"))?,
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
            usage,
        };
        port.send(encode(&hello.into())?)?;
        self.phase = Phase::AwaitingWelcome;
        Ok(())
    }

    /// # Errors
    /// Fails on a message the protocol does not allow in the current phase, a frame gap, or when
    /// the state cannot be saved or the port refuses a message. The connection must be closed then.
    pub fn receive(&mut self, text: &str, port: &mut impl Port) -> Result<Event, ChannelError> {
        if self.phase == Phase::Closed {
            return Ok(Event::None);
        }
        let envelope: ChannelControlEnvelope = serde_json::from_str(text)
            .map_err(|error| protocol(format!("the message matches no channel envelope: {error}")))?;
        let outcome = self.handle(envelope, port);
        if outcome.is_err() {
            self.phase = Phase::Closed;
        }
        outcome
    }

    fn handle(&mut self, envelope: ChannelControlEnvelope, port: &mut impl Port) -> Result<Event, ChannelError> {
        match (self.phase, envelope) {
            (Phase::AwaitingChallenge, ChannelControlEnvelope::Challenge(challenge)) => {
                self.phase = Phase::AwaitingHello;
                Ok(Event::Challenged(challenge.nonce.to_string()))
            }
            (Phase::AwaitingChallenge, other) => {
                Err(protocol(format!("expected Challenge first, got {}", kind(&other))))
            }
            (Phase::AwaitingWelcome, ChannelControlEnvelope::Welcome(welcome)) => self.welcomed(welcome.ack, port),
            (Phase::Open, ChannelControlEnvelope::ControlFrame(frame)) => self.accept(frame),
            (Phase::AwaitingWelcome | Phase::Open, ChannelControlEnvelope::Heartbeat(_)) => Ok(Event::Heartbeat),
            (Phase::Open, ChannelControlEnvelope::ReleasePublished(_)) => Ok(Event::ReleasePublished),
            (Phase::Open, ChannelControlEnvelope::FrameRefused(refused)) => self.refused(refused),
            (phase, other) => Err(protocol(format!("{} arrived in phase {phase:?}", kind(&other)))),
        }
    }

    fn welcomed(&mut self, ack: Option<Sequence>, port: &mut impl Port) -> Result<Event, ChannelError> {
        if let Some(ack) = ack {
            self.drop_acked(ack.0);
        }
        self.count_replay()?;
        self.store.save(&self.state)?;
        for frame in &self.state.unacked {
            let resent = ChannelStoredHostFrame {
                ack: Sequence(self.state.received.0),
                ..frame.clone()
            };
            port.send(
                serde_json::to_string(&resent)
                    .map_err(|error| protocol(format!("encoding a stored frame: {error}")))?,
            )?;
        }
        self.phase = Phase::Open;
        Ok(Event::Welcomed)
    }

    fn count_replay(&mut self) -> Result<(), ChannelError> {
        let Some(first) = self.state.unacked.first_mut() else {
            return Ok(());
        };
        let seq = first.seq.0;
        let replays = replays_of(&self.state.replays, seq);
        self.state.replays.retain(|replay| replay.seq.0 != seq);
        if replays < MAX_REPLAYS {
            self.state.replays.push(SupervisorChannelReplay {
                replays: NonZeroU32::MIN.saturating_add(replays),
                seq: Sequence(seq),
            });
            return Ok(());
        }
        let Some(failed) = snapshot_failed(first)? else {
            return Err(ChannelError::Undelivered(format!(
                "the control plane did not ack frame {seq} ({}) after the host resent it after {MAX_REPLAYS} Welcomes, and the host cannot skip a frame; check the control plane logs for this host",
                message_type(first)
            )));
        };
        first.message = failed;
        self.state.replays.push(SupervisorChannelReplay {
            replays: NonZeroU32::MIN,
            seq: Sequence(seq),
        });
        Ok(())
    }

    fn drop_acked(&mut self, ack: i64) {
        if self.state.unacked.first().is_some_and(|first| first.seq.0 <= ack) {
            self.progressed = true;
        }
        self.state.unacked.retain(|own| own.seq.0 > ack);
        self.state.replays.retain(|replay| replay.seq.0 > ack);
    }

    fn refused(&mut self, refused: ChannelFrameRefused) -> Result<Event, ChannelError> {
        self.progressed = true;
        self.state.unacked.retain(|own| own.seq.0 != refused.seq.0);
        self.state.replays.retain(|replay| replay.seq.0 != refused.seq.0);
        self.store.save(&self.state)?;
        Ok(Event::FrameRefused {
            seq: refused.seq.0,
            reason: String::from(refused.reason),
        })
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
        self.state.received = Sequence(frame.seq.0);
        self.progressed = true;
        self.drop_acked(frame.ack.0);
        self.store.save(&self.state)?;
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
        ChannelControlEnvelope::ControlFrame(_) => "Frame",
    }
}

#[cfg(test)]
mod tests;
