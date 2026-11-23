use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use chrono::{DateTime, FixedOffset, Utc};
use serde_json::Value;
use tumpel_api::frozen::v34;
use tumpel_api::types::{
    ChannelControlEnvelope, ChannelControlFrame, ChannelControlMessage, ChannelFrameRateLimited, ChannelFrameRefused,
    ChannelHello, ChannelHelloType, ChannelHostEnvelope, ChannelHostFrame, ChannelHostFrameType, ChannelHostHeartbeat,
    ChannelHostHeartbeatType, ChannelHostMessage, ChannelProtocol, ChannelStoredHostFrame, ChannelStoredHostFrameType,
    ChannelWelcome, EntityId, HostReportedHealth, HostUsage, Sequence, SupervisorChannelState,
};

pub const HEARTBEAT_DEADLINE: Duration = Duration::from_secs(30);
pub const FIRST_BACKOFF: Duration = Duration::from_secs(1);
pub const LAST_BACKOFF: Duration = Duration::from_secs(30);
const FROZEN_V34: u16 = 34;
pub const RATE_WINDOW: Duration = Duration::from_secs(60);
const ASSUMED_FRAMES_PER_MINUTE: u32 = 600;

const HELLO_CONTEXT: &[u8] = b"tumpel-channel-hello-v1";

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeldIds {
    pub session_ids: Vec<EntityId>,
    pub claude_setup_ids: Vec<EntityId>,
    pub turn_ids: Vec<EntityId>,
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
        replays: Vec::new(),
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
        if self.paused(now) {
            return RateBackoff::Paused;
        }
        let wait = (retry_after - now).to_std().unwrap_or_default().max(FIRST_BACKOFF);
        let jittered = wait + (wait / 2).mul_f64(f64::from(jitter) / f64::from(u16::MAX));
        let until = now + chrono::Duration::from_std(jittered).unwrap_or(chrono::Duration::MAX);
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
}

fn encode(envelope: &ChannelHostEnvelope) -> Result<String, ChannelError> {
    serde_json::to_string(envelope).map_err(|error| ChannelError::Protocol(format!("encoding a message: {error}")))
}

fn encode_hello(hello: ChannelHello, version: u16) -> Result<String, ChannelError> {
    if version > FROZEN_V34 {
        return encode(&hello.into());
    }
    let hello: v34::types::ChannelHello = serde_json::to_value(&hello)
        .and_then(serde_json::from_value)
        .map_err(|error| protocol(format!("encoding a protocol 34 Hello: {error}")))?;
    serde_json::to_string(&v34::types::ChannelHostEnvelope::from(hello))
        .map_err(|error| protocol(format!("encoding a protocol 34 Hello: {error}")))
}

fn encode_heartbeat(heartbeat: ChannelHostHeartbeat, version: u16) -> Result<String, ChannelError> {
    if version > FROZEN_V34 {
        return encode(&heartbeat.into());
    }
    let heartbeat: v34::types::ChannelHostHeartbeat = serde_json::to_value(&heartbeat)
        .and_then(serde_json::from_value)
        .map_err(|error| protocol(format!("encoding a protocol 34 Heartbeat: {error}")))?;
    serde_json::to_string(&v34::types::ChannelHostEnvelope::from(heartbeat))
        .map_err(|error| protocol(format!("encoding a protocol 34 Heartbeat: {error}")))
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
        let state = SupervisorChannelState {
            replays: Vec::new(),
            ..state
        };
        Self {
            state,
            store,
            signer,
            host_id,
            supervisor_version,
            phase: Phase::Closed,
            progressed: false,
            withheld: None,
            offered: Vec::new(),
            hello_version: ChannelProtocol::VERSION,
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

    pub fn step_down(&mut self) -> Option<u16> {
        if self.hello_version <= ChannelProtocol::OLDEST {
            return None;
        }
        self.hello_version = ChannelProtocol::OLDEST;
        Some(self.hello_version)
    }

    pub fn step_up(&mut self) {
        self.hello_version = ChannelProtocol::VERSION;
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
        if let Some(turn_id) = message.get("turnId").and_then(Value::as_str) {
            self.offered.retain(|offered| **offered != *turn_id);
        }
        self.state.sent = Sequence(frame.seq.0);
        self.state.unacked.push(ChannelStoredHostFrame {
            ack: Sequence(frame.ack.0),
            message,
            seq: Sequence(frame.seq.0),
            type_: ChannelStoredHostFrameType::Frame,
        });
        self.store.save(&self.state)?;
        if self.phase == Phase::Open && self.withheld.is_none() {
            port.send(encode(&frame.into())?)?;
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
        let waiting: Vec<&ChannelStoredHostFrame> = self.state.unacked.iter().filter(|own| own.seq.0 >= from).collect();
        for frame in waiting.iter().take(budget) {
            let resent = ChannelStoredHostFrame {
                ack: Sequence(self.state.received.0),
                ..(*frame).clone()
            };
            port.send(
                serde_json::to_string(&resent)
                    .map_err(|error| protocol(format!("encoding a stored frame: {error}")))?,
            )?;
        }
        self.withheld = waiting.get(budget).map(|next| next.seq.0);
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
        usage: HostUsage,
        held: HeldIds,
        health: HostReportedHealth,
        port: &mut impl Port,
    ) -> Result<(), ChannelError> {
        let heartbeat = ChannelHostHeartbeat {
            health: Some(health),
            held_claude_setup_ids: held.claude_setup_ids,
            held_session_ids: held.session_ids,
            held_turn_ids: self.held_turns(held.turn_ids),
            sent_at: now,
            type_: ChannelHostHeartbeatType::Heartbeat,
            usage,
        };
        port.send(encode_heartbeat(heartbeat, self.hello_version)?)
    }

    /// # Errors
    /// Fails when no Challenge is waiting for its Hello, or the port refuses the Hello.
    pub fn hello(
        &mut self,
        nonce: &str,
        box_capacity: NonZeroU32,
        usage: HostUsage,
        held_turn_ids: Vec<EntityId>,
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
            channel_protocol_oldest: NonZeroU32::new(u32::from(ChannelProtocol::OLDEST)),
            guest_protocol: None,
            held_turn_ids: self.held_turns(held_turn_ids),
            host_id: self.host_id.clone(),
            protocol_version: NonZeroU32::new(u32::from(self.hello_version))
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
        port.send(encode_hello(hello, self.hello_version)?)?;
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
            (Phase::AwaitingChallenge, ChannelControlEnvelope::Challenge(challenge)) => {
                self.phase = Phase::AwaitingHello;
                Ok(Event::Challenged(challenge.nonce.to_string()))
            }
            (Phase::AwaitingChallenge, other) => {
                Err(protocol(format!("expected Challenge first, got {}", kind(&other))))
            }
            (Phase::AwaitingWelcome, ChannelControlEnvelope::Welcome(welcome)) => self.welcomed(welcome),
            (Phase::Open, ChannelControlEnvelope::ControlFrame(frame)) => self.accept(frame),
            (Phase::AwaitingWelcome | Phase::Open, ChannelControlEnvelope::Heartbeat(_)) => Ok(Event::Heartbeat),
            (Phase::Open, ChannelControlEnvelope::ReleasePublished(_)) => Ok(Event::ReleasePublished),
            (Phase::Open, ChannelControlEnvelope::FrameRefused(refused)) => self.refused(refused),
            (Phase::Open, ChannelControlEnvelope::FrameRateLimited(limited)) => Ok(self.rate_limited(limited)),
            (phase, other) => Err(protocol(format!("{} arrived in phase {phase:?}", kind(&other)))),
        }
    }

    fn welcomed(&mut self, welcome: ChannelWelcome) -> Result<Event, ChannelError> {
        if let Some(ack) = welcome.ack {
            self.drop_acked(ack.0);
        }
        self.store.save(&self.state)?;
        self.withheld = self.state.unacked.first().map(|first| first.seq.0);
        self.phase = Phase::Open;
        Ok(Event::Welcomed)
    }

    fn drop_acked(&mut self, ack: i64) {
        if self.state.unacked.first().is_some_and(|first| first.seq.0 <= ack) {
            self.progressed = true;
        }
        self.state.unacked.retain(|own| own.seq.0 > ack);
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
        self.state.received = Sequence(frame.seq.0);
        self.progressed = true;
        self.drop_acked(frame.ack.0);
        self.store.save(&self.state)?;
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
        ChannelControlEnvelope::ControlFrame(_) => "Frame",
    }
}

#[cfg(test)]
mod tests;
