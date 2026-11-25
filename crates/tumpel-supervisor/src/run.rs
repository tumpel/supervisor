use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use ring::signature::Ed25519KeyPair;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tumpel_api::types::{
    BoxDiskLimit, BoxMemoryLimit, ChannelBoxDiscard, ChannelBoxTeardown, ChannelClaudeSetupBoxOffer, ChannelCloseCode,
    ChannelControlMessage, ChannelSnapshotRequest, ChannelTurnOffer, ClaudeSetupHarness, EntityId, HostReportedHealth,
    Sha256Digest, SupervisorChannelState, SupervisorConfig, SupervisorFailure, SupervisorHandover,
    SupervisorHandoverBoxState, SupervisorHostKey, SupervisorRuntime, SupervisorUpdatePending, TurnFailureReason,
};
use tumpel_channel::{
    Backoff, ChannelError, Event, HeldIds, HostChannel, Outgoing, Phase, Port, RateBackoff, RateLimit, Signer, Store,
    HEARTBEAT_DEADLINE,
};

use crate::boot::BootOrder;
use crate::dir::{self, ChannelFile, CHANNEL_FILE, CONFIG_FILE, KEY_FILE, RUNTIME_FILE};
use crate::exchange::{Answer, Answers, Cancels};
use crate::fd_store;
use crate::firecracker::Firecracker;
use crate::guest_drive;
use crate::lifecycle::{
    turn_failed, Abandoned, BoxTurn, Budgets, Command, IdleGate, Input, Lifecycle, Settings, SetupCommand, SetupOrder,
    Taken, TeardownOrder,
};
use crate::pool::{memory_budget_mib, mib, Held, Size, TapPool};
use crate::store::{jitter, HttpStore};
use crate::telemetry::Telemetry;
use crate::update::{self, HttpSource, Layout, Limits, Outcome, Startup, Updater};
use crate::usage;

const TICK: Duration = Duration::from_millis(500);
const UNIT_RELOAD: Duration = Duration::from_secs(30);
const CONNECT_DEADLINE: Duration = Duration::from_secs(10);
const ROLLED_BACK: &str = "the next start puts the previous binary back";
const RESTARTED: &str = "this supervisor stops, and systemd starts it again, which rewrites the unit once more";

pub struct Failure {
    pub kind: SupervisorFailure,
    pub detail: String,
}

fn fail(kind: SupervisorFailure, detail: impl Into<String>) -> Failure {
    Failure {
        kind,
        detail: detail.into(),
    }
}

struct HostKey(Ed25519KeyPair);

impl Signer for HostKey {
    fn sign(&self, bytes: &[u8]) -> String {
        STANDARD.encode(self.0.sign(bytes).as_ref())
    }
}

#[derive(Default)]
struct Outbox(Vec<String>);

impl Port for Outbox {
    fn send(&mut self, text: String) -> Result<(), ChannelError> {
        self.0.push(text);
        Ok(())
    }
}

pub enum Stopped {
    Revoked,
    Updated,
    Terminated,
}

enum Served {
    Revoked,
    Updated,
    Terminated,
    BoxesStopped(Failure),
}

type HostUpdater = Updater<HttpSource, IdleGate>;

type BoxRunner = JoinHandle<Result<(), String>>;

struct Updates {
    updater: Arc<HostUpdater>,
    layout: Layout,
    out: UnboundedSender<Outgoing>,
    restart: UnboundedSender<()>,
    trial: Option<SupervisorUpdatePending>,
    confirmed: watch::Sender<bool>,
    grace: Duration,
}

impl Updates {
    fn announced(&self) {
        let updater = Arc::clone(&self.updater);
        let restart = self.restart.clone();
        let grace = self.grace;
        tokio::spawn(async move {
            if updater.announced(grace).await == Outcome::Restart {
                let _ = restart.send(());
            }
        });
    }

    async fn welcomed(&mut self) -> Result<(), Failure> {
        if let Some(trial) = self.trial.take() {
            refresh_unit(&self.layout, ROLLED_BACK).await?;
            update::confirm(&self.layout, &trial, &self.out).map_err(|error| {
                fail(
                    SupervisorFailure::UpdateNotOnline,
                    format!("{error}; the next start puts the previous binary back"),
                )
            })?;
            self.confirmed.send_replace(true);
        }
        self.announced();
        Ok(())
    }
}

pub(crate) async fn refresh_unit(layout: &Layout, fix: &str) -> Result<(), Failure> {
    let failed = |detail: String| fail(SupervisorFailure::UpdateNotOnline, format!("{detail}; {fix}"));
    if !update::refresh_unit(layout).map_err(failed)? {
        return Ok(());
    }
    eprintln!(
        "rewrote {} to the unit this binary installs; reloading systemd",
        layout.unit.display()
    );
    let reloaded = tokio::time::timeout(
        UNIT_RELOAD,
        tokio::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .kill_on_drop(true)
            .status(),
    )
    .await;
    match reloaded {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(failed(format!("systemctl --user daemon-reload exited with {status}"))),
        Ok(Err(error)) => Err(failed(format!(
            "starting systemctl --user daemon-reload failed: {error}"
        ))),
        Err(_) => Err(failed(format!(
            "systemctl --user daemon-reload ran past {} s",
            UNIT_RELOAD.as_secs()
        ))),
    }
}

enum Ended {
    Revoked(String),
    Refused(String),
    Dropped { progressed: bool, reason: String },
}

type Channel = HostChannel<ChannelFile, HostKey>;

struct Usage {
    box_capacity: NonZeroU32,
    warm: watch::Receiver<u32>,
    running: watch::Receiver<u32>,
    sessions: watch::Receiver<Vec<EntityId>>,
    claude_setups: watch::Receiver<Vec<EntityId>>,
    turns: watch::Receiver<Vec<EntityId>>,
    holdings: watch::Receiver<Held>,
    data_dir: PathBuf,
    telemetry: Telemetry,
}

impl Usage {
    fn sample(&self) -> Result<tumpel_channel::Usage, Failure> {
        let held = self.holdings.borrow().clone();
        let stats = usage::sample(*self.warm.borrow(), &self.data_dir, &held)
            .map_err(|error| fail(SupervisorFailure::UsageUnreadable, format!("{error:#}")))?;
        self.telemetry.host(&stats, *self.running.borrow());
        Ok(tumpel_channel::Usage { host: stats.usage })
    }
}

fn hello(
    channel: &mut Channel,
    nonce: &str,
    usage: &Usage,
    from_box: &mut UnboundedReceiver<Outgoing>,
    outbox: &mut Outbox,
) -> Result<(), Failure> {
    let turns = usage.turns.borrow().clone();
    drain(channel, from_box, outbox)?;
    channel
        .hello(nonce, usage.box_capacity, usage.sample()?, turns, outbox)
        .map_err(|error| match error {
            ChannelError::Store(_) => store_lost(&error),
            error => fail(SupervisorFailure::ProtocolError, error.to_string()),
        })
}

fn drain(
    channel: &mut Channel,
    from_box: &mut UnboundedReceiver<Outgoing>,
    outbox: &mut Outbox,
) -> Result<(), Failure> {
    while let Ok(message) = from_box.try_recv() {
        channel.send(message, outbox).map_err(|error| store_lost(&error))?;
    }
    Ok(())
}

fn beat(
    channel: &mut Channel,
    usage: &Usage,
    from_box: &mut UnboundedReceiver<Outgoing>,
    health: HostReportedHealth,
    outbox: &mut Outbox,
) -> Result<(), Failure> {
    let held = HeldIds {
        session_ids: usage.sessions.borrow().clone(),
        claude_setup_ids: usage.claude_setups.borrow().clone(),
        turn_ids: usage.turns.borrow().clone(),
    };
    drain(channel, from_box, outbox)?;
    channel
        .heartbeat(
            chrono::Utc::now().fixed_offset(),
            usage.sample()?,
            held,
            health,
            || usage.telemetry.heartbeat_batch(),
            outbox,
        )
        .map_err(|error| fail(SupervisorFailure::ProtocolError, error.to_string()))
}

enum BoxCommand {
    Turn(Box<(ChannelTurnOffer, BootOrder)>),
    Snapshot(ChannelSnapshotRequest),
    Teardown(ChannelBoxTeardown),
    Discard(ChannelBoxDiscard),
    Answer(Answer),
    Cancel(EntityId),
    Setup(SetupCommand),
    HandOver(oneshot::Sender<SupervisorHandover>),
    Abandon(oneshot::Sender<()>, Abandoned),
    SnapshotAccepted(EntityId),
    SnapshotFailed(EntityId),
    Welcomed(u16),
}

fn setup_order(offer: ChannelClaudeSetupBoxOffer) -> Result<SetupCommand, String> {
    Ok(SetupCommand::Start(Box::new(SetupOrder {
        claude_setup_id: offer.claude_setup_id,
        tenant_id: offer.tenant_id,
        box_id: offer.box_id,
        image: BootOrder::from_offer(
            offer.image,
            offer.harness.unwrap_or(ClaudeSetupHarness::ClaudeCode).to_string(),
            offer.harness_version.to_string(),
        )?,
        token_url: offer.token_url,
        token_credential: offer.token_credential,
        url: Duration::from_secs(u64::from(offer.url_seconds.get())),
        code: Duration::from_secs(u64::from(offer.code_seconds.get())),
    })))
}

fn turn(offer: ChannelTurnOffer) -> Result<BoxCommand, String> {
    let image = BootOrder::from_offer(
        offer.image.clone(),
        offer.harness.to_string(),
        offer.harness_version.to_string(),
    )
    .map_err(|error| format!("turn {}: {error}", *offer.turn_id))?;
    Ok(BoxCommand::Turn(Box::new((offer, image))))
}

fn command(files_dir: &Path, offer: ChannelTurnOffer, image: BootOrder) -> Command {
    Command::Turn(Box::new(BoxTurn {
        turn_id: offer.turn_id,
        session_id: offer.session_id,
        box_id: offer.box_id,
        image,
        model: offer.model,
        prompt: offer.prompt,
        credential: offer.credential,
        last_line_seq: offer.last_line_seq,
        snapshots: offer.snapshots,
        persona_files: offer.persona_files,
        repository: offer.repository,
        git_author_name: offer.git_author_name,
        git_author_email: offer.git_author_email,
        files_dir: files_dir.to_path_buf(),
        prebuild_commit_sha: offer.prebuild_commit_sha.map(String::from),
        box_memory_mib: offer.box_memory_mib,
        box_disk_mib: offer.box_disk_mib,
        turn_seconds: offer.turn_seconds,
        idle_seconds: offer.idle_seconds,
        box_retention: offer.box_retention,
    }))
}

async fn boxes(
    files_dir: PathBuf,
    answers: Answers,
    cancels: Cancels,
    mut commands: UnboundedReceiver<BoxCommand>,
    lifecycle: UnboundedSender<Input>,
    setups: UnboundedSender<SetupCommand>,
) {
    while let Some(command) = commands.recv().await {
        let next = match command {
            BoxCommand::Turn(turn) => {
                let (offer, image) = *turn;
                self::command(&files_dir, offer, image)
            }
            BoxCommand::Snapshot(request) => Command::Snapshot(request.into()),
            BoxCommand::Teardown(teardown) => Command::Teardown(TeardownOrder {
                session_id: teardown.session_id,
                box_id: teardown.box_id,
            }),
            BoxCommand::Discard(discard) => Command::Discard(TeardownOrder {
                session_id: discard.session_id,
                box_id: discard.box_id,
            }),
            BoxCommand::Setup(setup) => {
                if setups.send(setup).is_err() {
                    eprintln!("tumpel-supervisor: the box runner stopped; restart the supervisor");
                    return;
                }
                continue;
            }
            BoxCommand::Answer(answer) => {
                answers.deliver(answer);
                continue;
            }
            BoxCommand::Cancel(turn_id) => {
                cancels.cancel(turn_id);
                continue;
            }
            BoxCommand::HandOver(reply) => {
                let _ = lifecycle.send(Input::HandOver(reply));
                continue;
            }
            BoxCommand::Abandon(reply, abandoned) => {
                let _ = lifecycle.send(Input::Abandon(reply, abandoned));
                continue;
            }
            BoxCommand::SnapshotAccepted(snapshot_id) => {
                if lifecycle.send(Input::SnapshotAccepted(snapshot_id)).is_err() {
                    eprintln!("tumpel-supervisor: the box runner stopped; restart the supervisor");
                    return;
                }
                continue;
            }
            BoxCommand::SnapshotFailed(snapshot_id) => {
                if lifecycle.send(Input::SnapshotFailed(snapshot_id)).is_err() {
                    eprintln!("tumpel-supervisor: the box runner stopped; restart the supervisor");
                    return;
                }
                continue;
            }
            BoxCommand::Welcomed(protocol) => {
                if lifecycle.send(Input::Welcomed(protocol)).is_err() {
                    eprintln!("tumpel-supervisor: the box runner stopped; restart the supervisor");
                    return;
                }
                continue;
            }
        };
        if lifecycle.send(Input::Command(next)).is_err() {
            eprintln!("tumpel-supervisor: the box runner stopped; restart the supervisor");
            return;
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "run hands the box runner each piece it built separately"
)]
fn start_boxes(
    runtime: &SupervisorRuntime,
    box_size: Size,
    store: HttpStore,
    received: UnboundedReceiver<BoxCommand>,
    out: UnboundedSender<Outgoing>,
    telemetry: Telemetry,
    taken: Option<(SupervisorHandover, Taken)>,
    standing: PathBuf,
) -> Result<(Usage, IdleGate, BoxRunner), Failure> {
    let unreadable = |error: anyhow::Error| fail(SupervisorFailure::UsageUnreadable, format!("{error:#}"));
    usage::prepare_data_dir(Path::new(&*runtime.data_dir)).map_err(unreadable)?;
    let total_mib = usage::memory_bytes().map_err(unreadable)?.get() / (1024 * 1024);
    let pool = TapPool::new(memory_budget_mib(total_mib), box_size);
    let settings = Settings {
        budgets: Budgets::from(&runtime.budgets),
        layer_cache_mib: runtime.layer_cache_mib,
    };
    let box_capacity = pool.capacity();
    let mut lifecycle = Lifecycle::new(Firecracker::new(runtime), store, settings, pool, out, standing);
    lifecycle
        .start(taken)
        .map_err(|error| fail(SupervisorFailure::NotEnrolled, error))?;
    let usage = Usage {
        box_capacity,
        warm: lifecycle.warm(),
        running: lifecycle.boxes(),
        sessions: lifecycle.sessions(),
        claude_setups: lifecycle.claude_setups(),
        turns: lifecycle.turns(),
        holdings: lifecycle.holdings(),
        data_dir: PathBuf::from(&*runtime.data_dir),
        telemetry,
    };
    let answers = lifecycle.answers();
    let cancels = lifecycle.cancels();
    let (to_lifecycle, from_boxes) = unbounded_channel();
    let idle = IdleGate::new(lifecycle.boxes(), lifecycle.busy());
    let (to_setups, from_setups) = unbounded_channel();
    let runner = tokio::spawn(lifecycle.run(from_boxes, from_setups));
    tokio::spawn(boxes(
        Path::new(&*runtime.data_dir).join(crate::persona_files::DIR),
        answers,
        cancels,
        received,
        to_lifecycle,
        to_setups,
    ));
    Ok((usage, idle, runner))
}

fn boxes_stopped(ended: Result<Result<(), String>, JoinError>) -> Failure {
    let detail = match ended {
        Ok(Ok(())) => "it took no more commands".to_string(),
        Ok(Err(error)) => error,
        Err(error) => error.to_string(),
    };
    fail(
        SupervisorFailure::ChannelStateLost,
        format!(
            "the box runner stopped: {detail}; systemd restarts the supervisor, which takes the running boxes over"
        ),
    )
}

async fn terminate(
    commands: &UnboundedSender<BoxCommand>,
    layout: &Layout,
    out: &UnboundedSender<Outgoing>,
) -> Result<(), Failure> {
    let (reply, answer) = oneshot::channel();
    let _ = commands.send(BoxCommand::HandOver(reply));
    let handover = answer.await.map_err(|_| {
        fail(
            SupervisorFailure::ChannelStateLost,
            "the box runner stopped before it handed its boxes over",
        )
    })?;
    if let Err(error) = dir::write_json(&layout.handover(), &handover) {
        for turn_id in handed_turns(&handover) {
            turn_failed(
                out,
                &turn_id,
                TurnFailureReason::HarnessExited,
                format!(
                    "the box of turn {} could not be handed to the next supervisor: {error:#}; send the turn again",
                    *turn_id
                ),
            );
        }
        return Err(fail(
            SupervisorFailure::ChannelStateLost,
            format!("{error:#}; the next start takes the running boxes over from the record of running boxes"),
        ));
    }
    eprintln!(
        "tumpel-supervisor: stopped and left {} boxes running; the next start of the unit takes them over",
        handover.boxes.len()
    );
    Ok(())
}

fn restarts(kind: &SupervisorFailure) -> bool {
    !matches!(kind, SupervisorFailure::NotEnrolled | SupervisorFailure::HostRevoked)
}

fn abandoned() -> Abandoned {
    Abandoned {
        reason: TurnFailureReason::HarnessExited,
        cause: "this supervisor binary stopped before it reached the control plane".to_string(),
    }
}

async fn abandon(commands: &UnboundedSender<BoxCommand>, abandoned: Abandoned) {
    let (reply, answer) = oneshot::channel();
    if commands.send(BoxCommand::Abandon(reply, abandoned)).is_ok() {
        let _ = answer.await;
    }
}

fn handed_turns(handover: &SupervisorHandover) -> Vec<EntityId> {
    handover
        .boxes
        .iter()
        .filter_map(|handed| match &handed.state {
            SupervisorHandoverBoxState::Turn(turn) => Some(turn.turn_id.clone()),
            SupervisorHandoverBoxState::Warm(_) => None,
            SupervisorHandoverBoxState::Unrecognised(_) => {
                tumpel_channel::update_required();
                None
            }
        })
        .collect()
}

fn named_turns(text: &str) -> Vec<EntityId> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };
    value["boxes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|handed| handed["state"]["turnId"].as_str())
        .filter_map(|turn_id| EntityId::try_from(turn_id.to_string()).ok())
        .collect()
}

fn handover_lost(layout: &Layout, out: &UnboundedSender<Outgoing>, cause: &str, turns: &[EntityId]) {
    eprintln!(
        "tumpel-supervisor: HandoverLost: {cause}; this supervisor fails the {} turns it names and starts without the handed boxes",
        turns.len()
    );
    for turn_id in turns {
        turn_failed(
            out,
            turn_id,
            TurnFailureReason::HarnessExited,
            format!(
                "the handover of the box of turn {} to this supervisor binary was lost: {cause}; send the turn again",
                **turn_id
            ),
        );
    }
    if let Err(error) = update::forget_handover(layout) {
        eprintln!("tumpel-supervisor: {error}; delete it by hand");
    }
}

fn read_handover(layout: &Layout, out: &UnboundedSender<Outgoing>, handed: bool) -> Option<SupervisorHandover> {
    let path = layout.handover();
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => {
            handover_lost(layout, out, &format!("reading {} failed: {error}", path.display()), &[]);
            return None;
        }
    };
    match serde_json::from_str::<SupervisorHandover>(&text) {
        Ok(handover) if handed => Some(handover),
        Ok(handover) => {
            handover_lost(
                layout,
                out,
                "this supervisor started without the boxes' descriptors, so it cannot reach the boxes",
                &handed_turns(&handover),
            );
            None
        }
        Err(error) => {
            handover_lost(
                layout,
                out,
                &format!("{} is no SupervisorHandover: {error}", path.display()),
                &named_turns(&text),
            );
            None
        }
    }
}

fn store_lost(error: &ChannelError) -> Failure {
    fail(SupervisorFailure::ChannelStateLost, error.to_string())
}

pub(crate) fn channel_url(endpoint: &str, host_id: &str) -> Result<String, Failure> {
    let base = if let Some(rest) = endpoint.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err(fail(
            SupervisorFailure::NotEnrolled,
            format!("the endpoint {endpoint} is neither http nor https; enrol again with a valid --endpoint"),
        ));
    };
    Ok(format!("{base}/v1/hosts/{host_id}/channel"))
}

pub(crate) fn load_runtime(dir: &Path) -> Result<SupervisorRuntime, Failure> {
    let path = dir.join(RUNTIME_FILE);
    let unreadable = |error: anyhow::Error| fail(SupervisorFailure::NotEnrolled, format!("{error:#}"));
    dir::read_json::<serde_json::Value>(&path, "JSON")
        .map_err(unreadable)?
        .ok_or_else(|| {
            fail(
                SupervisorFailure::NotEnrolled,
                format!(
                    "{} is missing; write it per SupervisorRuntime in typespec/lib/supervisor.tsp, or enrol again to get the defaults",
                    path.display()
                ),
            )
        })
        .and_then(|current| dir::runtime_with_defaults(&path, &current).map_err(unreadable))
}

pub(crate) fn load() -> Result<(SupervisorConfig, Ed25519KeyPair, SupervisorChannelState), Failure> {
    let dir = dir::supervisor_dir();
    let unreadable = |error: anyhow::Error| fail(SupervisorFailure::NotEnrolled, format!("{error:#}"));
    let config: SupervisorConfig = dir::read_json(&dir.join(CONFIG_FILE), "SupervisorConfig")
        .map_err(unreadable)?
        .ok_or_else(|| {
            fail(
                SupervisorFailure::NotEnrolled,
                format!(
                    "{} holds no enrolment; enrol with tumpel-supervisor enrol",
                    dir.display()
                ),
            )
        })?;
    let key: SupervisorHostKey = dir::read_json(&dir.join(KEY_FILE), "SupervisorHostKey")
        .map_err(unreadable)?
        .ok_or_else(|| {
            fail(
                SupervisorFailure::HostRevoked,
                format!(
                    "{} has no host key, so host {} was revoked or its key deleted; mint a token and enrol again",
                    dir.join(KEY_FILE).display(),
                    *config.host_id
                ),
            )
        })?;
    let state: SupervisorChannelState = dir::read_json(&dir.join(CHANNEL_FILE), "SupervisorChannelState")
        .map_err(unreadable)?
        .ok_or_else(|| {
            fail(
                SupervisorFailure::ChannelStateLost,
                format!(
                    "{} is missing, so frames the control plane has not acked are lost; delete {} and enrol this host again",
                    dir.join(CHANNEL_FILE).display(),
                    dir.display()
                ),
            )
        })?;
    let pkcs8 = STANDARD.decode(key.private_key.as_bytes()).map_err(|error| {
        fail(
            SupervisorFailure::NotEnrolled,
            format!("{KEY_FILE} privateKey is not base64: {error}"),
        )
    })?;
    let pair = Ed25519KeyPair::from_pkcs8(&pkcs8).map_err(|_| {
        fail(
            SupervisorFailure::NotEnrolled,
            format!("{KEY_FILE} privateKey is not an Ed25519 PKCS#8 document; enrol again"),
        )
    })?;
    Ok((config, pair, state))
}

pub(crate) struct Started {
    pub(crate) config: SupervisorConfig,
    pair: Ed25519KeyPair,
    state: SupervisorChannelState,
    runtime: SupervisorRuntime,
    pub(crate) url: String,
    layout: Layout,
    pub(crate) running: Sha256Digest,
    source: HttpSource,
    box_size: Size,
    store: HttpStore,
}

pub(crate) fn start() -> Result<Started, Failure> {
    let unusable = |detail: String| fail(SupervisorFailure::NotEnrolled, detail);
    let dir = dir::supervisor_dir();
    let binary = std::env::current_exe()
        .map_err(|error| unusable(format!("finding this supervisor's own binary failed: {error}")))?;
    let running = update::binary_sha256(&binary).map_err(unusable)?;
    let layout = Layout::new(binary, dir.clone(), dir::supervisor_unit_path());
    let (config, pair, state) = load()?;
    let runtime = load_runtime(&dir)?;
    let url = channel_url(&config.endpoint, &config.host_id)?;
    let source = HttpSource::new(&config.endpoint).map_err(unusable)?;
    let box_size = Size {
        memory_mib: mib(runtime.memory_mib.into()).ok_or_else(|| {
            unusable(format!(
                "memoryMib {} in runtime.json is not a positive number of MiB; set it to {}",
                runtime.memory_mib,
                BoxMemoryLimit::DEFAULT_MIB
            ))
        })?,
        disk_mib: mib(BoxDiskLimit::DEFAULT_MIB).ok_or_else(|| {
            unusable(format!(
                "the protocol's default box disk of {} MiB is not a positive size",
                BoxDiskLimit::DEFAULT_MIB
            ))
        })?,
    };
    let store = HttpStore::new().map_err(unusable)?;
    Ok(Started {
        config,
        pair,
        state,
        runtime,
        url,
        layout,
        running,
        source,
        box_size,
        store,
    })
}

async fn online_deadline(deadline: Option<tokio::time::Instant>, mut confirmed: watch::Receiver<bool>) {
    let Some(deadline) = deadline else {
        return std::future::pending().await;
    };
    tokio::select! {
        () = tokio::time::sleep_until(deadline) => {}
        _ = confirmed.wait_for(|online| *online) => std::future::pending::<()>().await,
    }
}

fn read_standing(layout: &Layout) -> Option<SupervisorHandover> {
    dir::read_json(&layout.boxes(), "SupervisorHandover").unwrap_or_else(|error| {
        eprintln!("tumpel-supervisor: {error:#}; the boxes it names are stopped");
        Some(SupervisorHandover {
            boxes: Vec::new(),
            handed_at: chrono::Utc::now().fixed_offset(),
        })
    })
}

fn taken(
    layout: &Layout,
    out: &UnboundedSender<Outgoing>,
    handed: bool,
    mut received: BTreeMap<String, OwnedFd>,
) -> Result<Option<(SupervisorHandover, Taken)>, Failure> {
    let taken = match read_handover(layout, out, handed || !received.is_empty()) {
        Some(handover) if received.is_empty() => Some((handover, Taken::Handed)),
        Some(handover) => Some((fd_store::restored(handover, &mut received), Taken::Handed)),
        None => read_standing(layout).map(|standing| (fd_store::restored(standing, &mut received), Taken::Crashed)),
    };
    if taken.is_some() {
        update::forget_handover(layout).map_err(|error| fail(SupervisorFailure::NotEnrolled, error))?;
    }
    Ok(taken)
}

pub async fn run(telemetry: Telemetry, handed: bool, received: BTreeMap<String, OwnedFd>) -> Result<Stopped, Failure> {
    let mut terminated = signal(SignalKind::terminate()).map_err(|error| {
        fail(
            SupervisorFailure::NotEnrolled,
            format!("listening for SIGTERM failed: {error}"),
        )
    })?;
    let (out, mut from_box) = unbounded_channel();
    let started = start()?;
    let trial = match update::startup(&started.layout, &started.running, &out)
        .map_err(|detail| fail(SupervisorFailure::NotEnrolled, detail))?
    {
        Startup::Steady => None,
        Startup::Trial(pending) => Some(pending),
    };
    let on_trial = trial.is_some();
    if handed {
        refresh_unit(&started.layout, RESTARTED).await?;
    }
    let taken = taken(&started.layout, &out, handed, received)?;
    let dir = dir::supervisor_dir();
    guest_drive::write(Path::new(&*started.runtime.image_cache_dir))
        .await
        .map_err(|error| fail(SupervisorFailure::GuestDriveUnwritable, error))?;
    let (commands, received) = unbounded_channel();
    let (usage, idle, mut runner) = start_boxes(
        &started.runtime,
        started.box_size,
        started.store,
        received,
        out.clone(),
        telemetry,
        taken,
        started.layout.boxes(),
    )?;
    let url = started.url;
    let config = started.config;
    let heartbeat = Duration::from_secs(u64::from(started.runtime.heartbeat_interval_seconds.get()));
    let grace = Duration::from_secs(u64::try_from(started.runtime.update_grace_seconds).unwrap_or(0));
    let mut channel = HostChannel::new(
        started.state,
        ChannelFile(dir.join(CHANNEL_FILE)),
        HostKey(started.pair),
        config.host_id.clone(),
        env!("TUMPEL_RELEASE_VERSION").to_string(),
    );
    let (restart, mut restarted) = unbounded_channel();
    let (confirmed, online) = watch::channel(false);
    let mut updates = Updates {
        updater: Arc::new(Updater::new(
            started.source,
            started.layout.clone(),
            Limits::HOST,
            started.running,
            env!("TUMPEL_RELEASE_VERSION"),
            idle.clone(),
            out.clone(),
        )),
        layout: started.layout,
        out,
        restart,
        trial,
        confirmed,
        grace,
    };
    let deadline = on_trial.then(|| tokio::time::Instant::now() + Limits::HOST.online);
    let served = tokio::select! {
        served = serve(&mut channel, &url, heartbeat, &usage, &commands, &mut from_box, &config, &dir, &mut updates) => served,
        Some(()) = restarted.recv() => Ok(Served::Updated),
        Some(()) = terminated.recv() => Ok(Served::Terminated),
        ended = &mut runner => Ok(Served::BoxesStopped(boxes_stopped(ended))),
        () = online_deadline(deadline, online) => Err(fail(
            SupervisorFailure::UpdateNotOnline,
            format!("this supervisor did not reach Welcome on {url} within {} s of its start after its swap; systemd restarts the unit, and its ExecStartPre puts the previous binary back", Limits::HOST.online.as_secs()),
        )),
    };
    let stopped = match served {
        Ok(Served::Revoked) => Ok(Stopped::Revoked),
        Ok(Served::Updated) => terminate(&commands, &updates.layout, &updates.out)
            .await
            .map(|()| Stopped::Updated),
        Ok(Served::Terminated) => terminate(&commands, &updates.layout, &updates.out)
            .await
            .map(|()| Stopped::Terminated),
        Err(failure) if restarts(&failure.kind) => {
            if let Err(lost) = terminate(&commands, &updates.layout, &updates.out).await {
                store_unsent(&mut channel, &mut from_box)?;
                return Err(lost);
            }
            Err(failure)
        }
        Ok(Served::BoxesStopped(failure)) | Err(failure) => Err(failure),
    };
    if stopped.is_err() {
        abandon(&commands, abandoned()).await;
    }
    store_unsent(&mut channel, &mut from_box)?;
    stopped
}

fn store_unsent(channel: &mut Channel, from_box: &mut UnboundedReceiver<Outgoing>) -> Result<(), Failure> {
    while let Ok(message) = from_box.try_recv() {
        channel
            .send(message, &mut Outbox::default())
            .map_err(|error| store_lost(&error))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn serve(
    channel: &mut Channel,
    url: &str,
    heartbeat: Duration,
    usage: &Usage,
    commands: &UnboundedSender<BoxCommand>,
    from_box: &mut UnboundedReceiver<Outgoing>,
    config: &SupervisorConfig,
    dir: &Path,
    updates: &mut Updates,
) -> Result<Served, Failure> {
    let mut backoff = Backoff::default();
    let mut limit = RateLimit::default();
    loop {
        let ended = session(channel, url, heartbeat, usage, commands, from_box, updates, &mut limit).await?;
        match next(channel, &mut backoff, url, ended, jitter()) {
            Next::Revoked(reason) => {
                let key = dir.join(KEY_FILE);
                std::fs::remove_file(&key).map_err(|error| {
                    fail(
                        SupervisorFailure::HostRevoked,
                        format!(
                            "host {} was revoked ({reason}) and deleting {} failed: {error}",
                            *config.host_id,
                            key.display()
                        ),
                    )
                })?;
                eprintln!(
                    "host {} was revoked: {reason}; deleted its key and stopped",
                    *config.host_id
                );
                return Ok(Served::Revoked);
            }
            Next::Update(reason) => {
                eprintln!("the control plane refused this supervisor's protocol ({reason}); updating now");
                if updates.updater.now().await == Outcome::Restart {
                    return Ok(Served::Updated);
                }
                return Err(fail(SupervisorFailure::ProtocolError, reason));
            }
            Next::Reconnect(wait) => {
                let wake = tokio::time::Instant::now() + wait;
                loop {
                    tokio::select! {
                        () = tokio::time::sleep_until(wake) => break,
                        Some(message) = from_box.recv() => channel.send(message, &mut Outbox::default()).map_err(|error| store_lost(&error))?,
                    }
                }
            }
        }
    }
}

enum Next {
    Revoked(String),
    Update(String),
    Reconnect(Duration),
}

fn next<S: Store, K: Signer>(
    channel: &mut HostChannel<S, K>,
    backoff: &mut Backoff,
    url: &str,
    ended: Ended,
    jitter: u16,
) -> Next {
    match ended {
        Ended::Revoked(reason) => Next::Revoked(reason),
        Ended::Refused(reason) => match channel.step_down() {
            Some(version) => {
                eprintln!("the control plane refused this supervisor's newest protocol ({reason}); reconnecting with protocol {version}");
                Next::Reconnect(Duration::ZERO)
            }
            None => Next::Update(reason),
        },
        Ended::Dropped { progressed, reason } => {
            channel.step_up();
            let wait = backoff.next(progressed, jitter);
            eprintln!(
                "channel to {url} dropped: {reason}; reconnecting in {:.1}s",
                wait.as_secs_f64()
            );
            Next::Reconnect(wait)
        }
    }
}

fn dropped(progressed: bool, reason: impl Into<String>) -> Ended {
    Ended::Dropped {
        progressed,
        reason: reason.into(),
    }
}

fn closed_by_peer(frame: Option<&CloseFrame>, progressed: bool) -> Result<Ended, Failure> {
    let Some(frame) = frame else {
        return Ok(dropped(progressed, "the control plane closed the connection"));
    };
    let reason = frame.reason.to_string();
    match u16::from(frame.code) {
        ChannelCloseCode::BAD_SIGNATURE => Err(fail(SupervisorFailure::ProtocolError, reason)),
        ChannelCloseCode::UNSUPPORTED_VERSION => Ok(Ended::Refused(reason)),
        ChannelCloseCode::REVOKED => Ok(Ended::Revoked(reason)),
        code => Ok(dropped(progressed, format!("closed with {code}: {reason}"))),
    }
}

fn box_command(message: ChannelControlMessage) -> Result<Option<BoxCommand>, String> {
    Ok(Some(match message {
        ChannelControlMessage::Unrecognised(_) => {
            tumpel_channel::update_required();
            return Ok(None);
        }
        ChannelControlMessage::TurnOffer(offer) => turn(offer)?,
        ChannelControlMessage::SnapshotRequest(request) => BoxCommand::Snapshot(request),
        ChannelControlMessage::BoxTeardown(teardown) => BoxCommand::Teardown(teardown),
        ChannelControlMessage::BoxDiscard(discard) => BoxCommand::Discard(discard),
        ChannelControlMessage::SnapshotChunksHeld(held) => BoxCommand::Answer(Answer::ChunksHeld(held)),
        ChannelControlMessage::SnapshotPackUploads(uploads) => BoxCommand::Answer(Answer::PackUploads(uploads)),
        ChannelControlMessage::RestoreDownloads(downloads) => BoxCommand::Answer(Answer::Downloads(downloads)),
        ChannelControlMessage::TurnWithdrawn(withdrawn) => BoxCommand::Answer(Answer::Withdrawn(withdrawn)),
        ChannelControlMessage::TurnClaimed(claimed) => BoxCommand::Answer(Answer::Claimed(claimed)),
        ChannelControlMessage::TurnCancel(cancel) => BoxCommand::Cancel(cancel.turn_id),
        ChannelControlMessage::ClaudeSetupBoxOffer(offer) => BoxCommand::Setup(setup_order(offer)?),
        ChannelControlMessage::ClaudeSetupBoxCode(code) => BoxCommand::Setup(SetupCommand::Code {
            claude_setup_id: code.claude_setup_id,
            code: code.code,
            mint_budget_seconds: code.mint_budget_seconds,
        }),
        ChannelControlMessage::ClaudeSetupBoxDiscard(discard) => BoxCommand::Setup(SetupCommand::Discard {
            claude_setup_id: discard.claude_setup_id,
        }),
    }))
}

fn accepted(channel: &mut Channel, commands: &UnboundedSender<BoxCommand>) -> Result<(), Failure> {
    let accepted = channel.take_accepted().into_iter().map(BoxCommand::SnapshotAccepted);
    let failed = channel.take_failed().into_iter().map(BoxCommand::SnapshotFailed);
    for command in accepted.chain(failed) {
        commands.send(command).map_err(|_| {
            fail(
                SupervisorFailure::ChannelStateLost,
                "the box runner stopped before it took an accepted or failed snapshot; systemd restarts the supervisor, which takes the running boxes over",
            )
        })?;
    }
    Ok(())
}

fn rate_limited(
    limit: &mut RateLimit,
    url: &str,
    reason: &str,
    frames_per_minute: u32,
    retry_after: chrono::DateTime<chrono::Utc>,
) {
    if let RateBackoff::Until(until) = limit.limited(chrono::Utc::now(), retry_after, frames_per_minute, jitter()) {
        eprintln!(
            "tumpel-supervisor: RateLimited: {url} refused a frame: {reason}; this host reports RateLimited and holds its frames until {}, then sends them again",
            until.to_rfc3339()
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn session(
    channel: &mut Channel,
    url: &str,
    heartbeat: Duration,
    usage: &Usage,
    commands: &UnboundedSender<BoxCommand>,
    from_box: &mut UnboundedReceiver<Outgoing>,
    updates: &mut Updates,
    limit: &mut RateLimit,
) -> Result<Ended, Failure> {
    let stream = match tokio::time::timeout(CONNECT_DEADLINE, tokio_tungstenite::connect_async(url)).await {
        Ok(Ok((stream, _))) => stream,
        Ok(Err(error)) => return Ok(dropped(false, format!("dialing failed: {error}"))),
        Err(_) => {
            return Ok(dropped(
                false,
                format!("connect timed out after {}s", CONNECT_DEADLINE.as_secs()),
            ))
        }
    };
    let (mut sink, mut source) = stream.split();
    channel.connect();
    let mut outbox = Outbox::default();
    let mut last_received = Instant::now();
    let mut last_beat = Instant::now();
    let mut tick = tokio::time::interval(TICK);
    loop {
        let ended = tokio::select! {
            message = source.next() => match message {
                None => Some(Ok(dropped(channel.progressed(), "the connection ended"))),
                Some(Err(error)) => Some(Ok(dropped(channel.progressed(), error.to_string()))),
                Some(Ok(Message::Close(frame))) => Some(closed_by_peer(frame.as_ref(), channel.progressed())),
                Some(Ok(Message::Text(text))) => {
                    last_received = Instant::now();
                    let received = channel.receive(text.as_str());
                    match accepted(channel, commands) {
                        Err(failure) => Some(Err(failure)),
                        Ok(()) => match received {
                            Ok(Event::Challenged(nonce)) => hello(channel, &nonce, usage, from_box, &mut outbox).err().map(Err),
                            Ok(Event::Welcomed) => {
                                eprintln!("connected to {url}");
                                last_beat = Instant::now();
                                match commands.send(BoxCommand::Welcomed(channel.version())) {
                                    Err(_) => Some(Err(fail(SupervisorFailure::ProtocolError, "the box task stopped; restart the supervisor"))),
                                    Ok(()) => updates.welcomed().await.err().map(Err),
                                }
                            }
                            Ok(Event::Message(message)) => match box_command(*message) {
                                Ok(Some(command)) => commands.send(command).err().map(|_| Err(fail(SupervisorFailure::ProtocolError, "the box task stopped; restart the supervisor"))),
                                Ok(None) => None,
                                Err(error) => Some(Err(fail(SupervisorFailure::ProtocolError, error))),
                            },
                            Ok(Event::FrameRefused { seq, reason, key }) => {
                                eprintln!("tumpel-supervisor: FrameRefused: the control plane refused frame {seq} and counted it received: {reason}; dropped it from channel.json");
                                if key.is_some_and(|key| commands.send(BoxCommand::Answer(Answer::refused(key, reason))).is_err()) {
                                    Some(Err(fail(SupervisorFailure::ProtocolError, "the box task stopped; restart the supervisor")))
                                } else {
                                    channel.transmit_due(Instant::now(), &mut outbox).err().map(|error| Err(store_lost(&error)))
                                }
                            }
                            Ok(Event::FrameRateLimited { seq: _, reason, frames_per_minute, retry_after }) => {
                                rate_limited(limit, url, &reason, frames_per_minute, retry_after);
                                None
                            }
                            Ok(Event::ReleasePublished) => {
                                updates.announced();
                                None
                            }
                            Ok(Event::None | Event::Heartbeat) => None,
                            Err(ChannelError::Protocol(reason)) => {
                                let close = CloseFrame { code: CloseCode::from(ChannelCloseCode::PROTOCOL_ERROR), reason: reason.chars().take(120).collect::<String>().into() };
                                let _ = sink.send(Message::Close(Some(close))).await;
                                Some(Ok(dropped(channel.progressed(), reason)))
                            }
                            Err(error) => Some(Err(fail(SupervisorFailure::ChannelStateLost, error.to_string()))),
                        },
                    }
                }
                Some(Ok(_)) => None,
            },
            Some(message) = from_box.recv() => channel.send(message, &mut outbox).err().map(|error| Err(store_lost(&error))),
            _ = tick.tick() => {
                let now = chrono::Utc::now();
                if last_received.elapsed() >= HEARTBEAT_DEADLINE {
                    Some(Ok(dropped(channel.progressed(), format!("nothing arrived for {} seconds", HEARTBEAT_DEADLINE.as_secs()))))
                } else if channel.phase() == Phase::Open && last_beat.elapsed() >= heartbeat {
                    last_beat = Instant::now();
                    beat(channel, usage, from_box, limit.health(now), &mut outbox).err().map(Err)
                } else if channel.phase() == Phase::Open && channel.withholding() && !limit.paused(now) {
                    channel.release(limit.release_budget(TICK), &mut outbox).err().map(|error| Err(store_lost(&error)))
                } else if channel.phase() == Phase::Open {
                    channel.transmit_due(Instant::now(), &mut outbox).err().map(|error| Err(store_lost(&error)))
                } else {
                    None
                }
            }
        };
        if let Some(ended) = ended.or_else(|| accepted(channel, commands).err().map(Err)) {
            channel.disconnect();
            return ended;
        }
        for text in outbox.0.drain(..) {
            if let Err(error) = sink.send(Message::Text(text.into())).await {
                channel.disconnect();
                return Ok(dropped(channel.progressed(), format!("sending failed: {error}")));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_setup_box_boots_the_harness_its_offer_names_and_claude_code_without_one() {
        let mut frame = serde_json::json!({
            "type": "ClaudeSetupBoxOffer",
            "claudeSetupId": "setup-1",
            "tenantId": "tenant-1",
            "boxId": "box-1",
            "harnessVersion": "0.169.0",
            "image": {
                "kind": "Full",
                "imageDigest": "a".repeat(64),
                "kernelDigest": "b".repeat(64),
                "manifestDigest": "c".repeat(64),
                "imageUrl": "https://r2.example.test/image",
                "kernelUrl": "https://r2.example.test/kernel",
                "manifestUrl": "https://r2.example.test/manifest",
                "downloadExpiresAt": "2026-11-19T00:00:00Z",
                "fetchSeconds": 300
            },
            "tokenUrl": "https://api.example.test/v1/claude-setups/setup-1/token",
            "tokenCredential": "t".repeat(43),
            "urlSeconds": 300,
            "codeSeconds": 600,
            "expiresAt": "2026-11-19T00:15:00Z"
        });
        let booted = |frame: &serde_json::Value| {
            let offer: ChannelClaudeSetupBoxOffer = serde_json::from_value(frame.clone()).expect("the offer parses");
            let Ok(SetupCommand::Start(order)) = setup_order(offer) else {
                panic!("a setup offer starts a setup box");
            };
            let (harness, version) = order.image.harness();
            format!("{harness} {version}")
        };
        assert_eq!(booted(&frame), "ClaudeCode 0.169.0");
        frame["harness"] = serde_json::json!("Codex");
        assert_eq!(booted(&frame), "Codex 0.169.0");
    }

    struct Unsaved;

    impl Store for Unsaved {
        fn save(&mut self, _: &tumpel_api::types::SupervisorChannelState) -> Result<(), ChannelError> {
            Ok(())
        }
    }

    struct Unsigned;

    impl Signer for Unsigned {
        fn sign(&self, _: &[u8]) -> String {
            "c2lnbmF0dXJl".to_string()
        }
    }

    fn hello_version(channel: &mut HostChannel<Unsaved, Unsigned>) -> serde_json::Value {
        channel.connect();
        let challenge = serde_json::json!({"type": "Challenge", "nonce": "n".repeat(43)}).to_string();
        let Ok(Event::Challenged(nonce)) = channel.receive(&challenge) else {
            panic!("the challenge asks for a Hello");
        };
        let usage = tumpel_channel::Usage {
            host: serde_json::from_value(serde_json::json!({"loadAverage": 0.0, "memoryAvailableMib": 1024, "diskBudgetMib": 0, "diskHeldMib": 0, "memoryBudgetMib": 0, "memoryHeldMib": 0}))
                .expect("usage"),
        };
        let mut outbox = Outbox::default();
        channel
            .hello(&nonce, std::num::NonZeroU32::MIN, usage, Vec::new(), &mut outbox)
            .expect("hello");
        channel.disconnect();
        let [hello] = outbox.0.try_into().expect("one hello");
        let hello: serde_json::Value = serde_json::from_str(&hello).expect("json");
        hello["protocolVersion"].clone()
    }

    #[test]
    fn a_refused_protocol_steps_down_one_version_at_a_time_and_a_dropped_connection_goes_back_to_39() {
        let mut channel = HostChannel::new(
            tumpel_channel::empty_state(),
            Unsaved,
            Unsigned,
            "host-1".try_into().expect("host id"),
            "0.1.0".to_string(),
        );
        let mut backoff = Backoff::default();
        let url = "wss://control.example.test/v1/channel";
        assert_eq!(hello_version(&mut channel), serde_json::json!(39));

        let refusal = CloseFrame {
            code: CloseCode::from(4426),
            reason: "this control plane speaks 34 to 34, update the supervisor".into(),
        };
        let Ok(ended) = closed_by_peer(Some(&refusal), false) else {
            panic!("a refusal ends the session");
        };
        let Next::Reconnect(wait) = next(&mut channel, &mut backoff, url, ended, 0) else {
            panic!("a refusal of the newest protocol reconnects");
        };
        assert_eq!(wait, Duration::ZERO);
        assert_eq!(hello_version(&mut channel), serde_json::json!(38));

        let Ok(ended) = closed_by_peer(Some(&refusal), false) else {
            panic!("a refusal ends the session");
        };
        let Next::Reconnect(wait) = next(&mut channel, &mut backoff, url, ended, 0) else {
            panic!("a refusal of 38 reconnects at the oldest");
        };
        assert_eq!(wait, Duration::ZERO);
        assert_eq!(hello_version(&mut channel), serde_json::json!(37));

        let Ok(ended) = closed_by_peer(Some(&refusal), false) else {
            panic!("a refusal ends the session");
        };
        assert!(matches!(
            next(&mut channel, &mut backoff, url, ended, 0),
            Next::Update(_)
        ));

        let Next::Reconnect(wait) = next(
            &mut channel,
            &mut backoff,
            url,
            dropped(false, "the connection ended"),
            u16::MAX,
        ) else {
            panic!("a dropped connection reconnects");
        };
        assert_eq!(wait, Duration::from_secs(1));
        assert_eq!(hello_version(&mut channel), serde_json::json!(39));
    }
}
