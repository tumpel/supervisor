use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io::BufWriter;
use std::num::{NonZeroU32, NonZeroU64};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, FixedOffset};
use futures_util::TryStreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{timeout, timeout_at, Instant};
use tracing::Instrument;
use tumpel_api::types::{
    BoxBudgets, BoxRetention, ChannelBoxGone, ChannelBoxGoneType, ChannelBoxTeardownFailed,
    ChannelBoxTeardownFailedType, ChannelBoxTeardownFailureReason, ChannelDispatchGrant, ChannelHostMessage,
    ChannelPack, ChannelPersonaFileDownload, ChannelRestoreChunks, ChannelRestoreChunksType, ChannelRestoreDownloads,
    ChannelSnapshotChunks, ChannelSnapshotChunksType, ChannelSnapshotDone, ChannelSnapshotDoneType,
    ChannelSnapshotDownload, ChannelSnapshotFailed, ChannelSnapshotFailedType, ChannelSnapshotFailureReason,
    ChannelSnapshotPacks, ChannelSnapshotPacksType, ChannelSnapshotRequest, ChannelTurnClaim, ChannelTurnClaimType,
    ChannelTurnComplete, ChannelTurnCompleteType, ChannelTurnCompleteWarning, ChannelTurnFailed, ChannelTurnFailedType,
    ChannelTurnImageFetching, ChannelTurnImageFetchingType, ChannelTurnLines, ChannelTurnLinesType,
    ChannelTurnRepositoryCloneFailed, ChannelTurnRepositoryCloneFailedType, ChannelTurnRepositoryReady,
    ChannelTurnRepositoryReadyType, CredentialText, DisplayName, EmailAddress, EntityId, GuestHello, GuestInput,
    GuestMessage, GuestPersonaFile, GuestProtocol, GuestTurnCancel, GuestTurnCancelType, GuestTurnStart,
    GuestTurnStartPrebuildCommitSha, GuestTurnStartType, ModelName, Prompt, RepositoryCloneFailureCause, RepositoryUrl,
    Sequence, Sha256Digest, SupervisorHandover, SupervisorHandoverBox, SupervisorHandoverBoxState,
    SupervisorHandoverCommand, SupervisorHandoverDiscard, SupervisorHandoverDiscardKind, SupervisorHandoverMachine,
    SupervisorHandoverSnapshot, SupervisorHandoverSnapshotKind, SupervisorHandoverTeardown,
    SupervisorHandoverTeardownKind, SupervisorHandoverTurn, SupervisorHandoverTurnError, SupervisorHandoverTurnKind,
    SupervisorHandoverWarm, SupervisorHandoverWarmKind, TurnFailureReason,
};
use tumpel_channel::{Outgoing, PACK_REUSE};

use crate::boot::{self, Boot, BootOrder, Caches, Hold, Holds};
use crate::bounded::{bounded, reported};
use crate::exchange::{part_number, Answer, Answers, Ask, Cancels, Key, Waiting, DIGESTS_PER_PART};
use crate::firecracker::Observation;
use crate::harness_result::{error_result, exit_result, turn_end};
use crate::images;
use crate::inventory::{Inventory, RecordedBox, StartupBoxes};
use crate::layers::LayerCache;
use crate::local_handover::Descriptors;
use crate::pool::{mib, restore_charge_mib, Charge, Disk, Held, Tally, Tap, TapPool};
use crate::snapshot::{
    assemble, cut, decode, decode_root, draft_digests, fetches, fresh, lacking, pack_parts, restore_parts, stage,
    unpack, write_restore, Base, Births, ChunkCache, Cut, Fetch, Kind, Manifest, PackFile, Place, Places, Span,
};
use crate::store::{wait, DownloadError, Store, UploadError, ATTEMPTS_PER_URL, DOWNLOAD_ATTEMPTS};
use crate::telemetry::{self, Phase};
use crate::update::Idle;

pub(crate) mod setup;

pub use setup::{SetupCommand, SetupInput, SetupOrder};

const TRANSFERS: usize = 16;
const CHUNKS: &str = "chunks";
const HANDOVER_REPORT_ALLOWANCE: Duration = Duration::from_secs(60);
const CANCEL_END: Duration = Duration::from_secs(10);
const DISK_CHECK: Duration = Duration::from_secs(1);
const RESTORE_BYTES_PER_SECOND: u64 = 32 * 1024 * 1024;
const RESTORE_ENTRIES_PER_SECOND: u64 = 2_000;
const OVERLAY_BYTES_PER_INODE: u64 = 16 * 1024;

fn restore_budget(floor: Duration, bytes: u64, entries: u64) -> Duration {
    floor
        + Duration::from_secs(bytes.div_ceil(RESTORE_BYTES_PER_SECOND))
        + Duration::from_secs(entries.div_ceil(RESTORE_ENTRIES_PER_SECOND))
}

fn positive_mib(value: Option<i32>) -> Result<Option<NonZeroU32>, i32> {
    value.map(|value| mib(value.into()).ok_or(value)).transpose()
}

fn restore_size(named: &str, manifest: &Manifest, overlay: u64) -> Result<(u64, u64), String> {
    let bytes = manifest
        .values()
        .map(|stored| match stored.entry.kind {
            Kind::File { size } => size,
            _ => 0,
        })
        .fold(0, u64::saturating_add);
    if bytes > overlay {
        return Err(format!(
            "{named} holds {bytes} bytes, more than the {overlay} bytes of the box's disk; raise the persona's boxDiskMib or start a new session"
        ));
    }
    let entries = manifest.len() as u64;
    let inodes = overlay / OVERLAY_BYTES_PER_INODE;
    if entries > inodes {
        return Err(format!(
            "{named} holds {entries} entries, more than the {inodes} files the box's disk of {overlay} bytes can hold; raise the persona's boxDiskMib or start a new session"
        ));
    }
    Ok((bytes, entries))
}

#[derive(Clone, Copy, Debug)]
pub struct Budgets {
    pub boot: Duration,
    pub first_line: Duration,
    pub snapshot: Duration,
}

impl From<&BoxBudgets> for Budgets {
    fn from(budgets: &BoxBudgets) -> Self {
        let seconds = |value: std::num::NonZeroU32| Duration::from_secs(u64::from(value.get()));
        Self {
            boot: seconds(budgets.boot_seconds),
            first_line: seconds(budgets.first_line_seconds),
            snapshot: seconds(budgets.snapshot_seconds),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub budgets: Budgets,
    pub layer_cache_mib: NonZeroU32,
}

pub trait Guest: Send + 'static {
    fn send(&mut self, input: &GuestInput) -> impl Future<Output = Result<(), String>> + Send;
    fn receive(&mut self) -> impl Future<Output = Result<GuestMessage, String>> + Send;
}

pub trait Machine: Send + 'static {
    type Guest: Guest;
    fn activate(&mut self) {}
    fn register_inherited(&self, _guest: &Self::Guest) -> Result<(), String> {
        Ok(())
    }
    fn start(
        &mut self,
        boot: &Boot,
        restore: Option<&Path>,
    ) -> impl Future<Output = Result<(Self::Guest, GuestHello), String>> + Send;
    fn pause(&mut self) -> impl Future<Output = Result<(), String>> + Send;
    fn resume(&mut self) -> impl Future<Output = Result<(), String>> + Send;
    fn drive(&self) -> PathBuf;
    fn stop(self) -> impl Future<Output = Stopped> + Send;
    fn console_tail(&self) -> String;
    fn clear_console(&mut self);
    fn hand_over(self, guest: Self::Guest) -> Result<SupervisorHandoverMachine, String>;
    fn record(&self) -> Result<SupervisorHandoverMachine, String>;
    fn handover_descriptors(&self, _guest: &Self::Guest) -> Result<Option<[OwnedFd; 3]>, String> {
        Ok(None)
    }
    fn observation(&self, _guest: &Self::Guest) -> Result<Option<Observation>, String> {
        Ok(None)
    }
    fn reconnect(&mut self, guest: &mut Self::Guest) -> impl Future<Output = Result<(), String>> + Send;
}

pub struct Stopped {
    pub exit: String,
    pub cleared: Result<(), String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BoxEnd {
    Discard,
    Teardown,
    NewBox,
    Idle,
    BootFailed,
    ResumeFailed,
    NotRecorded,
    RecoverFailed,
    PauseFailed,
    SnapshotNotRecorded,
    SetupBootFailed,
    SetupNotHanded,
    SetupEnded,
}

pub(crate) async fn end_box<M: Machine>(
    machine: M,
    index: u8,
    session_id: &EntityId,
    box_id: &EntityId,
    end: BoxEnd,
) -> Result<(), String> {
    let Stopped { exit, cleared } = machine.stop().await;
    eprintln!(
        "tumpel-supervisor: box {index} of session {}, box id {}, ended on {end:?}: Firecracker {exit}",
        **session_id, **box_id
    );
    cleared
}

pub trait Vmm: Send + Sync + 'static {
    type Machine: Machine;
    type Upper: crate::snapshot::Upper;
    fn machine(&self, session_id: &EntityId, tap: &Tap) -> Result<Self::Machine, String>;
    fn reattach(
        &self,
        session_id: &EntityId,
        index: i32,
        handed: &SupervisorHandoverMachine,
        observation: Option<&Observation>,
        pool: &mut TapPool,
        inherited: bool,
    ) -> Result<Option<Attached<Self::Machine>>, String>;
    fn recorded_process(
        &self,
        index: u8,
        handed: &SupervisorHandoverMachine,
        observation: Option<&Observation>,
    ) -> Result<bool, String>;
    fn disk(&self, drives: &[String]) -> Result<Disk, String>;
    fn unclaimed_scopes(&self, claimed: &[u8]) -> Result<Vec<u8>, String>;
    fn unclaimed_reservations(&self, claimed: &[u8]) -> Result<BTreeMap<u8, Vec<EntityId>>, String>;
    fn discard_incomplete_boot(&self, index: u8) -> impl Future<Output = Result<(), String>> + Send;
    fn discard_protected(
        &self,
        index: u8,
        saved: &SupervisorHandoverBox,
        observation: Option<&Observation>,
    ) -> impl Future<Output = Result<(), String>> + Send;
    fn remove_leftovers(&self, boxes: &[u8], sessions: &[EntityId]) -> Result<(), String>;
    fn open_upper(&self, drive: &Path) -> Result<Self::Upper, String>;
    fn staging(&self, session_id: &EntityId) -> Result<PathBuf, String>;
    fn image_cache(&self) -> PathBuf;
}

pub struct BoxTurn {
    pub turn_id: EntityId,
    pub session_id: EntityId,
    pub box_id: EntityId,
    pub image: BootOrder,
    pub model: ModelName,
    pub prompt: Prompt,
    pub credential: ChannelDispatchGrant,
    pub last_line_seq: Sequence,
    pub snapshots: Vec<ChannelSnapshotDownload>,
    pub persona_files: Vec<ChannelPersonaFileDownload>,
    pub repository: RepositoryUrl,
    pub git_author_name: DisplayName,
    pub git_author_email: EmailAddress,
    pub files_dir: PathBuf,
    pub prebuild_commit_sha: Option<String>,
    pub box_memory_mib: Option<i32>,
    pub box_disk_mib: Option<i32>,
    pub turn_seconds: NonZeroU32,
    pub idle_seconds: NonZeroU32,
    pub box_retention: BoxRetention,
}

pub struct SnapshotOrder {
    pub snapshot_id: EntityId,
    pub session_id: EntityId,
    pub box_id: EntityId,
    pub snapshot_seq: Sequence,
    pub upload_url: String,
    pub upload_expires_at: DateTime<FixedOffset>,
}

pub struct TeardownOrder {
    pub session_id: EntityId,
    pub box_id: EntityId,
}

pub enum Command {
    Turn(Box<BoxTurn>),
    Snapshot(SnapshotOrder),
    Teardown(TeardownOrder),
    Discard(TeardownOrder),
}

impl From<ChannelSnapshotRequest> for SnapshotOrder {
    fn from(request: ChannelSnapshotRequest) -> Self {
        Self {
            snapshot_id: request.snapshot_id,
            session_id: request.session_id,
            box_id: request.box_id,
            snapshot_seq: request.snapshot_seq,
            upload_url: request.upload_url.into(),
            upload_expires_at: request.upload_expires_at,
        }
    }
}

impl Command {
    fn record(&self) -> Result<Option<SupervisorHandoverCommand>, String> {
        Ok(match self {
            Self::Turn(_) => None,
            Self::Snapshot(order) => Some(SupervisorHandoverCommand::Snapshot(SupervisorHandoverSnapshot {
                kind: SupervisorHandoverSnapshotKind::Snapshot,
                snapshot_id: order.snapshot_id.clone(),
                snapshot_seq: order.snapshot_seq.clone(),
                upload_expires_at: order.upload_expires_at,
                upload_url: bounded("the snapshot upload URL", order.upload_url.clone())?,
            })),
            Self::Teardown(_) => Some(SupervisorHandoverCommand::Teardown(SupervisorHandoverTeardown {
                kind: SupervisorHandoverTeardownKind::Teardown,
            })),
            Self::Discard(_) => Some(SupervisorHandoverCommand::Discard(SupervisorHandoverDiscard {
                kind: SupervisorHandoverDiscardKind::Discard,
            })),
        })
    }

    fn records<'a>(queue: impl IntoIterator<Item = &'a Self>) -> Result<Vec<SupervisorHandoverCommand>, String> {
        queue
            .into_iter()
            .map(Self::record)
            .filter_map(Result::transpose)
            .collect()
    }

    fn from_record(record: SupervisorHandoverCommand, session_id: &EntityId, box_id: &EntityId) -> Option<Self> {
        let order = || TeardownOrder {
            session_id: session_id.clone(),
            box_id: box_id.clone(),
        };
        Some(match record {
            SupervisorHandoverCommand::Unrecognised(_) => {
                tumpel_channel::update_required();
                return None;
            }
            SupervisorHandoverCommand::Snapshot(snapshot) => Self::Snapshot(SnapshotOrder {
                snapshot_id: snapshot.snapshot_id,
                session_id: session_id.clone(),
                box_id: box_id.clone(),
                snapshot_seq: snapshot.snapshot_seq,
                upload_url: snapshot.upload_url.into(),
                upload_expires_at: snapshot.upload_expires_at,
            }),
            SupervisorHandoverCommand::Teardown(_) => Self::Teardown(order()),
            SupervisorHandoverCommand::Discard(_) => Self::Discard(order()),
        })
    }

    fn session(&self) -> String {
        match self {
            Self::Turn(turn) => turn.session_id.to_string(),
            Self::Snapshot(order) => order.session_id.to_string(),
            Self::Teardown(order) | Self::Discard(order) => order.session_id.to_string(),
        }
    }
}

struct LiveBox<M: Machine> {
    session_id: EntityId,
    box_id: EntityId,
    machine: M,
    guest: M::Guest,
    tap: Tap,
    base: Arc<Base>,
    image: Box<Boot>,
    _hold: Hold,
    dirty: bool,
    credential_file: Option<String>,
    box_retention: BoxRetention,
    idle: Duration,
}

impl<M: Machine> LiveBox<M> {
    fn booted(turn: &BoxTurn, machine: M, guest: M::Guest, tap: Tap, base: Base, image: Boot, hold: Hold) -> Self {
        Self {
            session_id: turn.session_id.clone(),
            box_id: turn.box_id.clone(),
            machine,
            guest,
            tap,
            base: Arc::new(base),
            image: Box::new(image),
            _hold: hold,
            dirty: false,
            credential_file: None,
            box_retention: turn.box_retention.clone(),
            idle: Duration::from_secs(u64::from(turn.idle_seconds.get())),
        }
    }
}

impl<M: Machine> LiveBox<M> {
    fn record(&self, turn_id: Option<&EntityId>) -> Result<SupervisorHandoverBox, String> {
        Ok(SupervisorHandoverBox {
            box_id: self.box_id.clone(),
            box_retention: self.box_retention.clone(),
            credential_file: credential_record(self.credential_file.clone())?,
            dirty: self.dirty || turn_id.is_some(),
            idle_milliseconds: milliseconds(self.idle),
            image: self.image.record()?,
            machine: self.machine.record()?,
            queued: Vec::new(),
            session_id: self.session_id.clone(),
            state: standing_state(turn_id.cloned()),
            tap_index: i32::from(self.tap.index()),
        })
    }
}

fn standing_state(turn_id: Option<EntityId>) -> SupervisorHandoverBoxState {
    match turn_id {
        Some(turn_id) => SupervisorHandoverBoxState::Turn(SupervisorHandoverTurn {
            first_line_milliseconds: 0,
            turn_milliseconds: 0,
            kind: SupervisorHandoverTurnKind::Turn,
            repository_ready: false,
            seen_line: false,
            error: None,
            cancelled: None,
            turn_id,
        }),
        None => SupervisorHandoverBoxState::Warm(SupervisorHandoverWarm {
            kind: SupervisorHandoverWarmKind::Warm,
        }),
    }
}

fn credential_record(file: Option<String>) -> Result<Option<CredentialText>, String> {
    file.map(|file| bounded("the turn's credential file", file)).transpose()
}

fn standing_record(handed: &SupervisorHandoverBox) -> Result<SupervisorHandoverBox, String> {
    let turn_id = match &handed.state {
        SupervisorHandoverBoxState::Turn(turn) => Some(turn.turn_id.clone()),
        SupervisorHandoverBoxState::Warm(_) => None,
        SupervisorHandoverBoxState::Unrecognised(_) => {
            tumpel_channel::update_required();
            return Err(tumpel_channel::UPDATE_REQUIRED.to_string());
        }
    };
    Ok(SupervisorHandoverBox {
        dirty: handed.dirty || turn_id.is_some(),
        machine: SupervisorHandoverMachine {
            console_tail: bounded("the empty console tail", String::new())?,
            guest_buffer: bounded("the empty guest buffer", String::new())?,
            guest_fd: -1,
            stderr_fd: -1,
            stdout_fd: -1,
            ..handed.machine.clone()
        },
        state: standing_state(turn_id),
        ..handed.clone()
    })
}

pub(crate) fn decode_box(
    handed: &SupervisorHandoverBox,
) -> Result<(SupervisorHandoverBox, Boot, VecDeque<Command>), String> {
    crate::dir::validate_session_id(&handed.session_id)?;
    let record = standing_record(handed)?;
    let image = Boot::from_record(handed.image.clone())?;
    let queue = handed
        .queued
        .iter()
        .cloned()
        .filter_map(|record| Command::from_record(record, &handed.session_id, &handed.box_id))
        .collect();
    Ok((record, image, queue))
}

struct StandingBox {
    saved: RecordedBox,
    descriptors: Result<Arc<[OwnedFd; 3]>, String>,
}

impl StandingBox {
    fn capture<M: Machine>(record: SupervisorHandoverBox, machine: &M, guest: &M::Guest) -> Self {
        let observation = report_observation(machine, guest, record.tap_index);
        let descriptors = machine.handover_descriptors(guest).and_then(|descriptors| {
            descriptors
                .map(Arc::new)
                .ok_or_else(|| "no saved descriptors for handover".to_string())
        });
        Self {
            saved: RecordedBox {
                record,
                observation,
                reconnect: false,
            },
            descriptors,
        }
    }
}

pub(crate) struct HandoverSource {
    standing: Standing,
    running: watch::Receiver<u32>,
}

impl HandoverSource {
    pub fn descriptors(&self) -> anyhow::Result<Descriptors> {
        let boxes = self
            .standing
            .boxes
            .lock()
            .map_err(|_| anyhow::anyhow!("the box inventory is unavailable"))?;
        anyhow::ensure!(
            u32::try_from(boxes.len())? == *self.running.borrow(),
            "a box has no standing record for handover"
        );
        boxes
            .iter()
            .map(|(index, box_)| {
                let tap = u8::try_from(*index)?;
                let descriptors = box_
                    .descriptors
                    .as_ref()
                    .map_err(|error| anyhow::anyhow!("box {tap}: {error}"))?;
                Ok((tap, Arc::clone(descriptors)))
            })
            .collect()
    }
}

fn report_observation<M: Machine>(machine: &M, guest: &M::Guest, tap: i32) -> Option<Observation> {
    machine.observation(guest).unwrap_or_else(|error| {
        eprintln!("tumpel-supervisor: cannot inspect box {tap} for the report: {error}");
        None
    })
}

#[derive(Clone)]
struct Standing {
    path: PathBuf,
    boxes: Arc<std::sync::Mutex<BTreeMap<i32, StandingBox>>>,
    idle_ends: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
}

impl Standing {
    fn write(&self, change: impl FnOnce(&mut BTreeMap<i32, StandingBox>)) -> Result<(), String> {
        let mut boxes = self
            .boxes
            .lock()
            .map_err(|_| "the record of running boxes was poisoned by a panic".to_string())?;
        change(&mut boxes);
        let idle_ends = self.idle_ends.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let written = Instant::now();
        for box_ in boxes.values_mut() {
            let record = &mut box_.saved.record;
            if let (SupervisorHandoverBoxState::Warm(_), Some(end)) =
                (&record.state, idle_ends.get(record.session_id.as_str()))
            {
                record.idle_milliseconds = milliseconds(end.saturating_duration_since(written));
            }
        }
        drop(idle_ends);
        let record = Inventory {
            boxes: boxes.values().map(|box_| box_.saved.clone()).collect(),
            handed_at: now(),
        };
        let text = serde_json::to_string(&record).map_err(|error| format!("encoding the running boxes: {error}"))?;
        let staged = self.path.with_extension("json.new");
        std::fs::write(&staged, text)
            .and_then(|()| std::fs::rename(&staged, &self.path))
            .map_err(|error| format!("writing {}: {error}", self.path.display()))
    }

    fn idle_until(&self, session: &str, end: Option<Instant>) {
        let mut idle_ends = self.idle_ends.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match end {
            Some(end) => idle_ends.insert(session.to_string(), end),
            None => idle_ends.remove(session),
        };
    }

    fn put<M: Machine>(&self, live: &LiveBox<M>, turn_id: Option<&EntityId>) -> Result<(), String> {
        let mut box_ = StandingBox::capture(live.record(turn_id)?, &live.machine, &live.guest);
        self.write(|boxes| {
            let record = &mut box_.saved.record;
            if let Some(kept) = boxes
                .remove(&record.tap_index)
                .map(|kept| kept.saved.record)
                .filter(|kept| kept.session_id == record.session_id)
            {
                record.queued = kept.queued;
            }
            boxes.insert(record.tap_index, box_);
        })
    }

    fn queue(&self, session: &str, queued: &[SupervisorHandoverCommand]) -> Result<(), String> {
        let unchanged = self
            .boxes
            .lock()
            .map_err(|_| "the record of running boxes was poisoned by a panic".to_string())?
            .values()
            .map(|box_| &box_.saved.record)
            .filter(|record| record.session_id.as_str() == session)
            .all(|record| record.queued == queued);
        if unchanged {
            return Ok(());
        }
        self.write(|boxes| {
            for record in boxes
                .values_mut()
                .map(|box_| &mut box_.saved.record)
                .filter(|record| record.session_id.as_str() == session)
            {
                record.queued = queued.to_vec();
            }
        })
    }
}

enum Outcome<M: Machine> {
    Kept(LiveBox<M>),
    Parked(LiveBox<M>, Box<Parked>),
    Gone(Tap, Result<(), String>),
    Protected(Tap),
}

struct Follow {
    end: Instant,
    first_line: Instant,
    repository_ready: bool,
    seen_line: bool,
    error: Option<(TurnFailureReason, String)>,
}

enum Followed {
    Ended(Result<(Sequence, Option<ChannelTurnCompleteWarning>), Failure>),
    Parked(Left),
}

struct Left {
    turn: Duration,
    first_line: Duration,
    repository_ready: bool,
    seen_line: bool,
    error: Option<(TurnFailureReason, String)>,
}

impl Follow {
    fn park(self) -> Left {
        let now = Instant::now();
        Left {
            turn: self.end.saturating_duration_since(now),
            first_line: self.first_line.saturating_duration_since(now),
            repository_ready: self.repository_ready,
            seen_line: self.seen_line,
            error: self.error,
        }
    }
}

impl Left {
    fn resume(self) -> Follow {
        let now = Instant::now();
        Follow {
            end: now + self.turn,
            first_line: now + self.first_line,
            repository_ready: self.repository_ready,
            seen_line: self.seen_line,
            error: self.error,
        }
    }
}

pub type Attached<M> = (M, <M as Machine>::Guest, Tap);

struct Parked {
    turn_id: EntityId,
    left: Left,
}

enum Running {
    Recovering,
    Turn(EntityId),
    Snapshot,
}

fn milliseconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn duration(milliseconds: i64) -> Duration {
    Duration::from_millis(u64::try_from(milliseconds).unwrap_or(0))
}

enum Work {
    Turn,
    Snapshot,
}

enum Event<M: Machine> {
    Finished {
        session: String,
        work: Work,
        outcome: Outcome<M>,
    },
    SetupFinished {
        claude_setup_id: String,
        stopped: Result<(), String>,
        tap: Tap,
    },
    HandoverExpired,
    IdleExpired {
        session: String,
        box_id: EntityId,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    TakingTurns,
    HandingOver,
}

struct Slot<M: Machine> {
    live: Option<LiveBox<M>>,
    busy: bool,
    paused: bool,
    parked: Option<Box<Parked>>,
    task: Option<(Running, JoinHandle<()>)>,
    boot_tap: Option<u8>,
    queue: VecDeque<Command>,
    idle: Option<AbortHandle>,
    idle_end: Option<Instant>,
    snapshot: Option<Awaited>,
}

enum Awaited {
    Pending(EntityId),
    Accepted,
}

impl<M: Machine> Slot<M> {
    fn begin_boot(&mut self, tap: &Tap) {
        self.busy = true;
        self.boot_tap = Some(tap.index());
    }

    fn turns(&self) -> Vec<EntityId> {
        let running = self.task.iter().filter_map(|(running, _)| match running {
            Running::Turn(turn_id) => Some(turn_id.clone()),
            Running::Snapshot | Running::Recovering => None,
        });
        let queued = self.queue.iter().filter_map(|command| match command {
            Command::Turn(turn) => Some(turn.turn_id.clone()),
            _ => None,
        });
        let parked = self.parked.iter().map(|parked| parked.turn_id.clone());
        running.chain(queued).chain(parked).collect()
    }
}

impl<M: Machine> Default for Slot<M> {
    fn default() -> Self {
        Self {
            live: None,
            busy: false,
            paused: false,
            parked: None,
            task: None,
            boot_tap: None,
            queue: VecDeque::new(),
            idle: None,
            idle_end: None,
            snapshot: None,
        }
    }
}

#[derive(Clone)]
enum Report {
    Turn(TurnFailureReason),
    Clone(RepositoryCloneFailureCause),
}

struct Failure {
    report: Report,
    detail: String,
}

impl Failure {
    fn turn(reason: TurnFailureReason, detail: impl Into<String>) -> Self {
        Self {
            report: Report::Turn(reason),
            detail: detail.into(),
        }
    }
}

fn now() -> DateTime<FixedOffset> {
    chrono::Utc::now().fixed_offset()
}

pub fn instant_at(at: DateTime<FixedOffset>) -> Instant {
    Instant::now() + (at - now()).to_std().unwrap_or(Duration::ZERO)
}

fn emit(out: &UnboundedSender<Outgoing>, message: impl Into<ChannelHostMessage>) {
    let message = message.into();
    telemetry::ended(&message);
    send_out(out, Outgoing::Message(message));
}

fn send_out(out: &UnboundedSender<Outgoing>, outgoing: Outgoing) {
    if out.send(outgoing).is_err() {
        eprintln!("tumpel-supervisor: the channel stopped taking box messages; the supervisor is shutting down");
    }
}

pub(crate) fn turn_failed(
    out: &UnboundedSender<Outgoing>,
    turn_id: &EntityId,
    reason: TurnFailureReason,
    detail: impl AsRef<str>,
) {
    emit(
        out,
        ChannelTurnFailed {
            credential_file: None,
            detail: reported("the turn failure detail", detail.as_ref()),
            failed_at: now(),
            reason,
            turn_id: turn_id.clone(),
            type_: ChannelTurnFailedType::TurnFailed,
        },
    );
}

fn report_failure(out: &UnboundedSender<Outgoing>, turn_id: &EntityId, report: Report, detail: String) {
    match report {
        Report::Turn(reason) => turn_failed(out, turn_id, reason, detail),
        Report::Clone(cause) => emit(
            out,
            ChannelTurnRepositoryCloneFailed {
                cause,
                detail: reported("the repository clone failure detail", &detail),
                failed_at: now(),
                turn_id: turn_id.clone(),
                type_: ChannelTurnRepositoryCloneFailedType::TurnRepositoryCloneFailed,
            },
        ),
    }
}

fn expired(credential: &ChannelDispatchGrant, when: &str) -> Option<String> {
    (credential.expires_at <= now()).then(|| credential_expired(credential, when))
}

fn credential_expired(credential: &ChannelDispatchGrant, when: &str) -> String {
    format!(
        "dispatch credential {} expired at {} {when}; the control plane mints a fresh one when it offers the turn again",
        *credential.credential_handover_id,
        credential.expires_at.to_rfc3339()
    )
}

fn remove_file(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            Err(format!("removing {}: {error}", path.display()))
        }
        _ => Ok(()),
    }
}

fn disk_checks() -> tokio::time::Interval {
    let mut checks = tokio::time::interval(DISK_CHECK);
    checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    checks
}

fn cancel_unanswered(turn_id: &EntityId) -> Failure {
    Failure::turn(
        TurnFailureReason::Cancelled,
        format!(
            "the guest did not end cancelled turn {} within {} s, so the host closed its connection, which stops the harness.",
            **turn_id,
            CANCEL_END.as_secs()
        ),
    )
}

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn rebuild_archive(
    manifest: Manifest,
    cache: &ChunkCache,
    output: &Path,
    chunks: &Path,
    cancelled: &AtomicBool,
) -> Result<(Manifest, u64), String> {
    let out = std::fs::File::create(output).map_err(|error| format!("creating {}: {error}", output.display()))?;
    let written = write_restore(&manifest, cache, BufWriter::new(out), cancelled);
    if cancelled.load(Ordering::Relaxed) {
        remove_file(output)?;
    }
    written?;
    remove_dir(chunks)?;
    let archived = std::fs::metadata(output)
        .map_err(|error| format!("reading the size of {}: {error}", output.display()))?
        .len();
    Ok((manifest, archived))
}

fn console_during_turn(tail: &str) -> String {
    if tail.is_empty() {
        "The box's console printed nothing during the turn.".to_string()
    } else {
        format!("The box's console during the turn: {tail}")
    }
}

async fn cancel<M: Machine>(live: &mut LiveBox<M>, turn_id: &EntityId, state: &mut Follow) -> Result<(), Failure> {
    state.end = state.end.min(Instant::now() + CANCEL_END);
    state.first_line = state.first_line.min(state.end);
    let cancel = GuestInput::TurnCancel(GuestTurnCancel {
        turn_id: turn_id.clone(),
        type_: GuestTurnCancelType::TurnCancel,
    });
    live.guest.send(&cancel).await.map_err(|error| {
        Failure::turn(
            TurnFailureReason::Cancelled,
            format!("handing the cancel of turn {} to the guest failed: {error}.", **turn_id),
        )
    })
}

async fn stop_box<M: Machine>(standing: &Standing, live: LiveBox<M>, end: BoxEnd) -> (Tap, String, Result<(), String>) {
    let tail = live.machine.console_tail();
    let LiveBox {
        machine,
        tap,
        session_id,
        box_id,
        ..
    } = live;
    let stopped = end_box(machine, tap.index(), &session_id, &box_id, end).await;
    let index = i32::from(tap.index());
    if stopped.is_ok() {
        if let Err(error) = standing.write(|boxes| {
            boxes.remove(&index);
        }) {
            eprintln!(
                "tumpel-supervisor: forgetting stopped box {index} in the record of running boxes failed: {error}; the next start finds it gone"
            );
        }
    }
    (tap, tail, stopped)
}

struct Asking {
    answers: Answers,
    key: Key,
    waiting: Option<oneshot::Receiver<Answer>>,
    deadline: Option<Instant>,
}

impl Asking {
    async fn answer(&mut self) -> Option<Answer> {
        let waiting = self.waiting.take()?;
        let answer = match self.deadline {
            Some(deadline) => timeout_at(deadline, waiting).await.ok().and_then(Result::ok),
            None => waiting.await.ok(),
        };
        if answer.is_none() {
            self.answers.forget(&self.key);
        }
        answer
    }
}

struct Claiming {
    asking: Asking,
    phase: tracing::Span,
}

impl Claiming {
    async fn answer(&mut self) -> Option<Answer> {
        let answer = self.asking.answer().await;
        if matches!(answer, Some(Answer::Claimed(_))) {
            telemetry::reached(&self.phase);
        }
        self.phase = tracing::Span::none();
        answer
    }
}

impl Drop for Asking {
    fn drop(&mut self) {
        if self.waiting.is_some() {
            self.answers.forget(&self.key);
        }
    }
}

type Restored = (PathBuf, Base, u64, Charge);

struct Runner<V: Vmm, S: Store> {
    vmm: Arc<V>,
    store: Arc<S>,
    settings: Settings,
    out: UnboundedSender<Outgoing>,
    answers: Answers,
    fetching: Arc<Mutex<()>>,
    layers: LayerCache,
    holds: Holds,
    parking: watch::Receiver<bool>,
    standing: Standing,
    cancels: Cancels,
    staging: Tally,
    reserves: Tally,
}

impl<V: Vmm, S: Store> Clone for Runner<V, S> {
    fn clone(&self) -> Self {
        Self {
            vmm: Arc::clone(&self.vmm),
            store: Arc::clone(&self.store),
            settings: self.settings,
            out: self.out.clone(),
            answers: self.answers.clone(),
            fetching: Arc::clone(&self.fetching),
            layers: self.layers.clone(),
            holds: self.holds.clone(),
            parking: self.parking.clone(),
            standing: self.standing.clone(),
            cancels: self.cancels.clone(),
            staging: self.staging.clone(),
            reserves: self.reserves.clone(),
        }
    }
}

impl<V: Vmm, S: Store> Runner<V, S> {
    async fn boot(
        &self,
        order: &BootOrder,
        fetching: impl FnOnce(Sha256Digest),
    ) -> Result<(Boot, Hold), images::Refusal> {
        let images = self.vmm.image_cache();
        let caches = Caches {
            store: &*self.store,
            images: &images,
            layers: &self.layers,
            holds: &self.holds,
            lock: &self.fetching,
        };
        boot::resolve(&caches, order, fetching).await
    }

    async fn image(&self, turn: &BoxTurn) -> Result<(Boot, Hold), images::Refusal> {
        let fetching = |image_digest| {
            emit(
                &self.out,
                ChannelTurnImageFetching {
                    image_digest,
                    turn_id: turn.turn_id.clone(),
                    type_: ChannelTurnImageFetchingType::TurnImageFetching,
                },
            );
        };
        let booted = telemetry::timed(Phase::ImageFetch, self.boot(&turn.image, fetching), Result::is_ok).await?;
        expired(&turn.credential, "after its guest image was fetched").map_or(Ok(booted), |detail| {
            Err(images::Refusal {
                reason: TurnFailureReason::DispatchCredentialExpired,
                detail,
            })
        })
    }

    async fn files(&self, turn: &BoxTurn) -> Option<Vec<GuestPersonaFile>> {
        match crate::persona_files::fetch(&*self.store, &turn.files_dir.join(&*turn.turn_id), &turn.persona_files).await
        {
            Ok(files) => Some(files),
            Err(detail) => {
                turn_failed(
                    &self.out,
                    &turn.turn_id,
                    TurnFailureReason::PersonaFileUnreadable,
                    detail,
                );
                None
            }
        }
    }

    fn claim(&self, turn: &BoxTurn) -> Claiming {
        let phase = telemetry::phase(Phase::Claim);
        let asking = self.send(
            Key::claim(&turn.turn_id),
            ChannelTurnClaim {
                box_id: turn.box_id.clone(),
                turn_id: turn.turn_id.clone(),
                type_: ChannelTurnClaimType::TurnClaim,
            },
            None,
        );
        Claiming { asking, phase }
    }

    async fn warm_turn(&self, mut live: LiveBox<V::Machine>, turn: &BoxTurn) -> Outcome<V::Machine> {
        live.box_retention = turn.box_retention.clone();
        live.idle = Duration::from_secs(u64::from(turn.idle_seconds.get()));
        let claiming = self.claim(turn);
        let Some(files) = self.files(turn).await else {
            return Outcome::Kept(live);
        };
        let seconds = self.settings.budgets.boot.as_secs();
        let failed = match self.standing.put(&live, Some(&turn.turn_id)) {
            Err(error) => format!(
                "recording the box of session {} for a supervisor restart failed: {error}",
                *turn.session_id
            ),
            Ok(()) => match timeout(
                self.settings.budgets.boot,
                telemetry::timed(Phase::Boot, live.machine.resume(), Result::is_ok),
            )
            .await
            {
                Ok(Ok(())) => return self.run_turn(live, turn, files, claiming).await,
                Ok(Err(error)) => format!(
                    "resuming the paused box of session {} failed: {error}",
                    *turn.session_id
                ),
                Err(_) => format!(
                    "the paused box of session {} did not resume within {seconds} s; send the turn again",
                    *turn.session_id
                ),
            },
        };
        let (tap, tail, stopped) = stop_box(&self.standing, live, BoxEnd::ResumeFailed).await;
        turn_failed(
            &self.out,
            &turn.turn_id,
            TurnFailureReason::BoxBootFailed,
            format!("{failed}. Console: {tail}"),
        );
        Outcome::Gone(tap, stopped)
    }

    async fn cold_turn(&self, tap: Tap, turn: &BoxTurn) -> Outcome<V::Machine> {
        let claiming = self.claim(turn);
        let Some(files) = self.files(turn).await else {
            return Outcome::Gone(tap, Ok(()));
        };
        let restoring = (!turn.snapshots.is_empty())
            .then(|| expired(&turn.credential, "before its snapshot was restored"))
            .flatten();
        if let Some(detail) = restoring {
            turn_failed(
                &self.out,
                &turn.turn_id,
                TurnFailureReason::DispatchCredentialExpired,
                detail,
            );
            return Outcome::Gone(tap, Ok(()));
        }
        let restored = match timeout_at(instant_at(turn.credential.expires_at), self.restore(turn, &tap)).await {
            Ok(Ok(restored)) => restored,
            Ok(Err((reason, detail))) => {
                turn_failed(&self.out, &turn.turn_id, reason, detail);
                return Outcome::Gone(tap, Ok(()));
            }
            Err(_) => {
                turn_failed(
                    &self.out,
                    &turn.turn_id,
                    TurnFailureReason::DispatchCredentialExpired,
                    credential_expired(&turn.credential, "while its snapshot was restored"),
                );
                return Outcome::Gone(tap, Ok(()));
            }
        };
        let (image, hold) = match self.image(turn).await {
            Ok(booted) => booted,
            Err(refusal) => {
                let detail = match restored.as_ref().map(|(path, _, _, _)| remove_file(path)) {
                    Some(Err(error)) => format!("{}; then {error}", refusal.detail),
                    _ => refusal.detail,
                };
                turn_failed(&self.out, &turn.turn_id, refusal.reason, detail);
                return Outcome::Gone(tap, Ok(()));
            }
        };
        let mut machine = match self.vmm.machine(&turn.session_id, &tap) {
            Ok(machine) => machine,
            Err(error) => {
                turn_failed(
                    &self.out,
                    &turn.turn_id,
                    TurnFailureReason::BoxBootFailed,
                    format!("preparing box {}: {error}", *turn.box_id),
                );
                return Outcome::Gone(tap, Ok(()));
            }
        };
        let (restore, base, archived, charge) = restored
            .map_or((None, Base::default(), 0, None), |(path, base, archived, charge)| {
                (Some(path), base, archived, Some(charge))
            });
        let entries = base.manifest.len() as u64;
        let budget = restore_budget(self.settings.budgets.boot, archived, entries);
        let booting = telemetry::timed(Phase::Boot, machine.start(&image, restore.as_deref()), Result::is_ok);
        let started = timeout(budget, booting).await;
        let cleaned = restore.as_deref().map_or(Ok(()), remove_file);
        drop(charge);
        let failed = match (started, cleaned) {
            (Ok(Ok((guest, hello))), Ok(())) => match mismatch(&hello, &image) {
                None => {
                    let live = LiveBox::booted(turn, machine, guest, tap, base, image, hold);
                    return self.first_turn(live, turn, files, claiming).await;
                }
                Some(mismatch) => mismatch,
            },
            (Ok(Ok(_)), Err(error)) => format!("box {} booted, then {error}", *turn.box_id),
            (Ok(Err(error)), _) => format!("booting box {} failed: {error}", *turn.box_id),
            (Err(_), _) => format!(
                "box {} did not say GuestHello within {} s of booting with a restore archive of {archived} bytes in {entries} entries; send the turn again",
                *turn.box_id,
                budget.as_secs()
            ),
        };
        let tail = machine.console_tail();
        let stopped = end_box(machine, tap.index(), &turn.session_id, &turn.box_id, BoxEnd::BootFailed).await;
        turn_failed(
            &self.out,
            &turn.turn_id,
            TurnFailureReason::BoxBootFailed,
            format!("{failed}. Console: {tail}"),
        );
        Outcome::Gone(tap, stopped)
    }

    async fn first_turn(
        &self,
        live: LiveBox<V::Machine>,
        turn: &BoxTurn,
        files: Vec<GuestPersonaFile>,
        claiming: Claiming,
    ) -> Outcome<V::Machine> {
        let Err(error) = self.standing.put(&live, Some(&turn.turn_id)) else {
            return self.run_turn(live, turn, files, claiming).await;
        };
        let (tap, tail, stopped) = stop_box(&self.standing, live, BoxEnd::NotRecorded).await;
        turn_failed(
            &self.out,
            &turn.turn_id,
            TurnFailureReason::BoxBootFailed,
            format!(
                "box {} booted, then recording it for a supervisor restart failed: {error}. Console: {tail}",
                *turn.box_id
            ),
        );
        Outcome::Gone(tap, stopped)
    }

    fn send(&self, key: Key, message: impl Into<ChannelHostMessage>, deadline: Option<Instant>) -> Asking {
        self.send_outgoing(key, Outgoing::Message(message.into()), deadline)
    }

    fn send_outgoing(&self, key: Key, outgoing: Outgoing, deadline: Option<Instant>) -> Asking {
        let Waiting { key, answer } = self.answers.expect(key);
        send_out(&self.out, outgoing);
        Asking {
            answers: self.answers.clone(),
            key,
            waiting: Some(answer),
            deadline,
        }
    }

    async fn ask(&self, key: Key, message: impl Into<ChannelHostMessage>, deadline: Instant) -> Result<Answer, String> {
        self.ask_outgoing(key, Outgoing::Message(message.into()), deadline)
            .await
    }

    async fn ask_outgoing(&self, key: Key, outgoing: Outgoing, deadline: Instant) -> Result<Answer, String> {
        let mut asking = self.send_outgoing(key, outgoing, Some(deadline));
        asking.answer().await.ok_or_else(|| {
            format!(
                "the control plane did not answer {} before its URLs would expire; check the channel to the control plane",
                asking.key
            )
        })
    }

    async fn held(&self, snapshot_id: &EntityId, digests: &[String], deadline: Instant) -> Result<Places, String> {
        let mut held = Places::new();
        for (index, part) in digests.chunks(DIGESTS_PER_PART).enumerate() {
            let key = Key {
                ask: Ask::SnapshotChunks,
                id: snapshot_id.clone(),
                part: part_number(index)?,
            };
            let message = ChannelSnapshotChunks {
                digests: part
                    .iter()
                    .map(|digest| sha256_digest(digest))
                    .collect::<Result<_, _>>()?,
                part: key.part,
                snapshot_id: snapshot_id.clone(),
                type_: ChannelSnapshotChunksType::SnapshotChunks,
            };
            let Answer::ChunksHeld(answer) = self.ask(key.clone(), message, deadline).await? else {
                return Err(unexpected(&key));
            };
            for placement in &answer.held {
                let place = Place::from_wire(placement)?;
                held.insert(place.digest.clone(), place);
            }
        }
        Ok(held)
    }

    async fn pack_uploads(
        &self,
        snapshot_id: &EntityId,
        packs: &[PackFile],
        reused: &[String],
        protocol: u16,
        deadline: Instant,
    ) -> Result<BTreeMap<String, String>, String> {
        let sizes: BTreeMap<&str, u64> = packs.iter().map(|pack| (pack.digest.as_str(), pack.size)).collect();
        let placed: Vec<&Place> = packs.iter().flat_map(|pack| pack.chunks.iter()).collect();
        let parts = if protocol < PACK_REUSE {
            placed.chunks(DIGESTS_PER_PART).map(|part| (part, &[][..])).collect()
        } else {
            pack_parts(&placed, reused, DIGESTS_PER_PART)
        };
        let mut urls = BTreeMap::new();
        for (index, (part, reused)) in parts.into_iter().enumerate() {
            let key = Key {
                ask: Ask::SnapshotPacks,
                id: snapshot_id.clone(),
                part: part_number(index)?,
            };
            let listed: BTreeSet<&str> = part.iter().map(|place| place.pack.as_str()).collect();
            let message = ChannelSnapshotPacks {
                chunks: part.iter().map(|place| place.to_wire()).collect::<Result<_, _>>()?,
                packs: listed
                    .into_iter()
                    .map(|digest| {
                        let size = sizes
                            .get(digest)
                            .copied()
                            .and_then(NonZeroU64::new)
                            .ok_or_else(|| format!("pack {digest} is empty or was never written"))?;
                        Ok(ChannelPack {
                            pack_digest: sha256_digest(digest)?,
                            size_bytes: size,
                        })
                    })
                    .collect::<Result<_, String>>()?,
                part: key.part,
                reused_pack_digests: reused
                    .iter()
                    .map(|digest| sha256_digest(digest))
                    .collect::<Result<_, _>>()?,
                snapshot_id: snapshot_id.clone(),
                type_: ChannelSnapshotPacksType::SnapshotPacks,
            };
            let outgoing = Outgoing::Snapshot(message.into(), protocol);
            let Answer::PackUploads(answer) = self.ask_outgoing(key.clone(), outgoing, deadline).await? else {
                return Err(unexpected(&key));
            };
            urls.extend(
                answer
                    .uploads
                    .into_iter()
                    .map(|upload| (upload.pack_digest.as_str().to_string(), upload.url.into())),
            );
        }
        Ok(urls)
    }

    async fn fetch(
        &self,
        turn_id: &EntityId,
        wanted: Vec<Place>,
        cache: &Arc<ChunkCache>,
        dir: &Path,
        deadline: Instant,
    ) -> Result<(), String> {
        let parts = restore_parts(wanted, DIGESTS_PER_PART);
        let (jobs, queue) = unbounded_channel::<(Arc<PartUrls>, Fetch)>();
        let asking = async move {
            for (index, part) in parts.into_iter().enumerate() {
                let key = Key {
                    ask: Ask::RestoreChunks,
                    id: turn_id.clone(),
                    part: part_number(index)?,
                };
                let message = ChannelRestoreChunks {
                    chunks: part.iter().map(Place::to_wire).collect::<Result<_, _>>()?,
                    part: key.part,
                    turn_id: turn_id.clone(),
                    type_: ChannelRestoreChunksType::RestoreChunks,
                };
                let answer = self.restore_urls(&key, message.clone(), deadline).await?;
                let sizes = answer
                    .packs
                    .iter()
                    .map(|pack| (pack.pack_digest.as_str().to_string(), pack.size_bytes.get()))
                    .collect();
                let urls = Arc::new(PartUrls {
                    key,
                    message,
                    urls: Mutex::new((0, pack_urls(answer))),
                });
                for fetch in fetches(&part, &sizes)? {
                    jobs.send((Arc::clone(&urls), fetch))
                        .map_err(|_| "the pack downloads stopped".to_string())?;
                }
            }
            Ok::<(), String>(())
        };
        let runner = self.clone();
        let downloading = futures_util::stream::unfold(queue, |mut queue| async move {
            queue.recv().await.map(|job| (Ok::<_, String>(job), queue))
        })
        .try_for_each_concurrent(TRANSFERS, move |(urls, fetch)| {
            let runner = runner.clone();
            let cache = Arc::clone(cache);
            let dir = dir.to_path_buf();
            async move { runner.download_pack(urls, fetch, cache, dir, deadline).await }
        });
        futures_util::future::try_join(asking, downloading).await.map(|_| ())
    }

    async fn restore_urls(
        &self,
        key: &Key,
        message: ChannelRestoreChunks,
        deadline: Instant,
    ) -> Result<ChannelRestoreDownloads, String> {
        let Answer::Downloads(answer) = self.ask(key.clone(), message, deadline).await? else {
            return Err(unexpected(key));
        };
        Ok(answer)
    }

    async fn refresh(
        &self,
        urls: &PartUrls,
        seen: u32,
        pack: &str,
        deadline: Instant,
    ) -> Result<(u32, String), String> {
        let mut held = urls.urls.lock().await;
        if held.0 == seen {
            let answer = self.restore_urls(&urls.key, urls.message.clone(), deadline).await?;
            *held = (seen + 1, pack_urls(answer));
        }
        url_of(&held, pack)
    }

    async fn download_pack(
        &self,
        urls: Arc<PartUrls>,
        fetch: Fetch,
        cache: Arc<ChunkCache>,
        dir: PathBuf,
        deadline: Instant,
    ) -> Result<(), String> {
        let named = fetch
            .chunks
            .first()
            .map_or(fetch.pack.as_str(), |place| place.digest.as_str());
        let staged = dir.join(format!("pack-{named}.part"));
        let (mut generation, mut url) = url_of(&*urls.urls.lock().await, &fetch.pack)?;
        let mut attempt = 1;
        loop {
            let fetched = match fetch.span {
                Span::Whole => self.store.download(&url, &staged).await,
                Span::Range { first, last } => self.store.download_range(&url, &staged, first, last).await,
            };
            let stale = match fetched {
                Ok(()) => break,
                Err(DownloadError::Refused(error)) => {
                    return Err(format!("pack {} is unreadable: {error}", fetch.pack))
                }
                Err(error) if attempt >= DOWNLOAD_ATTEMPTS => {
                    return Err(format!("pack {} is unreadable: {error}", fetch.pack))
                }
                Err(DownloadError::Expired(_)) => true,
                Err(DownloadError::Transient(_)) => attempt % ATTEMPTS_PER_URL == 0,
            };
            wait(attempt).await;
            if stale {
                (generation, url) = self.refresh(&urls, generation, &fetch.pack, deadline).await?;
            }
            attempt += 1;
        }
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            let bytes = std::fs::read(&staged).map_err(|error| format!("reading {}: {error}", staged.display()))?;
            remove_file(&staged)?;
            unpack(&fetch, &bytes, &cache)
        })
        .await
        .map_err(|error| format!("unpacking a pack stopped: {error}"))?
    }

    async fn download_root(&self, url: &str, file: &Path) -> Result<(), DownloadError> {
        let mut attempt = 1;
        loop {
            match self.store.download(url, file).await {
                Err(DownloadError::Transient(_)) if attempt < DOWNLOAD_ATTEMPTS => wait(attempt).await,
                done => return done,
            }
            attempt += 1;
        }
    }

    async fn fetch_within(
        &self,
        turn: &BoxTurn,
        wanted: Vec<Place>,
        cache: &Arc<ChunkCache>,
        dir: &Path,
        named: &str,
        expires: DateTime<FixedOffset>,
    ) -> Result<(), String> {
        if wanted.is_empty() {
            return Ok(());
        }
        let deadline = instant_at(expires);
        match timeout_at(deadline, self.fetch(&turn.turn_id, wanted, cache, dir, deadline)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(format!(
                "restoring {named}: {error}; offer the turn again for fresh download URLs, or start a new session when the snapshot is damaged"
            )),
            Err(_) => Err(format!(
                "the packs of {named} did not download before their URLs expired at {}; check the host's bandwidth to object storage and offer the turn again",
                expires.to_rfc3339()
            )),
        }
    }

    async fn restore(&self, turn: &BoxTurn, tap: &Tap) -> Result<Option<Restored>, (TurnFailureReason, String)> {
        telemetry::timed(Phase::Restore, self.restore_snapshot(turn, tap), |restored| {
            matches!(restored, Ok(Some(_)))
        })
        .await
        .map_err(|(reason, detail)| match self.host_disk_full() {
            Some(full) if reason == TurnFailureReason::SnapshotUnreadable => {
                (TurnFailureReason::BoxDiskFull, format!("{full}; {detail}"))
            }
            _ => (reason, detail),
        })
    }

    fn host_disk_full(&self) -> Option<String> {
        match self.vmm.disk(&[]) {
            Ok(disk) => disk.full(),
            Err(error) => Some(format!(
                "reading the host's free disk failed: {error}, so the box stopped rather than risk filling it"
            )),
        }
    }

    async fn restore_root(&self, snapshot: &ChannelSnapshotDownload, file: &Path, named: &str) -> Result<(), String> {
        let expires = snapshot.download_expires_at;
        match timeout_at(instant_at(expires), self.download_root(&snapshot.download_url, file)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                Err(format!(
                    "{named} is unreadable: {error}; offer the turn again for fresh download URLs"
                ))
            }
            Err(_) => {
                Err(format!(
                    "the root of {named} did not download before its URL expired at {}; check the host's bandwidth to object storage and offer the turn again",
                    expires.to_rfc3339()
                ))
            }
        }
    }

    fn charge_restore(&self, named: &str, bytes: u64, tap: &Tap) -> Result<Charge, (TurnFailureReason, String)> {
        let charge_mib = restore_charge_mib(bytes, tap.disk_mib());
        self.vmm
            .disk(&[])
            .map_err(|error| format!("reading the host's free disk failed: {error}"))
            .and_then(|disk| disk.stages(self.reserves.mib(), self.staging.mib(), charge_mib))
            .map_err(|detail| {
                (
                    TurnFailureReason::BoxDiskFull,
                    format!("restoring {named} of {bytes} bytes: {detail}"),
                )
            })?;
        Ok(self.staging.charge(charge_mib))
    }

    fn disk_failure(&self) -> Option<Failure> {
        self.host_disk_full().map(|full| {
            Failure::turn(
                TurnFailureReason::BoxDiskFull,
                format!("The turn stopped before the host disk filled under the box: {full}."),
            )
        })
    }

    async fn restore_snapshot(
        &self,
        turn: &BoxTurn,
        tap: &Tap,
    ) -> Result<Option<Restored>, (TurnFailureReason, String)> {
        let unreadable = |detail: String| (TurnFailureReason::SnapshotUnreadable, detail);
        let Some(snapshot) = turn.snapshots.last() else {
            return Ok(None);
        };
        let named = format!(
            "snapshot {} (seq {}) of session {}",
            *snapshot.snapshot_id, snapshot.snapshot_seq.0, *turn.session_id
        );
        let dir = self.vmm.staging(&turn.session_id).map_err(unreadable)?;
        std::fs::create_dir_all(&dir).map_err(|error| unreadable(format!("creating {}: {error}", dir.display())))?;
        let expires = snapshot.download_expires_at;
        let file = dir.join("restore-root.json");
        self.restore_root(snapshot, &file, &named).await.map_err(unreadable)?;
        let damaged = |error: String| {
            unreadable(format!(
                "{named} is unreadable: {error}; the stored snapshot is damaged, so start a new session"
            ))
        };
        let cache = Arc::new(ChunkCache::new(dir.join(CHUNKS)));
        let reading = Arc::clone(&cache);
        let root_file = file.clone();
        let read = tokio::task::spawn_blocking(move || -> Result<(Vec<Place>, Vec<Place>), String> {
            let bytes =
                std::fs::read(&root_file).map_err(|error| format!("reading {}: {error}", root_file.display()))?;
            let root = decode_root(&bytes)?;
            reading.create()?;
            let wanted = lacking(&root, &reading)?;
            Ok((root, wanted))
        })
        .await
        .map_err(|error| unreadable(format!("reading the root of {named} stopped: {error}")))?;
        remove_file(&file).map_err(unreadable)?;
        let (root, wanted) = read.map_err(damaged)?;
        self.fetch_within(turn, wanted, &cache, &dir, &named, expires)
            .await
            .map_err(unreadable)?;
        let reading = Arc::clone(&cache);
        let manifest_places = root.clone();
        let read = tokio::task::spawn_blocking(move || -> Result<(Manifest, Vec<Place>), String> {
            let manifest = decode(&assemble(&manifest_places, &reading)?)?;
            let wanted = lacking(manifest.values().flat_map(|stored| stored.chunks.iter()), &reading)?;
            Ok((manifest, wanted))
        })
        .await
        .map_err(|error| unreadable(format!("reading the manifest of {named} stopped: {error}")))?;
        let (manifest, wanted) = read.map_err(damaged)?;
        let overlay = u64::from(tap.disk_mib().get()) * 1024 * 1024;
        let (bytes, entries) = restore_size(&named, &manifest, overlay).map_err(unreadable)?;
        let charge = self.charge_restore(&named, bytes, tap)?;
        self.fetch_within(turn, wanted, &cache, &dir, &named, expires)
            .await
            .map_err(unreadable)?;
        let target = dir.join("restore.tar");
        let output = target.clone();
        let chunks = dir.join(CHUNKS);
        let cancel = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let cancelled = Arc::clone(&cancel.0);
        let rebuild =
            tokio::task::spawn_blocking(move || rebuild_archive(manifest, &cache, &output, &chunks, &cancelled));
        let budget = restore_budget(self.settings.budgets.boot, bytes, entries);
        let seconds = budget.as_secs();
        let outcome = timeout(budget, rebuild).await;
        if outcome.is_err() {
            cancel.0.store(true, Ordering::Relaxed);
            remove_file(&target).map_err(unreadable)?;
        }
        match outcome {
            Ok(Ok(Ok((manifest, archived)))) => Ok(Some((
                target,
                Base {
                    manifest,
                    root,
                    births: Births::new(),
                },
                archived,
                charge,
            ))),
            Ok(Ok(Err(error))) => Err(unreadable(format!(
                "reassembling the files of {named} failed: {error}; the stored snapshot is damaged, so start a new session"
            ))),
            Ok(Err(error)) => Err(unreadable(format!("reassembling the files of {named} stopped: {error}"))),
            Err(_) => Err(unreadable(format!(
                "reassembling the files of {named} took longer than {seconds} s for {bytes} bytes in {entries} entries; send the turn again"
            ))),
        }
    }

    async fn start(&self, live: &mut LiveBox<V::Machine>, turn: &BoxTurn, start: GuestTurnStart) -> Followed {
        let started = Instant::now();
        if let Err(error) = live.guest.send(&GuestInput::TurnStart(start)).await {
            return Followed::Ended(Err(Failure::turn(
                TurnFailureReason::BoxBootFailed,
                format!("handing turn {} to the guest failed: {error}", *turn.turn_id),
            )));
        }
        live.machine.clear_console();
        let clone = Duration::from_secs(u64::from(live.image.harness().clone_seconds.get()));
        let state = Follow {
            end: started + Duration::from_secs(u64::from(turn.turn_seconds.get())),
            first_line: started + clone + self.settings.budgets.first_line,
            repository_ready: false,
            seen_line: false,
            error: None,
        };
        let phases = (
            Some(telemetry::phase(Phase::Clone)),
            Some(telemetry::phase(Phase::FirstOutput)),
        );
        self.follow(live, &turn.turn_id, state, phases).await
    }

    async fn resume_turn(&self, mut live: LiveBox<V::Machine>, parked: Box<Parked>) -> Outcome<V::Machine> {
        let parked = *parked;
        let followed = self
            .follow(&mut live, &parked.turn_id, parked.left.resume(), (None, None))
            .await;
        self.end_turn(live, &parked.turn_id, followed).await
    }

    async fn run_turn(
        &self,
        mut live: LiveBox<V::Machine>,
        turn: &BoxTurn,
        files: Vec<GuestPersonaFile>,
        mut claiming: Claiming,
    ) -> Outcome<V::Machine> {
        let start = turn
            .prebuild_commit_sha
            .as_deref()
            .map(GuestTurnStartPrebuildCommitSha::try_from)
            .transpose()
            .map(|prebuild_commit_sha| GuestTurnStart {
                credential: turn.credential.clone(),
                files,
                git_author_email: turn.git_author_email.clone(),
                git_author_name: turn.git_author_name.clone(),
                last_line_seq: turn.last_line_seq.clone(),
                model: turn.model.clone(),
                prompt: turn.prompt.clone(),
                repository: turn.repository.clone(),
                turn_id: turn.turn_id.clone(),
                type_: GuestTurnStartType::TurnStart,
                prebuild_commit_sha,
                turn_seconds: Some(turn.turn_seconds),
            })
            .map_err(|error| {
                Failure::turn(
                    TurnFailureReason::BoxBootFailed,
                    format!(
                        "turn {} names a prebuild commit the guest does not take: {error}",
                        *turn.turn_id
                    ),
                )
            });
        let mut parking = self.parking.clone();
        let answered = tokio::select! {
            biased;
            answer = claiming.answer() => Some(answer),
            Ok(_) = parking.wait_for(|parking| *parking) => None,
        };
        let Some(answer) = answered else {
            turn_failed(
                &self.out,
                &turn.turn_id,
                TurnFailureReason::HostUpdating,
                format!(
                    "this host started handing its boxes to a new supervisor binary before the control plane answered the claim of turn {}, so its harness never started and its box stays warm; the control plane offers the turn again",
                    *turn.turn_id
                ),
            );
            return self.pause(live).await;
        };
        let followed = match answer {
            Some(Answer::Claimed(_)) => match start {
                Ok(start) => self.start(&mut live, turn, start).await,
                Err(failure) => Followed::Ended(Err(failure)),
            },
            Some(Answer::Withdrawn(_)) => {
                telemetry::withdrawn();
                eprintln!(
                    "tumpel-supervisor: the control plane withdrew turn {} before its harness started; the box stays warm and ran nothing",
                    *turn.turn_id
                );
                return self.pause(live).await;
            }
            Some(_) => Followed::Ended(Err(Failure::turn(
                TurnFailureReason::NotClaimed,
                unexpected(&claiming.asking.key),
            ))),
            None => Followed::Ended(Err(Failure::turn(
                TurnFailureReason::NotClaimed,
                format!(
                    "this supervisor stopped waiting for the answer to the claim of turn {}, so its harness never started; the control plane offers the turn again",
                    *turn.turn_id
                ),
            ))),
        };
        self.end_turn(live, &turn.turn_id, followed).await
    }

    async fn end_turn(
        &self,
        mut live: LiveBox<V::Machine>,
        turn_id: &EntityId,
        followed: Followed,
    ) -> Outcome<V::Machine> {
        let result = match followed {
            Followed::Ended(result) => result,
            Followed::Parked(left) => {
                return Outcome::Parked(
                    live,
                    Box::new(Parked {
                        turn_id: turn_id.clone(),
                        left,
                    }),
                )
            }
        };
        self.cancels.forget(turn_id);
        live.dirty = true;
        let result = result.map_err(|failure| match failure.report {
            Report::Turn(_) => Failure {
                detail: format!(
                    "{} {}",
                    failure.detail,
                    console_during_turn(&live.machine.console_tail())
                ),
                ..failure
            },
            Report::Clone(_) => failure,
        });
        match (result, live.credential_file.take()) {
            (Ok((last_line_seq, warning)), file) => emit(
                &self.out,
                ChannelTurnComplete {
                    credential_file: file.and_then(|file| file.try_into().ok()),
                    completed_at: now(),
                    last_line_seq,
                    warning,
                    turn_id: turn_id.clone(),
                    type_: ChannelTurnCompleteType::TurnComplete,
                },
            ),
            (
                Err(Failure {
                    report: Report::Turn(reason),
                    detail,
                }),
                Some(file),
            ) => emit(
                &self.out,
                ChannelTurnFailed {
                    credential_file: file.try_into().ok(),
                    detail: reported("the turn failure detail", &detail),
                    failed_at: now(),
                    reason,
                    turn_id: turn_id.clone(),
                    type_: ChannelTurnFailedType::TurnFailed,
                },
            ),
            (Err(failure), _) => report_failure(&self.out, turn_id, failure.report, failure.detail),
        }
        self.recover(live).await
    }

    async fn recover(&self, mut live: LiveBox<V::Machine>) -> Outcome<V::Machine> {
        let failed = match timeout(self.settings.budgets.boot, live.machine.reconnect(&mut live.guest)).await {
            Ok(Ok(())) => return self.pause(live).await,
            Ok(Err(error)) => error,
            Err(_) => format!(
                "the guest did not answer a new connection within {} s",
                self.settings.budgets.boot.as_secs()
            ),
        };
        let session = live.session_id.to_string();
        let (tap, tail, stopped) = stop_box(&self.standing, live, BoxEnd::RecoverFailed).await;
        eprintln!(
            "tumpel-supervisor: the box of session {session} did not take a new guest connection and was stopped: {failed}. Console: {tail}"
        );
        Outcome::Gone(tap, stopped)
    }

    async fn recover_adopted(&self, mut live: LiveBox<V::Machine>) -> Outcome<V::Machine> {
        let restore = async {
            live.machine.reconnect(&mut live.guest).await?;
            live.machine.pause().await?;
            self.standing.put(&live, None)
        };
        let error = match timeout(self.settings.budgets.boot, restore).await {
            Ok(Ok(())) => return Outcome::Kept(live),
            Ok(Err(error)) => error,
            Err(_) => "the guest did not reconnect and pause in time".to_string(),
        };
        eprintln!(
            "tumpel-supervisor: keeping box {} of session {} after recovery failed: {error}",
            live.box_id, live.session_id
        );
        Outcome::Protected(live.tap)
    }

    async fn pause(&self, mut live: LiveBox<V::Machine>) -> Outcome<V::Machine> {
        let failed = match timeout(self.settings.budgets.boot, live.machine.pause()).await {
            Ok(Ok(())) => match self.standing.put(&live, None) {
                Ok(()) => return Outcome::Kept(live),
                Err(error) => format!("recording it for a supervisor restart failed: {error}"),
            },
            Ok(Err(error)) => error,
            Err(_) => format!(
                "pausing did not finish within {} s",
                self.settings.budgets.boot.as_secs()
            ),
        };
        let (tap, tail, stopped) = stop_box(&self.standing, live, BoxEnd::PauseFailed).await;
        eprintln!(
            "tumpel-supervisor: a box could not be paused after its turn and was stopped: {failed}. Console: {tail}"
        );
        Outcome::Gone(tap, stopped)
    }

    fn no_first_output(&self, turn_id: &EntityId, clone: Duration, repository_ready: bool) -> Failure {
        let first_line = self.settings.budgets.first_line;
        let detail = if repository_ready {
            format!(
                "turn {} sent no line within {} s of its repository being ready (budgets.firstLineSeconds in runtime.json).",
                **turn_id,
                first_line.as_secs()
            )
        } else {
            format!(
                "turn {} had no repository ready and no line within {} s, the image's cloneSeconds {} plus budgets.firstLineSeconds {} in runtime.json.",
                **turn_id,
                (clone + first_line).as_secs(),
                clone.as_secs(),
                first_line.as_secs()
            )
        };
        Failure::turn(TurnFailureReason::NoFirstOutput, detail)
    }

    fn timed_out(turn_id: &EntityId) -> Failure {
        Failure::turn(
            TurnFailureReason::TurnTimedOut,
            format!(
                "turn {} ran past the turnSeconds of its offer; send a smaller prompt or raise turnTotalBudgetSeconds.",
                **turn_id
            ),
        )
    }

    fn silent(&self, turn_id: &EntityId, cancelling: bool, first: Option<(Duration, bool)>) -> Failure {
        if cancelling {
            return cancel_unanswered(turn_id);
        }
        match first {
            Some((clone, repository_ready)) => self.no_first_output(turn_id, clone, repository_ready),
            None => Self::timed_out(turn_id),
        }
    }

    async fn follow(
        &self,
        live: &mut LiveBox<V::Machine>,
        turn_id: &EntityId,
        mut state: Follow,
        phases: (Option<tracing::Span>, Option<tracing::Span>),
    ) -> Followed {
        let clone = Duration::from_secs(u64::from(live.image.harness().clone_seconds.get()));
        let protocol = live.image.harness().protocol.clone();
        let ((mut cloning, mut first_output), mut parking) = (phases, self.parking.clone());
        let (mut cancelling, mut disk_check) = (false, disk_checks());
        loop {
            let waiting_for_first = !state.seen_line && state.first_line < state.end;
            let deadline = if waiting_for_first { state.first_line } else { state.end };
            let received = tokio::select! {
                biased;
                () = self.cancels.cancelled(turn_id), if !cancelling => None,
                Ok(_) = parking.wait_for(|parking| *parking) => return Followed::Parked(state.park()),
                _ = disk_check.tick() => match self.disk_failure() {
                    Some(failure) => return Followed::Ended(Err(failure)),
                    None => continue,
                },
                received = timeout_at(deadline, live.guest.receive()) => Some(received),
            };
            let Some(received) = received else {
                cancelling = true;
                if let Err(failure) = cancel(live, turn_id, &mut state).await {
                    return Followed::Ended(Err(failure));
                }
                continue;
            };
            let Ok(received) = received else {
                let first = waiting_for_first.then_some((clone, state.repository_ready));
                return Followed::Ended(Err(self.silent(turn_id, cancelling, first)));
            };
            match received {
                Ok(GuestMessage::Unrecognised(_)) => tumpel_channel::update_required(),
                Err(error) => {
                    return Followed::Ended(Err(Failure::turn(
                        TurnFailureReason::HarnessExited,
                        format!("the guest connection dropped mid-turn: {error}."),
                    )))
                }
                Ok(GuestMessage::TurnRepositoryReady(ready))
                    if ready.turn_id == *turn_id && !state.repository_ready =>
                {
                    state.repository_ready = true;
                    if let Some(phase) = cloning.take() {
                        telemetry::reached(&phase);
                    }
                    state.first_line = Instant::now() + self.settings.budgets.first_line;
                    emit(
                        &self.out,
                        ChannelTurnRepositoryReady {
                            turn_id: ready.turn_id,
                            type_: ChannelTurnRepositoryReadyType::TurnRepositoryReady,
                        },
                    );
                }
                Ok(GuestMessage::TurnLine(output)) if output.turn_id == *turn_id => {
                    state.seen_line = true;
                    if let Some(phase) = first_output.take() {
                        telemetry::reached(&phase);
                    }
                    state.error = state.error.or_else(|| error_result(&protocol, &output.text));
                    let Some(first_line_seq) = u64::try_from(output.line_seq.0).ok().and_then(NonZeroU64::new) else {
                        return Followed::Ended(Err(Failure::turn(
                            TurnFailureReason::HarnessExited,
                            format!("the guest sent line {}; line numbers start at 1", output.line_seq.0),
                        )));
                    };
                    emit(
                        &self.out,
                        ChannelTurnLines {
                            first_line_seq,
                            lines: vec![output.text],
                            turn_id: output.turn_id,
                            type_: ChannelTurnLinesType::TurnLines,
                        },
                    );
                }
                Ok(GuestMessage::TurnEnd(end)) if end.turn_id == *turn_id => {
                    live.credential_file = end.credential_file.as_deref().cloned();
                    telemetry::guest_metrics(&end.metrics);
                    let error = state.error.or_else(|| exit_result(&protocol, &end));
                    return Followed::Ended(
                        turn_end(end, error).map_err(|(reason, detail)| Failure::turn(reason, detail)),
                    );
                }
                Ok(GuestMessage::TurnRepositoryCloneFailed(failed)) if failed.turn_id == *turn_id => {
                    return Followed::Ended(Err(Failure {
                        report: Report::Clone(failed.cause),
                        detail: failed.detail.into(),
                    }));
                }
                Ok(other) => {
                    return Followed::Ended(Err(Failure::turn(
                        TurnFailureReason::HarnessExited,
                        format!("the guest sent {other:?} during turn {}.", **turn_id),
                    )))
                }
            }
        }
    }

    async fn snapshot(
        &self,
        mut live: LiveBox<V::Machine>,
        order: &SnapshotOrder,
        protocol: u16,
    ) -> Outcome<V::Machine> {
        let drive = live.machine.drive();
        let stored = self.store_snapshot(drive, &live.base, order, protocol).await;
        self.cancels.forget(&order.snapshot_id);
        let outgoing = match stored {
            Ok(None) => return Outcome::Kept(live),
            Ok(Some((uploaded_bytes, chunk_count, base))) => {
                live.base = Arc::new(base);
                let done = ChannelSnapshotDone {
                    chunk_count,
                    snapshot_id: order.snapshot_id.clone(),
                    type_: ChannelSnapshotDoneType::SnapshotDone,
                    uploaded_bytes,
                };
                Outgoing::Snapshot(done.into(), protocol)
            }
            Err((reason, detail)) => {
                let failed = ChannelSnapshotFailed {
                    detail: reported("the snapshot failure detail", &detail),
                    failed_at: now(),
                    reason,
                    snapshot_id: order.snapshot_id.clone(),
                    type_: ChannelSnapshotFailedType::SnapshotFailed,
                };
                Outgoing::Message(failed.into())
            }
        };
        let (Outgoing::Snapshot(message, _) | Outgoing::Message(message)) = &outgoing;
        telemetry::ended(message);
        send_out(&self.out, outgoing);
        Outcome::Kept(live)
    }

    async fn store_snapshot(
        &self,
        drive: PathBuf,
        base: &Arc<Base>,
        order: &SnapshotOrder,
        protocol: u16,
    ) -> Result<Option<(i64, i32, Base)>, (ChannelSnapshotFailureReason, String)> {
        let unreadable = |detail: String| (ChannelSnapshotFailureReason::UpperDirectoryUnreadable, detail);
        let dir = self
            .vmm
            .staging(&order.session_id)
            .map_err(unreadable)?
            .join(format!("snapshot-{:08}", order.snapshot_seq.0));
        std::fs::create_dir_all(&dir).map_err(|error| unreadable(format!("creating {}: {error}", dir.display())))?;
        let staged = tokio::select! {
            biased;
            () = self.cancels.cancelled(&order.snapshot_id) => Ok(None),
            staged = self.stage_snapshot(drive, base, order, &dir, protocol) => staged.map(Some),
        };
        let stored = staged.map_err(|(reason, detail)| match self.host_disk_full() {
            Some(full) => (reason, format!("{full}; {detail}")),
            None => (reason, detail),
        });
        let removed = remove_dir(&dir).map_err(unreadable);
        let stored = stored?;
        removed?;
        Ok(stored)
    }

    async fn cut_files(
        &self,
        drive: PathBuf,
        base: Arc<Base>,
        cache: Arc<ChunkCache>,
        order: &SnapshotOrder,
    ) -> Result<Cut, (ChannelSnapshotFailureReason, String)> {
        let vmm = Arc::clone(&self.vmm);
        let shown = drive.display().to_string();
        let cutting = tokio::task::spawn_blocking(move || -> Result<Cut, String> {
            let upper = vmm.open_upper(&drive)?;
            cache.create()?;
            cut(&upper, &base, &cache)
        });
        match timeout_at(instant_at(order.upload_expires_at), cutting).await {
            Ok(Ok(Ok(cut))) => Ok(cut),
            Ok(Ok(Err(error))) => Err((
                ChannelSnapshotFailureReason::UpperDirectoryUnreadable,
                format!("reading the upper directory of the box's drive {shown} failed: {error}; the next turn's snapshot tries again"),
            )),
            Ok(Err(error)) => Err((
                ChannelSnapshotFailureReason::UpperDirectoryUnreadable,
                format!("cutting the box's files stopped: {error}"),
            )),
            Err(_) => Err((
                ChannelSnapshotFailureReason::SnapshotTimedOut,
                format!(
                    "reading the box's drive did not finish before the upload URLs of snapshot {} expired at {}; raise snapshotDeadlineSeconds on the control plane or check the host's disk",
                    *order.snapshot_id,
                    order.upload_expires_at.to_rfc3339()
                ),
            )),
        }
    }

    async fn stage_snapshot(
        &self,
        drive: PathBuf,
        base: &Arc<Base>,
        order: &SnapshotOrder,
        dir: &Path,
        protocol: u16,
    ) -> Result<(i64, i32, Base), (ChannelSnapshotFailureReason, String)> {
        let unreadable = |detail: String| (ChannelSnapshotFailureReason::UpperDirectoryUnreadable, detail);
        let expired = |detail: String| {
            (
                ChannelSnapshotFailureReason::UploadExpired,
                format!(
                    "the upload URLs of snapshot {} expired at {} before it was stored: {detail}; raise snapshotDeadlineSeconds on the control plane or check the host's bandwidth",
                    *order.snapshot_id,
                    order.upload_expires_at.to_rfc3339()
                ),
            )
        };
        let rejected = |detail: String| {
            (
                ChannelSnapshotFailureReason::UploadRejected,
                format!(
                    "object storage refused snapshot {}: {detail}; check the control plane's storage signing",
                    *order.snapshot_id
                ),
            )
        };
        let deadline = instant_at(order.upload_expires_at);
        if deadline <= Instant::now() {
            return Err(expired("the request reached the box too late".to_string()));
        }
        let known = base.places();
        let cache = Arc::new(ChunkCache::new(dir.join(CHUNKS)));
        let cut = self
            .cut_files(drive, Arc::clone(base), Arc::clone(&cache), order)
            .await?;
        let new = fresh(draft_digests(&cut.draft), &known);
        let held = self.held(&order.snapshot_id, &new, deadline).await.map_err(expired)?;
        let staging = dir.to_path_buf();
        let packing = tokio::task::spawn_blocking(move || stage(cut, known, held, &new, &cache, &staging));
        let staged = match timeout_at(deadline, packing).await {
            Ok(Ok(Ok(staged))) => staged,
            Ok(Ok(Err(error))) => return Err(unreadable(format!("packing the snapshot's chunks failed: {error}"))),
            Ok(Err(error)) => return Err(unreadable(format!("packing the snapshot's chunks stopped: {error}"))),
            Err(_) => return Err(expired("its chunks were still being packed".to_string())),
        };
        let urls = self
            .pack_uploads(&order.snapshot_id, &staged.packs, &staged.reused, protocol, deadline)
            .await
            .map_err(expired)?;
        let uploads: Vec<(&String, &PackFile)> = staged
            .packs
            .iter()
            .filter_map(|pack| urls.get(&pack.digest).map(|url| (url, pack)))
            .collect();
        let store = &*self.store;
        let sent = timeout_at(
            deadline,
            futures_util::stream::iter(uploads.iter().map(Ok)).try_for_each_concurrent(TRANSFERS, |(url, pack)| {
                store.upload_checked(url, &pack.path, &pack.digest)
            }),
        )
        .await;
        match sent {
            Ok(Ok(())) => {}
            Ok(Err(UploadError::Rejected(detail))) => return Err(rejected(detail)),
            Ok(Err(UploadError::Unreadable(detail))) => return Err(unreadable(detail)),
            Err(_) => return Err(expired(format!("{} packs were still uploading", uploads.len()))),
        }
        let root_bytes = std::fs::metadata(&staged.root)
            .map_err(|error| unreadable(format!("sizing {}: {error}", staged.root.display())))?
            .len();
        match timeout_at(deadline, self.store.upload(&order.upload_url, &staged.root)).await {
            Ok(Ok(())) => {}
            Ok(Err(UploadError::Rejected(detail))) => return Err(rejected(detail)),
            Ok(Err(UploadError::Unreadable(detail))) => return Err(unreadable(detail)),
            Err(_) => return Err(expired("the root was still uploading".to_string())),
        }
        let uploaded = uploads.iter().map(|(_, pack)| pack.size).sum::<u64>() + root_bytes;
        let uploaded = i64::try_from(uploaded).map_err(|_| {
            unreadable(format!(
                "snapshot {} uploaded {uploaded} bytes, more than an int64 counts",
                *order.snapshot_id
            ))
        })?;
        Ok((uploaded, staged.chunk_count, staged.base))
    }
}

struct PartUrls {
    key: Key,
    message: ChannelRestoreChunks,
    urls: Mutex<(u32, BTreeMap<String, String>)>,
}

fn pack_urls(answer: ChannelRestoreDownloads) -> BTreeMap<String, String> {
    answer
        .packs
        .into_iter()
        .map(|pack| (pack.pack_digest.as_str().to_string(), String::from(pack.url)))
        .collect()
}

fn url_of(held: &(u32, BTreeMap<String, String>), pack: &str) -> Result<(u32, String), String> {
    held.1
        .get(pack)
        .map(|url| (held.0, url.clone()))
        .ok_or_else(|| format!("pack {pack} has no URL"))
}

fn sha256_digest(digest: &str) -> Result<Sha256Digest, String> {
    digest
        .to_string()
        .try_into()
        .map_err(|error| format!("{digest} is no Sha256Digest: {error}"))
}

fn unexpected(key: &Key) -> String {
    format!("the control plane answered {key} with an answer of another kind; update the supervisor and the control plane together")
}

pub(crate) fn remove_dir(path: &Path) -> Result<(), String> {
    match std::fs::remove_dir_all(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            Err(format!("removing {}: {error}", path.display()))
        }
        _ => Ok(()),
    }
}

fn mismatch(hello: &GuestHello, boot: &Boot) -> Option<String> {
    let spoken = hello.protocol_version.get();
    let oldest = u32::from(GuestProtocol::OLDEST);
    let newest = u32::from(GuestProtocol::VERSION);
    let named = boot.named();
    if spoken < oldest {
        return Some(format!(
            "the guest of {named} speaks guest protocol {spoken}, this supervisor speaks {oldest} to {newest}; the image is older than the supervisor, push an images-vX.Y.Z tag to publish images built for it"
        ));
    }
    if spoken > newest {
        return Some(format!(
            "the guest of {named} speaks guest protocol {spoken}, this supervisor speaks {oldest} to {newest}; the image is newer than the supervisor, update the supervisor"
        ));
    }
    let expected = boot.harness();
    if *hello.harness == *expected.harness && *hello.harness_version == *expected.harness_version {
        return None;
    }
    Some(format!(
        "the guest runs {} {}, {named} promises {} {}; publish the release again so its images and layers are built again",
        *hello.harness, *hello.harness_version, *expected.harness, *expected.harness_version
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Taken {
    Handed,
    Crashed,
}

fn close_inherited_descriptor(fd: std::os::fd::RawFd) -> Result<(), String> {
    if unsafe { libc::close(fd) } == 0 {
        return Ok(());
    }
    Err(format!(
        "releasing an inherited box descriptor failed: {}",
        std::io::Error::last_os_error()
    ))
}

pub struct Abandoned {
    pub reason: TurnFailureReason,
    pub cause: String,
}

pub enum Input {
    Command(Command),
    HandOver(oneshot::Sender<SupervisorHandover>),
    Abandon(oneshot::Sender<()>, Abandoned),
    SnapshotAccepted(EntityId),
    SnapshotFailed(EntityId),
    Welcomed(u16),
}

#[derive(Clone)]
pub struct IdleGate {
    boxes: watch::Receiver<u32>,
    busy: watch::Receiver<u32>,
}

impl IdleGate {
    #[must_use]
    pub fn new(boxes: watch::Receiver<u32>, busy: watch::Receiver<u32>) -> Self {
        Self { boxes, busy }
    }
}

impl Idle for IdleGate {
    fn boxes(&self) -> u32 {
        *self.boxes.borrow()
    }

    fn busy(&self) -> u32 {
        *self.busy.borrow()
    }

    fn settled(&self) -> impl Future<Output = ()> + Send {
        let mut busy = self.busy.clone();
        async move {
            if busy.wait_for(|running| *running == 0).await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

#[expect(
    clippy::expect_used,
    reason = "every slot key is the to_string of the EntityId of the command, handover or event that made the slot"
)]
fn held_session(session: &str) -> EntityId {
    EntityId::try_from(session).expect("a slot key is an EntityId")
}

#[expect(
    clippy::expect_used,
    reason = "every setup key is the to_string of the claudeSetupId of its SetupOrder"
)]
fn held_setup(claude_setup_id: &str) -> EntityId {
    EntityId::try_from(claude_setup_id).expect("a setup key is an EntityId")
}

fn protected_detail(index: i32) -> String {
    format!("could not attach its saved box on TAP {index}; explicitly discard the box, or inspect tumpel-box{index}.scope before stopping it and restarting")
}

pub struct Lifecycle<V: Vmm, S: Store> {
    runner: Runner<V, S>,
    pool: TapPool,
    slots: HashMap<String, Slot<V::Machine>>,
    events: UnboundedSender<Event<V::Machine>>,
    finished: Option<UnboundedReceiver<Event<V::Machine>>>,
    warm: watch::Sender<u32>,
    boxes: watch::Sender<u32>,
    busy: watch::Sender<u32>,
    sessions: watch::Sender<Vec<EntityId>>,
    claude_setups: watch::Sender<Vec<EntityId>>,
    turns: watch::Sender<Vec<EntityId>>,
    holdings: watch::Sender<Held>,
    setups: HashMap<String, UnboundedSender<SetupInput>>,
    setup_tasks: HashMap<String, (u8, JoinHandle<()>)>,
    mode: Mode,
    parking: watch::Sender<bool>,
    handing: Option<(oneshot::Sender<SupervisorHandover>, AbortHandle)>,
    session_protocol: Option<u16>,
    adopted: Vec<String>,
    protected_sessions: HashMap<String, Tap>,
    unclaimed: BTreeMap<u8, Vec<EntityId>>,
}

impl<V: Vmm, S: Store> Drop for Lifecycle<V, S> {
    fn drop(&mut self) {
        for slot in self.slots.values() {
            if let Some((_, task)) = &slot.task {
                task.abort();
            }
            if let Some(idle) = &slot.idle {
                idle.abort();
            }
        }
        for (_, task) in self.setup_tasks.values() {
            task.abort();
        }
        if let Some((_, timer)) = &self.handing {
            timer.abort();
        }
    }
}

impl<V: Vmm, S: Store> Lifecycle<V, S> {
    pub fn new(
        vmm: V,
        store: S,
        settings: Settings,
        pool: TapPool,
        out: UnboundedSender<Outgoing>,
        standing: PathBuf,
    ) -> Self {
        let (events, finished) = unbounded_channel();
        let layers = LayerCache::new(&vmm.image_cache(), settings.layer_cache_mib);
        let (parking, parked) = watch::channel(false);
        let holdings = watch::Sender::new(Held {
            memory_budget_mib: pool.budget_mib(),
            ..Held::default()
        });
        Self {
            runner: Runner {
                layers,
                holds: Holds::default(),
                vmm: Arc::new(vmm),
                store: Arc::new(store),
                settings,
                out,
                answers: Answers::default(),
                fetching: Arc::default(),
                parking: parked,
                standing: Standing {
                    path: standing,
                    boxes: Arc::default(),
                    idle_ends: Arc::default(),
                },
                cancels: Cancels::default(),
                staging: Tally::default(),
                reserves: pool.reserves(),
            },
            pool,
            slots: HashMap::new(),
            events,
            finished: Some(finished),
            warm: watch::Sender::new(0),
            boxes: watch::Sender::new(0),
            busy: watch::Sender::new(0),
            sessions: watch::Sender::new(Vec::new()),
            claude_setups: watch::Sender::new(Vec::new()),
            turns: watch::Sender::new(Vec::new()),
            holdings,
            setups: HashMap::new(),
            setup_tasks: HashMap::new(),
            mode: Mode::TakingTurns,
            parking,
            handing: None,
            session_protocol: None,
            adopted: Vec::new(),
            protected_sessions: HashMap::new(),
            unclaimed: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn warm(&self) -> watch::Receiver<u32> {
        self.warm.subscribe()
    }

    #[must_use]
    pub fn boxes(&self) -> watch::Receiver<u32> {
        self.boxes.subscribe()
    }

    #[must_use]
    pub fn busy(&self) -> watch::Receiver<u32> {
        self.busy.subscribe()
    }

    #[must_use]
    pub fn sessions(&self) -> watch::Receiver<Vec<EntityId>> {
        self.sessions.subscribe()
    }

    #[must_use]
    pub fn claude_setups(&self) -> watch::Receiver<Vec<EntityId>> {
        self.claude_setups.subscribe()
    }

    #[must_use]
    pub fn turns(&self) -> watch::Receiver<Vec<EntityId>> {
        self.turns.subscribe()
    }

    #[must_use]
    pub fn holdings(&self) -> watch::Receiver<Held> {
        self.holdings.subscribe()
    }

    #[must_use]
    pub fn answers(&self) -> Answers {
        self.runner.answers.clone()
    }

    #[must_use]
    pub fn cancels(&self) -> Cancels {
        self.runner.cancels.clone()
    }

    pub(crate) fn handover_source(&self) -> HandoverSource {
        HandoverSource {
            standing: self.runner.standing.clone(),
            running: self.boxes(),
        }
    }

    fn publish(&self) {
        let warm = self.slots.values().filter(|slot| slot.paused).count();
        self.warm.send_replace(u32::try_from(warm).unwrap_or(u32::MAX));
        let retained = self
            .unclaimed
            .keys()
            .copied()
            .chain(self.protected_sessions.values().map(Tap::index))
            .collect::<BTreeSet<_>>();
        let boxes = self.slots.len() + self.setups.len() + retained.len();
        self.boxes.send_replace(u32::try_from(boxes).unwrap_or(u32::MAX));
        self.busy.send_replace(u32::try_from(boxes - warm).unwrap_or(u32::MAX));
        self.sessions.send_replace(
            self.slots
                .keys()
                .chain(self.protected_sessions.keys())
                .map(|session| held_session(session))
                .chain(self.unclaimed.values().flatten().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        );
        self.claude_setups.send_replace(
            self.setups
                .keys()
                .map(|claude_setup_id| held_setup(claude_setup_id))
                .collect(),
        );
        self.turns
            .send_replace(self.slots.values().flat_map(Slot::turns).collect());
        self.holdings.send_replace(self.current_holdings());
    }

    /// # Errors
    /// Fails when a box ends and its files cannot be deleted.
    pub async fn run(
        mut self,
        mut inputs: UnboundedReceiver<Input>,
        mut setups: UnboundedReceiver<SetupCommand>,
    ) -> Result<(), String> {
        let Some(mut finished) = self.finished.take() else {
            return Ok(());
        };
        let queued: Vec<(String, bool)> = self
            .slots
            .iter()
            .filter(|(_, slot)| !slot.queue.is_empty())
            .map(|(session, slot)| {
                let snapshot = slot.queue.iter().any(|command| matches!(command, Command::Snapshot(_)));
                (session.clone(), snapshot)
            })
            .collect();
        for (session, snapshot) in queued {
            if snapshot {
                self.adopted.push(session);
            } else {
                self.drain(session).await?;
            }
        }
        let held: Vec<String> = self.slots.keys().cloned().collect();
        for session in held {
            self.arm_idle(&session);
        }
        self.publish();
        loop {
            tokio::select! {
                input = inputs.recv() => match input {
                    Some(Input::Command(command)) => self.command(command).await?,
                    Some(Input::HandOver(reply)) => self.start_handover(reply),
                    Some(Input::Abandon(reply, abandoned)) => self.abandon(reply, abandoned).await,
                    Some(Input::SnapshotAccepted(snapshot_id)) => self.snapshot_accepted(&snapshot_id).await?,
                    Some(Input::SnapshotFailed(snapshot_id)) => self.snapshot_failed(snapshot_id),
                    Some(Input::Welcomed(protocol)) => self.welcomed(protocol).await?,
                    None => break,
                },
                Some(setup) = setups.recv() => self.setup_command(setup),
                Some(event) = finished.recv() => self.event(event).await?,
            }
            self.settle_handover();
            self.publish();
        }
        Ok(())
    }

    fn box_count(&self) -> u32 {
        u32::try_from(self.slots.len() + self.setups.len()).unwrap_or(u32::MAX)
    }

    fn handover_report_interval(&self) -> Duration {
        let budgets = self.runner.settings.budgets;
        HANDOVER_REPORT_ALLOWANCE + budgets.boot + budgets.snapshot
    }

    fn refusal(&self) -> Option<&'static str> {
        match self.mode {
            Mode::TakingTurns => None,
            Mode::HandingOver => Some("is handing its boxes to a new supervisor binary"),
        }
    }

    fn refuse_turn(&self, turn_id: &EntityId, why: &str) {
        turn_failed(
            &self.runner.out,
            turn_id,
            TurnFailureReason::HostUpdating,
            format!(
                "this host {why} and took no turn {}; the control plane offers it to another host",
                **turn_id
            ),
        );
    }

    fn start_handover(&mut self, reply: oneshot::Sender<SupervisorHandover>) {
        self.mode = Mode::HandingOver;
        self.parking.send_replace(true);
        eprintln!(
            "tumpel-supervisor: handing {} boxes to the new supervisor binary: this host claims no new turn, parks each running turn at its next guest message, gives back each turn still waiting for its claim and waits for the boots and snapshots in flight, logging every {} s while any runs",
            self.box_count(),
            self.handover_report_interval().as_secs()
        );
        let mut queued = Vec::new();
        for slot in self.slots.values_mut() {
            if let Some(armed) = slot.idle.take() {
                armed.abort();
            }
            let (turns, rest): (VecDeque<Command>, VecDeque<Command>) = std::mem::take(&mut slot.queue)
                .into_iter()
                .partition(|command| matches!(command, Command::Turn(_)));
            slot.queue = rest;
            queued.extend(turns);
        }
        for command in queued {
            if let Command::Turn(turn) = command {
                self.refuse_turn(&turn.turn_id, "is handing its boxes to a new supervisor binary");
            }
        }
        self.handing = Some((reply, self.handover_timer()));
    }

    fn handover_timer(&self) -> AbortHandle {
        let events = self.events.clone();
        let limit = self.handover_report_interval();
        tokio::spawn(async move {
            tokio::time::sleep(limit).await;
            let _ = events.send(Event::HandoverExpired);
        })
        .abort_handle()
    }

    fn settle_handover(&mut self) {
        if self.handing.is_some() && self.setups.is_empty() && !self.slots.values().any(|slot| slot.busy) {
            self.finish_handover();
        }
    }

    async fn handover_expired(&mut self) {
        if self.handing.is_none() {
            return;
        }
        for (claude_setup_id, (index, task)) in std::mem::take(&mut self.setup_tasks) {
            task.abort();
            let _ = task.await;
            self.setups.remove(&claude_setup_id);
            if let Err(error) = self
                .runner
                .vmm
                .discard_incomplete_boot(index)
                .await
                .and_then(|()| self.free_disk(&claude_setup_id))
            {
                eprintln!("tumpel-supervisor: discarding the setup box on TAP {index} failed: {error}; kept its files");
            }
        }
        let busy = self.slots.values().filter(|slot| slot.busy).count();
        if busy == 0 {
            self.finish_handover();
            return;
        }
        eprintln!(
            "tumpel-supervisor: HandoverTimedOut: {busy} boxes are still booting or snapshotting {} s into the handover; this supervisor keeps them running and hands every box over once each reaches a point it can be handed over",
            self.handover_report_interval().as_secs()
        );
        let timer = self.handover_timer();
        if let Some((_, waiting)) = self.handing.as_mut() {
            *waiting = timer;
        }
    }

    async fn abandon(&mut self, reply: oneshot::Sender<()>, abandoned: Abandoned) {
        let sessions: Vec<String> = self.slots.keys().cloned().collect();
        for session in sessions {
            let Some(slot) = self.slots.get_mut(&session) else {
                continue;
            };
            let turn_id = match (slot.task.take(), slot.parked.take()) {
                (Some((running, task)), _) => {
                    task.abort();
                    let _ = task.await;
                    if let Some(index) = slot.boot_tap.take() {
                        let recorded = self
                            .runner
                            .standing
                            .boxes
                            .lock()
                            .map_or(true, |boxes| boxes.contains_key(&i32::from(index)));
                        if !recorded {
                            if let Err(error) = self.runner.vmm.discard_incomplete_boot(index).await {
                                eprintln!(
                                    "tumpel-supervisor: discarding the incomplete boot on TAP {index} failed: {error}"
                                );
                            }
                        }
                    }
                    match running {
                        Running::Turn(turn_id) => Some(turn_id),
                        Running::Snapshot | Running::Recovering => None,
                    }
                }
                (None, Some(parked)) => Some(parked.turn_id),
                (None, None) => None,
            };
            if let Some(turn_id) = turn_id {
                turn_failed(
                    &self.runner.out,
                    &turn_id,
                    abandoned.reason.clone(),
                    format!(
                        "{}; turn {} no longer has a supervisor following it, so send the turn again",
                        abandoned.cause, *turn_id
                    ),
                );
            }
        }
        let _ = reply.send(());
    }

    fn answer_lost(&self, session: &str, queue: VecDeque<Command>) {
        for command in queue {
            match command {
                Command::Turn(turn) => self.refuse_turn(&turn.turn_id, "is handing its boxes to a new supervisor binary"),
                Command::Snapshot(order) => emit(
                    &self.runner.out,
                    ChannelSnapshotFailed {
                        detail: reported(
                            "the snapshot failure detail",
                            &format!(
                                "this host runs no box of session {session}; it was stopped while the host handed its boxes to a new supervisor binary, so the session's last turn is not on this host"
                            ),
                        ),
                        failed_at: now(),
                        reason: ChannelSnapshotFailureReason::BoxNotFound,
                        snapshot_id: order.snapshot_id,
                        type_: ChannelSnapshotFailedType::SnapshotFailed,
                    },
                ),
                Command::Teardown(order) | Command::Discard(order) => self.box_gone(order),
            }
        }
    }

    fn finish_handover(&mut self) {
        let Some((reply, timer)) = self.handing.take() else {
            return;
        };
        timer.abort();
        let mut boxes = Vec::new();
        let sessions: Vec<String> = self.slots.keys().cloned().collect();
        for session in sessions {
            let Some(mut slot) = self.slots.remove(&session) else {
                continue;
            };
            slot.paused = false;
            let queue = std::mem::take(&mut slot.queue);
            let Some(live) = slot.live.take() else {
                self.answer_lost(&session, queue);
                continue;
            };
            let state = match slot.parked.take() {
                Some(parked) => SupervisorHandoverBoxState::Turn(SupervisorHandoverTurn {
                    first_line_milliseconds: milliseconds(parked.left.first_line),
                    turn_milliseconds: milliseconds(parked.left.turn),
                    kind: SupervisorHandoverTurnKind::Turn,
                    repository_ready: parked.left.repository_ready,
                    seen_line: parked.left.seen_line,
                    error: parked.left.error.map(|(reason, detail)| SupervisorHandoverTurnError {
                        detail: reported("the parked turn's failure detail", &detail),
                        reason,
                    }),
                    cancelled: self.runner.cancels.contains(&parked.turn_id).then_some(true),
                    turn_id: parked.turn_id,
                }),
                None => SupervisorHandoverBoxState::Warm(SupervisorHandoverWarm {
                    kind: SupervisorHandoverWarmKind::Warm,
                }),
            };
            let idle = slot
                .idle_end
                .map_or(live.idle, |end| end.saturating_duration_since(Instant::now()));
            let queued = Command::records(&queue);
            let LiveBox {
                session_id,
                box_id,
                machine,
                guest,
                tap,
                image,
                dirty,
                credential_file,
                box_retention,
                ..
            } = live;
            let tap_index = i32::from(tap.index());
            let handed = queued.and_then(|queued| {
                let credential_file = credential_record(credential_file)?;
                let image = image.record()?;
                machine
                    .hand_over(guest)
                    .map(|machine| (machine, queued, credential_file, image))
            });
            let (machine, queued, credential_file, image) = match handed {
                Ok(handed) => handed,
                Err(error) => {
                    self.failed_handover(tap_index, &session, &box_id, state, queue, &error);
                    continue;
                }
            };
            boxes.push(SupervisorHandoverBox {
                box_id,
                box_retention,
                credential_file,
                dirty,
                idle_milliseconds: milliseconds(idle),
                image,
                machine,
                queued,
                session_id,
                state,
                tap_index,
            });
        }
        let _ = reply.send(SupervisorHandover {
            boxes,
            handed_at: now(),
        });
    }

    fn failed_handover(
        &self,
        tap_index: i32,
        session: &str,
        box_id: &EntityId,
        state: SupervisorHandoverBoxState,
        queue: VecDeque<Command>,
        error: &str,
    ) {
        eprintln!("tumpel-supervisor: handing over box {box_id} of session {session} failed: {error}; kept its saved box record");
        let kept = self.runner.standing.write(|boxes| {
            if let Some(saved) = boxes.get_mut(&tap_index) {
                Self::interrupt(&mut saved.saved);
                saved.saved.record.queued.clear();
            }
        });
        if let Err(error) = kept {
            eprintln!("tumpel-supervisor: recording the interrupted box of session {session} failed: {error}");
        } else if let SupervisorHandoverBoxState::Turn(turn) = state {
            turn_failed(
                &self.runner.out,
                &turn.turn_id,
                TurnFailureReason::HarnessExited,
                format!("handing over box {box_id} failed: {error}; send the turn again"),
            );
        }
        for command in queue {
            self.answer_protected(command, &protected_detail(tap_index));
        }
    }

    fn resume(&mut self, session: String) {
        let Some(slot) = self.slots.get_mut(&session) else {
            return;
        };
        let Some(parked) = slot.parked.take() else {
            return;
        };
        let Some(live) = slot.live.take() else {
            return;
        };
        slot.busy = true;
        let runner = self.runner.clone();
        let running = Running::Turn(parked.turn_id.clone());
        self.spawn(session, Work::Turn, running, async move {
            runner.resume_turn(live, parked).await
        });
    }

    pub fn start(&mut self, taken: Option<(SupervisorHandover, Taken)>) -> Result<(), String> {
        self.start_inventory(taken.map(|(record, state)| StartupBoxes::from_record(record.into(), state)))
    }

    pub(crate) fn start_inventory(&mut self, taken: Option<StartupBoxes>) -> Result<(), String> {
        let recorded = taken.is_some();
        let mut prepared = Vec::new();
        let mut standing = BTreeMap::new();
        let mut ended = Vec::new();
        let mut inherited_fds = HashSet::new();
        let mut failed_turns = Vec::new();
        if let Some(StartupBoxes {
            inventory,
            handed_count,
            inherited,
            ..
        }) = taken
        {
            let age = milliseconds((now() - inventory.handed_at).to_std().unwrap_or(Duration::ZERO));
            crate::inventory::validate(inventory.boxes.iter().map(|box_| &box_.record))?;
            for (index, mut saved) in inventory.boxes.into_iter().enumerate() {
                let taken = if index < handed_count {
                    Taken::Handed
                } else {
                    Taken::Crashed
                };
                let inherited = inherited && index < handed_count;
                let mut handed = saved.record.clone();
                if matches!(handed.state, SupervisorHandoverBoxState::Warm(_)) {
                    handed.idle_milliseconds = handed.idle_milliseconds.saturating_sub(age).max(0);
                }
                let attempt = (|| {
                    let tap = u8::try_from(handed.tap_index).map_err(|error| error.to_string())?;
                    if !self
                        .runner
                        .vmm
                        .recorded_process(tap, &handed.machine, saved.observation.as_ref())?
                    {
                        return Ok(None);
                    }
                    let (record, image, queue) = decode_box(&handed)?;
                    let descriptors = [
                        handed.machine.guest_fd,
                        handed.machine.stdout_fd,
                        handed.machine.stderr_fd,
                    ];
                    if inherited && descriptors.iter().any(|fd| inherited_fds.contains(fd)) {
                        return Err("an inherited descriptor belongs to more than one box".to_string());
                    }
                    let attached = self.runner.vmm.reattach(
                        &handed.session_id,
                        handed.tap_index,
                        &handed.machine,
                        saved.observation.as_ref(),
                        &mut self.pool,
                        inherited,
                    )?;
                    let Some((machine, guest, tap)) = attached else {
                        return Ok(None);
                    };
                    if inherited {
                        if let Err(error) = machine.register_inherited(&guest) {
                            self.pool.release(tap);
                            return Err(error);
                        }
                        inherited_fds.extend(descriptors);
                    }
                    Ok(Some((record, image, queue, (machine, guest, tap))))
                })();
                let (record, image, queue, (machine, guest, tap)) = match attempt {
                    Ok(Some(attached)) => attached,
                    Ok(None) => {
                        ended.push(saved);
                        continue;
                    }
                    Err(error) => {
                        if let Some(failed) = self.protect_box(&mut saved, &error)? {
                            failed_turns.push(failed);
                        }
                        standing.insert(
                            saved.record.tap_index,
                            StandingBox {
                                saved,
                                descriptors: Err(error),
                            },
                        );
                        continue;
                    }
                };
                handed.dirty = record.dirty;
                let mut standing_box = StandingBox::capture(record, &machine, &guest);
                standing_box.saved.reconnect = saved.reconnect;
                if matches!(taken, Taken::Crashed) {
                    Self::interrupt(&mut standing_box.saved);
                }
                let reconnect = standing_box.saved.reconnect;
                standing.insert(standing_box.saved.record.tap_index, standing_box);
                prepared.push((handed, taken, image, queue, (machine, guest, tap), reconnect));
            }
        }
        let claimed: Vec<u8> = prepared
            .iter()
            .map(|(_, _, _, _, (_, _, tap), _)| tap.index())
            .collect();
        self.finish_startup(&mut standing, ended, &claimed, recorded, &mut failed_turns)?;
        self.runner.standing.write(|boxes| *boxes = standing)?;
        for (turn_id, detail) in failed_turns {
            turn_failed(&self.runner.out, &turn_id, TurnFailureReason::HarnessExited, detail);
        }
        for fd in inherited_fds {
            close_inherited_descriptor(fd)?;
        }
        for (handed, taken, image, queue, attached, reconnect) in prepared {
            self.adopt(handed, taken, image, queue, attached, reconnect);
        }
        self.publish();
        Ok(())
    }

    fn finish_startup(
        &mut self,
        standing: &mut BTreeMap<i32, StandingBox>,
        ended: Vec<RecordedBox>,
        claimed: &[u8],
        recorded: bool,
        failed_turns: &mut Vec<(EntityId, String)>,
    ) -> Result<(), String> {
        self.unclaimed = self.runner.vmm.unclaimed_reservations(claimed)?;
        for mut saved in ended {
            let tap = u8::try_from(saved.record.tap_index).map_err(|error| error.to_string())?;
            if self.unclaimed.contains_key(&tap) {
                if let Some(failed) = self.protect_box(
                    &mut saved,
                    "its scope is live, but the recorded Firecracker process no longer owns it",
                )? {
                    failed_turns.push(failed);
                }
                standing.insert(
                    saved.record.tap_index,
                    StandingBox {
                        saved,
                        descriptors: Err("the retained box does not have an attached process".to_string()),
                    },
                );
            } else {
                let record = saved.record;
                if let Some(failed) = Self::failed_saved_turn(
                    &record,
                    &format!("its Firecracker process {} ended", record.machine.process_id),
                ) {
                    failed_turns.push(failed);
                }
                let queue = record
                    .queued
                    .into_iter()
                    .filter_map(|queued| Command::from_record(queued, &record.session_id, &record.box_id))
                    .collect();
                self.answer_lost(record.session_id.as_str(), queue);
            }
        }
        for &index in self.unclaimed.keys() {
            if !self.protected_sessions.values().any(|tap| tap.index() == index) {
                self.pool.reserve_scope(index);
                eprintln!("tumpel-supervisor: keeping unclaimed tumpel-box{index}.scope and reserving its TAP");
            }
        }
        if recorded || !self.unclaimed.is_empty() {
            let mut kept = claimed.to_vec();
            kept.extend(self.protected_sessions.values().map(Tap::index));
            kept.extend(self.unclaimed.keys());
            kept.sort_unstable();
            kept.dedup();
            let sessions = standing
                .values()
                .map(|box_| box_.saved.record.session_id.clone())
                .chain(self.unclaimed.values().flatten().cloned())
                .collect::<Vec<_>>();
            self.runner.vmm.remove_leftovers(&kept, &sessions)?;
        }
        Ok(())
    }

    fn failed_saved_turn(record: &SupervisorHandoverBox, detail: &str) -> Option<(EntityId, String)> {
        let SupervisorHandoverBoxState::Turn(turn) = &record.state else {
            return None;
        };
        Some((
            turn.turn_id.clone(),
            format!(
                "box {} of session {}: {detail}; send the turn again",
                record.box_id, record.session_id
            ),
        ))
    }

    fn interrupt(saved: &mut RecordedBox) {
        if matches!(saved.record.state, SupervisorHandoverBoxState::Turn(_)) {
            saved.record.state = standing_state(None);
            saved.record.dirty = true;
            saved.reconnect = true;
        }
    }

    fn protect_box(&mut self, saved: &mut RecordedBox, error: &str) -> Result<Option<(EntityId, String)>, String> {
        let record = &mut saved.record;
        eprintln!(
            "tumpel-supervisor: box {} of session {} is kept after attachment failed: {error}",
            record.box_id, record.session_id
        );
        let tap = self.pool.claim_device(record.tap_index, self.pool.size(None, None))?;
        let detail = protected_detail(record.tap_index);
        let failed = Self::failed_saved_turn(record, error);
        for queued in std::mem::take(&mut record.queued) {
            if let Some(command) = Command::from_record(queued, &record.session_id, &record.box_id) {
                self.answer_protected(command, &detail);
            }
        }
        self.protected_sessions.insert(record.session_id.to_string(), tap);
        Self::interrupt(saved);
        Ok(failed)
    }

    fn adopt(
        &mut self,
        handed: SupervisorHandoverBox,
        taken: Taken,
        image: Boot,
        queue: VecDeque<Command>,
        (mut machine, guest, tap): Attached<V::Machine>,
        reconnect: bool,
    ) {
        let session = handed.session_id.to_string();
        machine.activate();
        let hold = self.runner.holds.hold_boot(&image);
        let slot = self.slots.entry(session.clone()).or_default();
        slot.queue = queue;
        slot.live = Some(LiveBox {
            session_id: handed.session_id,
            box_id: handed.box_id,
            machine,
            guest,
            tap,
            base: Arc::new(Base::default()),
            image: Box::new(image),
            _hold: hold,
            dirty: handed.dirty,
            credential_file: handed.credential_file.map(String::from),
            box_retention: handed.box_retention,
            idle: duration(handed.idle_milliseconds),
        });
        if reconnect {
            if let SupervisorHandoverBoxState::Turn(turn) = handed.state {
                turn_failed(
                    &self.runner.out, &turn.turn_id, TurnFailureReason::HarnessExited,
                    format!("the supervisor stopped unexpectedly during turn {}, so the turn ended; its box is kept, so send the turn again", *turn.turn_id),
                );
            }
            let Some(live) = slot.live.take() else {
                return;
            };
            slot.busy = true;
            let runner = self.runner.clone();
            self.spawn(session, Work::Turn, Running::Recovering, async move {
                runner.recover_adopted(live).await
            });
            return;
        }
        match (handed.state, taken) {
            (SupervisorHandoverBoxState::Unrecognised(_), _) => {
                tumpel_channel::update_required();
            }
            (SupervisorHandoverBoxState::Warm(_), _) => {
                slot.paused = true;
                slot.idle_end = Some(Instant::now() + duration(handed.idle_milliseconds));
                self.runner.standing.idle_until(&session, slot.idle_end);
            }
            (SupervisorHandoverBoxState::Turn(_), Taken::Crashed) => {
                unreachable!("crashed turns recover before matching handover state")
            }
            (SupervisorHandoverBoxState::Turn(turn), Taken::Handed) => {
                if turn.cancelled == Some(true) {
                    self.runner.cancels.cancel(turn.turn_id.clone());
                }
                slot.parked = Some(Box::new(Parked {
                    turn_id: turn.turn_id,
                    left: Left {
                        turn: duration(turn.turn_milliseconds),
                        first_line: duration(turn.first_line_milliseconds),
                        repository_ready: turn.repository_ready,
                        seen_line: turn.seen_line,
                        error: turn.error.map(|error| (error.reason, error.detail.into())),
                    },
                }));
                self.resume(session);
            }
        }
    }

    async fn command(&mut self, command: Command) -> Result<(), String> {
        if let (Some(why), Command::Turn(turn)) = (self.refusal(), &command) {
            self.refuse_turn(&turn.turn_id, why);
            return Ok(());
        }
        let session = command.session();
        if let Some(index) = self.protected_sessions.get(&session).map(Tap::index) {
            if let Command::Discard(order) = command {
                return self.discard_protected(&session, index, order).await;
            }
        }
        if let Some(detail) = self.reserved_disk(&session) {
            self.answer_protected(command, &detail);
            return Ok(());
        }
        let slot = self.slots.entry(session.clone()).or_default();
        if slot.busy || slot.parked.is_some() {
            slot.queue.push_back(command);
            return self.record_queue(&session);
        }
        self.dispatch(session, command).await
    }

    fn answer_protected(&self, command: Command, detail: &str) {
        match command {
            Command::Turn(turn) => self.refuse_turn(&turn.turn_id, detail),
            Command::Snapshot(order) => emit(
                &self.runner.out,
                ChannelSnapshotFailed {
                    detail: reported("the snapshot failure detail", detail),
                    failed_at: now(),
                    reason: ChannelSnapshotFailureReason::BoxNotFound,
                    snapshot_id: order.snapshot_id,
                    type_: ChannelSnapshotFailedType::SnapshotFailed,
                },
            ),
            Command::Teardown(order) | Command::Discard(order) => self.refuse_teardown(order, detail),
        }
    }

    fn refuse_teardown(&self, order: TeardownOrder, detail: &str) {
        emit(
            &self.runner.out,
            ChannelBoxTeardownFailed {
                detail: reported("the teardown failure detail", detail),
                failed_at: now(),
                reason: ChannelBoxTeardownFailureReason::BoxDirty,
                session_id: order.session_id,
                box_id: order.box_id,
                type_: ChannelBoxTeardownFailedType::BoxTeardownFailed,
            },
        );
    }

    async fn discard_protected(&mut self, session: &str, index: u8, order: TeardownOrder) -> Result<(), String> {
        let saved = self
            .runner
            .standing
            .boxes
            .lock()
            .map_err(|_| "the box inventory is unavailable")?
            .get(&i32::from(index))
            .map(|box_| box_.saved.clone())
            .ok_or("the retained box record is missing")?;
        if saved.record.box_id != order.box_id {
            self.box_gone(order);
            return Ok(());
        }
        if let Err(error) = self
            .runner
            .vmm
            .discard_protected(index, &saved.record, saved.observation.as_ref())
            .await
        {
            self.refuse_teardown(
                order,
                &format!("{error}; inspect tumpel-box{index}.scope before stopping it"),
            );
            return Ok(());
        }
        self.free_disk(session)?;
        self.runner.standing.write(|boxes| {
            boxes.remove(&i32::from(index));
        })?;
        self.unclaimed.remove(&index);
        if let Some(tap) = self.protected_sessions.remove(session) {
            self.pool.release(tap);
        }
        self.box_gone(order);
        Ok(())
    }

    async fn dispatch(&mut self, session: String, command: Command) -> Result<(), String> {
        match command {
            Command::Turn(turn) => {
                let turn_id = turn.turn_id.clone();
                self.start_turn(session, *turn).await.inspect_err(|error| {
                    turn_failed(
                        &self.runner.out,
                        &turn_id,
                        TurnFailureReason::BoxBootFailed,
                        format!("stopping the earlier box of its session failed: {error}"),
                    );
                })
            }
            Command::Snapshot(order) => {
                self.start_snapshot(session, order);
                Ok(())
            }
            Command::Teardown(order) => self.start_teardown(session, order).await,
            Command::Discard(order) => self.start_discard(session, order).await,
        }
    }

    fn setup_command(&mut self, command: SetupCommand) {
        match command {
            SetupCommand::Start(order) => self.start_setup(*order),
            SetupCommand::Code {
                claude_setup_id,
                code,
                mint_budget_seconds,
            } => self.setup_input(
                &claude_setup_id,
                SetupInput::Code {
                    code,
                    mint_budget_seconds,
                },
            ),
            SetupCommand::Discard { claude_setup_id } => self.setup_input(&claude_setup_id, SetupInput::Discard),
        }
    }

    fn setup_input(&self, claude_setup_id: &EntityId, input: SetupInput) {
        if let Some(inputs) = self.setups.get(claude_setup_id.as_str()) {
            let _ = inputs.send(input);
        }
    }

    fn reserved_disk(&self, session: &str) -> Option<String> {
        if let Some(tap) = self.protected_sessions.get(session) {
            return Some(protected_detail(i32::from(tap.index())));
        }
        self.unclaimed.values().flatten().any(|held| held.as_str() == session).then(|| {
            "has a disk held for an unclaimed Firecracker scope; inspect the supervisor log before stopping that scope".to_string()
        })
    }

    fn start_setup(&mut self, order: SetupOrder) {
        let id = order.claude_setup_id.to_string();
        if self.setups.contains_key(&id) {
            return;
        }
        let refused = |detail: String| tumpel_api::types::ChannelClaudeSetupBoxFailed {
            claude_setup_id: order.claude_setup_id.clone(),
            detail: reported("the Claude setup failure detail", &detail),
            failed_at: now(),
            reason: tumpel_api::types::ClaudeSetupFailureReason::NoHostOnline,
            type_: tumpel_api::types::ChannelClaudeSetupBoxFailedType::ClaudeSetupBoxFailed,
        };
        if self.refusal().is_some() {
            emit(
                &self.runner.out,
                refused(
                    "this host is updating its supervisor and took no setup box; start the Claude setup again once the host is back online"
                        .to_string(),
                ),
            );
            return;
        }
        if let Some(detail) = self.reserved_disk(&id) {
            emit(&self.runner.out, refused(format!("the setup box {detail}")));
            return;
        }
        let tap = match self.claim(None, None) {
            Ok(tap) => tap,
            Err(full) => {
                emit(
                    &self.runner.out,
                    refused(format!(
                        "{full}; the setup box found no free slot, so start the Claude setup again once a box is free"
                    )),
                );
                return;
            }
        };
        let (inputs, received) = unbounded_channel();
        self.setups.insert(id.clone(), inputs);
        let runner = self.runner.clone();
        let events = self.events.clone();
        let named = id.clone();
        let index = tap.index();
        let task = tokio::spawn(async move {
            let (tap, stopped) = runner.claude_setup(tap, order, received).await;
            let _ = events.send(Event::SetupFinished {
                claude_setup_id: named,
                stopped,
                tap,
            });
        });
        self.setup_tasks.insert(id, (index, task));
    }

    fn spawn(
        &mut self,
        session: String,
        work: Work,
        running: Running,
        task: impl Future<Output = Outcome<V::Machine>> + Send + 'static,
    ) {
        self.slots.entry(session.clone()).or_default();
        self.publish();
        let events = self.events.clone();
        let named = session.clone();
        let handle = tokio::spawn(async move {
            let outcome = task.await;
            let _ = events.send(Event::Finished {
                session: named,
                work,
                outcome,
            });
        });
        self.slots.entry(session).or_default().task = Some((running, handle));
    }

    async fn start_turn(&mut self, session: String, turn: BoxTurn) -> Result<(), String> {
        let (harness, harness_version) = turn.image.harness();
        let root = telemetry::turn(&turn.turn_id, &turn.session_id, harness, harness_version, &turn.model);
        let out = &self.runner.out;
        if let Some(detail) = expired(&turn.credential, "before the turn reached a box") {
            root.in_scope(|| turn_failed(out, &turn.turn_id, TurnFailureReason::DispatchCredentialExpired, detail));
            self.prune(&session);
            return Ok(());
        }
        if self.live_box(&session).is_some_and(|live| live.box_id != turn.box_id) {
            self.teardown(&session, BoxEnd::NewBox).await?;
        }
        let warm = self.slots.get_mut(&session).and_then(|slot| {
            if let Some(armed) = slot.idle.take() {
                armed.abort();
            }
            slot.idle_end = None;
            slot.snapshot = None;
            let live = slot.live.take()?;
            slot.paused = false;
            slot.busy = true;
            Some(live)
        });
        self.runner.standing.idle_until(&session, None);
        let runner = self.runner.clone();
        telemetry::start_kind(&root, warm.is_some());
        if let Some(live) = warm {
            let running = Running::Turn(turn.turn_id.clone());
            self.spawn(
                session,
                Work::Turn,
                running,
                async move { runner.warm_turn(live, &turn).await }.instrument(root),
            );
            return Ok(());
        }
        let memory_mib = match positive_mib(turn.box_memory_mib) {
            Ok(memory_mib) => memory_mib,
            Err(mib) => {
                let detail = format!(
                    "turn {} asks for a box of {mib} MiB, which is not a positive size; set the persona's box memory between 128 and 65536 MiB",
                    *turn.turn_id
                );
                root.in_scope(|| {
                    turn_failed(
                        &self.runner.out,
                        &turn.turn_id,
                        TurnFailureReason::BoxBootFailed,
                        detail,
                    );
                });
                self.prune(&session);
                return Ok(());
            }
        };
        let disk_mib = match positive_mib(turn.box_disk_mib) {
            Ok(disk_mib) => disk_mib,
            Err(mib) => {
                let detail = format!(
                    "turn {} asks for a box disk of {mib} MiB, which is not a positive size; set the persona's boxDiskMib between 32768 and 262144 MiB",
                    *turn.turn_id
                );
                root.in_scope(|| {
                    turn_failed(
                        &self.runner.out,
                        &turn.turn_id,
                        TurnFailureReason::BoxBootFailed,
                        detail,
                    );
                });
                self.prune(&session);
                return Ok(());
            }
        };
        let tap = match self.claim(memory_mib, disk_mib) {
            Ok(tap) => tap,
            Err(refused) => {
                root.in_scope(|| {
                    turn_failed(
                        &self.runner.out,
                        &turn.turn_id,
                        TurnFailureReason::PoolExhausted,
                        format!("{refused}; the control plane offers the turn again"),
                    );
                });
                self.prune(&session);
                return Ok(());
            }
        };
        self.slots.entry(session.clone()).or_default().begin_boot(&tap);
        let running = Running::Turn(turn.turn_id.clone());
        self.spawn(
            session,
            Work::Turn,
            running,
            async move { runner.cold_turn(tap, &turn).await }.instrument(root),
        );
        Ok(())
    }

    async fn welcomed(&mut self, protocol: u16) -> Result<(), String> {
        self.session_protocol = Some(protocol);
        for session in std::mem::take(&mut self.adopted) {
            self.drain(session).await?;
        }
        Ok(())
    }

    fn start_snapshot(&mut self, session: String, order: SnapshotOrder) {
        let Some(protocol) = self.session_protocol else {
            emit(
                &self.runner.out,
                ChannelSnapshotFailed {
                    detail: reported(
                        "the snapshot failure detail",
                        "the snapshot was asked for before the channel to the control plane opened, so no protocol version is negotiated; ask for it again",
                    ),
                    failed_at: now(),
                    reason: ChannelSnapshotFailureReason::NotAnswered,
                    snapshot_id: order.snapshot_id,
                    type_: ChannelSnapshotFailedType::SnapshotFailed,
                },
            );
            return;
        };
        let live = self.slots.get_mut(&session).and_then(|slot| {
            let live = slot.live.take()?;
            slot.busy = true;
            slot.snapshot = Some(Awaited::Pending(order.snapshot_id.clone()));
            Some(live)
        });
        let Some(live) = live else {
            let detail = format!(
                "this host runs no box of session {}; it was torn down or never started here, so the session's last turn is not on this host",
                *order.session_id
            );
            emit(
                &self.runner.out,
                ChannelSnapshotFailed {
                    detail: reported("the snapshot failure detail", &detail),
                    failed_at: now(),
                    reason: ChannelSnapshotFailureReason::BoxNotFound,
                    snapshot_id: order.snapshot_id,
                    type_: ChannelSnapshotFailedType::SnapshotFailed,
                },
            );
            self.prune(&session);
            return;
        };
        let runner = self.runner.clone();
        let running = Running::Snapshot;
        self.spawn(session, Work::Snapshot, running, async move {
            runner.snapshot(live, &order, protocol).await
        });
    }

    fn live_box(&self, session: &str) -> Option<&LiveBox<V::Machine>> {
        self.slots.get(session).and_then(|slot| slot.live.as_ref())
    }

    fn box_gone(&self, order: TeardownOrder) {
        emit(
            &self.runner.out,
            ChannelBoxGone {
                box_id: order.box_id,
                session_id: order.session_id,
                type_: ChannelBoxGoneType::BoxGone,
            },
        );
    }

    async fn start_discard(&mut self, session: String, order: TeardownOrder) -> Result<(), String> {
        match self.live_box(&session) {
            Some(live) if live.box_id == order.box_id => self.teardown(&session, BoxEnd::Discard).await?,
            Some(_) => {}
            None => self.free_disk(&session)?,
        }
        self.prune(&session);
        self.box_gone(order);
        Ok(())
    }

    async fn start_teardown(&mut self, session: String, order: TeardownOrder) -> Result<(), String> {
        if let Some(slot) = self.slots.get_mut(&session).filter(|slot| {
            matches!(slot.snapshot, Some(Awaited::Pending(_)))
                && slot.live.as_ref().is_some_and(|live| live.box_id == order.box_id)
        }) {
            slot.snapshot = Some(Awaited::Accepted);
            self.clean(&session).await?;
        }
        let Some(dirty) = self
            .live_box(&session)
            .filter(|live| live.box_id == order.box_id)
            .map(|live| live.dirty)
        else {
            if self.live_box(&session).is_none() {
                self.free_disk(&session)?;
            }
            self.prune(&session);
            self.box_gone(order);
            return Ok(());
        };
        if dirty {
            emit(
                &self.runner.out,
                ChannelBoxTeardownFailed {
                    detail: reported(
                        "the teardown failure detail",
                        &format!(
                            "box {} of session {} holds a turn that no snapshot holds yet, so the host keeps it; stop the session again to snapshot it first",
                            *order.box_id, *order.session_id
                        ),
                    ),
                    failed_at: now(),
                    reason: ChannelBoxTeardownFailureReason::BoxDirty,
                    session_id: order.session_id,
                    box_id: order.box_id,
                    type_: ChannelBoxTeardownFailedType::BoxTeardownFailed,
                },
            );
            return Ok(());
        }
        self.teardown(&session, BoxEnd::Teardown).await?;
        self.prune(&session);
        self.box_gone(order);
        Ok(())
    }

    fn claim(&mut self, memory_mib: Option<NonZeroU32>, disk_mib: Option<NonZeroU32>) -> Result<Tap, String> {
        let size = self.pool.size(memory_mib, disk_mib);
        self.pool
            .claim(size, self.disk()?)
            .map_err(|exhausted| exhausted.to_string())
    }

    fn current_holdings(&self) -> Held {
        Held {
            drives: self
                .slots
                .keys()
                .chain(self.setups.keys())
                .chain(self.protected_sessions.keys())
                .cloned()
                .chain(self.unclaimed.values().flatten().map(ToString::to_string))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            disk_reserved_mib: self.pool.disk_reserved_mib(),
            memory_budget_mib: self.pool.budget_mib(),
            memory_held_mib: self.pool.held_mib(),
        }
    }

    fn disk(&self) -> Result<Disk, String> {
        self.runner
            .vmm
            .disk(&self.current_holdings().drives)
            .map(|disk| Disk {
                free_mib: disk.free_mib.saturating_sub(self.runner.staging.mib()),
                ..disk
            })
            .map_err(|error| format!("reading the disk this host has free failed: {error}"))
    }

    fn free_disk(&self, session: &str) -> Result<(), String> {
        self.runner
            .vmm
            .staging(&held_session(session))
            .and_then(|dir| remove_dir(&dir))
            .map_err(|error| format!("DiskNotFreed: the box of session {session} ended, then {error}"))
    }

    async fn teardown(&mut self, session: &str, end: BoxEnd) -> Result<(), String> {
        let Some(slot) = self.slots.get_mut(session) else {
            return Ok(());
        };
        slot.paused = false;
        let Some(live) = slot.live.take() else {
            return Ok(());
        };
        let (tap, _, stopped) = stop_box(&self.runner.standing, live, end).await;
        stopped?;
        self.pool.release(tap);
        self.free_disk(session)?;
        self.prune(session);
        Ok(())
    }

    fn prune(&mut self, session: &str) {
        if self
            .slots
            .get(session)
            .is_some_and(|slot| !slot.busy && slot.live.is_none() && slot.queue.is_empty())
        {
            self.slots.remove(session);
        }
    }

    async fn event(&mut self, event: Event<V::Machine>) -> Result<(), String> {
        match event {
            Event::Finished { session, work, outcome } => self.finished(session, work, outcome).await,
            Event::SetupFinished {
                claude_setup_id,
                tap,
                stopped,
            } => {
                self.setups.remove(&claude_setup_id);
                self.setup_tasks.remove(&claude_setup_id);
                stopped?;
                self.pool.release(tap);
                self.free_disk(&claude_setup_id)
            }
            Event::HandoverExpired => {
                self.handover_expired().await;
                Ok(())
            }
            Event::IdleExpired { session, box_id } => self.idle_expired(&session, box_id).await,
        }
    }

    fn idle_box(&self, session: &str) -> Option<(&EntityId, Instant)> {
        let slot = self.slots.get(session)?;
        if self.mode != Mode::TakingTurns || slot.busy || slot.parked.is_some() || !slot.queue.is_empty() {
            return None;
        }
        let live = slot
            .live
            .as_ref()
            .filter(|live| !live.dirty && live.box_retention == BoxRetention::Auto)?;
        Some((&live.box_id, slot.idle_end?))
    }

    fn arm_idle(&mut self, session: &str) {
        let idle = self.idle_box(session).map(|(box_id, end)| (box_id.clone(), end));
        let Some(slot) = self.slots.get_mut(session) else {
            return;
        };
        if let Some(armed) = slot.idle.take() {
            armed.abort();
        }
        let Some((box_id, end)) = idle else {
            return;
        };
        let events = self.events.clone();
        let session = session.to_string();
        slot.idle = Some(
            tokio::spawn(async move {
                tokio::time::sleep_until(end).await;
                let _ = events.send(Event::IdleExpired { session, box_id });
            })
            .abort_handle(),
        );
    }

    async fn idle_expired(&mut self, session: &str, box_id: EntityId) -> Result<(), String> {
        if self.idle_box(session).map(|(idle, _)| idle) != Some(&box_id) {
            return Ok(());
        }
        self.teardown(session, BoxEnd::Idle).await?;
        self.box_gone(TeardownOrder {
            session_id: held_session(session),
            box_id,
        });
        Ok(())
    }

    async fn finished(&mut self, session: String, work: Work, outcome: Outcome<V::Machine>) -> Result<(), String> {
        let slot = self.slots.entry(session.clone()).or_default();
        slot.busy = false;
        slot.task = None;
        slot.boot_tap = None;
        let mut freed = None;
        match outcome {
            Outcome::Parked(live, parked) => {
                slot.live = Some(live);
                slot.parked = Some(parked);
                return Ok(());
            }
            Outcome::Kept(live) => {
                if matches!(work, Work::Turn) {
                    slot.paused = true;
                    slot.idle_end = Some(Instant::now() + live.idle);
                    self.runner.standing.idle_until(&session, slot.idle_end);
                }
                slot.live = Some(live);
            }
            Outcome::Gone(tap, stopped) => {
                slot.paused = false;
                freed = Some((tap, stopped));
            }
            Outcome::Protected(tap) => {
                let detail = protected_detail(i32::from(tap.index()));
                for command in std::mem::take(&mut slot.queue) {
                    self.answer_protected(command, &detail);
                }
                self.runner.standing.queue(&session, &[])?;
                self.slots.remove(&session);
                self.protected_sessions.insert(session, tap);
                return Ok(());
            }
        }
        if let Some((tap, stopped)) = freed {
            stopped?;
            self.pool.release(tap);
            self.free_disk(&session)?;
        }
        self.clean(&session).await?;
        self.drain(session).await
    }

    fn snapshot_failed(&mut self, snapshot_id: EntityId) {
        let running = self.slots.values_mut().find_map(|slot| {
            if !matches!(&slot.snapshot, Some(Awaited::Pending(pending)) if *pending == snapshot_id) {
                return None;
            }
            slot.snapshot = None;
            Some(matches!(slot.task, Some((Running::Snapshot, _))))
        });
        if running == Some(true) {
            self.runner.cancels.cancel(snapshot_id);
        }
    }

    async fn snapshot_accepted(&mut self, snapshot_id: &EntityId) -> Result<(), String> {
        let waiting = self.slots.iter_mut().find_map(|(session, slot)| {
            if !matches!(&slot.snapshot, Some(Awaited::Pending(pending)) if pending == snapshot_id) {
                return None;
            }
            slot.snapshot = Some(Awaited::Accepted);
            Some(session.clone())
        });
        let Some(session) = waiting else {
            eprintln!(
                "tumpel-supervisor: the control plane accepted SnapshotDone {}, but no box here waits on that snapshot; a later turn ran or the box is gone, so nothing is marked clean",
                **snapshot_id
            );
            return Ok(());
        };
        self.clean(&session).await?;
        self.arm_idle(&session);
        Ok(())
    }

    async fn clean(&mut self, session: &str) -> Result<(), String> {
        let Some(slot) = self
            .slots
            .get_mut(session)
            .filter(|slot| matches!(slot.snapshot, Some(Awaited::Accepted)) && !slot.busy)
        else {
            return Ok(());
        };
        let Some(live) = slot.live.as_mut() else {
            return Ok(());
        };
        slot.snapshot = None;
        live.dirty = false;
        let Err(error) = self.runner.standing.put(live, None) else {
            return Ok(());
        };
        let box_id = live.box_id.clone();
        self.teardown(session, BoxEnd::SnapshotNotRecorded).await?;
        eprintln!(
            "tumpel-supervisor: recording session {session} box {} as clean for a supervisor restart failed and the box was stopped: {error}",
            *box_id
        );
        Ok(())
    }

    async fn drain(&mut self, session: String) -> Result<(), String> {
        loop {
            let next = self
                .slots
                .get_mut(&session)
                .filter(|slot| !slot.busy && slot.parked.is_none())
                .and_then(|slot| slot.queue.pop_front());
            let Some(command) = next else {
                break;
            };
            self.record_queue(&session)?;
            self.dispatch(session.clone(), command).await?;
        }
        self.prune(&session);
        self.arm_idle(&session);
        Ok(())
    }

    fn record_queue(&self, session: &str) -> Result<(), String> {
        let queued: Vec<SupervisorHandoverCommand> = self
            .slots
            .get(session)
            .map_or_else(|| Ok(Vec::new()), |slot| Command::records(&slot.queue))?;
        self.runner.standing.queue(session, &queued)
    }
}

#[cfg(test)]
mod tests;
