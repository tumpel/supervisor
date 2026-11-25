use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::io::{Cursor, Read};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tumpel_api::types::{
    BoxRetention, ChannelBoxTeardownFailureReason, ChannelChunkPlacement, ChannelHostMessage, ChannelPackUrl,
    ChannelProtocol, ChannelRestoreDownloads, ChannelRestoreDownloadsType, ChannelSnapshotChunksHeld,
    ChannelSnapshotChunksHeldType, ChannelSnapshotDownload, ChannelSnapshotFailed, ChannelSnapshotFailedType,
    ChannelSnapshotFailureReason, ChannelSnapshotPackUploads, ChannelSnapshotPackUploadsType, ChannelSnapshotPacks,
    ChannelTurnClaimed, ChannelTurnClaimedType, ChannelTurnWithdrawn, ChannelTurnWithdrawnType, EntityId,
    GuestHarnessProtocol, GuestHello, GuestInput, GuestMessage, Sequence, SupervisorHandover, SupervisorHandoverBox,
    SupervisorHandoverBoxState, SupervisorHandoverCommand, SupervisorHandoverMachine, SupervisorHandoverSnapshot,
    SupervisorHandoverSnapshotKind, SupervisorHandoverTeardown, SupervisorHandoverTeardownKind, TurnFailureReason,
};
use tumpel_channel::Outgoing;

use super::{
    restore_budget, BoxTurn, Budgets, Command, Guest, Input, Lifecycle, Machine, Settings, SnapshotOrder, Stopped,
    Taken, TeardownOrder, Vmm,
};
use crate::boot::BootOrder;
use crate::exchange::{Answer, Answers, Ask, Cancels, Key, DIGESTS_PER_PART};
use crate::images::ImageOrder;
use crate::pool::{Disk, Held, Size, Tap, TapPool};
use crate::snapshot::{decode, decode_root, Born, Entry, Found, Kind, Scan, Stored, Upper};
use crate::store::{DownloadError, Store, UploadError, DOWNLOAD_ATTEMPTS};

#[derive(Clone, Copy, PartialEq)]
enum Harness {
    Echo,
    Reader,
    Silent,
    StallsAfterFirstLine,
    SlowClone,
    HangsCloning,
    RefusesClone,
}

#[derive(Clone, Copy, PartialEq)]
enum Boot {
    Works,
    Slow,
    Hangs,
    OldImage,
    NewImage,
    RefusesHandover,
}

type Files = BTreeMap<Vec<u8>, (Found, Vec<u8>)>;

#[derive(Clone)]
struct Tree(Files);

impl Upper for Tree {
    fn scan(&self) -> Result<Scan, String> {
        Ok(self
            .0
            .iter()
            .map(|(path, (found, _))| (path.clone(), found.clone()))
            .collect())
    }

    fn open(&self, path: &[u8]) -> Result<Box<dyn Read + '_>, String> {
        let (_, text) = self.0.get(path).ok_or("no such file")?;
        if path.starts_with(b"root/slow") {
            std::thread::sleep(SLOW_CUT);
        }
        Ok(Box::new(Cursor::new(text.clone())))
    }
}

type Drives = Arc<Mutex<HashMap<PathBuf, Files>>>;
type Log = Arc<Mutex<Vec<String>>>;
type Hands = Arc<Mutex<HashMap<i32, (MemoryGuest, NonZeroU32)>>>;

fn note(log: &Log, event: String) {
    log.lock().expect("log").push(event);
}

struct MemoryGuest {
    harness: Harness,
    log: Log,
    drive: PathBuf,
    drives: Drives,
    lines: i64,
    pending: VecDeque<GuestMessage>,
    ready_at: Option<tokio::time::Instant>,
}

fn message(value: serde_json::Value) -> GuestMessage {
    serde_json::from_value(value).expect("a guest message")
}

impl MemoryGuest {
    fn send_now(&mut self, input: &GuestInput) {
        if let (GuestInput::TurnCancel(cancel), Harness::StallsAfterFirstLine) = (input, self.harness) {
            self.pending.push_back(message(json!({
                "type": "TurnEnd", "turnId": *cancel.turn_id, "reason": "Cancelled", "lastLineSeq": self.lines,
                "detail": "the host cancelled the turn", "credentialFile": "refreshed"
            })));
        }
        let GuestInput::TurnStart(start) = input else {
            return;
        };
        if start.turn_id.starts_with("late") {
            note(&self.log, format!("start {}", *start.turn_id));
        }
        self.lines = start.last_line_seq.0;
        self.ready_at = Some(tokio::time::Instant::now() + SLOW_CLONE);
        if self.harness == Harness::HangsCloning {
            return;
        }
        if self.harness == Harness::RefusesClone {
            self.pending.push_back(message(json!({
                "type": "TurnRepositoryCloneFailed", "turnId": *start.turn_id, "cause": "Refused", "detail": "git reported: denied"
            })));
            return;
        }
        self.pending.push_back(message(
            json!({ "type": "TurnRepositoryReady", "turnId": *start.turn_id }),
        ));
        let texts: Vec<&str> = match self.harness {
            Harness::Echo | Harness::Reader | Harness::SlowClone => start.prompt.lines().collect(),
            Harness::StallsAfterFirstLine => start.prompt.lines().take(1).collect(),
            Harness::HangsCloning | Harness::RefusesClone | Harness::Silent => Vec::new(),
        };
        for text in texts {
            self.lines += 1;
            self.pending.push_back(message(
                json!({ "type": "TurnLine", "turnId": *start.turn_id, "lineSeq": self.lines, "text": text }),
            ));
        }
        if matches!(self.harness, Harness::Echo | Harness::SlowClone) {
            let file = format!("root/{}", *start.turn_id).into_bytes();
            let found = Found {
                entry: Entry {
                    kind: Kind::File {
                        size: start.prompt.len() as u64,
                    },
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    mtime: 1,
                    mtime_nanos: 0,
                },
                born: Born { seconds: 1, nanos: 0 },
            };
            self.drives
                .lock()
                .expect("drives")
                .entry(self.drive.clone())
                .or_default()
                .insert(file, (found, start.prompt.as_bytes().to_vec()));
        }
        if matches!(self.harness, Harness::Echo | Harness::Reader | Harness::SlowClone) {
            self.pending.push_back(message(json!({
                "type": "TurnEnd", "turnId": *start.turn_id, "reason": "Completed", "lastLineSeq": self.lines, "detail": ""
            })));
        }
    }
}

impl Guest for MemoryGuest {
    fn send(&mut self, input: &GuestInput) -> impl Future<Output = Result<(), String>> + Send {
        self.send_now(input);
        std::future::ready(Ok(()))
    }

    async fn receive(&mut self) -> Result<GuestMessage, String> {
        let cloning = matches!(self.pending.front(), Some(GuestMessage::TurnRepositoryReady(_)));
        if let (true, Harness::SlowClone, Some(ready_at)) = (cloning, self.harness, self.ready_at) {
            tokio::time::sleep_until(ready_at).await;
        }
        match self.pending.pop_front() {
            Some(message) => Ok(message),
            None => std::future::pending().await,
        }
    }
}

fn restored(archive: &[u8]) -> Files {
    let mut files = Files::new();
    let mut archive = tar::Archive::new(Cursor::new(archive));
    for entry in archive.entries().expect("entries") {
        let mut entry = entry.expect("entry");
        let path = entry.path_bytes().into_owned();
        let header = entry.header().clone();
        let mut text = Vec::new();
        entry.read_to_end(&mut text).expect("content");
        let kind = if header.entry_type().is_dir() {
            Kind::Dir
        } else {
            Kind::File {
                size: text.len() as u64,
            }
        };
        let found = Found {
            entry: Entry {
                kind,
                mode: header.mode().expect("mode"),
                uid: header.uid().expect("uid"),
                gid: header.gid().expect("gid"),
                mtime: header.mtime().expect("mtime"),
                mtime_nanos: 0,
            },
            born: Born { seconds: 2, nanos: 0 },
        };
        files.insert(path, (found, text));
    }
    files
}

struct MemoryMachine {
    harness: Harness,
    boot: Boot,
    session: String,
    tap: String,
    drive: PathBuf,
    drives: Drives,
    log: Log,
    hands: Hands,
    memory_mib: NonZeroU32,
    console: String,
}

impl MemoryMachine {
    fn guest(&self) -> MemoryGuest {
        MemoryGuest {
            harness: self.harness,
            log: Arc::clone(&self.log),
            drive: self.drive.clone(),
            drives: Arc::clone(&self.drives),
            lines: 0,
            pending: VecDeque::new(),
            ready_at: None,
        }
    }
}

impl Machine for MemoryMachine {
    type Guest = MemoryGuest;

    async fn start(
        &mut self,
        _image: &crate::boot::Boot,
        restore: Option<&Path>,
    ) -> Result<(MemoryGuest, GuestHello), String> {
        let files = match restore {
            Some(path) => {
                let archive = std::fs::read(path).expect("restore drive");
                let files = restored(&archive);
                let names: Vec<String> = files
                    .keys()
                    .map(|key| String::from_utf8_lossy(key).into_owned())
                    .collect();
                note(&self.log, format!("restore {} {}", self.session, names.join(",")));
                files
            }
            None => Files::new(),
        };
        if self.boot == Boot::Slow {
            tokio::time::sleep(SLOW_BOOT).await;
        }
        note(&self.log, format!("boot {} on {}", self.session, self.tap));
        self.console = "[ boot console ]".to_string();
        if self.boot == Boot::Hangs {
            std::future::pending::<()>().await;
        }
        self.drives.lock().expect("drives").insert(self.drive.clone(), files);
        let hello = serde_json::from_value(json!({
            "type": "GuestHello", "protocolVersion": match self.boot { Boot::OldImage => 1, Boot::NewImage => tumpel_api::types::GuestProtocol::VERSION + 1, _ => tumpel_api::types::GuestProtocol::VERSION }, "harness": "Shell", "harnessVersion": "1", "lastLineSeq": 0
        }))
        .expect("hello");
        Ok((self.guest(), hello))
    }

    fn pause(&mut self) -> impl Future<Output = Result<(), String>> + Send {
        note(&self.log, format!("pause {}", self.session));
        std::future::ready(Ok(()))
    }

    fn resume(&mut self) -> impl Future<Output = Result<(), String>> + Send {
        note(&self.log, format!("resume {}", self.session));
        std::future::ready(Ok(()))
    }

    fn drive(&self) -> PathBuf {
        self.drive.clone()
    }

    fn stop(self) -> impl Future<Output = Stopped> + Send {
        self.drives.lock().expect("drives").remove(&self.drive);
        note(&self.log, format!("stop {}", self.session));
        std::future::ready(Stopped {
            exit: "was stopped".to_string(),
            cleared: Ok(()),
        })
    }

    fn console_tail(&self) -> String {
        self.console.clone()
    }

    fn clear_console(&mut self) {
        self.console.clear();
    }

    fn hand_over(self, guest: MemoryGuest) -> Result<SupervisorHandoverMachine, String> {
        if self.boot == Boot::RefusesHandover {
            return Err("its Firecracker process refused the handover".to_string());
        }
        let mut hands = self.hands.lock().expect("hands");
        let guest_fd = i32::try_from(hands.len()).expect("a descriptor") + 3;
        hands.insert(guest_fd, (guest, self.memory_mib));
        note(&self.log, format!("hand over {}", self.session));
        let machine = serde_json::from_value(json!({
            "processId": 100 + guest_fd, "apiSocket": "api.sock", "vsock": "v.sock", "guestFd": guest_fd,
            "guestBuffer": "", "stdoutFd": 0, "stderrFd": 0, "consoleTail": ""
        }))
        .expect("a handed machine");
        Ok(machine)
    }

    fn record(&self) -> Result<SupervisorHandoverMachine, String> {
        Ok(serde_json::from_value(json!({
            "processId": 99, "apiSocket": "api.sock", "vsock": "v.sock", "guestFd": -1,
            "guestBuffer": "", "stdoutFd": -1, "stderrFd": -1, "consoleTail": ""
        }))
        .expect("a recorded machine"))
    }

    fn reconnect(&mut self, guest: &mut MemoryGuest) -> impl Future<Output = Result<(), String>> + Send {
        note(&self.log, format!("reconnect {}", self.session));
        *guest = self.guest();
        std::future::ready(Ok(()))
    }
}

#[derive(Clone)]
struct MemoryVmm {
    harness: Harness,
    boot: Boot,
    staging: PathBuf,
    drives: Drives,
    log: Log,
    hands: Hands,
    free_disk_mib: Arc<AtomicU64>,
}

impl Vmm for MemoryVmm {
    type Machine = MemoryMachine;
    type Upper = Tree;

    fn machine(&self, session_id: &EntityId, tap: &Tap) -> Result<MemoryMachine, String> {
        Ok(MemoryMachine {
            harness: self.harness,
            boot: self.boot,
            session: session_id.to_string(),
            tap: tap.device(),
            drive: self.staging.join(&**session_id).join("overlay"),
            drives: Arc::clone(&self.drives),
            log: Arc::clone(&self.log),
            hands: Arc::clone(&self.hands),
            memory_mib: tap.memory_mib(),
            console: String::new(),
        })
    }

    fn reattach(
        &self,
        session_id: &EntityId,
        tap: &Tap,
        handed: &SupervisorHandoverMachine,
    ) -> Result<(MemoryMachine, MemoryGuest), String> {
        let (guest, _) = self
            .hands
            .lock()
            .expect("hands")
            .remove(&handed.guest_fd)
            .ok_or_else(|| {
                format!(
                    "its Firecracker process {} ended during the handover",
                    handed.process_id
                )
            })?;
        note(&self.log, format!("reattach {} on {}", **session_id, tap.device()));
        Ok((self.machine(session_id, tap)?, guest))
    }

    fn booted_memory(&self, handed: &SupervisorHandoverMachine) -> Result<NonZeroU32, String> {
        self.hands
            .lock()
            .expect("hands")
            .get(&handed.guest_fd)
            .map(|(_, memory_mib)| *memory_mib)
            .ok_or_else(|| {
                format!(
                    "its Firecracker process {} ended during the handover",
                    handed.process_id
                )
            })
    }

    fn booted_disk(&self, _: &EntityId) -> Result<NonZeroU32, String> {
        Ok(BOX.disk_mib)
    }

    fn disk(&self, _: &[String]) -> Result<Disk, String> {
        Ok(Disk {
            total_mib: 100_000,
            free_mib: self.free_disk_mib.load(Ordering::SeqCst),
            written_mib: 0,
        })
    }

    fn discard(&self, handed: &SupervisorHandoverMachine) {
        self.hands.lock().expect("hands").remove(&handed.guest_fd);
        note(&self.log, format!("discard {}", handed.process_id));
    }

    fn end_unclaimed(&self, claimed: &[u8]) -> Result<(), String> {
        note(&self.log, format!("keep boxes {claimed:?} and stop the rest"));
        Ok(())
    }

    fn remove_leftovers(&self, _: &[u8], _: &[EntityId]) -> Result<(), String> {
        Ok(())
    }

    fn open_upper(&self, drive: &Path) -> Result<Tree, String> {
        self.drives
            .lock()
            .expect("drives")
            .get(drive)
            .cloned()
            .map(Tree)
            .ok_or_else(|| format!("no drive at {}", drive.display()))
    }

    fn staging(&self, session_id: &EntityId) -> Result<PathBuf, String> {
        Ok(self.staging.join(&**session_id))
    }

    fn image_cache(&self) -> PathBuf {
        self.staging.join("images")
    }
}

#[derive(Default)]
struct Index {
    chunks: HashMap<String, ChannelChunkPlacement>,
    packs: HashMap<String, std::num::NonZeroU64>,
}

#[derive(Default, Clone)]
struct MemoryStore(
    Arc<Mutex<HashMap<String, Vec<u8>>>>,
    Log,
    Arc<Mutex<Index>>,
    Duration,
    Arc<AtomicU32>,
    Option<Arc<(Notify, Notify)>>,
);

const FETCHED_IMAGE_URL: &str = "https://r2.example.test/fetched/image";
const PERSONA_FILE_URL: &str = "https://r2.example.test/persona/file";

impl MemoryStore {
    fn holds(&self, url: &str) -> bool {
        self.0.lock().expect("store").contains_key(url)
    }

    fn uploads(&self) -> Vec<String> {
        std::mem::take(&mut *self.1.lock().expect("uploads"))
    }

    fn get(&self, url: &str) -> Result<Vec<u8>, String> {
        self.0
            .lock()
            .expect("store")
            .get(url)
            .cloned()
            .ok_or_else(|| format!("GET {url} answered 404 Not Found"))
    }

    fn upload_checked_now(&self, url: &str, file: &Path, sha256: &str) -> Result<(), UploadError> {
        let bytes = std::fs::read(file).map_err(|error| UploadError::Unreadable(error.to_string()))?;
        if crate::snapshot::verified(sha256, &bytes).is_err() || !url.ends_with(sha256) {
            return Err(UploadError::Rejected(format!("PUT {url} answered 400 BadDigest")));
        }
        self.0.lock().expect("store").insert(url.to_string(), bytes);
        note(&self.1, url.to_string());
        Ok(())
    }

    fn upload_now(&self, url: &str, file: &Path) -> Result<(), UploadError> {
        let bytes = std::fs::read(file).map_err(|error| UploadError::Unreadable(error.to_string()))?;
        self.0.lock().expect("store").insert(url.to_string(), bytes);
        note(&self.1, url.to_string());
        Ok(())
    }

    fn failing(&self, url: &str) -> Result<(), DownloadError> {
        let failing = url.contains("/packs/")
            && self
                .4
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| left.checked_sub(1))
                .is_ok();
        if failing {
            return Err(DownloadError::Transient(format!("GET {url}: error sending request")));
        }
        Ok(())
    }

    fn download_now(&self, url: &str, file: &Path) -> Result<(), DownloadError> {
        self.failing(url)?;
        let bytes = self.get(url).map_err(DownloadError::Refused)?;
        std::fs::write(file, bytes).map_err(|error| DownloadError::Refused(error.to_string()))
    }

    fn download_range_now(&self, url: &str, file: &Path, first: u64, last: u64) -> Result<(), DownloadError> {
        self.failing(url)?;
        let bytes = self.get(url).map_err(DownloadError::Refused)?;
        let range = usize::try_from(first).expect("first")..=usize::try_from(last).expect("last");
        let part = bytes
            .get(range)
            .ok_or_else(|| DownloadError::Refused(format!("GET {url} answered 416")))?;
        std::fs::write(file, part).map_err(|error| DownloadError::Refused(error.to_string()))
    }

    async fn hold_download(&self, url: &str) {
        if let Some((started, release)) = self
            .5
            .as_deref()
            .filter(|_| url.contains("/packs/") || url == PERSONA_FILE_URL)
        {
            started.notify_one();
            release.notified().await;
        }
    }
}

impl Store for MemoryStore {
    fn upload(&self, url: &str, file: &Path) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(self.upload_now(url, file))
    }

    fn upload_checked(
        &self,
        url: &str,
        file: &Path,
        sha256: &str,
    ) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(self.upload_checked_now(url, file, sha256))
    }

    async fn download(&self, url: &str, file: &Path) -> Result<(), DownloadError> {
        self.hold_download(url).await;
        if url == FETCHED_IMAGE_URL {
            tokio::time::sleep(self.3).await;
        }
        self.download_now(url, file)
    }

    async fn download_range(&self, url: &str, file: &Path, first: u64, last: u64) -> Result<(), DownloadError> {
        self.hold_download(url).await;
        self.download_range_now(url, file, first, last)
    }
}

const BOX: Size = Size {
    memory_mib: NonZeroU32::new(4096).expect("a positive size"),
    disk_mib: NonZeroU32::new(32_768).expect("a positive size"),
};

const SETTLE: Duration = Duration::from_millis(60);
const SLOW_CLONE: Duration = Duration::from_millis(300);
const SLOW_BOOT: Duration = Duration::from_millis(50);
const SLOW_CUT: Duration = Duration::from_millis(300);
const LATE_CLAIM: Duration = Duration::from_millis(150);
const STALLED_CLAIM: Duration = Duration::from_secs(600);

fn settings() -> Settings {
    Settings {
        budgets: Budgets {
            boot: Duration::from_millis(300),
            first_line: Duration::from_millis(200),
            snapshot: Duration::from_secs(5),
        },
        layer_cache_mib: std::num::NonZeroU32::MIN,
    }
}

fn describe(message: ChannelHostMessage) -> Option<String> {
    Some(match message {
        ChannelHostMessage::TurnClaim(claim) => format!("claim {}", *claim.turn_id),
        ChannelHostMessage::TurnRepositoryReady(_)
        | ChannelHostMessage::SnapshotChunks(_)
        | ChannelHostMessage::SnapshotPacks(_)
        | ChannelHostMessage::RestoreChunks(_)
        | ChannelHostMessage::SupervisorUpdateStep(_)
        | ChannelHostMessage::SupervisorUpdateFailed(_)
        | ChannelHostMessage::Unrecognised(_) => return None,
        ChannelHostMessage::TurnImageFetching(fetching) => format!("fetching {}", *fetching.image_digest),
        ChannelHostMessage::TurnLines(lines) => format!(
            "line {} {}",
            lines.first_line_seq,
            lines
                .lines
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ")
        ),
        ChannelHostMessage::TurnComplete(done) => format!("complete {}", done.last_line_seq.0),
        ChannelHostMessage::TurnRepositoryCloneFailed(failed) => {
            format!("clone failed {}: {}", failed.cause, failed.detail)
        }
        ChannelHostMessage::TurnFailed(failed) => match failed.credential_file {
            Some(file) => format!(
                "failed {}: {} with credential file {}",
                failed.reason, failed.detail, *file
            ),
            None => format!("failed {}: {}", failed.reason, failed.detail),
        },
        ChannelHostMessage::SnapshotDone(_) => "snapshot done".to_string(),
        ChannelHostMessage::SnapshotFailed(failed) => {
            format!("snapshot failed {}: {}", failed.reason, failed.detail)
        }
        ChannelHostMessage::BoxGone(gone) => format!("box gone {}", *gone.session_id),
        ChannelHostMessage::BoxTeardownFailed(failed) => {
            format!("teardown failed {}: {}", failed.reason, failed.detail)
        }
        ChannelHostMessage::ClaudeSetupBoxUrl(printed) => format!("setup url {}", printed.sign_in_url),
        ChannelHostMessage::ClaudeSetupBoxFailed(failed) => {
            format!("setup failed {}: {}", failed.reason, failed.detail)
        }
    })
}

struct Host {
    commands: UnboundedSender<Input>,
    cancels: Cancels,
    received: UnboundedReceiver<ChannelHostMessage>,
    store: MemoryStore,
    log: Log,
    seq: i64,
    clone_seconds: u32,
    turn_seconds: NonZeroU32,
    idle_seconds: NonZeroU32,
    images: PathBuf,
    vmm: MemoryVmm,
    capacity: u32,
    settings: Settings,
    sessions: tokio::sync::watch::Receiver<Vec<EntityId>>,
    holdings: tokio::sync::watch::Receiver<Held>,
    runner: tokio::task::JoinHandle<Result<(), String>>,
    answering: Arc<AtomicBool>,
    accepting: Arc<AtomicBool>,
    session: Arc<AtomicU16>,
    answers: Answers,
}

fn staging(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lifecycle-tests")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("clearing the last run");
    }
    dir
}

fn host(name: &str, capacity: u32, harness: Harness, boot: Boot) -> Host {
    host_with(name, capacity, harness, boot, settings(), 120)
}

fn host_with(name: &str, capacity: u32, harness: Harness, boot: Boot, settings: Settings, clone_seconds: u32) -> Host {
    host_on(
        name,
        MemoryStore::default(),
        capacity,
        harness,
        boot,
        settings,
        clone_seconds,
    )
}

fn pack_url(digest: &str) -> String {
    format!("https://store.example/packs/{digest}")
}

fn claim_answer(turn_id: EntityId) -> Answer {
    if turn_id.starts_with("withdrawn") {
        return Answer::Withdrawn(ChannelTurnWithdrawn {
            turn_id,
            type_: ChannelTurnWithdrawnType::TurnWithdrawn,
        });
    }
    Answer::Claimed(ChannelTurnClaimed {
        turn_id,
        type_: ChannelTurnClaimedType::TurnClaimed,
    })
}

fn forward(sender: &UnboundedSender<ChannelHostMessage>, message: ChannelHostMessage) {
    sender
        .send(message)
        .expect("the host reads what the control plane forwards");
}

fn swapped(snapshot_id: EntityId, built: u16) -> ChannelHostMessage {
    ChannelSnapshotFailed {
        detail: format!("built for {built}").try_into().expect("detail"),
        failed_at: chrono::Utc::now().fixed_offset(),
        reason: ChannelSnapshotFailureReason::NotAnswered,
        snapshot_id,
        type_: ChannelSnapshotFailedType::SnapshotFailed,
    }
    .into()
}

fn channel(
    mut from_lifecycle: UnboundedReceiver<Outgoing>,
    session: Arc<AtomicU16>,
    commands: UnboundedSender<Input>,
) -> UnboundedReceiver<ChannelHostMessage> {
    let (sender, sent) = unbounded_channel();
    tokio::spawn(async move {
        while let Some(outgoing) = from_lifecycle.recv().await {
            let message = match outgoing {
                Outgoing::Snapshot(message, built) if built != session.load(Ordering::SeqCst) => {
                    let snapshot_id = match message {
                        ChannelHostMessage::SnapshotPacks(packs) => packs.snapshot_id,
                        ChannelHostMessage::SnapshotDone(done) => done.snapshot_id,
                        other => panic!("{other:?} is no snapshot frame"),
                    };
                    let _ = commands.send(Input::SnapshotFailed(snapshot_id.clone()));
                    swapped(snapshot_id, built)
                }
                Outgoing::Message(message) | Outgoing::Snapshot(message, _) => message,
            };
            let _ = sender.send(message);
        }
    });
    sent
}

fn control_plane(
    store: MemoryStore,
    answers: Answers,
    log: Log,
    answering: Arc<AtomicBool>,
    mut from_lifecycle: UnboundedReceiver<ChannelHostMessage>,
) -> UnboundedReceiver<ChannelHostMessage> {
    let (sender, received) = unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = from_lifecycle.recv().await {
            let mut index = store.2.lock().expect("index");
            match message {
                ChannelHostMessage::SnapshotChunks(chunks) => {
                    answers.deliver(Answer::ChunksHeld(ChannelSnapshotChunksHeld {
                        held: chunks
                            .digests
                            .iter()
                            .filter_map(|digest| index.chunks.get(digest.as_str()).cloned())
                            .collect(),
                        part: chunks.part,
                        snapshot_id: chunks.snapshot_id,
                        type_: ChannelSnapshotChunksHeldType::SnapshotChunksHeld,
                    }));
                }
                ChannelHostMessage::SnapshotPacks(packs)
                    if packs.snapshot_id.as_str() == "refused-snapshot-packs" && packs.part.get() == 2 =>
                {
                    forward(&sender, ChannelHostMessage::SnapshotPacks(packs));
                }
                ChannelHostMessage::SnapshotPacks(packs) => {
                    for chunk in &packs.chunks {
                        index
                            .chunks
                            .entry(chunk.digest.as_str().to_string())
                            .or_insert_with(|| chunk.clone());
                    }
                    for pack in &packs.packs {
                        index
                            .packs
                            .insert(pack.pack_digest.as_str().to_string(), pack.size_bytes);
                    }
                    answers.deliver(Answer::PackUploads(ChannelSnapshotPackUploads {
                        part: packs.part,
                        snapshot_id: packs.snapshot_id.clone(),
                        type_: ChannelSnapshotPackUploadsType::SnapshotPackUploads,
                        upload_expires_at: in_seconds(60),
                        uploads: packs
                            .packs
                            .iter()
                            .filter(|pack| !store.holds(&pack_url(&pack.pack_digest)))
                            .map(|pack| ChannelPackUrl {
                                pack_digest: pack.pack_digest.clone(),
                                size_bytes: pack.size_bytes,
                                url: pack_url(&pack.pack_digest).parse().expect("a pack URL"),
                            })
                            .collect(),
                    }));
                    forward(&sender, ChannelHostMessage::SnapshotPacks(packs));
                }
                ChannelHostMessage::RestoreChunks(chunks) if chunks.turn_id.as_str() == "refused-restore-chunks" => {
                    forward(&sender, ChannelHostMessage::RestoreChunks(chunks));
                }
                ChannelHostMessage::RestoreChunks(chunks) => {
                    let mut packs: Vec<ChannelPackUrl> = Vec::new();
                    for chunk in &chunks.chunks {
                        let Some(size) = index.packs.get(chunk.pack_digest.as_str()).copied() else {
                            continue;
                        };
                        if packs.iter().all(|pack| pack.pack_digest != chunk.pack_digest) {
                            packs.push(ChannelPackUrl {
                                pack_digest: chunk.pack_digest.clone(),
                                size_bytes: size,
                                url: pack_url(&chunk.pack_digest).parse().expect("a pack URL"),
                            });
                        }
                    }
                    answers.deliver(Answer::Downloads(ChannelRestoreDownloads {
                        download_expires_at: in_seconds(60),
                        packs,
                        part: chunks.part,
                        turn_id: chunks.turn_id,
                        type_: ChannelRestoreDownloadsType::RestoreDownloads,
                    }));
                }
                ChannelHostMessage::TurnClaim(claim) => {
                    let turn_id = claim.turn_id.clone();
                    forward(&sender, ChannelHostMessage::TurnClaim(claim));
                    let delay = if turn_id.starts_with("late") {
                        LATE_CLAIM
                    } else if turn_id.starts_with("stalled") {
                        STALLED_CLAIM
                    } else if turn_id.as_str() == "refused-claim" {
                        continue;
                    } else {
                        answers.deliver(claim_answer(turn_id));
                        continue;
                    };
                    note(&log, format!("claim {}", *turn_id));
                    let answers = answers.clone();
                    let log = Arc::clone(&log);
                    let answering = Arc::clone(&answering);
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        if !answering.load(Ordering::SeqCst) {
                            return;
                        }
                        note(&log, format!("claimed {}", *turn_id));
                        answers.deliver(claim_answer(turn_id));
                    });
                }
                other => forward(&sender, other),
            }
        }
    });
    received
}

fn acking(
    mut received: UnboundedReceiver<ChannelHostMessage>,
    accepting: Arc<AtomicBool>,
    commands: UnboundedSender<Input>,
) -> UnboundedReceiver<ChannelHostMessage> {
    let (forward, acked) = unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = received.recv().await {
            let accepted = match &message {
                ChannelHostMessage::SnapshotDone(done) if accepting.load(Ordering::SeqCst) => {
                    Some(Input::SnapshotAccepted(done.snapshot_id.clone()))
                }
                _ => None,
            };
            let _ = forward.send(message);
            if let Some(accepted) = accepted {
                let _ = commands.send(accepted);
            }
        }
    });
    acked
}

fn host_on(
    name: &str,
    store: MemoryStore,
    capacity: u32,
    harness: Harness,
    boot: Boot,
    settings: Settings,
    clone_seconds: u32,
) -> Host {
    let staging = staging(name);
    std::fs::create_dir_all(&staging).expect("the staging directory is created");
    let vmm = MemoryVmm {
        harness,
        boot,
        staging,
        drives: Arc::default(),
        log: Arc::default(),
        hands: Arc::default(),
        free_disk_mib: Arc::new(AtomicU64::new(1 << 30)),
    };
    start_host(
        vmm,
        store,
        capacity,
        settings,
        clone_seconds,
        None,
        Some(ChannelProtocol::VERSION),
    )
}

fn start_host(
    vmm: MemoryVmm,
    store: MemoryStore,
    capacity: u32,
    settings: Settings,
    clone_seconds: u32,
    taken: Option<(SupervisorHandover, Taken)>,
    welcome: Option<u16>,
) -> Host {
    let log = Arc::clone(&vmm.log);
    let images = vmm.staging.join("images");
    let (out, from_lifecycle) = unbounded_channel();
    let mut lifecycle = Lifecycle::new(
        vmm.clone(),
        store.clone(),
        settings,
        TapPool::new(u64::from(capacity) * 4096, BOX),
        out,
        vmm.staging.join("boxes.json"),
    );
    let answering = Arc::new(AtomicBool::new(true));
    let accepting = Arc::new(AtomicBool::new(true));
    let (commands, from_host) = unbounded_channel();
    let session = Arc::new(AtomicU16::new(welcome.unwrap_or(ChannelProtocol::VERSION)));
    let answers = lifecycle.answers();
    let received = control_plane(
        store.clone(),
        answers.clone(),
        Arc::clone(&log),
        Arc::clone(&answering),
        channel(from_lifecycle, Arc::clone(&session), commands.clone()),
    );
    let received = acking(received, Arc::clone(&accepting), commands.clone());
    lifecycle.start(taken).expect("the lifecycle starts");
    let sessions = lifecycle.sessions();
    let holdings = lifecycle.holdings();
    let cancels = lifecycle.cancels();
    if let Some(protocol) = welcome {
        commands
            .send(Input::Welcomed(protocol))
            .expect("the lifecycle reads its inputs");
    }
    let runner = tokio::spawn(lifecycle.run(from_host, unbounded_channel().1));
    Host {
        commands,
        cancels,
        received,
        store,
        log,
        seq: 0,
        clone_seconds,
        turn_seconds: NonZeroU32::MIN,
        idle_seconds: NonZeroU32::new(900).expect("fifteen minutes"),
        images,
        vmm,
        capacity,
        settings,
        sessions,
        holdings,
        runner,
        answering,
        accepting,
        session,
        answers,
    }
}

fn image(image_digest: &str, clone_seconds: u32, protocol: &GuestHarnessProtocol) -> serde_json::Value {
    json!({
        "imageDigest": image_digest,
        "kernelUrl": "https://example.com/vmlinux",
        "kernelDigest": "b".repeat(64),
        "baseRootfsUrl": "https://example.com/rootfs.squashfs",
        "baseRootfsDigest": "c".repeat(64),
        "harness": {
            "harness": "Shell", "harnessVersion": "1", "protocol": protocol,
            "command": ["/bin/sh", "-s"], "workingDirectory": "/root",
            "environment": { "PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8" }, "fixedVariables": [], "cloneSeconds": clone_seconds
        },
        "components": []
    })
}

fn in_seconds(seconds: i64) -> chrono::DateTime<chrono::FixedOffset> {
    (chrono::Utc::now() + chrono::Duration::seconds(seconds)).fixed_offset()
}

impl Host {
    fn ask_handover(&self) -> tokio::sync::oneshot::Receiver<SupervisorHandover> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        self.answering.store(false, Ordering::SeqCst);
        self.commands.send(Input::HandOver(reply)).expect("send");
        answer
    }

    fn successor(self, handover: &SupervisorHandover) -> Host {
        self.successor_welcomed(handover, Some(ChannelProtocol::VERSION))
    }

    fn successor_welcomed(self, handover: &SupervisorHandover, welcome: Option<u16>) -> Host {
        let text = serde_json::to_string(handover).expect("the handover is JSON");
        let record = serde_json::from_str(&text).expect("the handover reads back as a SupervisorHandover");
        let Host {
            vmm,
            store,
            capacity,
            settings,
            clone_seconds,
            ..
        } = self;
        start_host(
            vmm,
            store,
            capacity,
            settings,
            clone_seconds,
            Some((record, Taken::Handed)),
            welcome,
        )
    }

    fn standing(&self) -> SupervisorHandover {
        let text = std::fs::read_to_string(self.vmm.staging.join("boxes.json")).expect("boxes.json is written");
        serde_json::from_str(&text).expect("boxes.json is a SupervisorHandover")
    }

    fn keep_guest(&self, handed: &mut SupervisorHandoverBox, fd: i32) {
        let memory_mib = NonZeroU32::new(4096).expect("4096");
        let size = Size {
            memory_mib,
            disk_mib: BOX.disk_mib,
        };
        let tap = TapPool::new(u64::from(self.capacity) * 4096, size)
            .claim_device(handed.tap_index, size)
            .expect("tap");
        let guest = self.vmm.machine(&handed.session_id, &tap).expect("machine").guest();
        self.vmm.hands.lock().expect("hands").insert(fd, (guest, memory_mib));
        handed.machine.guest_fd = fd;
    }

    fn after_crash(&self, standing: SupervisorHandover) -> Host {
        let vmm = MemoryVmm {
            log: Arc::default(),
            ..self.vmm.clone()
        };
        start_host(
            vmm,
            self.store.clone(),
            self.capacity,
            self.settings,
            self.clone_seconds,
            Some((standing, Taken::Crashed)),
            Some(ChannelProtocol::VERSION),
        )
    }

    async fn hand_over(self) -> Host {
        let handover = self.ask_handover().await.expect("the boxes are handed over");
        self.successor(&handover)
    }

    fn send(&self, command: Command) {
        self.commands.send(Input::Command(command)).expect("send");
    }

    fn welcome(&self, protocol: u16) {
        self.session.store(protocol, Ordering::SeqCst);
        self.commands.send(Input::Welcomed(protocol)).expect("send");
    }

    fn turn(&self, turn_id: &str, session_id: &str, prompt: &str, snapshots: Vec<ChannelSnapshotDownload>) {
        self.turn_expiring(turn_id, session_id, prompt, snapshots, 300);
    }

    fn turn_expiring(
        &self,
        turn_id: &str,
        session_id: &str,
        prompt: &str,
        snapshots: Vec<ChannelSnapshotDownload>,
        expires_in: i64,
    ) {
        let turn = self.box_turn(turn_id, session_id, prompt, snapshots, expires_in);
        self.send(Command::Turn(Box::new(turn)));
    }

    fn turn_speaking(&self, protocol: &GuestHarnessProtocol, turn_id: &str, session_id: &str, prompt: &str) {
        let mut turn = self.box_turn(turn_id, session_id, prompt, Vec::new(), 300);
        turn.image = BootOrder::Full(self.cached_image(protocol));
        self.send(Command::Turn(Box::new(turn)));
    }

    fn cached_image(&self, protocol: &GuestHarnessProtocol) -> ImageOrder {
        let image_digest = format!("{:0>64}", format!("{}{}", self.clone_seconds, protocol_digit(protocol)));
        std::fs::create_dir_all(&self.images).expect("the image cache is created");
        std::fs::write(
            self.images.join(format!("{image_digest}.json")),
            image(&image_digest, self.clone_seconds, protocol).to_string(),
        )
        .expect("the manifest is cached");
        ImageOrder {
            download: serde_json::from_value(json!({
                "kind": "Full",
                "imageDigest": image_digest,
                "kernelDigest": "b".repeat(64),
                "manifestDigest": "d".repeat(64),
                "imageUrl": "https://r2.example.test/image",
                "kernelUrl": "https://r2.example.test/kernel",
                "manifestUrl": "https://r2.example.test/manifest",
                "downloadExpiresAt": in_seconds(300).to_rfc3339(),
                "fetchSeconds": 5
            }))
            .expect("download"),
            harness: "Shell".to_string(),
            harness_version: "1".to_string(),
        }
    }

    fn uncached_image(&self) -> ImageOrder {
        let sha256 = |bytes: &[u8]| crate::snapshot::hex(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref());
        let rootfs = b"fetched rootfs".to_vec();
        let kernel = b"fetched kernel".to_vec();
        let image_digest = sha256(&rootfs);
        let mut manifest = image(&image_digest, self.clone_seconds, &GuestHarnessProtocol::ShellLines);
        manifest["kernelDigest"] = json!(sha256(&kernel));
        let manifest = manifest.to_string().into_bytes();
        let order = ImageOrder {
            download: serde_json::from_value(json!({
                "kind": "Full",
                "imageDigest": image_digest,
                "kernelDigest": sha256(&kernel),
                "manifestDigest": sha256(&manifest),
                "imageUrl": FETCHED_IMAGE_URL,
                "kernelUrl": "https://r2.example.test/fetched/kernel",
                "manifestUrl": "https://r2.example.test/fetched/manifest",
                "downloadExpiresAt": in_seconds(300).to_rfc3339(),
                "fetchSeconds": 5
            }))
            .expect("download"),
            harness: "Shell".to_string(),
            harness_version: "1".to_string(),
        };
        let mut store = self.store.0.lock().expect("store");
        store.insert(order.download.image_url.to_string(), rootfs);
        store.insert(order.download.kernel_url.to_string(), kernel);
        store.insert(order.download.manifest_url.to_string(), manifest);
        order
    }

    fn turn_fetching(&self, turn_id: &str, snapshots: Vec<ChannelSnapshotDownload>, expires_in: i64) -> ImageOrder {
        let mut turn = self.box_turn(turn_id, "session-1", "two", snapshots, expires_in);
        let order = self.uncached_image();
        turn.image = BootOrder::Full(order.clone());
        self.send(Command::Turn(Box::new(turn)));
        order
    }

    fn box_turn(
        &self,
        turn_id: &str,
        session_id: &str,
        prompt: &str,
        snapshots: Vec<ChannelSnapshotDownload>,
        expires_in: i64,
    ) -> BoxTurn {
        BoxTurn {
            turn_id: turn_id.try_into().expect("turn id"),
            session_id: session_id.try_into().expect("session id"),
            box_id: format!("box-{session_id}").try_into().expect("box id"),
            image: BootOrder::Full(self.cached_image(&GuestHarnessProtocol::ShellLines)),
            model: "default".try_into().expect("model"),
            prompt: prompt.to_string().try_into().expect("prompt"),
            credential: serde_json::from_value(json!({
                "credentialHandoverId": "credential-1",
                "expiresAt": in_seconds(expires_in).to_rfc3339(),
                "variables": [{ "name": "ANTHROPIC_API_KEY", "value": "in-memory" }],
                "connectors": []
            }))
            .expect("grant"),
            last_line_seq: Sequence(0),
            snapshots,
            persona_files: Vec::new(),
            repository: "https://github.com/acme/app.git".try_into().expect("repository"),
            git_author_name: "Tumpel Agent".try_into().expect("author name"),
            git_author_email: "agent@tumpel.invalid".try_into().expect("author email"),
            files_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/lifecycle-tests/persona-files"),
            prebuild_commit_sha: None,
            box_memory_mib: None,
            box_disk_mib: None,
            turn_seconds: self.turn_seconds,
            idle_seconds: self.idle_seconds,
            box_retention: BoxRetention::Auto,
        }
    }

    fn turn_in_box(&self, turn_id: &str, session_id: &str, box_id: &str, prompt: &str) {
        let mut turn = self.box_turn(turn_id, session_id, prompt, Vec::new(), 300);
        turn.box_id = box_id.try_into().expect("box id");
        self.send(Command::Turn(Box::new(turn)));
    }

    fn cancel(&self, turn_id: &str) {
        self.cancels.cancel(turn_id.try_into().expect("turn id"));
    }

    fn discard(&self, session_id: &str) {
        let order = TeardownOrder {
            session_id: session_id.try_into().expect("session id"),
            box_id: format!("box-{session_id}").try_into().expect("box id"),
        };
        self.send(Command::Discard(order));
    }

    fn teardown(&self, session_id: &str) {
        let order = TeardownOrder {
            session_id: session_id.try_into().expect("session id"),
            box_id: format!("box-{session_id}").try_into().expect("box id"),
        };
        self.send(Command::Teardown(order));
    }

    fn snapshot(&mut self, session_id: &str) -> ChannelSnapshotDownload {
        self.snapshot_expiring(session_id, 60)
    }

    fn snapshot_expiring(&mut self, session_id: &str, expires_in: i64) -> ChannelSnapshotDownload {
        self.seq += 1;
        let url = format!("https://store.example/{session_id}/{}", self.seq);
        let order = SnapshotOrder {
            snapshot_id: format!("snapshot-{}", self.seq).try_into().expect("snapshot id"),
            session_id: session_id.try_into().expect("session id"),
            box_id: format!("box-{session_id}").try_into().expect("box id"),
            snapshot_seq: Sequence(self.seq),
            upload_url: url.clone(),
            upload_expires_at: in_seconds(expires_in),
        };
        self.send(Command::Snapshot(order));
        serde_json::from_value(json!({
            "snapshotId": format!("snapshot-{}", self.seq),
            "snapshotSeq": self.seq,
            "storageKey": format!("tenant/{session_id}/{}", self.seq),
            "downloadUrl": url,
            "downloadExpiresAt": in_seconds(60).to_rfc3339(),
        }))
        .expect("download")
    }

    async fn settle(&mut self, wait: Duration) -> Vec<String> {
        tokio::time::sleep(wait).await;
        let mut seen = Vec::new();
        while let Ok(message) = self.received.try_recv() {
            seen.extend(describe(message));
        }
        seen
    }

    async fn settle_until(&mut self, last: &str) -> Vec<String> {
        let mut seen = Vec::new();
        while seen.last().map(String::as_str) != Some(last) {
            let message = tokio::time::timeout(settings().budgets.snapshot, self.received.recv())
                .await
                .unwrap_or_else(|_| panic!("no {last} within the snapshot budget, saw {seen:?}"))
                .expect("an open channel");
            seen.extend(describe(message));
        }
        seen
    }

    async fn settle_until_starts(&mut self, prefix: &str) -> Vec<String> {
        let mut seen = Vec::new();
        while !seen.last().is_some_and(|last: &String| last.starts_with(prefix)) {
            let message = tokio::time::timeout(Duration::from_secs(5), self.received.recv())
                .await
                .unwrap_or_else(|_| panic!("no {prefix} within five seconds, saw {seen:?}"))
                .expect("an open channel");
            seen.extend(describe(message));
        }
        seen
    }

    fn log(&self) -> Vec<String> {
        std::mem::take(&mut *self.log.lock().expect("log"))
    }
}

#[tokio::test]
async fn a_turn_for_a_paused_box_resumes_it() {
    let mut host = host("resume", 2, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one\ntwo", Vec::new());
    assert_eq!(
        host.settle(SETTLE).await,
        ["claim turn-1", "line 1 one", "line 2 two", "complete 2"]
    );
    host.turn("turn-2", "session-1", "three", Vec::new());
    assert_eq!(
        host.settle(SETTLE).await,
        ["claim turn-2", "line 1 three", "complete 1"]
    );
    assert_eq!(
        host.log(),
        [
            "boot session-1 on tumpel0",
            "reconnect session-1",
            "pause session-1",
            "resume session-1",
            "reconnect session-1",
            "pause session-1"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn the_claim_goes_out_before_the_boot_and_the_harness_starts_only_once_both_are_done() {
    let mut host = host("claim-before-boot", 1, Harness::Echo, Boot::Slow);
    note(&host.log, "offer late-1".to_string());
    host.turn("late-1", "session-1", "one", Vec::new());
    assert_eq!(
        host.settle(LATE_CLAIM + SETTLE).await,
        ["claim late-1", "line 1 one", "complete 1"]
    );
    assert_eq!(
        host.log(),
        [
            "offer late-1",
            "claim late-1",
            "boot session-1 on tumpel0",
            "claimed late-1",
            "start late-1",
            "reconnect session-1",
            "pause session-1"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_claim_the_control_plane_answers_ten_minutes_late_still_starts_the_harness() {
    let mut host = host("claim-stalled", 1, Harness::Echo, Boot::Works);
    host.turn("stalled-1", "session-1", "one", Vec::new());
    assert_eq!(
        host.settle(STALLED_CLAIM + SETTLE).await,
        ["claim stalled-1", "line 1 one", "complete 1"]
    );
}

#[tokio::test]
async fn a_turn_the_control_plane_withdraws_at_its_claim_never_reaches_the_harness() {
    let mut host = host("claim-withdrawn", 2, Harness::Echo, Boot::Works);
    host.turn("withdrawn-1", "session-1", "one", Vec::new());
    assert_eq!(host.settle(SETTLE).await, ["claim withdrawn-1"]);
    assert_eq!(host.log(), ["boot session-1 on tumpel0", "pause session-1"]);
    host.turn("turn-2", "session-1", "two", Vec::new());
    assert_eq!(host.settle(SETTLE).await, ["claim turn-2", "line 1 two", "complete 1"]);
}

#[tokio::test(start_paused = true)]
async fn a_warm_box_outlives_any_delay_once_a_snapshot_holds_it() {
    let mut host = host("outlives", 1, Harness::Echo, Boot::Works);
    let mut turn = host.box_turn("turn-1", "session-1", "one", Vec::new(), 300);
    turn.box_retention = BoxRetention::Warm;
    host.send(Command::Turn(Box::new(turn)));
    host.settle(SETTLE).await;
    host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    host.log();
    host.settle(Duration::from_hours(720)).await;
    assert_eq!(host.log(), Vec::<String>::new());
    host.turn("turn-2", "session-1", "two", Vec::new());
    assert_eq!(host.settle(SETTLE).await, ["claim turn-2", "line 1 two", "complete 1"]);
    assert_eq!(
        host.log(),
        ["resume session-1", "reconnect session-1", "pause session-1"]
    );
}

#[tokio::test]
async fn a_snapshot_whose_drive_reads_past_the_snapshot_budget_is_stored_before_its_upload_deadline() {
    let mut fast = settings();
    fast.budgets.snapshot = Duration::from_millis(50);
    let mut host = host_with("slow-cut", 1, Harness::Echo, Boot::Works, fast, 120);
    host.turn("slow-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.snapshot("session-1");
    assert_eq!(host.settle(SLOW_CUT * 2).await, ["snapshot done"]);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_over_a_base_names_each_reused_pack_once_and_counts_every_placement() {
    let mut host = host("reused-packs", 1, Harness::Echo, Boot::Works);
    let mut snapshots = Vec::new();
    for (turn_id, prompt) in [("turn-1", "one"), ("turn-2", "one")] {
        host.turn(turn_id, "session-1", prompt, Vec::new());
        host.settle(SETTLE).await;
        let order = host.snapshot("session-1");
        let mut parts = Vec::new();
        let done = loop {
            match tokio::time::timeout(settings().budgets.snapshot, host.received.recv())
                .await
                .expect("the snapshot ends within its budget")
                .expect("the lifecycle is running")
            {
                ChannelHostMessage::SnapshotPacks(packs) => parts.push(packs),
                ChannelHostMessage::SnapshotDone(done) => break done,
                _ => {}
            }
        };
        snapshots.push((order, parts, done));
    }
    let [(_, first_parts, first_done), (second, parts, done)] = <[_; 2]>::try_from(snapshots).expect("two snapshots");
    let packs_of = |parts: &[ChannelSnapshotPacks]| -> BTreeSet<String> {
        parts
            .iter()
            .flat_map(|part| part.packs.iter().map(|pack| pack.pack_digest.as_str().to_string()))
            .collect()
    };

    assert_eq!(first_parts[0].part.get(), 1);
    assert!(first_parts.iter().all(|part| part.reused_pack_digests.is_empty()));
    assert_eq!(first_done.chunk_count, 1);
    assert_eq!(done.chunk_count, 2);
    let first_packs = packs_of(&first_parts);
    let written = packs_of(&parts);
    let reused: Vec<String> = parts
        .iter()
        .flat_map(|part| {
            part.reused_pack_digests
                .iter()
                .map(|digest| digest.as_str().to_string())
        })
        .collect();
    let once: BTreeSet<String> = reused.iter().cloned().collect();
    assert_eq!(once.len(), reused.len(), "{reused:?}");
    assert!(!once.is_empty());
    assert!(once.is_subset(&first_packs), "{once:?} {first_packs:?}");
    assert!(once.is_disjoint(&written), "{once:?} {written:?}");
    let root = decode_root(&host.store.get(second.download_url.as_str()).expect("the root")).expect("the root json");
    let mut bytes = Vec::new();
    for place in &root {
        let pack = host.store.get(&pack_url(&place.pack)).expect("a pack");
        let from = usize::try_from(place.offset).expect("an offset");
        bytes.extend_from_slice(&pack[from..from + place.length as usize]);
    }
    let manifest = decode(&bytes).expect("the manifest");
    let referenced: BTreeSet<String> = manifest
        .values()
        .flat_map(|stored| stored.chunks.iter())
        .chain(&root)
        .map(|place| place.pack.clone())
        .collect();
    assert_eq!(referenced, written.union(&once).cloned().collect::<BTreeSet<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_over_a_base_at_38_names_no_reused_packs_and_sends_only_parts_with_chunks() {
    let mut host = host("reused-packs-38", 1, Harness::Echo, Boot::Works);
    host.welcome(38);
    let mut snapshots = Vec::new();
    for turn_id in ["turn-1", "turn-2"] {
        host.turn(turn_id, "session-1", "one", Vec::new());
        host.settle(SETTLE).await;
        host.snapshot("session-1");
        let mut parts = Vec::new();
        loop {
            match tokio::time::timeout(settings().budgets.snapshot, host.received.recv())
                .await
                .expect("the snapshot ends within its budget")
                .expect("the lifecycle is running")
            {
                ChannelHostMessage::SnapshotPacks(packs) => parts.push(packs),
                ChannelHostMessage::SnapshotDone(_) => break,
                _ => {}
            }
        }
        snapshots.push(parts);
    }

    for parts in &snapshots {
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part.get(), 1);
        assert_ne!(parts[0].chunks.len(), 0);
        assert_eq!(parts[0].reused_pack_digests.len(), 0);
    }
}

fn pack_uploads(uploads: &[String]) -> usize {
    uploads.iter().filter(|url| url.contains("/packs/")).count()
}

#[tokio::test]
async fn a_torn_down_box_restores_its_next_turn_from_the_latest_snapshot() {
    let mut host = host("restore-latest", 2, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    assert_eq!(pack_uploads(&host.store.uploads()), 2);
    host.turn("turn-2", "session-1", "two", Vec::new());
    host.settle(SETTLE).await;
    host.log();
    let second = host.snapshot("session-1");
    host.teardown("session-1");
    assert_eq!(
        host.settle_until("box gone session-1").await,
        ["snapshot done", "box gone session-1"]
    );
    assert_eq!(host.log(), ["stop session-1"]);
    let uploads = host.store.uploads();
    assert_eq!(pack_uploads(&uploads), 2, "{uploads:?}");
    assert_eq!(uploads.last().map(String::as_str), Some(second.download_url.as_str()));

    host.turn("turn-3", "session-1", "three", vec![second]);
    assert_eq!(
        host.settle(SETTLE).await,
        ["claim turn-3", "line 1 three", "complete 1"]
    );
    assert_eq!(
        host.log(),
        [
            "restore session-1 root/turn-1,root/turn-2",
            "boot session-1 on tumpel0",
            "reconnect session-1",
            "pause session-1"
        ]
    );
}

#[tokio::test]
async fn a_snapshot_uploads_no_chunk_the_store_holds_already() {
    let mut host = host("dedup", 2, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "same bytes", Vec::new());
    host.turn("turn-2", "session-2", "same bytes", Vec::new());
    host.settle(SETTLE).await;
    host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    assert_eq!(pack_uploads(&host.store.uploads()), 2);

    let second = host.snapshot("session-2");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    let uploads = host.store.uploads();
    assert_eq!(uploads.last().map(String::as_str), Some(second.download_url.as_str()));
    let packs: Vec<Vec<u8>> = uploads
        .iter()
        .filter(|url| url.contains("/packs/"))
        .map(|url| host.store.get(url).expect("pack"))
        .collect();
    assert_eq!(packs.len(), 1, "{uploads:?}");
    assert!(!packs[0].windows(10).any(|bytes| bytes == b"same bytes"));
}

#[tokio::test]
async fn a_box_on_another_host_downloads_the_chunks_its_cache_lacks() {
    let mut first = host("first-host", 1, Harness::Echo, Boot::Works);
    first.turn("turn-1", "session-1", "one", Vec::new());
    first.settle(SETTLE).await;
    let snapshot = first.snapshot("session-1");
    assert_eq!(first.settle_until("snapshot done").await, ["snapshot done"]);

    let mut second = host_on(
        "second-host",
        first.store.clone(),
        1,
        Harness::Echo,
        Boot::Works,
        settings(),
        120,
    );
    second.turn("turn-2", "session-1", "two", vec![snapshot]);
    assert_eq!(
        second.settle(SETTLE).await,
        ["claim turn-2", "line 1 two", "complete 1"]
    );
    assert_eq!(second.log()[0], "restore session-1 root/turn-1");
    let left: Vec<_> = std::fs::read_dir(second.vmm.staging.join("session-1"))
        .expect("the box disk directory")
        .map(|entry| entry.expect("an entry").file_name())
        .collect();
    assert!(
        left.is_empty(),
        "the restore archive and its chunks outlived the boot: {left:?}"
    );
}

#[tokio::test]
async fn a_snapshot_after_a_restore_on_another_host_uploads_no_chunk() {
    let mut first = host("restored-first", 1, Harness::Echo, Boot::Works);
    first.turn("turn-1", "session-1", "one", Vec::new());
    first.settle(SETTLE).await;
    let snapshot = first.snapshot("session-1");
    assert_eq!(first.settle_until("snapshot done").await, ["snapshot done"]);
    first.store.uploads();

    let mut second = host_on(
        "restored-second",
        first.store.clone(),
        1,
        Harness::Reader,
        Boot::Works,
        settings(),
        120,
    );
    second.seq = first.seq;
    second.turn("turn-2", "session-1", "two", vec![snapshot]);
    assert_eq!(
        second.settle(SETTLE).await,
        ["claim turn-2", "line 1 two", "complete 1"]
    );
    let again = second.snapshot("session-1");
    assert_eq!(second.settle_until("snapshot done").await, ["snapshot done"]);
    assert_eq!(second.store.uploads(), [again.download_url.to_string()]);
}

#[tokio::test]
async fn a_restore_missing_a_chunk_fails_the_turn_snapshot_unreadable() {
    let mut first = host("missing-chunk", 1, Harness::Echo, Boot::Works);
    first.turn("turn-1", "session-1", "one", Vec::new());
    first.settle(SETTLE).await;
    let snapshot = first.snapshot("session-1");
    first.settle(SETTLE).await;
    first
        .store
        .0
        .lock()
        .expect("store")
        .retain(|url, _| !url.contains("/packs/"));

    let mut second = host_on(
        "missing-chunk-2",
        first.store.clone(),
        1,
        Harness::Echo,
        Boot::Works,
        settings(),
        120,
    );
    second.turn("turn-2", "session-1", "two", vec![snapshot]);
    let seen = second.settle(SETTLE).await;
    assert!(
        seen.len() == 2
            && seen[0] == "claim turn-2"
            && seen[1].starts_with(&format!("failed {}", TurnFailureReason::SnapshotUnreadable))
            && seen[1].contains("snapshot-1 (seq 1) of session session-1"),
        "{seen:?}"
    );
    assert!(!second.vmm.staging.join("session-1").exists());
}

#[tokio::test(start_paused = true)]
async fn a_refused_claim_stops_a_cold_turn_while_its_restore_pack_is_held() {
    let mut first = host("refused-claim-source", 1, Harness::Echo, Boot::Works);
    first.turn("turn-1", "session-1", "one", Vec::new());
    first.settle_until("complete 1").await;
    let snapshot = first.snapshot("session-1");
    assert_eq!(first.settle_until("snapshot done").await, ["snapshot done"]);

    let hold = Arc::new((Notify::new(), Notify::new()));
    let mut store = first.store.clone();
    store.5 = Some(Arc::clone(&hold));
    let mut second = host_on(
        "refused-claim-restore",
        store,
        1,
        Harness::Echo,
        Boot::Works,
        settings(),
        120,
    );
    second.turn("refused-claim", "session-1", "two", vec![snapshot]);
    let claim = second.received.recv().await.expect("claim");
    assert!(matches!(&claim, ChannelHostMessage::TurnClaim(claim) if claim.turn_id.as_str() == "refused-claim"));
    hold.0.notified().await;

    let refused_at = tokio::time::Instant::now();
    second
        .answers
        .deliver(Answer::ClaimRefused("refused-claim".try_into().expect("turn id")));
    let handover = tokio::time::timeout(SETTLE, second.ask_handover())
        .await
        .expect("handover before the pack is released")
        .expect("handover settles");
    assert_eq!(tokio::time::Instant::now(), refused_at);
    assert_eq!(handover.boxes, []);
    hold.1.notify_one();
    assert!(second.log().is_empty(), "the cancelled restore never starts a VM");
    assert!(!second.vmm.staging.join("session-1").exists());
    assert!(
        second.received.try_recv().is_err(),
        "the channel owns the terminal failure"
    );
}

#[tokio::test(start_paused = true)]
async fn a_refused_warm_claim_pauses_the_box_and_keeps_it_for_another_turn() {
    let mut host = host("refused-warm-claim", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle_until("complete 1").await;
    host.log();

    host.turn("refused-claim", "session-1", "two", Vec::new());
    let turn_id: EntityId = "refused-claim".try_into().expect("turn id");
    assert_eq!(host.settle_until("claim refused-claim").await, ["claim refused-claim"]);
    host.cancels.cancel(turn_id.clone());
    host.answers.deliver(Answer::ClaimRefused(turn_id.clone()));
    assert_eq!(host.settle(SETTLE).await, Vec::<String>::new());
    assert!(!host.cancels.contains(&turn_id));
    assert_eq!(host.log(), ["resume session-1", "pause session-1"]);

    host.turn("turn-3", "session-1", "three", Vec::new());
    assert_eq!(
        host.settle_until("complete 1").await,
        ["claim turn-3", "line 1 three", "complete 1"]
    );
    assert_eq!(
        host.log(),
        ["resume session-1", "reconnect session-1", "pause session-1"]
    );
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
    assert_eq!(host.settle_until("claim refused-claim").await, ["claim refused-claim"]);
    tokio::time::timeout(SETTLE, hold.0.notified())
        .await
        .expect("the download starts");
    assert!(staged.is_dir());
    host.cancels.cancel(turn_id.clone());

    let refused_at = tokio::time::Instant::now();
    host.answers.deliver(Answer::ClaimRefused(turn_id.clone()));
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
}

#[tokio::test(start_paused = true)]
async fn a_restore_retries_a_pack_download_that_fails_once_and_runs_the_turn() {
    let (mut host, snapshot) = restoring("pack-retry", settings()).await;
    host.store.4.store(1, Ordering::SeqCst);
    host.turn("turn-2", "session-1", "two", vec![snapshot]);
    assert_eq!(
        host.settle_until("complete 1").await,
        ["claim turn-2", "line 1 two", "complete 1"]
    );
    assert_eq!(host.store.4.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn a_restore_stops_retrying_a_pack_after_ten_attempts_and_fails_snapshot_unreadable() {
    let (mut host, snapshot) = restoring("pack-retry-cap", settings()).await;
    host.store.4.store(2 * DOWNLOAD_ATTEMPTS, Ordering::SeqCst);
    host.turn("turn-2", "session-1", "two", vec![snapshot]);
    let seen = host.settle(Duration::from_secs(55)).await;
    assert!(
        seen.len() == 2
            && seen[0] == "claim turn-2"
            && seen[1].starts_with(&format!("failed {}", TurnFailureReason::SnapshotUnreadable))
            && seen[1].contains("error sending request"),
        "{seen:?}"
    );
    assert_eq!(host.store.4.load(Ordering::SeqCst), DOWNLOAD_ATTEMPTS);
}

#[tokio::test(start_paused = true)]
async fn a_full_pool_refuses_a_box_and_tears_down_no_other_box() {
    let mut host = host("pool", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    host.log();
    host.turn("turn-2", "session-2", "two", Vec::new());
    let refused = host.settle(SETTLE).await;
    assert!(
        refused.len() == 1
            && refused[0].starts_with(&format!("failed {}", TurnFailureReason::PoolExhausted))
            && refused[0].contains(
                "this host's 1 boxes hold 4096 MiB of the 4096 MiB it gives boxes, so a box of 4096 MiB does not fit"
            ),
        "{refused:?}"
    );
    assert_eq!(host.log(), Vec::<String>::new(), "the clean paused box stays");
    host.teardown("session-1");
    assert_eq!(host.settle(SETTLE).await, ["box gone session-1"]);
    host.log();
    host.turn("turn-3", "session-2", "three", Vec::new());
    assert_eq!(
        host.settle(SETTLE).await,
        ["claim turn-3", "line 1 three", "complete 1"]
    );
    assert_eq!(
        host.log(),
        ["boot session-2 on tumpel0", "reconnect session-2", "pause session-2"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_box_handed_to_a_new_supervisor_is_charged_the_memory_it_booted_with() {
    let mut old = host("handover-memory", 2, Harness::Echo, Boot::Works);
    let mut large = old.box_turn("turn-1", "session-1", "one", Vec::new(), 300);
    large.box_memory_mib = Some(8192);
    old.send(Command::Turn(Box::new(large)));
    old.settle_until("complete 1").await;
    let mut new = old.hand_over().await;
    new.log();
    new.turn("turn-2", "session-2", "two", Vec::new());
    let refused = new.settle(SETTLE).await;
    assert!(
        refused.len() == 1
            && refused[0].contains(
                "this host's 1 boxes hold 8192 MiB of the 8192 MiB it gives boxes, so a box of 4096 MiB does not fit"
            ),
        "{refused:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_turn_mid_flight_is_handed_to_the_new_supervisor_and_completes_there() {
    let mut old = host("handover-turn", 2, Harness::SlowClone, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    assert_eq!(old.settle_until("claim turn-1").await, ["claim turn-1"]);
    tokio::time::sleep(SLOW_CLONE / 2).await;
    old.log();
    let answer = old.ask_handover();
    old.turn("turn-2", "session-2", "two", Vec::new());
    let handover = answer.await.expect("the boxes are handed over");
    let refused = format!(
        "failed {}: this host is handing its boxes to a new supervisor binary and took no turn turn-2; the control plane offers it to another host",
        TurnFailureReason::HostUpdating
    );
    assert_eq!(old.settle_until(&refused).await, [refused]);
    let mut new = old.successor(&handover);
    assert_eq!(new.settle_until("complete 1").await, ["line 1 one", "complete 1"]);
    assert_eq!(
        new.log(),
        [
            "hand over session-1",
            "reattach session-1 on tumpel0",
            "keep boxes [0] and stop the rest",
            "reconnect session-1",
            "pause session-1"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_turn_waiting_for_its_claim_at_a_handover_is_given_back_and_its_warm_box_finishes_it_on_the_new_supervisor() {
    let mut old = host("handover-unclaimed", 1, Harness::Echo, Boot::Works);
    old.turn("late-1", "session-1", "one", Vec::new());
    assert_eq!(old.settle_until("claim late-1").await, ["claim late-1"]);
    let handover = old.ask_handover().await.expect("the boxes are handed over");
    let given_back = format!(
        "failed {}: this host started handing its boxes to a new supervisor binary before the control plane answered the claim of turn late-1, so its harness never started and its box stays warm; the control plane offers the turn again",
        TurnFailureReason::HostUpdating
    );
    assert_eq!(old.settle(LATE_CLAIM + SETTLE).await, [given_back]);
    assert_eq!(handover.boxes.len(), 1);
    let old_log = old.log();
    assert!(
        old_log.contains(&"hand over session-1".to_string()) && !old_log.contains(&"claimed late-1".to_string()),
        "{old_log:?}"
    );
    let mut new = old.successor(&handover);
    new.turn("late-1", "session-1", "one", Vec::new());
    assert_eq!(
        new.settle_until("complete 1").await,
        ["claim late-1", "line 1 one", "complete 1"]
    );
    let new_log = new.log();
    assert!(
        new_log.contains(&"resume session-1".to_string()) && !new_log.iter().any(|event| event.starts_with("boot")),
        "the handed warm box takes the turn without a boot: {new_log:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_supervisor_started_after_a_crash_keeps_every_box_and_fails_only_the_cut_turn() {
    let mut old = host("crash-keeps-boxes", 2, Harness::SlowClone, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("complete 1").await;
    old.turn("turn-2", "session-2", "two", Vec::new());
    old.settle_until("claim turn-2").await;
    tokio::time::sleep(SLOW_CLONE / 2).await;
    let mut standing = old.standing();
    assert_eq!(standing.boxes.len(), 2, "both running boxes are on record");
    old.keep_guest(&mut standing.boxes[0], 40);
    old.keep_guest(&mut standing.boxes[1], 41);
    let mut new = old.after_crash(standing);
    let cut = format!(
        "failed {}: the supervisor stopped unexpectedly during turn turn-2, so the turn ended; its box is kept, so send the turn again",
        TurnFailureReason::HarnessExited
    );
    assert_eq!(new.settle_until(&cut).await, [cut.as_str()]);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(
        new.log(),
        [
            "reattach session-1 on tumpel0",
            "reattach session-2 on tumpel1",
            "keep boxes [0, 1] and stop the rest",
            "reconnect session-2",
            "pause session-2"
        ]
    );
    new.turn("turn-3", "session-2", "three", Vec::new());
    assert_eq!(
        new.settle_until("complete 1").await,
        ["claim turn-3", "line 1 three", "complete 1"]
    );
    assert_eq!(
        new.log(),
        ["resume session-2", "reconnect session-2", "pause session-2"],
        "the kept box takes the next turn without a boot"
    );
}

#[tokio::test(start_paused = true)]
async fn a_host_with_no_record_of_boxes_starts_without_the_systemd_user_manager_and_the_next_start_sweeps() {
    let first = host("start-without-record", 1, Harness::SlowClone, Boot::Works);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(
        first.log(),
        Vec::<String>::new(),
        "a host with nothing on record lists no box scopes"
    );
    let standing = first.standing();
    assert_eq!(standing.boxes.len(), 0);
    let next = first.after_crash(standing);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(next.log(), ["keep boxes [] and stop the rest"]);
}

#[tokio::test(start_paused = true)]
async fn a_box_on_record_that_cannot_be_reattached_after_a_crash_is_stopped_with_the_rest() {
    let mut old = host("crash-stops-lost-box", 2, Harness::SlowClone, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("complete 1").await;
    old.turn("turn-2", "session-2", "two", Vec::new());
    old.settle_until("complete 1").await;
    let mut standing = old.standing();
    old.keep_guest(&mut standing.boxes[0], 40);
    standing.boxes[1].machine.guest_fd = 41;
    let new = old.after_crash(standing);
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(
        new.log(),
        [
            "reattach session-1 on tumpel0",
            "discard 99",
            "keep boxes [0] and stop the rest"
        ],
        "the box of session-2 on tumpel1 is not kept, so its scope is stopped"
    );
}

#[tokio::test(start_paused = true)]
async fn a_box_that_cannot_be_handed_over_answers_its_queued_teardown_with_box_gone() {
    let mut old = host("handover-refused", 1, Harness::SlowClone, Boot::RefusesHandover);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("claim turn-1").await;
    tokio::time::sleep(SLOW_CLONE / 2).await;
    old.teardown("session-1");
    let handover = old.ask_handover().await.expect("the handover answers");
    assert_eq!(handover.boxes.len(), 0);
    let failed = format!(
        "failed {}: box box-session-1 of session session-1 could not be handed to the new supervisor binary and was stopped: its Firecracker process refused the handover; send the turn again",
        TurnFailureReason::HarnessExited
    );
    assert_eq!(
        old.settle_until("box gone session-1").await,
        [failed.as_str(), "box gone session-1"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_handed_box_whose_process_ended_fails_its_turn_and_frees_its_tap() {
    let mut old = host("handover-ended", 1, Harness::SlowClone, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("claim turn-1").await;
    tokio::time::sleep(SLOW_CLONE / 2).await;
    let handover = old.ask_handover().await.expect("the boxes are handed over");
    old.vmm.hands.lock().expect("hands").clear();
    let mut new = old.successor(&handover);
    let failed = format!(
        "failed {}: box box-session-1 of session session-1 was lost in the handover to this supervisor binary: its Firecracker process 103 ended during the handover; send the turn again",
        TurnFailureReason::HarnessExited
    );
    assert_eq!(new.settle_until(&failed).await, [failed]);
    new.log();
    new.turn("turn-2", "session-2", "two", Vec::new());
    new.settle_until("complete 1").await;
    assert_eq!(
        new.log()[0],
        "boot session-2 on tumpel0",
        "the only TAP device of the host is free again"
    );
}

#[tokio::test(start_paused = true)]
async fn a_warm_box_handed_over_takes_the_next_turn_without_a_boot() {
    let mut old = host("handover-warm", 1, Harness::Echo, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("complete 1").await;
    old.log();
    let mut new = old.hand_over().await;
    new.turn("turn-2", "session-1", "two", Vec::new());
    assert_eq!(
        new.settle_until("complete 1").await,
        ["claim turn-2", "line 1 two", "complete 1"]
    );
    assert_eq!(
        new.log(),
        [
            "hand over session-1",
            "reattach session-1 on tumpel0",
            "keep boxes [0] and stop the rest",
            "resume session-1",
            "reconnect session-1",
            "pause session-1"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_warm_box_handed_over_with_a_queued_teardown_is_stopped_and_answers_box_gone() {
    let mut old = host("handover-queued-teardown", 1, Harness::Echo, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("complete 1").await;
    old.snapshot("session-1");
    old.settle_until("snapshot done").await;
    old.settle(SETTLE).await;
    assert!(
        !old.standing().boxes[0].dirty,
        "a snapshot that holds the turn records the box clean"
    );
    old.log();
    let mut handover = old.ask_handover().await.expect("the boxes are handed over");
    handover.boxes[0]
        .queued
        .push(SupervisorHandoverCommand::Teardown(SupervisorHandoverTeardown {
            kind: SupervisorHandoverTeardownKind::Teardown,
        }));
    let mut new = old.successor(&handover);
    assert_eq!(new.settle_until("box gone session-1").await, ["box gone session-1"]);
    assert_eq!(
        new.log(),
        [
            "hand over session-1",
            "reattach session-1 on tumpel0",
            "keep boxes [0] and stop the rest",
            "stop session-1"
        ]
    );
    assert_eq!(new.standing().boxes.len(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_handed_over_in_the_queue_waits_for_the_control_plane_welcome() {
    let mut old = host("handover-queued-snapshot", 1, Harness::Echo, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("complete 1").await;
    let mut handover = old.ask_handover().await.expect("the boxes are handed over");
    handover.boxes[0]
        .queued
        .push(SupervisorHandoverCommand::Snapshot(SupervisorHandoverSnapshot {
            kind: SupervisorHandoverSnapshotKind::Snapshot,
            snapshot_id: "snapshot-9".try_into().expect("snapshot id"),
            snapshot_seq: Sequence(9),
            upload_expires_at: in_seconds(600),
            upload_url: "https://store.example/session-1/9".try_into().expect("url"),
        }));
    let mut new = old.successor_welcomed(&handover, None);
    assert_eq!(new.settle(SETTLE).await, Vec::<String>::new());
    new.welcome(ChannelProtocol::VERSION);
    assert_eq!(new.settle_until("snapshot done").await, ["snapshot done"]);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_built_at_38_in_a_session_at_39_ends_its_task_at_once_with_one_snapshot_failed() {
    let mut host = host("snapshot-built-at-38", 1, Harness::Echo, Boot::Works);
    host.welcome(38);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle_until("complete 1").await;
    let started = tokio::time::Instant::now();
    host.snapshot("session-1");
    host.welcome(ChannelProtocol::VERSION);
    let failed = host.settle_until_starts("snapshot failed").await;
    host.turn("turn-2", "session-1", "two", Vec::new());
    let seen = host.settle_until_starts("complete").await;
    assert!(started.elapsed() < Duration::from_secs(60), "{:?}", started.elapsed());
    let later = host.settle(Duration::from_secs(120)).await;
    let failures = failed
        .iter()
        .chain(&seen)
        .chain(&later)
        .filter(|line| line.starts_with("snapshot failed"))
        .count();
    assert_eq!(failures, 1, "{failed:?} {seen:?} {later:?}");
    assert!(host.standing().boxes[0].dirty, "a failed snapshot leaves the box dirty");
}

#[tokio::test]
async fn a_handed_box_is_held_before_the_new_supervisor_takes_any_command() {
    let mut old = host("handover-held", 1, Harness::Echo, Boot::Works);
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle_until("complete 1").await;
    assert_eq!(*old.sessions.borrow(), [EntityId::try_from("session-1").expect("id")]);
    let new = old.hand_over().await;
    assert_eq!(*new.sessions.borrow(), [EntityId::try_from("session-1").expect("id")]);
}

#[tokio::test]
async fn a_snapshot_with_no_box_of_the_session_fails_box_not_found() {
    let mut host = host("missing", 1, Harness::Echo, Boot::Works);
    host.snapshot("session-1");
    let seen = host.settle(SETTLE).await;
    assert!(
        seen[0].starts_with(&format!(
            "snapshot failed {}",
            ChannelSnapshotFailureReason::BoxNotFound
        )),
        "{seen:?}"
    );
}

#[tokio::test]
async fn an_upload_past_its_url_expiry_fails_upload_expired_and_keeps_the_box_dirty() {
    let mut host = host("upload", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.snapshot_expiring("session-1", -1);
    let seen = host.settle(SETTLE).await;
    assert!(
        seen[0].starts_with(&format!(
            "snapshot failed {}",
            ChannelSnapshotFailureReason::UploadExpired
        )),
        "{seen:?}"
    );
    host.turn("turn-2", "session-2", "two", Vec::new());
    let refused = host.settle(SETTLE).await;
    assert!(
        refused[0].starts_with(&format!("failed {}", TurnFailureReason::PoolExhausted)),
        "{refused:?}"
    );
}

#[tokio::test]
async fn a_guest_on_another_protocol_fails_the_boot_naming_both_versions_and_the_fix() {
    let oldest = tumpel_api::types::GuestProtocol::OLDEST;
    let supported = tumpel_api::types::GuestProtocol::VERSION;
    let cases = [
        (
            Boot::OldImage,
            format!("speaks guest protocol 1, this supervisor speaks {oldest} to {supported}; the image is older than the supervisor, push an images-vX.Y.Z tag to publish images built for it"),
        ),
        (
            Boot::NewImage,
            format!("speaks guest protocol {}, this supervisor speaks {oldest} to {supported}; the image is newer than the supervisor, update the supervisor", supported + 1),
        ),
    ];
    for (index, (boot, expected)) in cases.into_iter().enumerate() {
        let mut host = host(&format!("image-{index}"), 1, Harness::Echo, boot);
        host.turn("turn-1", "session-1", "one", Vec::new());
        let seen = host.settle(Duration::from_millis(600)).await;
        let last = seen.last().expect("a failure");
        assert!(last.starts_with("failed BoxBootFailed"), "{seen:?}");
        assert!(last.contains(&expected), "{last}");
    }
}

#[tokio::test]
async fn a_turn_that_runs_out_its_budget_names_its_failure_and_keeps_its_box_until_a_snapshot_holds_it() {
    let cases = [
        (
            Harness::Silent,
            TurnFailureReason::NoFirstOutput,
            "firstLineSeconds",
            "The box's console printed nothing during the turn.",
        ),
        (
            Harness::StallsAfterFirstLine,
            TurnFailureReason::TurnTimedOut,
            "turnSeconds",
            "The box's console printed nothing during the turn.",
        ),
    ];
    for (index, (harness, reason, budget, console)) in cases.into_iter().enumerate() {
        let mut host = host(&format!("budget-{index}"), 1, harness, Boot::Works);
        host.turn("turn-1", "session-1", "one\ntwo", Vec::new());
        let seen = host.settle_until_starts("failed ").await;
        let last = seen.last().expect("a failure");
        assert!(last.starts_with(&format!("failed {reason}")), "{seen:?}");
        assert!(last.contains(budget) && last.ends_with(console), "{last}");
        assert_eq!(
            host.log(),
            ["boot session-1 on tumpel0", "reconnect session-1", "pause session-1"]
        );
        host.teardown("session-1");
        let seen = host.settle(SETTLE).await;
        assert!(
            seen.len() == 1
                && seen[0].starts_with(&format!(
                    "teardown failed {}",
                    ChannelBoxTeardownFailureReason::BoxDirty
                )),
            "{seen:?}"
        );
        host.snapshot("session-1");
        assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    }
}

#[tokio::test]
async fn a_boot_that_runs_out_its_budget_fails_box_boot_failed_and_returns_the_tap() {
    let mut host = host("budget-boot", 1, Harness::Echo, Boot::Hangs);
    host.turn("turn-1", "session-1", "one", Vec::new());
    let seen = host.settle(Duration::from_millis(600)).await;
    let last = seen.last().expect("a failure");
    assert!(
        last.starts_with(&format!("failed {}", TurnFailureReason::BoxBootFailed)),
        "{seen:?}"
    );
    assert!(
        last.contains("; send the turn again")
            && !last.contains("runtime.json")
            && last.ends_with("Console: [ boot console ]"),
        "{last}"
    );
    host.log();
    host.turn_expiring("turn-2", "session-2", "three", Vec::new(), 300);
    tokio::time::sleep(SETTLE).await;
    assert_eq!(host.log()[0], "boot session-2 on tumpel0");
}

#[tokio::test]
async fn a_clone_that_hangs_fails_no_first_output_after_clone_seconds_and_the_first_line_budget() {
    let mut host = host_with("hangs-cloning", 1, Harness::HangsCloning, Boot::Works, settings(), 1);
    host.turn_seconds = NonZeroU32::new(5).expect("five seconds");
    host.turn("turn-1", "session-1", "one", Vec::new());
    let seen = host.settle(Duration::from_millis(1500)).await;
    let last = seen.last().expect("a failure");
    assert!(
        last.starts_with(&format!("failed {}", TurnFailureReason::NoFirstOutput)),
        "{seen:?}"
    );
    assert!(
        last.contains("cloneSeconds 1") && last.contains("firstLineSeconds"),
        "{last}"
    );
}

#[tokio::test]
async fn a_refused_clone_reaches_the_channel_with_its_cause_after_the_claim() {
    let mut host = host("refuses-clone", 1, Harness::RefusesClone, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    assert_eq!(
        host.settle(SETTLE).await,
        ["claim turn-1", "clone failed Refused: git reported: denied"]
    );
}

#[tokio::test]
async fn the_first_line_budget_starts_when_the_repository_is_ready() {
    let mut host = host("slow-clone", 1, Harness::SlowClone, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    assert_eq!(
        host.settle(SLOW_CLONE + SETTLE).await,
        ["claim turn-1", "line 1 one", "complete 1"]
    );
}

#[tokio::test]
async fn a_line_after_the_error_result_keeps_the_turn_failed() {
    let mut host = host("error-result", 1, Harness::Echo, Boot::Works);
    let prompt = "{\"type\":\"result\",\"is_error\":true,\"result\":\"API Error: 500\"}\n{\"type\":\"system\"}";
    host.turn_speaking(&GuestHarnessProtocol::ClaudeStreamJson, "turn-1", "session-1", prompt);
    let seen = host.settle(SETTLE).await;
    let last = seen.last().expect("a failure");
    assert!(
        last.starts_with(&format!("failed {}", TurnFailureReason::HarnessReportedError)),
        "{seen:?}"
    );
    assert!(last.contains("API Error: 500"), "{last}");
}

#[tokio::test]
async fn an_expired_grant_is_refused_before_any_box_boots() {
    let mut host = host("grant", 1, Harness::Echo, Boot::Works);
    host.turn_expiring("turn-1", "session-1", "one", Vec::new(), -1);
    let seen = host.settle(SETTLE).await;
    assert!(
        seen.len() == 1 && seen[0].starts_with(&format!("failed {}", TurnFailureReason::DispatchCredentialExpired)),
        "{seen:?}"
    );
    assert_eq!(host.log(), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn a_snapshotted_box_idle_for_the_offered_idle_time_is_stopped_and_answers_box_gone() {
    let mut host = host("idle-teardown", 1, Harness::Echo, Boot::Works);
    host.idle_seconds = NonZeroU32::new(300).expect("five minutes");
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    let first = host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    host.log();
    assert_eq!(host.settle(Duration::from_mins(4)).await, Vec::<String>::new());
    assert_eq!(host.settle(Duration::from_mins(2)).await, ["box gone session-1"]);
    assert_eq!(host.log(), ["stop session-1"]);
    assert!(!host.vmm.staging.join("session-1").exists());

    host.turn("turn-2", "session-1", "two", vec![first]);
    assert_eq!(host.settle(SETTLE).await, ["claim turn-2", "line 1 two", "complete 1"]);
    assert_eq!(host.log()[0], "restore session-1 root/turn-1");
}

#[tokio::test(start_paused = true)]
async fn a_box_whose_snapshot_done_is_never_accepted_stays_dirty_past_its_idle_time() {
    let mut host = host("idle-refused", 1, Harness::Echo, Boot::Works);
    host.idle_seconds = NonZeroU32::new(300).expect("five minutes");
    host.accepting.store(false, Ordering::SeqCst);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    host.log();
    assert_eq!(host.settle(Duration::from_mins(20)).await, Vec::<String>::new());
    assert_eq!(host.log(), Vec::<String>::new());
    assert!(host.standing().boxes[0].dirty);
}

#[tokio::test(start_paused = true)]
async fn a_box_is_clean_once_its_snapshot_done_is_accepted_and_then_ends_on_idle() {
    let mut host = host("idle-accepted", 1, Harness::Echo, Boot::Works);
    host.idle_seconds = NonZeroU32::new(300).expect("five minutes");
    host.accepting.store(false, Ordering::SeqCst);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    let first = host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    assert_eq!(host.settle(Duration::from_mins(2)).await, Vec::<String>::new());
    assert!(host.standing().boxes[0].dirty);
    host.commands
        .send(Input::SnapshotAccepted(first.snapshot_id))
        .expect("send");
    host.settle(SETTLE).await;
    assert!(!host.standing().boxes[0].dirty);
    assert_eq!(host.settle(Duration::from_mins(4)).await, ["box gone session-1"]);
}

#[tokio::test]
async fn a_teardown_that_arrives_before_the_ack_of_its_snapshot_done_stops_the_box() {
    let mut host = host("teardown-before-ack", 1, Harness::Echo, Boot::Works);
    host.accepting.store(false, Ordering::SeqCst);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    let first = host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    host.log();
    host.teardown("session-1");
    assert_eq!(host.settle(SETTLE).await, ["box gone session-1"]);
    host.commands
        .send(Input::SnapshotAccepted(first.snapshot_id))
        .expect("send");
    assert_eq!(host.settle(SETTLE).await, Vec::<String>::new());
    assert_eq!(host.log(), ["stop session-1"]);
}

#[tokio::test(start_paused = true)]
async fn a_refused_snapshot_packs_part_fails_the_snapshot_at_once() {
    let mut host = host("refused-snapshot-packs", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle_until("complete 1").await;
    {
        let mut drives = host.vmm.drives.lock().expect("drives");
        let drive = host.vmm.staging.join("session-1/overlay");
        let files = drives.get_mut(&drive).expect("box drive");
        let mut found = files[b"root/turn-1".as_slice()].0.clone();
        found.entry.kind = Kind::File { size: 4 };
        for index in 0..u32::try_from(DIGESTS_PER_PART).expect("digests per part") {
            files.insert(
                format!("root/file-{index}").into_bytes(),
                (found.clone(), index.to_le_bytes().to_vec()),
            );
        }
    }
    let snapshot_id: EntityId = "refused-snapshot-packs".try_into().expect("snapshot id");
    host.send(Command::Snapshot(SnapshotOrder {
        snapshot_id: snapshot_id.clone(),
        session_id: "session-1".try_into().expect("session id"),
        box_id: "box-session-1".try_into().expect("box id"),
        snapshot_seq: Sequence(1),
        upload_url: "https://store.example/session-1/1".to_string(),
        upload_expires_at: in_seconds(60),
    }));
    for part in [1, 2] {
        let Some(ChannelHostMessage::SnapshotPacks(packs)) =
            tokio::time::timeout(Duration::from_secs(10), host.received.recv())
                .await
                .expect("snapshot packs")
        else {
            panic!("expected SnapshotPacks part {part}");
        };
        assert_eq!(packs.snapshot_id, snapshot_id);
        assert_eq!(packs.part.get(), part);
    }

    let reason = "the snapshot upload window closed";
    let refused_at = tokio::time::Instant::now();
    host.answers.deliver(Answer::Refused(
        Key {
            ask: Ask::SnapshotPacks,
            id: snapshot_id.clone(),
            part: NonZeroU32::new(2).expect("part"),
        },
        reason.to_string(),
    ));
    let Some(ChannelHostMessage::SnapshotFailed(failed)) = tokio::time::timeout(SETTLE, host.received.recv())
        .await
        .expect("SnapshotFailed immediately after refusal")
    else {
        panic!("expected SnapshotFailed");
    };
    assert_eq!(tokio::time::Instant::now(), refused_at);
    assert_eq!(failed.snapshot_id, snapshot_id);
    assert_eq!(failed.reason, ChannelSnapshotFailureReason::NotAnswered);
    assert_eq!(failed.detail.as_str(), reason);
    assert!(host.standing().boxes[0].dirty);
    assert_eq!(host.store.uploads(), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn a_refused_restore_chunks_part_fails_the_turn_at_once() {
    let (mut host, snapshot) = restoring("refused-restore-chunks", settings()).await;
    host.turn("refused-restore-chunks", "session-1", "two", vec![snapshot]);
    assert_eq!(
        host.settle_until("claim refused-restore-chunks").await,
        ["claim refused-restore-chunks"]
    );
    let Some(ChannelHostMessage::RestoreChunks(chunks)) =
        tokio::time::timeout(Duration::from_secs(10), host.received.recv())
            .await
            .expect("restore chunks")
    else {
        panic!("expected RestoreChunks part 1");
    };
    assert_eq!(chunks.turn_id.as_str(), "refused-restore-chunks");
    assert_eq!(chunks.part.get(), 1);

    let reason = "the restore index is unreadable";
    let refused_at = tokio::time::Instant::now();
    host.answers.deliver(Answer::Refused(
        Key {
            ask: Ask::RestoreChunks,
            id: chunks.turn_id.clone(),
            part: chunks.part,
        },
        reason.to_string(),
    ));
    let Some(ChannelHostMessage::TurnFailed(failed)) = tokio::time::timeout(SETTLE, host.received.recv())
        .await
        .expect("TurnFailed immediately after refusal")
    else {
        panic!("expected TurnFailed");
    };
    assert_eq!(tokio::time::Instant::now(), refused_at);
    assert_eq!(failed.turn_id, chunks.turn_id);
    assert_eq!(failed.reason, TurnFailureReason::SnapshotUnreadable);
    assert_eq!(
        failed.detail.as_str(),
        format!("restoring snapshot snapshot-1 (seq 1) of session session-1: {reason}; offer the turn again for fresh download URLs, or start a new session when the snapshot is damaged")
    );
    assert_eq!(host.log(), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn a_teardown_after_a_failed_snapshot_keeps_the_dirty_box() {
    let mut host = host("teardown-after-snapshot-refusal", 1, Harness::Echo, Boot::Works);
    host.accepting.store(false, Ordering::SeqCst);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    let snapshot = host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    host.log();

    host.commands
        .send(Input::SnapshotFailed(snapshot.snapshot_id))
        .expect("send");
    host.teardown("session-1");
    let seen = host.settle(SETTLE).await;
    assert!(
        matches!(seen.as_slice(), [failure] if failure.starts_with(&format!(
            "teardown failed {}:",
            ChannelBoxTeardownFailureReason::BoxDirty
        ))),
        "{seen:?}"
    );
    assert_eq!(host.log(), Vec::<String>::new());
    assert!(host.standing().boxes[0].dirty);
}

#[tokio::test(start_paused = true)]
async fn a_box_snapshotted_late_is_stopped_once_the_idle_time_since_its_turn_runs_out() {
    let mut host = host("idle-late-snapshot", 1, Harness::Echo, Boot::Works);
    host.idle_seconds = NonZeroU32::new(900).expect("fifteen minutes");
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    assert_eq!(host.settle(Duration::from_mins(10)).await, Vec::<String>::new());
    host.snapshot("session-1");
    assert_eq!(host.settle_until("snapshot done").await, ["snapshot done"]);
    assert_eq!(host.settle(Duration::from_mins(4)).await, Vec::<String>::new());
    assert_eq!(host.settle(Duration::from_mins(2)).await, ["box gone session-1"]);
}

#[tokio::test(start_paused = true)]
async fn a_box_snapshotted_late_keeps_its_idle_deadline_across_a_crash() {
    let mut old = host("idle-crash", 1, Harness::Echo, Boot::Works);
    old.idle_seconds = NonZeroU32::new(900).expect("fifteen minutes");
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle(SETTLE).await;
    assert_eq!(old.settle(Duration::from_mins(10)).await, Vec::<String>::new());
    old.snapshot("session-1");
    assert_eq!(old.settle_until("snapshot done").await, ["snapshot done"]);
    old.settle(SETTLE).await;
    let mut standing = old.standing();
    old.keep_guest(&mut standing.boxes[0], 40);
    let mut new = old.after_crash(standing);
    drop(old);
    assert_eq!(new.settle(Duration::from_mins(4)).await, Vec::<String>::new());
    assert_eq!(new.settle(Duration::from_mins(2)).await, ["box gone session-1"]);
}

#[tokio::test(start_paused = true)]
async fn a_box_handed_over_while_idle_is_stopped_when_the_idle_time_left_runs_out() {
    let mut old = host("idle-handover", 1, Harness::Echo, Boot::Works);
    old.idle_seconds = NonZeroU32::new(900).expect("fifteen minutes");
    old.turn("turn-1", "session-1", "one", Vec::new());
    old.settle(SETTLE).await;
    old.snapshot("session-1");
    assert_eq!(old.settle_until("snapshot done").await, ["snapshot done"]);
    assert_eq!(old.settle(Duration::from_mins(10)).await, Vec::<String>::new());
    let mut new = old.hand_over().await;
    assert_eq!(new.settle(Duration::from_mins(4)).await, Vec::<String>::new());
    assert_eq!(new.settle(Duration::from_mins(2)).await, ["box gone session-1"]);
}

#[tokio::test]
async fn a_teardown_after_the_snapshot_stops_a_clean_box_and_the_next_turn_boots_cold() {
    let mut host = host("teardown-clean", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    let first = host.snapshot("session-1");
    host.teardown("session-1");
    assert_eq!(
        host.settle_until("box gone session-1").await,
        ["snapshot done", "box gone session-1"]
    );
    assert_eq!(
        host.log(),
        [
            "boot session-1 on tumpel0",
            "reconnect session-1",
            "pause session-1",
            "stop session-1"
        ]
    );
    assert!(!host.vmm.staging.join("session-1").exists());
    std::fs::create_dir_all(host.vmm.staging.join("session-1")).expect("a disk a lost box left");

    host.teardown("session-1");
    assert_eq!(host.settle(SETTLE).await, ["box gone session-1"]);
    assert_eq!(host.log(), Vec::<String>::new());
    assert!(!host.vmm.staging.join("session-1").exists());

    host.turn("turn-2", "session-1", "two", vec![first]);
    assert_eq!(host.settle(SETTLE).await, ["claim turn-2", "line 1 two", "complete 1"]);
    assert_eq!(
        host.log(),
        [
            "restore session-1 root/turn-1",
            "boot session-1 on tumpel0",
            "reconnect session-1",
            "pause session-1"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_turn_ends_in_the_guest_with_its_credential_file_and_the_plain_stop_frees_the_box() {
    let mut host = host("cancel-turn", 1, Harness::StallsAfterFirstLine, Boot::Works);
    host.turn("turn-1", "session-1", "one\ntwo", Vec::new());
    assert_eq!(host.settle_until("line 1 one").await, ["claim turn-1", "line 1 one"]);
    host.cancel("turn-1");
    let failed = format!(
        "failed {}: the host cancelled the turn The box's console printed nothing during the turn. with credential file refreshed",
        TurnFailureReason::Cancelled
    );
    assert_eq!(host.settle_until(&failed).await, [failed]);
    host.snapshot("session-1");
    host.teardown("session-1");
    assert_eq!(
        host.settle_until("box gone session-1").await,
        ["snapshot done", "box gone session-1"]
    );
    assert_eq!(
        host.log(),
        [
            "boot session-1 on tumpel0",
            "reconnect session-1",
            "pause session-1",
            "stop session-1"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancel_an_older_guest_never_answers_ends_the_turn_by_closing_the_guest_connection() {
    let mut host = host("cancel-old-guest", 1, Harness::Silent, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    assert_eq!(host.settle_until("claim turn-1").await, ["claim turn-1"]);
    host.cancel("turn-1");
    let failed = format!(
        "failed {}: the guest did not end cancelled turn turn-1 within 10 s, so the host closed its connection, which stops the harness. The box's console printed nothing during the turn.",
        TurnFailureReason::Cancelled
    );
    assert_eq!(host.settle_until(&failed).await, [failed]);
    assert_eq!(
        host.log(),
        ["boot session-1 on tumpel0", "reconnect session-1", "pause session-1"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancel_that_crosses_a_handover_reaches_the_guest_through_the_successor() {
    let mut old = host("cancel-handover", 1, Harness::StallsAfterFirstLine, Boot::Works);
    old.turn("turn-1", "session-1", "one\ntwo", Vec::new());
    assert_eq!(old.settle_until("line 1 one").await, ["claim turn-1", "line 1 one"]);
    let handover = old.ask_handover();
    old.cancel("turn-1");
    let handover = handover.await.expect("the boxes are handed over");
    let SupervisorHandoverBoxState::Turn(turn) = &handover.boxes[0].state else {
        panic!("the running turn is handed over");
    };
    assert_eq!(turn.cancelled, Some(true));
    let mut new = old.successor(&handover);
    let failed = format!(
        "failed {}: the host cancelled the turn The box's console printed nothing during the turn. with credential file refreshed",
        TurnFailureReason::Cancelled
    );
    assert_eq!(new.settle_until(&failed).await, [failed]);
}

#[tokio::test]
async fn a_teardown_of_a_box_with_an_unsnapshotted_turn_fails_box_dirty_and_keeps_the_box() {
    let mut host = host("teardown-dirty", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.log();
    host.teardown("session-1");
    let seen = host.settle(SETTLE).await;
    assert!(
        seen.len() == 1
            && seen[0].starts_with(&format!(
                "teardown failed {}",
                ChannelBoxTeardownFailureReason::BoxDirty
            ))
            && seen[0].contains("session-1"),
        "{seen:?}"
    );
    assert_eq!(host.log(), Vec::<String>::new());

    host.turn("turn-2", "session-1", "two", Vec::new());
    assert_eq!(host.settle(SETTLE).await, ["claim turn-2", "line 1 two", "complete 1"]);
    assert_eq!(
        host.log(),
        ["resume session-1", "reconnect session-1", "pause session-1"]
    );
}

#[tokio::test]
async fn a_teardown_after_a_failed_turn_fails_box_dirty_and_keeps_the_box() {
    let mut host = host("failed-dirty", 1, Harness::Echo, Boot::Works);
    let prompt = "{\"type\":\"result\",\"is_error\":true,\"result\":\"API Error: 500\"}";
    host.turn_speaking(&GuestHarnessProtocol::ClaudeStreamJson, "turn-1", "session-1", prompt);
    let seen = host.settle(SETTLE).await;
    assert!(
        seen.last()
            .is_some_and(|last| last.starts_with(&format!("failed {}", TurnFailureReason::HarnessReportedError))),
        "{seen:?}"
    );
    host.log();
    host.teardown("session-1");
    let seen = host.settle(SETTLE).await;
    assert!(
        seen.len() == 1
            && seen[0].starts_with(&format!(
                "teardown failed {}",
                ChannelBoxTeardownFailureReason::BoxDirty
            )),
        "{seen:?}"
    );
    assert_eq!(host.log(), Vec::<String>::new());
}

#[tokio::test]
async fn a_turn_for_another_box_of_the_session_stops_the_warm_box_and_boots_fresh() {
    let mut host = host("moved-back", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.log();
    host.turn_in_box("turn-2", "session-1", "box-session-1-again", "two");
    assert_eq!(host.settle(SETTLE).await, ["claim turn-2", "line 1 two", "complete 1"]);
    assert_eq!(
        host.log(),
        [
            "stop session-1",
            "boot session-1 on tumpel0",
            "reconnect session-1",
            "pause session-1"
        ]
    );
}

#[tokio::test]
async fn a_discard_drops_a_box_with_an_unsnapshotted_turn_and_answers_box_gone() {
    let mut host = host("discard-dirty", 1, Harness::Echo, Boot::Works);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.log();
    host.discard("session-1");
    assert_eq!(host.settle(SETTLE).await, ["box gone session-1"]);
    assert_eq!(host.log(), ["stop session-1"]);
}

const IMAGE_FETCH: Duration = Duration::from_millis(1200);

fn fetching_host(name: &str, store: &MemoryStore) -> Host {
    let mut store = store.clone();
    store.3 = IMAGE_FETCH;
    host_on(name, store, 1, Harness::Echo, Boot::Works, settings(), 120)
}

#[tokio::test(start_paused = true)]
async fn a_cold_turn_downloads_its_snapshot_before_a_fetch_that_outlasts_the_snapshot_urls() {
    let mut first = host("fetch-first", 1, Harness::Echo, Boot::Works);
    first.turn("turn-1", "session-1", "one", Vec::new());
    first.settle(SETTLE).await;
    let mut snapshot = first.snapshot("session-1");
    assert_eq!(first.settle_until("snapshot done").await, ["snapshot done"]);
    snapshot.download_expires_at = in_seconds(1);

    let mut second = fetching_host("fetch-second", &first.store);
    let order = second.turn_fetching("turn-2", vec![snapshot], 300);
    let digest = order.download.image_digest.as_str().to_string();
    assert_eq!(
        second.settle(IMAGE_FETCH + SETTLE * 4).await,
        [
            "claim turn-2".to_string(),
            format!("fetching {digest}"),
            "line 1 two".to_string(),
            "complete 1".to_string()
        ]
    );
    assert_eq!(
        second.log(),
        [
            "restore session-1 root/turn-1",
            "boot session-1 on tumpel0",
            "reconnect session-1",
            "pause session-1"
        ]
    );
    assert!(second.images.join(format!("{digest}.ext4")).exists());
}

#[tokio::test]
async fn a_dispatch_credential_that_expires_during_the_fetch_fails_the_turn_before_the_box_boots() {
    let store = MemoryStore::default();
    let mut host = fetching_host("fetch-expired", &store);
    host.turn_fetching("turn-1", Vec::new(), 1);
    let seen = host.settle(IMAGE_FETCH + SETTLE * 4).await;
    assert!(
        seen.len() == 3
            && seen[0] == "claim turn-1"
            && seen[2].starts_with(&format!("failed {}", TurnFailureReason::DispatchCredentialExpired))
            && seen[2].contains("after its guest image was fetched"),
        "{seen:?}"
    );
    assert_eq!(host.log(), Vec::<String>::new());
}

#[test]
fn the_restore_budget_grows_from_the_boot_budget_by_started_32_mib_and_started_2000_entries() {
    let boot = Duration::from_secs(30);
    let mib = 1024 * 1024;
    assert_eq!(restore_budget(boot, 0, 0), boot);
    assert_eq!(restore_budget(boot, 1, 0), Duration::from_secs(31));
    assert_eq!(restore_budget(boot, 32 * mib, 0), Duration::from_secs(31));
    assert_eq!(restore_budget(boot, 32 * mib + 1, 0), Duration::from_secs(32));
    assert_eq!(restore_budget(boot, 0, 1), Duration::from_secs(31));
    assert_eq!(restore_budget(boot, 0, 2_001), Duration::from_secs(32));
    assert_eq!(
        restore_budget(boot, 8 * 1024 * mib, 400_000),
        Duration::from_secs(30 + 256 + 200)
    );
}

async fn restoring(name: &str, settings: Settings) -> (Host, ChannelSnapshotDownload) {
    let mut first = host(&format!("{name}-first"), 1, Harness::Echo, Boot::Works);
    first.turn("turn-1", "session-1", "one", Vec::new());
    first.settle(SETTLE).await;
    let snapshot = first.snapshot("session-1");
    assert_eq!(first.settle_until("snapshot done").await, ["snapshot done"]);
    let mut second = host_on(name, first.store.clone(), 1, Harness::Echo, Boot::Works, settings, 120);
    second.seq = first.seq;
    (second, snapshot)
}

#[test]
fn a_snapshot_larger_than_the_box_disk_names_both_sizes_and_the_persona_setting() {
    let file = Stored {
        entry: Entry {
            kind: Kind::File { size: 2 * 1024 * 1024 },
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime: 1,
            mtime_nanos: 0,
        },
        chunks: Vec::new(),
    };
    let manifest = BTreeMap::from([(b"root/target".to_vec(), file)]);
    assert_eq!(
        super::restore_size("snapshot 1", &manifest, 1024 * 1024),
        Err("snapshot 1 holds 2097152 bytes, more than the 1048576 bytes of the box's disk; raise the persona's boxDiskMib or start a new session".to_string())
    );
}

#[tokio::test(start_paused = true)]
async fn a_turn_on_a_host_disk_under_its_headroom_fails_box_disk_full_naming_the_host_disk() {
    let mut host = host("disk-full", 3, Harness::Echo, Boot::Works);
    host.vmm.free_disk_mib.store(5000, Ordering::SeqCst);
    host.turn("turn-1", "session-1", "one", Vec::new());
    let seen = host.settle(SETTLE).await;
    assert!(
        seen.iter().any(
            |line| line.starts_with(&format!("failed {}", TurnFailureReason::BoxDiskFull))
                && line.contains("the host's disk has 5000 MiB free, under the 10000 MiB it keeps free")
        ),
        "{seen:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_box_whose_disk_does_not_fit_the_host_is_refused_until_a_teardown_frees_disk() {
    let mut host = host("disk", 3, Harness::Echo, Boot::Works);
    host.vmm.free_disk_mib.store(30_000, Ordering::SeqCst);
    host.turn("turn-1", "session-1", "one", Vec::new());
    host.settle(SETTLE).await;
    host.turn("turn-2", "session-2", "two", Vec::new());
    host.settle(SETTLE).await;
    host.log();
    host.turn("turn-3", "session-3", "three", Vec::new());
    let refused = host.settle(SETTLE).await;
    assert!(
        refused.len() == 1
            && refused[0].starts_with(&format!("failed {}", TurnFailureReason::PoolExhausted))
            && refused[0].contains(
                "this host has 30000 MiB of free disk and keeps 10000 MiB of it free; its 2 boxes keep 16384 MiB to grow into, so the 8192 MiB a box disk of 32768 MiB keeps does not fit"
            ),
        "{refused:?}"
    );
    assert!(host.log().is_empty(), "a refused box never boots");
    let held = host.holdings.borrow().clone();
    assert_eq!(
        (held.disk_reserved_mib, held.memory_budget_mib, held.memory_held_mib),
        (16_384, 3 * 4096, 2 * u64::from(BOX.memory_mib.get()))
    );
    host.snapshot("session-1");
    host.settle(SETTLE).await;
    let held = host.holdings.borrow().clone();
    let mut drives = held.drives.clone();
    drives.sort();
    assert_eq!(
        (drives, held.disk_reserved_mib),
        (vec!["session-1".to_string(), "session-2".to_string()], 16_384),
        "a clean paused box still holds its disk"
    );
    host.teardown("session-1");
    host.settle(SETTLE).await;
    host.log();
    host.turn("turn-4", "session-3", "four", Vec::new());
    assert_eq!(host.settle(SETTLE).await, ["claim turn-4", "line 1 four", "complete 1"]);
    assert_eq!(
        host.log(),
        ["boot session-3 on tumpel0", "reconnect session-3", "pause session-3"]
    );
}

const O_NONBLOCK: i32 = 0o4000;

fn stuck_archive(name: &str) -> PathBuf {
    let session = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/lifecycle-tests")
        .join(name)
        .join("session-1");
    std::fs::create_dir_all(&session).expect("session staging");
    let archive = session.join("restore.tar");
    let made = std::process::Command::new("mkfifo")
        .arg(&archive)
        .status()
        .expect("mkfifo");
    assert!(made.success(), "mkfifo {}", archive.display());
    let held = session.with_file_name("restore.fifo");
    std::fs::hard_link(&archive, &held).expect("a second name for the fifo outlives the box disk");
    held
}

fn release(archive: &Path) {
    use std::os::unix::fs::OpenOptionsExt;
    drop(
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(O_NONBLOCK)
            .open(archive)
            .expect("releasing the stuck writer"),
    );
}

#[tokio::test]
async fn a_restore_without_host_disk_for_its_staging_fails_box_disk_full_and_releases_its_charge() {
    let (mut host, snapshot) = restoring("restore-disk", settings()).await;
    host.vmm.free_disk_mib.store(10_001, Ordering::SeqCst);
    let mut turn = host.box_turn("turn-2", "session-1", "two", vec![snapshot.clone()], 300);
    turn.box_disk_mib = Some(1);
    host.send(Command::Turn(Box::new(turn)));
    let refused = host.settle(Duration::from_secs(2)).await;
    assert!(
        refused.iter().any(|line| line.starts_with(&format!("failed {}", TurnFailureReason::BoxDiskFull))
            && line.contains("staging it needs 1 MiB of host disk beyond the box's reserve, but the host has 10001 MiB free, keeps 10000 MiB free, its boxes keep 1 MiB to grow into and other restores hold 0 MiB")),
        "{refused:?}"
    );
    host.vmm.free_disk_mib.store(10_002, Ordering::SeqCst);
    let mut again = host.box_turn("turn-3", "session-1", "three", vec![snapshot], 300);
    again.box_disk_mib = Some(1);
    host.send(Command::Turn(Box::new(again)));
    let restored = host.settle_until("complete 1").await;
    assert!(!restored.iter().any(|line| line.starts_with("failed")), "{restored:?}");
}

#[tokio::test]
async fn a_stuck_reassembly_outlasts_the_boot_budget_then_fails_with_its_size() {
    let (mut host, snapshot) = restoring("stuck-reassembly", settings()).await;
    let archive = stuck_archive("stuck-reassembly");
    host.turn("turn-2", "session-1", "two", vec![snapshot]);
    let early = host.settle(settings().budgets.boot * 2).await;
    let seen = host.settle(Duration::from_secs(3)).await;
    release(&archive);
    assert_eq!(early, ["claim turn-2"]);
    assert!(
        seen.len() == 1
            && seen[0].starts_with(&format!("failed {}", TurnFailureReason::SnapshotUnreadable))
            && seen[0].contains("took longer than 2 s for")
            && seen[0].contains("bytes in"),
        "{seen:?}"
    );
    assert!(!host.vmm.staging.join("session-1").exists());
}

#[tokio::test]
async fn a_restore_that_outlasts_the_dispatch_credential_fails_dispatch_credential_expired() {
    let (mut host, snapshot) = restoring("stuck-credential", settings()).await;
    let archive = stuck_archive("stuck-credential");
    host.turn_expiring("turn-2", "session-1", "two", vec![snapshot], 1);
    let seen = host.settle(Duration::from_millis(1500)).await;
    release(&archive);
    assert!(
        seen.len() == 2
            && seen[1].starts_with(&format!("failed {}", TurnFailureReason::DispatchCredentialExpired))
            && seen[1].contains("while its snapshot was restored"),
        "{seen:?}"
    );
}

#[tokio::test]
async fn a_box_disk_that_cannot_be_deleted_stops_the_box_runner_with_disk_not_freed() {
    let host = host("disk-not-freed", 1, Harness::Echo, Boot::Works);
    std::fs::write(
        host.vmm.staging.join("session-1"),
        b"a file where the disk directory goes",
    )
    .expect("a file");

    host.teardown("session-1");

    let stopped = tokio::time::timeout(SETTLE, host.runner)
        .await
        .expect("the box runner stops")
        .expect("the box runner ends without a panic");
    assert!(
        stopped
            .as_ref()
            .is_err_and(|error| error.starts_with("DiskNotFreed: the box of session session-1 ended, then removing")),
        "{stopped:?}"
    );
}

fn protocol_digit(protocol: &GuestHarnessProtocol) -> usize {
    [
        GuestHarnessProtocol::ClaudeStreamJson,
        GuestHarnessProtocol::PiRpc,
        GuestHarnessProtocol::PromptOnStdin,
        GuestHarnessProtocol::CodexExecJson,
        GuestHarnessProtocol::ShellLines,
    ]
    .iter()
    .position(|known| known == protocol)
    .expect("a known harness protocol")
}
