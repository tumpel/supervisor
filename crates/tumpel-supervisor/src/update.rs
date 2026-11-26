use chrono::Utc;
use ring::digest::{digest, SHA256};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::ffi::OsString;
use std::fs::File;
use std::future::Future;
use std::io::{ErrorKind, Write};
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tumpel_api::types::{
    ChannelHostMessage, ChannelSupervisorUpdateFailed, ChannelSupervisorUpdateFailedType, ChannelSupervisorUpdateStep,
    ChannelSupervisorUpdateStepReleaseVersion, ChannelSupervisorUpdateStepType, HostArchitecture,
    ReleaseManifestReleaseVersion, ReleaseSupervisorSupervisorVersion, Sha256Digest, SupervisorUpdateFailureReason,
    SupervisorUpdatePhase, SupervisorUpdateRefusal,
};
use tumpel_guest::setup_output::{redacted, strip_ansi};

use crate::bounded::reported;
use crate::dir;
use crate::install::supervisor_unit_text;
use crate::report::CHANNEL_PROTOCOL_LINE;
use crate::snapshot::hex;

pub const REFUSAL_FILE: &str = "update-refusal.json";
pub const HANDOVER_FILE: &str = "handover.json";
pub const BOXES_FILE: &str = "boxes.json";
const TAIL_LINES: usize = 20;
const TAIL_LINE_CHARS: usize = 400;
const DRAIN: Duration = Duration::from_secs(5);

pub struct Limits {
    pub release: Duration,
    pub download: Duration,
    pub verify: Duration,
    pub place: Duration,
    pub dry_run: Duration,
}

impl Limits {
    pub const HOST: Self = Self {
        release: Duration::from_secs(30),
        download: Duration::from_secs(300),
        verify: Duration::from_secs(30),
        place: Duration::from_secs(60),
        dry_run: Duration::from_secs(60),
    };
}

#[derive(Clone)]
pub struct Layout {
    pub binary: PathBuf,
    pub candidate: PathBuf,
    pub state: PathBuf,
    pub unit: PathBuf,
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().map_or_else(OsString::new, OsString::from);
    name.push(suffix);
    path.with_file_name(name)
}

impl Layout {
    #[must_use]
    pub fn new(binary: PathBuf, state: PathBuf, unit: PathBuf) -> Self {
        Self {
            candidate: sibling(&binary, ".candidate"),
            binary,
            state,
            unit,
        }
    }

    pub(crate) fn clear_candidate(&self) -> Result<(), String> {
        remove(&self.candidate)
    }

    fn refusal(&self) -> PathBuf {
        self.state.join(REFUSAL_FILE)
    }

    pub(crate) fn handover(&self) -> PathBuf {
        self.state.join(HANDOVER_FILE)
    }

    pub(crate) fn boxes(&self) -> PathBuf {
        self.state.join(BOXES_FILE)
    }
}

pub trait Idle: Send + Sync {
    fn boxes(&self) -> u32;
    fn busy(&self) -> u32;
    fn settled(&self) -> impl Future<Output = ()> + Send;
}

pub struct Release {
    pub version: ChannelSupervisorUpdateStepReleaseVersion,
    pub sha256: Sha256Digest,
    pub supervisor_version: ReleaseSupervisorSupervisorVersion,
    pub channel_protocol_version: u32,
    pub rollback: bool,
}

pub trait Source: Send + Sync {
    fn release(&self) -> impl Future<Output = Result<Release, String>> + Send;
    fn binary(&self) -> impl Future<Output = Result<Vec<u8>, String>> + Send;
}

pub struct HttpSource {
    client: reqwest::Client,
    endpoint: String,
    architecture: HostArchitecture,
}

impl HttpSource {
    pub fn new(endpoint: &str) -> Result<Self, String> {
        let client = reqwest::Client::builder().build().map_err(|error| {
            format!(
                "building the HTTP client for supervisor updates failed: {}",
                error.without_url()
            )
        })?;
        let architecture = if std::env::consts::ARCH == "aarch64" {
            HostArchitecture::Arm64
        } else {
            HostArchitecture::Amd64
        };
        Ok(Self {
            client,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            architecture,
        })
    }

    async fn get(&self, url: &str) -> Result<reqwest::Response, String> {
        let response = self.client.get(url).send().await.map_err(|error| {
            format!(
                "GET {url} failed: {}; check that this host reaches the control plane",
                error.without_url()
            )
        })?;
        if !response.status().is_success() {
            return Err(format!(
                "GET {url} answered {}; check that the stage has published a release",
                response.status()
            ));
        }
        Ok(response)
    }
}

impl Source for HttpSource {
    async fn release(&self) -> Result<Release, String> {
        let url = format!("{}/v1/releases/current", self.endpoint);
        let body = self
            .get(&url)
            .await?
            .bytes()
            .await
            .map_err(|error| format!("reading {url} failed: {}", error.without_url()))?;
        announced_release(&body, &url, &self.architecture)
    }

    async fn binary(&self) -> Result<Vec<u8>, String> {
        let url = format!("{}/v1/releases/supervisor/{}", self.endpoint, self.architecture);
        let bytes = self
            .get(&url)
            .await?
            .bytes()
            .await
            .map_err(|error| format!("downloading the supervisor from {url} failed: {}", error.without_url()))?;
        Ok(bytes.to_vec())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnnouncedRelease {
    release_version: ReleaseManifestReleaseVersion,
    channel_protocol_version: NonZeroU32,
    supervisors: Vec<AnnouncedSupervisor>,
    #[serde(default)]
    rollback: bool,
}

#[derive(Deserialize)]
struct AnnouncedSupervisor {
    architecture: HostArchitecture,
    sha256: Sha256Digest,
    #[serde(rename = "supervisorVersion")]
    supervisor_version: ReleaseSupervisorSupervisorVersion,
}

fn announced_release(body: &[u8], url: &str, architecture: &HostArchitecture) -> Result<Release, String> {
    let manifest: AnnouncedRelease = serde_json::from_slice(body).map_err(|error| {
        format!("{url} is no ReleaseManifest: {error}; check that the stage has published a release")
    })?;
    if manifest
        .supervisors
        .iter()
        .any(|supervisor| matches!(supervisor.architecture, HostArchitecture::Unrecognised(_)))
    {
        tumpel_channel::update_required();
    }
    let supervisor = manifest
        .supervisors
        .into_iter()
        .find(|supervisor| supervisor.architecture == *architecture)
        .ok_or_else(|| {
            format!(
                "release {} carries no {architecture} supervisor; publish one for this architecture",
                *manifest.release_version
            )
        })?;
    Ok(Release {
        version: manifest
            .release_version
            .parse()
            .map_err(|error| format!("{url} names an empty release version: {error}"))?,
        sha256: supervisor.sha256,
        supervisor_version: supervisor.supervisor_version,
        channel_protocol_version: manifest.channel_protocol_version.get(),
        rollback: manifest.rollback,
    })
}

fn version_refusal(running: &str, announced: &str, rollback: bool) -> Option<String> {
    let parse = |version: &str| semver::Version::parse(version.trim_start_matches('v'));
    let Ok(running_version) = parse(running) else {
        return None;
    };
    let announced_version = match parse(announced) {
        Ok(version) => version,
        Err(error) => {
            return Some(format!(
                "the release names supervisor version {announced}, which is no semver version ({error}); publish a release with a semver supervisor version"
            ))
        }
    };
    match announced_version.cmp_precedence(&running_version) {
        std::cmp::Ordering::Greater => None,
        std::cmp::Ordering::Less if rollback => None,
        std::cmp::Ordering::Less => Some(format!(
            "the release names supervisor {announced}, older than the running {running}, and is no rollback; publish a newer release or a rollback"
        )),
        std::cmp::Ordering::Equal => Some(format!(
            "the release names supervisor {announced}, the running version {running}, with another binary; publish the new binary under a new version"
        )),
    }
}

#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(digest(&SHA256, bytes).as_ref())
}

pub fn binary_sha256(path: &Path) -> Result<Sha256Digest, String> {
    let bytes = std::fs::read(path).map_err(|error| {
        format!(
            "reading {} to learn its sha256 failed: {error}; run the supervisor from its installed binary",
            path.display()
        )
    })?;
    digest_of(&sha256_hex(&bytes))
}

fn digest_of(hex: &str) -> Result<Sha256Digest, String> {
    hex.parse()
        .map_err(|error| format!("{hex} is no sha256 digest: {error}"))
}

fn short(sha256: &Sha256Digest) -> &str {
    sha256.get(..12).unwrap_or(sha256)
}

fn named(release: &Release) -> String {
    format!("{} ({})", *release.supervisor_version, short(&release.sha256))
}

fn read<T: DeserializeOwned>(path: &Path, schema: &str) -> Result<Option<T>, String> {
    dir::read_json(path, schema).map_err(|error| format!("{error:#}; delete it to start the update over"))
}

fn remove(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("removing {} failed: {error}", path.display())),
    }
}

fn synced(path: &Path) -> Result<(), String> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("flushing {} to disk failed: {error}", path.display()))
}

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Current,
    Refused,
    Busy,
    Failed(SupervisorUpdateFailureReason),
    Restart,
}

struct Stop {
    reason: SupervisorUpdateFailureReason,
    detail: String,
    tail: Vec<String>,
}

fn stop(reason: SupervisorUpdateFailureReason, detail: String) -> Stop {
    Stop {
        reason,
        detail,
        tail: Vec::new(),
    }
}

fn tail_of(stderr: &[u8], stdout: &[u8]) -> Vec<String> {
    let text = format!(
        "{}\n{}",
        strip_ansi(&String::from_utf8_lossy(stderr)),
        strip_ansi(&String::from_utf8_lossy(stdout))
    );
    let lines: Vec<&str> = text.lines().map(str::trim).filter(|line| !line.is_empty()).collect();
    lines[lines.len().saturating_sub(TAIL_LINES)..]
        .iter()
        .map(|line| redacted(&line.chars().take(TAIL_LINE_CHARS).collect::<String>()))
        .collect()
}

async fn drain(stream: Option<impl AsyncRead + Unpin>) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Some(mut stream) = stream {
        let _ = stream.read_to_end(&mut bytes).await;
    }
    bytes
}

pub struct Updater<S: Source, I: Idle> {
    source: S,
    layout: Layout,
    limits: Limits,
    running: Sha256Digest,
    version: ChannelSupervisorUpdateStepReleaseVersion,
    idle: I,
    out: UnboundedSender<ChannelHostMessage>,
    running_update: Mutex<()>,
    online: AtomicBool,
}

impl<S: Source, I: Idle> Updater<S, I> {
    pub fn new(
        source: S,
        layout: Layout,
        limits: Limits,
        running: Sha256Digest,
        version: ChannelSupervisorUpdateStepReleaseVersion,
        idle: I,
        out: UnboundedSender<ChannelHostMessage>,
    ) -> Self {
        Self {
            source,
            layout,
            limits,
            running,
            version,
            idle,
            out,
            running_update: Mutex::new(()),
            online: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub fn first_online(&self) -> bool {
        if self.online.swap(true, Ordering::SeqCst) {
            return false;
        }
        self.step(
            &self.version,
            &self.running,
            SupervisorUpdatePhase::Online,
            self.idle.boxes(),
        );
        true
    }

    pub async fn now(&self) -> Outcome {
        let _running = self.running_update.lock().await;
        match self.due().await {
            Ok(release) => self.apply(&release).await,
            Err(outcome) => outcome,
        }
    }

    pub async fn announced(&self, grace: Duration) -> Outcome {
        let Ok(_running) = self.running_update.try_lock() else {
            return Outcome::Busy;
        };
        let release = match self.due().await {
            Ok(release) => release,
            Err(outcome) => return outcome,
        };
        if let Some(refusal) = version_refusal(&self.version, &release.supervisor_version, release.rollback) {
            eprintln!(
                "tumpel-supervisor: refusing supervisor update to {}: {refusal}; supervisor {} ({}) keeps running",
                named(&release),
                *self.version,
                short(&self.running)
            );
            return Outcome::Refused;
        }
        if self.idle.busy() > 0 {
            eprintln!(
                "tumpel-supervisor: supervisor {} is out and {} boxes run a turn; updating once they finish or in {} s, whichever comes first",
                named(&release),
                self.idle.busy(),
                grace.as_secs()
            );
            self.step(
                &release.version,
                &release.sha256,
                SupervisorUpdatePhase::WaitingForIdle,
                self.idle.boxes(),
            );
            tokio::select! {
                () = tokio::time::sleep(grace) => {}
                () = self.idle.settled() => {}
            }
        }
        self.apply(&release).await
    }

    async fn due(&self) -> Result<Release, Outcome> {
        use SupervisorUpdateFailureReason as Reason;
        let release = match timeout(self.limits.release, self.source.release()).await {
            Ok(Ok(release)) => release,
            Ok(Err(error)) => {
                eprintln!("tumpel-supervisor: SupervisorUpdateFetchFailed: {error}");
                return Err(Outcome::Failed(Reason::SupervisorUpdateFetchFailed));
            }
            Err(_) => {
                eprintln!(
                    "tumpel-supervisor: SupervisorUpdateFetchFailed: reading the current release ran past {} s; check that this host reaches the control plane",
                    self.limits.release.as_secs()
                );
                return Err(Outcome::Failed(Reason::SupervisorUpdateFetchFailed));
            }
        };
        if release.sha256 == self.running {
            return Err(Outcome::Current);
        }
        match read::<SupervisorUpdateRefusal>(&self.layout.refusal(), "SupervisorUpdateRefusal") {
            Ok(Some(refusal)) if refusal.to_sha256 == release.sha256 => Err(Outcome::Refused),
            Ok(_) => Ok(release),
            Err(error) => {
                self.fail(&release.sha256, stop(Reason::SupervisorUpdateStateUnreadable, error));
                Err(Outcome::Failed(Reason::SupervisorUpdateStateUnreadable))
            }
        }
    }

    async fn apply(&self, release: &Release) -> Outcome {
        if let Err(stopped) = self.prepare(release).await {
            let reason = stopped.reason.clone();
            self.fail(&release.sha256, stopped);
            return Outcome::Failed(reason);
        }
        eprintln!(
            "tumpel-supervisor: swapping supervisor {} ({}) for {} and handing its boxes over",
            *self.version,
            short(&self.running),
            named(release)
        );
        self.step(
            &release.version,
            &release.sha256,
            SupervisorUpdatePhase::Restarting,
            self.idle.boxes(),
        );
        match self.swap(&release.sha256) {
            Ok(()) => Outcome::Restart,
            Err(stopped) => {
                let reason = stopped.reason.clone();
                self.fail(&release.sha256, stopped);
                Outcome::Failed(reason)
            }
        }
    }

    fn step(
        &self,
        version: &ChannelSupervisorUpdateStepReleaseVersion,
        to: &Sha256Digest,
        phase: SupervisorUpdatePhase,
        boxes: u32,
    ) {
        let _ = self.out.send(
            ChannelHostMessage::SupervisorUpdateStep(ChannelSupervisorUpdateStep {
                type_: ChannelSupervisorUpdateStepType::SupervisorUpdateStep,
                from_sha256: self.running.clone(),
                to_sha256: to.clone(),
                release_version: version.clone(),
                boxes_running: i32::try_from(boxes).unwrap_or(i32::MAX),
                phase,
                at: Utc::now().fixed_offset(),
            })
            .into(),
        );
    }

    fn fail(&self, to: &Sha256Digest, stopped: Stop) {
        let detail = redacted(&stopped.detail);
        eprintln!("tumpel-supervisor: {}: {detail}", stopped.reason);
        let _ = self.out.send(
            ChannelHostMessage::SupervisorUpdateFailed(ChannelSupervisorUpdateFailed {
                type_: ChannelSupervisorUpdateFailedType::SupervisorUpdateFailed,
                from_sha256: self.running.clone(),
                to_sha256: to.clone(),
                reason: stopped.reason,
                detail: reported("the update failure detail", &detail),
                output_tail: stopped
                    .tail
                    .into_iter()
                    .map(|line| reported("an update output line", &line))
                    .collect(),
                failed_at: Utc::now().fixed_offset(),
            })
            .into(),
        );
    }

    fn refuse(&self, to: &Sha256Digest, stopped: Stop) -> Stop {
        let candidate = self.layout.clear_candidate();
        let refusal = SupervisorUpdateRefusal {
            to_sha256: to.clone(),
            reason: stopped.reason.clone(),
            refused_at: Utc::now().fixed_offset(),
        };
        let written = dir::write_json(&self.layout.refusal(), &refusal).map_err(|error| format!("{error:#}"));
        let consequence = match (candidate, written) {
            (Ok(()), Ok(())) => format!("this host skips {} until the release names another binary", short(to)),
            (Err(error), _) | (_, Err(error)) => error,
        };
        Stop {
            detail: format!("{}; the running supervisor stays, and {consequence}", stopped.detail),
            ..stopped
        }
    }

    async fn prepare(&self, release: &Release) -> Result<(), Stop> {
        use SupervisorUpdateFailureReason as Reason;
        self.step(
            &release.version,
            &release.sha256,
            SupervisorUpdatePhase::Fetching,
            self.idle.boxes(),
        );
        let bytes = match timeout(self.limits.download, self.source.binary()).await {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => return Err(stop(Reason::SupervisorUpdateFetchFailed, error)),
            Err(_) => {
                return Err(stop(
                    Reason::SupervisorUpdateFetchFailed,
                    format!(
                        "downloading supervisor {} ran past {} s; the next trigger downloads it again",
                        short(&release.sha256),
                        self.limits.download.as_secs()
                    ),
                ))
            }
        };
        let hashed = timeout(
            self.limits.verify,
            tokio::task::spawn_blocking(move || (sha256_hex(&bytes), bytes)),
        )
        .await;
        let Ok(Ok((actual, bytes))) = hashed else {
            return Err(stop(
                Reason::SupervisorUpdateChecksumMismatch,
                format!(
                    "hashing the downloaded supervisor did not finish within {} s; the next trigger downloads it again",
                    self.limits.verify.as_secs()
                ),
            ));
        };
        if actual != *release.sha256 {
            return Err(stop(
                Reason::SupervisorUpdateChecksumMismatch,
                format!(
                    "the downloaded supervisor hashes to {actual}, and the release names {}; the release changed during the download or the download broke, so the next trigger downloads it again",
                    *release.sha256
                ),
            ));
        }
        self.place_candidate(&bytes).await?;
        self.step(
            &release.version,
            &release.sha256,
            SupervisorUpdatePhase::DryRunning,
            self.idle.boxes(),
        );
        if let Err(stopped) = self.dry_run(release).await {
            return Err(self.refuse(&release.sha256, stopped));
        }
        Ok(())
    }

    async fn place_candidate(&self, bytes: &[u8]) -> Result<(), Stop> {
        let candidate = &self.layout.candidate;
        let staged = sibling(candidate, ".part");
        let failed = |detail: String| {
            stop(
                SupervisorUpdateFailureReason::SupervisorUpdateFetchFailed,
                format!("{detail}; free space beside the installed binary"),
            )
        };
        File::create(&staged)
            .and_then(|mut file| file.write_all(bytes))
            .map_err(|error| failed(format!("writing {} failed: {error}", staged.display())))?;
        let copied = timeout(
            self.limits.place,
            Command::new("cp")
                .arg(&staged)
                .arg(candidate)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .output(),
        )
        .await;
        let removed = remove(&staged);
        let output = match copied {
            Err(_) => {
                return Err(failed(format!(
                    "copying {} to {} ran past {} s",
                    staged.display(),
                    candidate.display(),
                    self.limits.place.as_secs()
                )))
            }
            Ok(Err(error)) => {
                return Err(failed(format!(
                    "starting cp for {} failed: {error}",
                    candidate.display()
                )))
            }
            Ok(Ok(output)) => output,
        };
        if !output.status.success() {
            return Err(failed(format!(
                "copying {} to {} exited with {}: {}",
                staged.display(),
                candidate.display(),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        removed.map_err(failed)?;
        synced(candidate).map_err(failed)?;
        std::fs::set_permissions(candidate, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| failed(format!("making {} executable failed: {error}", candidate.display())))
    }

    async fn dry_run(&self, release: &Release) -> Result<(), Stop> {
        use SupervisorUpdateFailureReason::SupervisorUpdateDryRunFailed as DryRunFailed;
        let candidate = &self.layout.candidate;
        let mut child = Command::new(candidate)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                stop(
                    DryRunFailed,
                    format!("starting the report from {} failed: {error}", candidate.display()),
                )
            })?;
        let stdout = tokio::spawn(drain(child.stdout.take()));
        let stderr = tokio::spawn(drain(child.stderr.take()));
        let waited = timeout(self.limits.dry_run, child.wait()).await;
        if waited.is_err() {
            let _ = child.start_kill();
        }
        let stdout = timeout(DRAIN, stdout)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        let stderr = timeout(DRAIN, stderr)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        let stdout_text = strip_ansi(&String::from_utf8_lossy(&stdout));
        let failed = |detail: String| {
            for line in stdout_text.lines().filter(|line| line.starts_with("FAIL ")) {
                eprintln!("tumpel-supervisor: {}", redacted(line));
            }
            Stop {
                reason: DryRunFailed,
                detail,
                tail: tail_of(&stderr, &stdout),
            }
        };
        let name = format!("the report from supervisor {}", named(release));
        let status = match waited {
            Err(_) => return Err(failed(format!("{name} ran past {} s", self.limits.dry_run.as_secs()))),
            Ok(Err(error)) => return Err(failed(format!("waiting for {name} failed: {error}"))),
            Ok(Ok(status)) => status,
        };
        if !status.success() {
            return Err(failed(format!("{name} exited with {status}")));
        }
        let protocol = stdout_text
            .lines()
            .find_map(|line| line.strip_prefix(CHANNEL_PROTOCOL_LINE))
            .ok_or_else(|| failed(format!("{name} printed no channel protocol")))?
            .parse::<NonZeroU32>()
            .map_err(|error| failed(format!("{name} printed an invalid channel protocol: {error}")))?;
        if protocol.get() != release.channel_protocol_version {
            return Err(failed(format!(
                "{name} speaks channel protocol {}, and the release speaks {}",
                protocol, release.channel_protocol_version
            )));
        }
        Ok(())
    }

    fn swap(&self, to: &Sha256Digest) -> Result<(), Stop> {
        let candidate = &self.layout.candidate;
        let binary = &self.layout.binary;
        std::fs::rename(candidate, binary).map_err(|error| {
            let detail = format!(
                "moving {} over {} failed: {error}",
                candidate.display(),
                binary.display()
            );
            self.refuse(
                to,
                stop(SupervisorUpdateFailureReason::SupervisorUpdateSwapFailed, detail),
            )
        })
    }
}

pub fn forget_handover(layout: &Layout) -> Result<(), String> {
    remove(&layout.handover())
}

pub fn refresh_unit(layout: &Layout) -> Result<bool, String> {
    let unit = supervisor_unit_text(&layout.binary);
    match std::fs::read_to_string(&layout.unit) {
        Ok(current) if current == unit => return Ok(false),
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(format!("reading {} failed: {error}", layout.unit.display())),
    }
    let staged = sibling(&layout.unit, ".part");
    std::fs::write(&staged, &unit).map_err(|error| format!("writing {} failed: {error}", staged.display()))?;
    synced(&staged)?;
    std::fs::rename(&staged, &layout.unit).map_err(|error| {
        format!(
            "moving {} to {} failed: {error}",
            staged.display(),
            layout.unit.display()
        )
    })?;
    Ok(true)
}

#[cfg(test)]
mod tests;
