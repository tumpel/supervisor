use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::watch;
use tumpel_api::types::{
    ChannelHostMessage, ChannelProtocol, HostArchitecture, Sha256Digest, SupervisorUpdateFailureReason,
    SupervisorUpdatePhase,
};

use super::{
    announced_release, confirm, named, refresh_unit, sha256_hex, startup, Idle, Layout, Limits, Outcome, Release,
    Source, Startup, Updater, PENDING_FILE, REFUSAL_FILE, ROLLED_BACK_FILE, TRIAL_FILE,
};
use crate::install::supervisor_unit_text;

const OLD_BINARY: &str = "#!/bin/sh\necho old supervisor\n";
const RELEASE_VERSION: &str = "v0.3.0";

struct FakeIdle(watch::Sender<u32>);

impl Default for FakeIdle {
    fn default() -> Self {
        Self(watch::Sender::new(0))
    }
}

impl Idle for &FakeIdle {
    fn boxes(&self) -> u32 {
        *self.0.borrow()
    }

    fn busy(&self) -> u32 {
        self.boxes()
    }

    fn settled(&self) -> impl Future<Output = ()> + Send {
        let mut busy = self.0.subscribe();
        async move {
            let _ = busy.wait_for(|running| *running == 0).await;
        }
    }
}

struct FakeSource {
    bytes: Vec<u8>,
    sha256: String,
    supervisor_version: Option<&'static str>,
    channel_protocol_version: u32,
    downloads: AtomicU32,
}

impl FakeSource {
    fn binary(script: &str) -> Self {
        Self {
            bytes: script.as_bytes().to_vec(),
            sha256: sha256_hex(script.as_bytes()),
            supervisor_version: None,
            channel_protocol_version: u32::from(ChannelProtocol::VERSION),
            downloads: AtomicU32::new(0),
        }
    }
}

impl Source for FakeSource {
    fn release(&self) -> impl Future<Output = Result<Release, String>> + Send {
        std::future::ready(Ok(Release {
            version: RELEASE_VERSION.parse().expect("a release version"),
            sha256: digest(&self.sha256),
            supervisor_version: self
                .supervisor_version
                .map(|version| version.parse().expect("a supervisor version")),
            channel_protocol_version: self.channel_protocol_version,
        }))
    }

    fn binary(&self) -> impl Future<Output = Result<Vec<u8>, String>> + Send {
        self.downloads.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(self.bytes.clone()))
    }
}

fn digest(hex: &str) -> Sha256Digest {
    hex.parse().expect("a sha256 digest")
}

fn report_script(protocol: u32) -> String {
    format!(
        "#!/bin/sh\necho 'supervisor starts as host h' >&2\necho '{{\"supervisorVersion\":\"0.2.0\",\"channelProtocolVersion\":{protocol},\"guestProtocolVersion\":6}}'\n"
    )
}

fn good_script() -> String {
    report_script(u32::from(ChannelProtocol::VERSION))
}

fn layout(name: &str) -> Layout {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/test-updates")
        .join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("the old fixture is cleared");
    }
    let bin = root.join("bin");
    let state = root.join("state");
    std::fs::create_dir_all(&bin).expect("the bin directory is made");
    std::fs::create_dir_all(&state).expect("the state directory is made");
    let binary = bin.join("tumpel-supervisor");
    std::fs::write(&binary, OLD_BINARY).expect("the running binary is written");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).expect("the binary is executable");
    Layout::new(binary, state, root.join("tumpel-supervisor.service"))
}

fn limits() -> Limits {
    Limits {
        release: Duration::from_secs(5),
        download: Duration::from_secs(5),
        verify: Duration::from_secs(5),
        place: Duration::from_secs(5),
        dry_run: Duration::from_secs(10),
        swap: Duration::from_secs(5),
        online: Duration::from_secs(120),
    }
}

struct Run<'a> {
    updater: Updater<FakeSource, &'a FakeIdle>,
    events: UnboundedReceiver<ChannelHostMessage>,
}

fn updater<'a>(layout: &Layout, source: FakeSource, idle: &'a FakeIdle, limits: Limits) -> Run<'a> {
    let (out, events) = unbounded_channel();
    let running = digest(&sha256_hex(OLD_BINARY.as_bytes()));
    Run {
        updater: Updater::new(source, layout.clone(), limits, running, idle, out),
        events,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Step(SupervisorUpdatePhase),
    Failed(SupervisorUpdateFailureReason),
}

fn seen(events: &mut UnboundedReceiver<ChannelHostMessage>) -> Vec<Seen> {
    std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            ChannelHostMessage::SupervisorUpdateStep(step) => Some(Seen::Step(step.phase)),
            ChannelHostMessage::SupervisorUpdateFailed(failed) => Some(Seen::Failed(failed.reason)),
            _ => None,
        })
        .collect()
}

fn failure(events: &mut UnboundedReceiver<ChannelHostMessage>) -> (String, Vec<String>) {
    std::iter::from_fn(|| events.try_recv().ok())
        .find_map(|event| match event {
            ChannelHostMessage::SupervisorUpdateFailed(failed) => Some((failed.detail, failed.output_tail)),
            _ => None,
        })
        .expect("a SupervisorUpdateFailed was sent")
}

fn text(path: &Path) -> String {
    std::fs::read_to_string(path).expect("the file is readable")
}

fn exec_start_pre(layout: &Layout) {
    let unit = supervisor_unit_text(&layout.binary);
    let line = unit
        .lines()
        .find_map(|line| line.strip_prefix("ExecStartPre="))
        .expect("the unit has an ExecStartPre")
        .replace("$$", "$");
    let (shell, quoted) = line.split_once(" -c '").expect("the ExecStartPre runs a shell script");
    let (script, arguments) = quoted.rsplit_once("' ").expect("the script is quoted");
    let status = std::process::Command::new(shell)
        .arg("-c")
        .arg(script)
        .args(arguments.split(' '))
        .env_remove("XDG_STATE_HOME")
        .env("TUMPEL_SUPERVISOR_DIR", &layout.state)
        .status()
        .expect("the ExecStartPre script runs");
    assert!(status.success(), "the ExecStartPre script exits 0");
}

#[tokio::test]
async fn a_binary_that_does_not_hash_to_the_release_sha256_is_never_run() {
    let layout = layout("checksum");
    let idle = FakeIdle::default();
    let mut source = FakeSource::binary(&good_script());
    source.bytes = b"#!/bin/sh\ntouch ran\n".to_vec();
    let mut run = updater(&layout, source, &idle, limits());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Failed(SupervisorUpdateFailureReason::SupervisorUpdateChecksumMismatch)
    );
    assert_eq!(
        seen(&mut run.events),
        vec![
            Seen::Step(SupervisorUpdatePhase::Fetching),
            Seen::Failed(SupervisorUpdateFailureReason::SupervisorUpdateChecksumMismatch)
        ]
    );
    assert!(
        !layout.candidate.exists(),
        "a mismatched download is never written as the candidate"
    );
    assert_eq!(text(&layout.binary), OLD_BINARY);
    assert!(
        !layout.state.join(REFUSAL_FILE).exists(),
        "a mismatch is fetched again on the next trigger"
    );
}

#[tokio::test]
async fn a_release_naming_the_running_binary_changes_nothing() {
    let layout = layout("current");
    let idle = FakeIdle::default();
    let mut run = updater(&layout, FakeSource::binary(OLD_BINARY), &idle, limits());
    assert_eq!(run.updater.announced(Duration::ZERO).await, Outcome::Current);
    assert_eq!(seen(&mut run.events), vec![]);
}

#[tokio::test]
async fn a_failed_dry_run_keeps_the_running_binary_and_refuses_the_candidate() {
    let layout = layout("dry-run-fails");
    let idle = FakeIdle::default();
    let token = format!("{}-{}", ["sk", "ant", "oat01"].join("-"), "Ab3d".repeat(10));
    let script =
        format!("#!/bin/sh\necho 'NotEnrolled: runtime.json is missing' >&2\necho 'token {token}' >&2\nexit 3\n");
    let mut run = updater(&layout, FakeSource::binary(&script), &idle, limits());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Failed(SupervisorUpdateFailureReason::SupervisorUpdateDryRunFailed)
    );
    let (detail, tail) = failure(&mut run.events);
    assert!(detail.contains("exited with exit status: 3"), "{detail}");
    assert!(detail.contains("the running supervisor stays"), "{detail}");
    assert_eq!(tail, vec!["NotEnrolled: runtime.json is missing", "token [token]"]);
    assert_eq!(text(&layout.binary), OLD_BINARY, "the running binary is untouched");
    assert!(!layout.previous.exists(), "nothing was swapped");
    assert!(!layout.candidate.exists());
    assert!(!layout.state.join(PENDING_FILE).exists());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Refused,
        "the refused candidate is skipped"
    );
    assert_eq!(seen(&mut run.events), vec![]);
}

#[tokio::test]
async fn a_dry_run_killed_by_a_signal_names_the_version_and_the_signal() {
    let layout = layout("dry-run-signal");
    let idle = FakeIdle::default();
    let script = "#!/bin/sh\nkill -FPE $$\n";
    let source = FakeSource {
        supervisor_version: Some("0.1.0"),
        ..FakeSource::binary(script)
    };
    let short = source.sha256[..12].to_string();
    let mut run = updater(&layout, source, &idle, limits());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Failed(SupervisorUpdateFailureReason::SupervisorUpdateDryRunFailed)
    );
    let (detail, _) = failure(&mut run.events);
    assert!(
        detail.starts_with(&format!(
            "the check of supervisor 0.1.0 ({short}) exited with signal: 8 (SIGFPE)"
        )),
        "{detail}"
    );
    assert_eq!(text(&layout.binary), OLD_BINARY, "the running binary is untouched");
}

#[tokio::test]
async fn a_dry_run_speaking_another_channel_protocol_fails() {
    let layout = layout("dry-run-protocol");
    let idle = FakeIdle::default();
    let protocol = u32::from(ChannelProtocol::VERSION);
    let mut source = FakeSource::binary(&report_script(protocol + 1));
    source.channel_protocol_version = protocol;
    let mut run = updater(&layout, source, &idle, limits());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Failed(SupervisorUpdateFailureReason::SupervisorUpdateDryRunFailed)
    );
    let (detail, _) = failure(&mut run.events);
    assert!(
        detail.contains(&format!("speaks channel protocol {}", protocol + 1)),
        "{detail}"
    );
    assert_eq!(text(&layout.binary), OLD_BINARY);
}

#[tokio::test]
async fn a_dry_run_past_its_limit_is_killed_and_fails() {
    let layout = layout("dry-run-hangs");
    let idle = FakeIdle::default();
    let mut limits = limits();
    limits.dry_run = Duration::from_millis(300);
    let mut run = updater(
        &layout,
        FakeSource::binary("#!/bin/sh\necho 'dialing the channel' >&2\nexec sleep 30\n"),
        &idle,
        limits,
    );
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Failed(SupervisorUpdateFailureReason::SupervisorUpdateDryRunFailed)
    );
    let (detail, tail) = failure(&mut run.events);
    assert!(detail.contains("ran past"), "{detail}");
    assert_eq!(tail, vec!["dialing the channel"]);
    assert_eq!(text(&layout.binary), OLD_BINARY);
}

#[tokio::test]
async fn an_unreadable_refusal_file_is_reported_and_not_skipped_silently() {
    let layout = layout("refusal-corrupt");
    let idle = FakeIdle::default();
    std::fs::write(layout.state.join(REFUSAL_FILE), "{").expect("the refusal file is written");
    let mut run = updater(&layout, FakeSource::binary(&good_script()), &idle, limits());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Failed(SupervisorUpdateFailureReason::SupervisorUpdateStateUnreadable)
    );
    let (detail, _) = failure(&mut run.events);
    assert!(detail.contains(REFUSAL_FILE), "{detail}");
    assert!(detail.contains("delete it"), "{detail}");
    assert_eq!(text(&layout.binary), OLD_BINARY);
}

async fn swapped(layout: &Layout) -> (Sha256Digest, Sha256Digest) {
    let script = good_script();
    let idle = FakeIdle::default();
    let run = updater(layout, FakeSource::binary(&script), &idle, limits());
    assert_eq!(run.updater.announced(Duration::ZERO).await, Outcome::Restart);
    (
        digest(&sha256_hex(OLD_BINARY.as_bytes())),
        digest(&sha256_hex(script.as_bytes())),
    )
}

#[tokio::test]
async fn the_start_after_a_swap_puts_the_new_binary_on_trial_before_it_runs() {
    let layout = layout("trial-marked");
    let (_, new) = swapped(&layout).await;
    exec_start_pre(&layout);
    assert!(!layout.state.join(PENDING_FILE).exists());
    assert!(layout.state.join(TRIAL_FILE).exists());
    assert_eq!(*super::binary_sha256(&layout.binary).expect("the binary hashes"), *new);
}

#[tokio::test]
async fn a_new_binary_that_reaches_welcome_ends_its_trial_deletes_the_binary_it_replaced_and_the_next_start_keeps_it() {
    let layout = layout("confirmed");
    let (_, new) = swapped(&layout).await;
    exec_start_pre(&layout);
    assert!(layout.previous.exists());
    let (out, mut events) = unbounded_channel();
    let Startup::Trial(trial) = startup(&layout, &new, &out).expect("startup reads the state") else {
        panic!("the new binary starts its trial");
    };
    confirm(&layout, &trial, &out).expect("the trial ends");
    assert_eq!(seen(&mut events), vec![Seen::Step(SupervisorUpdatePhase::Online)]);
    assert!(!layout.state.join(TRIAL_FILE).exists());
    assert!(!layout.previous.exists());
    exec_start_pre(&layout);
    assert_eq!(*super::binary_sha256(&layout.binary).expect("the binary hashes"), *new);
    assert_eq!(startup(&layout, &new, &out), Ok(Startup::Steady));
}

#[tokio::test]
async fn a_new_binary_that_crashes_before_it_runs_any_code_gives_way_to_the_previous_one_which_reports_it() {
    let layout = layout("crash-before-mark");
    let (old, _) = swapped(&layout).await;
    exec_start_pre(&layout);
    exec_start_pre(&layout);
    assert_eq!(text(&layout.binary), OLD_BINARY, "the previous binary runs again");
    assert!(!layout.state.join(TRIAL_FILE).exists());
    assert!(!layout.state.join(PENDING_FILE).exists());
    assert!(layout.state.join(ROLLED_BACK_FILE).exists());
    let (out, mut events) = unbounded_channel();
    assert_eq!(
        startup(&layout, &old, &out),
        Ok(Startup::Steady),
        "the restored binary starts no trial"
    );
    assert_eq!(
        seen(&mut events),
        vec![Seen::Failed(SupervisorUpdateFailureReason::SupervisorUpdateRolledBack)]
    );
    assert!(
        !layout.state.join(ROLLED_BACK_FILE).exists(),
        "the rollback is reported once"
    );
    exec_start_pre(&layout);
    assert_eq!(text(&layout.binary), OLD_BINARY, "a later start rolls back nothing");
    let idle = FakeIdle::default();
    let run = updater(&layout, FakeSource::binary(&good_script()), &idle, limits());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Refused,
        "the rolled back binary is skipped"
    );
}

#[tokio::test]
async fn a_trial_naming_another_binary_is_deleted_and_starts_no_trial() {
    let layout = layout("trial-stale");
    let (old, _) = swapped(&layout).await;
    exec_start_pre(&layout);
    let (out, _events) = unbounded_channel();
    assert_eq!(startup(&layout, &old, &out), Ok(Startup::Steady));
    assert!(!layout.state.join(TRIAL_FILE).exists());
}

fn unit_before_the_shell_owned_trial(layout: &Layout) -> String {
    format!(
        "[Unit]\nDescription=tumpel supervisor\n\n[Service]\nExecStartPre=/bin/sh -c 'if [ -x \"$$0\" ]; then exec \"$$0\" rollback --binary \"$$1\"; fi' {previous} {binary}\nExecStart={binary} run --update-binary {binary}\nRestart=on-failure\n",
        previous = layout.previous.display(),
        binary = layout.binary.display()
    )
}

#[tokio::test]
async fn a_binary_swapped_in_under_the_old_unit_ends_its_trial_and_rewrites_the_unit() {
    let layout = layout("old-unit");
    let (old, new) = swapped(&layout).await;
    std::fs::write(&layout.unit, unit_before_the_shell_owned_trial(&layout)).expect("the old unit is written");
    let mut legacy: serde_json::Value =
        serde_json::from_str(&text(&layout.state.join(PENDING_FILE))).expect("update-pending.json is JSON");
    legacy["onlineBy"] = serde_json::Value::String("2026-11-16T09:30:00+00:00".to_string());
    std::fs::write(layout.state.join(PENDING_FILE), legacy.to_string()).expect("the old pending file is written");
    let (out, mut events) = unbounded_channel();
    let Startup::Trial(trial) = startup(&layout, &new, &out).expect("startup reads the state") else {
        panic!("a pending swap to the running binary is its trial when no shell marked it");
    };
    assert_eq!(trial.from_sha256, old);
    assert_eq!(refresh_unit(&layout), Ok(true));
    assert_eq!(text(&layout.unit), supervisor_unit_text(&layout.binary));
    confirm(&layout, &trial, &out).expect("the trial ends");
    assert_eq!(seen(&mut events), vec![Seen::Step(SupervisorUpdatePhase::Online)]);
    assert!(!layout.state.join(PENDING_FILE).exists());
    assert!(!layout.state.join(TRIAL_FILE).exists());
    assert_eq!(refresh_unit(&layout), Ok(false), "a current unit is left alone");
    exec_start_pre(&layout);
    assert_eq!(*super::binary_sha256(&layout.binary).expect("the binary hashes"), *new);
    assert_eq!(startup(&layout, &new, &out), Ok(Startup::Steady));
}

#[tokio::test]
async fn a_refused_binary_is_never_downloaded_again() {
    let layout = layout("refused-no-download");
    let idle = FakeIdle::default();
    let script = good_script();
    let refusal = serde_json::json!({
        "toSha256": sha256_hex(script.as_bytes()),
        "reason": "SupervisorUpdateDryRunFailed",
        "refusedAt": "2026-11-16T08:49:00+00:00",
    });
    std::fs::write(layout.state.join(REFUSAL_FILE), refusal.to_string()).expect("the refusal is written");
    let mut run = updater(&layout, FakeSource::binary(&script), &idle, limits());
    assert_eq!(run.updater.announced(Duration::ZERO).await, Outcome::Refused);
    assert_eq!(run.updater.now().await, Outcome::Refused);
    assert_eq!(run.updater.source.downloads.load(Ordering::SeqCst), 0);
    assert_eq!(seen(&mut run.events), vec![]);
    assert!(!layout.candidate.exists());
    assert_eq!(text(&layout.binary), OLD_BINARY);
}

const GRACE: Duration = Duration::from_secs(600);

#[tokio::test]
async fn a_release_notice_updates_an_idle_host_at_once() {
    let layout = layout("notice-idle");
    let idle = FakeIdle::default();
    let mut run = updater(&layout, FakeSource::binary(&good_script()), &idle, limits());
    assert_eq!(run.updater.announced(GRACE).await, Outcome::Restart);
    assert!(!seen(&mut run.events).contains(&Seen::Step(SupervisorUpdatePhase::WaitingForIdle)));
    assert_eq!(text(&layout.binary), good_script());
}

#[tokio::test]
async fn a_release_notice_to_a_busy_host_waits_out_the_grace_period_then_updates() {
    let layout = layout("notice-busy");
    let busy = FakeIdle(watch::Sender::new(1));
    let mut run = updater(&layout, FakeSource::binary(&good_script()), &busy, limits());
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let notice = run.updater.announced(GRACE);
    tokio::pin!(notice);
    tokio::select! {
        outcome = &mut notice => panic!("the busy host updated before its grace period ended: {outcome:?}"),
        () = tokio::time::sleep(Duration::from_secs(599)) => {}
    }
    assert_eq!(
        seen(&mut run.events),
        vec![Seen::Step(SupervisorUpdatePhase::WaitingForIdle)]
    );
    assert_eq!(text(&layout.binary), OLD_BINARY);
    tokio::time::resume();
    assert_eq!(notice.await, Outcome::Restart);
    assert!(started.elapsed() >= GRACE);
    assert_eq!(text(&layout.binary), good_script());
}

#[tokio::test]
async fn a_release_notice_to_a_busy_host_updates_once_its_turns_finish_inside_the_grace_period() {
    let layout = layout("notice-settled");
    let busy = FakeIdle(watch::Sender::new(1));
    let run = updater(&layout, FakeSource::binary(&good_script()), &busy, limits());
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let notice = run.updater.announced(GRACE);
    tokio::pin!(notice);
    tokio::select! {
        outcome = &mut notice => panic!("the busy host updated while its turns ran: {outcome:?}"),
        () = tokio::time::sleep(Duration::from_secs(60)) => {}
    }
    busy.0.send_replace(0);
    tokio::time::resume();
    assert_eq!(notice.await, Outcome::Restart);
    assert!(started.elapsed() < GRACE);
    assert_eq!(text(&layout.binary), good_script());
}

#[test]
fn a_release_manifest_from_a_control_plane_that_serves_no_supervisor_version_is_read() {
    let body = br#"{
        "releaseVersion": "0.0.0-2a2e3f8c1d9b",
        "commit": "2a2e3f8c1d9b2a2e3f8c1d9b2a2e3f8c1d9b2a2e",
        "channelProtocolVersion": 19,
        "supervisors": [
            {
                "architecture": "Amd64",
                "sha256": "1111111111111111111111111111111111111111111111111111111111111111",
                "sizeBytes": 24000000,
                "guestProtocolVersion": 3
            }
        ],
        "clis": [],
        "images": [],
        "bases": [],
        "harnessLayers": []
    }"#;

    let release = announced_release(body, "https://api.example/v1/releases/current", HostArchitecture::Amd64)
        .expect("the manifest an older control plane serves");

    assert_eq!(release.version.as_str(), "0.0.0-2a2e3f8c1d9b");
    assert_eq!(
        release.sha256.as_str(),
        "1111111111111111111111111111111111111111111111111111111111111111"
    );
    assert_eq!(release.channel_protocol_version, 19);
    assert_eq!(named(&release), "111111111111");
}

#[test]
fn a_release_manifest_with_fields_this_supervisor_does_not_know_is_read() {
    let body = br#"{
        "releaseVersion": "v1.4.0",
        "channelProtocolVersion": 20,
        "supervisors": [
            {
                "architecture": "Arm64",
                "sha256": "2222222222222222222222222222222222222222222222222222222222222222",
                "supervisorVersion": "0.4.0",
                "signature": "a field a newer control plane adds"
            },
            {
                "architecture": "Amd64",
                "sha256": "3333333333333333333333333333333333333333333333333333333333333333",
                "supervisorVersion": "0.4.0"
            }
        ],
        "rolloutWindow": { "startsAt": "2026-11-17T09:00:00+01:00" }
    }"#;

    let release = announced_release(body, "https://api.example/v1/releases/current", HostArchitecture::Amd64)
        .expect("the manifest a newer control plane serves");

    assert_eq!(release.version.as_str(), "v1.4.0");
    assert_eq!(
        release.sha256.as_str(),
        "3333333333333333333333333333333333333333333333333333333333333333"
    );
    assert_eq!(release.channel_protocol_version, 20);
    assert_eq!(named(&release), "0.4.0 (333333333333)");
}
