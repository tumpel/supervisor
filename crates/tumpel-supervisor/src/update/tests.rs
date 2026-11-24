use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::watch;
use tumpel_api::types::{
    ChannelHostMessage, ChannelProtocol, HostArchitecture, Sha256Digest, SupervisorUpdateFailureReason,
    SupervisorUpdatePhase, SupervisorUpdateRefusal,
};
use tumpel_channel::Outgoing;

use super::{
    announced_release, named, refresh_unit, sha256_hex, version_refusal, Idle, Layout, Limits, Outcome, Release,
    Source, Updater, REFUSAL_FILE,
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
    supervisor_version: &'static str,
    channel_protocol_version: u32,
    rollback: bool,
    downloads: AtomicU32,
}

impl FakeSource {
    fn binary(script: &str) -> Self {
        Self {
            bytes: script.as_bytes().to_vec(),
            sha256: sha256_hex(script.as_bytes()),
            supervisor_version: "0.4.0",
            channel_protocol_version: u32::from(ChannelProtocol::VERSION),
            rollback: false,
            downloads: AtomicU32::new(0),
        }
    }
}

impl Source for FakeSource {
    fn release(&self) -> impl Future<Output = Result<Release, String>> + Send {
        std::future::ready(Ok(Release {
            version: RELEASE_VERSION.parse().expect("a release version"),
            sha256: digest(&self.sha256),
            supervisor_version: self.supervisor_version.parse().expect("a supervisor version"),
            channel_protocol_version: self.channel_protocol_version,
            rollback: self.rollback,
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
        "#!/bin/sh\n[ \"$#\" -eq 0 ] || exit 2\necho 'PASS startup: host settings loaded'\necho 'PASS channel protocol {protocol}'\n"
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
    }
}

struct Run<'a> {
    updater: Updater<FakeSource, &'a FakeIdle>,
    events: UnboundedReceiver<Outgoing>,
}

fn updater<'a>(layout: &Layout, source: FakeSource, idle: &'a FakeIdle, limits: Limits) -> Run<'a> {
    let (out, events) = unbounded_channel();
    let running = digest(&sha256_hex(OLD_BINARY.as_bytes()));
    Run {
        updater: Updater::new(source, layout.clone(), limits, running, "0.3.0-rc.5", idle, out),
        events,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Step(SupervisorUpdatePhase),
    Failed(SupervisorUpdateFailureReason),
}

fn seen(events: &mut UnboundedReceiver<Outgoing>) -> Vec<Seen> {
    std::iter::from_fn(|| events.try_recv().ok())
        .filter_map(|event| match event {
            Outgoing::Message(ChannelHostMessage::SupervisorUpdateStep(step)) => Some(Seen::Step(step.phase)),
            Outgoing::Message(ChannelHostMessage::SupervisorUpdateFailed(failed)) => Some(Seen::Failed(failed.reason)),
            _ => None,
        })
        .collect()
}

fn failure(events: &mut UnboundedReceiver<Outgoing>) -> (String, Vec<String>) {
    std::iter::from_fn(|| events.try_recv().ok())
        .find_map(|event| match event {
            Outgoing::Message(ChannelHostMessage::SupervisorUpdateFailed(failed)) => Some((
                failed.detail.into(),
                failed.output_tail.into_iter().map(String::from).collect(),
            )),
            _ => None,
        })
        .expect("a SupervisorUpdateFailed was sent")
}

fn text(path: &Path) -> String {
    std::fs::read_to_string(path).expect("the file is readable")
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
    let boxes = "FAIL boxes.json: invalid box record";
    let firecracker = "FAIL Firecracker: binary is missing";
    let script = format!("#!/bin/sh\necho '{boxes}'\necho '{firecracker}'\necho 'token {token}' >&2\nexit 1\n");
    let mut run = updater(&layout, FakeSource::binary(&script), &idle, limits());
    assert_eq!(
        run.updater.announced(Duration::ZERO).await,
        Outcome::Failed(SupervisorUpdateFailureReason::SupervisorUpdateDryRunFailed)
    );
    let (detail, tail) = failure(&mut run.events);
    assert!(detail.contains("exited with exit status: 1"), "{detail}");
    assert!(detail.contains(boxes), "{detail}");
    assert!(detail.contains(firecracker), "{detail}");
    assert!(detail.contains("the running supervisor stays"), "{detail}");
    assert_eq!(tail, vec!["token [token]", boxes, firecracker]);
    assert_eq!(text(&layout.binary), OLD_BINARY, "the running binary is untouched");
    assert!(!layout.candidate.exists());
    let refusal: SupervisorUpdateRefusal =
        serde_json::from_str(&text(&layout.refusal())).expect("the refusal is written");
    assert_eq!(*refusal.to_sha256, sha256_hex(script.as_bytes()));
    assert_eq!(
        refusal.reason,
        SupervisorUpdateFailureReason::SupervisorUpdateDryRunFailed
    );
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
        supervisor_version: "0.3.1",
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
            "the report from supervisor 0.3.1 ({short}) exited with signal: 8 (SIGFPE)"
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
    assert_eq!(run.updater.announced(Duration::ZERO).await, Outcome::Refused);
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

#[test]
fn refresh_unit_replaces_an_installed_unit_with_a_rollback_line() {
    let layout = layout("old-unit");
    let current = supervisor_unit_text(&layout.binary);
    let old = current.replacen("[Service]\n", "[Service]\nExecStartPre=/bin/sh -c 'exit 0'\n", 1);
    std::fs::write(&layout.unit, old).expect("the old unit is written");

    assert_eq!(refresh_unit(&layout), Ok(true));
    let rewritten = text(&layout.unit);
    assert_eq!(rewritten, current);
    assert!(!rewritten.contains("ExecStartPre="));
    let exec_start = format!("ExecStart={} run", layout.binary.display());
    assert!(rewritten.lines().any(|line| line == exec_start));
    assert_eq!(refresh_unit(&layout), Ok(false));
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
async fn a_release_notice_replaces_the_binary_on_an_idle_host() {
    let layout = layout("notice-idle");
    let idle = FakeIdle::default();
    let mut run = updater(&layout, FakeSource::binary(&good_script()), &idle, limits());
    assert_eq!(run.updater.announced(GRACE).await, Outcome::Restart);
    assert!(!seen(&mut run.events).contains(&Seen::Step(SupervisorUpdatePhase::WaitingForIdle)));
    assert_eq!(text(&layout.binary), good_script());
    assert!(!layout.candidate.exists());
}

#[tokio::test]
async fn an_announced_rollback_installs_an_older_supervisor_version() {
    assert_eq!(version_refusal("0.3.0-rc.5", "0.3.0-rc.4", true), None);
    assert!(version_refusal("0.3.0-rc.5", "0.3.0-rc.5", true).is_some());

    let layout = layout("rollback");
    let idle = FakeIdle::default();
    let source = FakeSource {
        supervisor_version: "0.2.1-main.1",
        rollback: true,
        ..FakeSource::binary(&good_script())
    };
    let run = updater(&layout, source, &idle, limits());
    assert_eq!(run.updater.announced(Duration::ZERO).await, Outcome::Restart);
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
fn a_release_manifest_with_unknown_architectures_and_extra_fields_keeps_the_known_release() {
    use tracing_subscriber::layer::SubscriberExt;
    let logs = crate::test_logs::UpdateLogs::default();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.clone()));
    let body = br#"{
        "releaseVersion": "v1.4.0",
        "channelProtocolVersion": 20,
        "supervisors": [
            {
                "architecture": "FutureArchitecture",
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

    let release = announced_release(
        body,
        "https://api.example/v1/releases/current",
        &HostArchitecture::Amd64,
    )
    .expect("the manifest a newer control plane serves");

    assert_eq!(release.version.as_str(), "v1.4.0");
    assert_eq!(
        release.sha256.as_str(),
        "3333333333333333333333333333333333333333333333333333333333333333"
    );
    assert_eq!(release.channel_protocol_version, 20);
    assert_eq!(named(&release), "0.4.0 (333333333333)");
    assert!(!release.rollback);

    let rollback = String::from_utf8(body.to_vec()).expect("a utf-8 manifest").replacen(
        "\"releaseVersion\"",
        "\"rollback\": true,\n        \"releaseVersion\"",
        1,
    );
    let release = announced_release(
        rollback.as_bytes(),
        "https://api.example/v1/releases/current",
        &HostArchitecture::Amd64,
    )
    .expect("the manifest of a rollback");
    assert!(release.rollback);
    assert_eq!(*logs.0.lock().expect("logs"), [tumpel_channel::UPDATE_REQUIRED; 2]);
}

#[tokio::test]
async fn an_update_goes_only_to_a_newer_supervisor_version() {
    assert_eq!(version_refusal("0.3.0-rc.4", "0.3.0-rc.5", false), None);
    assert_eq!(version_refusal("0.3.0-rc.5", "0.3.0", false), None);
    assert_eq!(version_refusal("v0.3.0-rc.5", "0.3.0", false), None);
    assert_eq!(version_refusal("0.3.0-main.1", "0.3.0-rc.5", false), None);
    assert_eq!(
        version_refusal("0.3.0-rc.5", "0.3.0-rc.4", false),
        Some(
            "the release names supervisor 0.3.0-rc.4, older than the running 0.3.0-rc.5, and is no rollback; publish a newer release or a rollback"
                .to_string()
        )
    );
    assert!(version_refusal("0.3.0", "0.3.0-rc.5", false).is_some());
    assert!(version_refusal("0.3.0-rc.5", "0.3.0-main.1", false).is_some());
    assert!(version_refusal("0.3.0-rc.5", "0.2.1-main.1", false).is_some());
    assert_eq!(
        version_refusal("0.3.0-rc.5", "0.3.0-rc.5", false),
        Some("the release names supervisor 0.3.0-rc.5, the running version 0.3.0-rc.5, with another binary; publish the new binary under a new version".to_string())
    );
    assert_eq!(version_refusal("0.0.0-012345678901", "0.3.0-rc.4", false), None);
    assert!(version_refusal("0.3.0-rc.5", "latest", false).is_some());

    let layout = layout("older-version");
    let idle = FakeIdle::default();
    let source = FakeSource {
        supervisor_version: "0.2.1-main.1",
        ..FakeSource::binary(&good_script())
    };
    let run = updater(&layout, source, &idle, limits());
    assert_eq!(run.updater.announced(Duration::ZERO).await, Outcome::Refused);
    assert_eq!(
        text(&layout.binary),
        OLD_BINARY,
        "an announced downgrade leaves the running binary"
    );
    assert_eq!(
        run.updater.now().await,
        Outcome::Restart,
        "a downgrade the control plane demands installs"
    );
    assert_eq!(text(&layout.binary), good_script());
}
