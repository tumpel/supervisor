use std::collections::BTreeMap;
use std::io::{self, Write};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::Instant;

use serde::Serialize;
use tumpel_api::types::{ChannelProtocol, GuestProtocol, Sha256Digest, SupervisorCheckReport};

use crate::firecracker::Firecracker;
use crate::inventory::{self, RecordedBox};
use crate::lifecycle::Vmm;
use crate::pool::Held;
use crate::run::{box_size, channel_url, load_channel_state, load_config, load_key, load_runtime};
use crate::update::{self, Layout};
use crate::{dir, images, lifecycle, usage};

const VERSION: &str = env!("TUMPEL_RELEASE_VERSION");
const CHECKS: [(&str, &str); 11] = [
    ("binary", "read the running binary and its version"),
    ("config", "read the host enrolment and control-plane address"),
    ("runtime", "read the host runtime settings"),
    ("host_key", "read the Ed25519 host key pair"),
    ("channel_state", "read the saved channel ledger"),
    ("inventory", "read the box inventory, images and turns"),
    ("image_cache", "read the cached image manifests"),
    ("usage", "read the host's CPU, memory and disk usage"),
    (
        "attachment",
        "inspect the recorded processes, descriptors and Firecracker APIs",
    ),
    ("handover", "decode the saved legacy handover"),
    ("update_state", "read the saved update records"),
];
const MANIFEST: &str = "manifest";

#[derive(Clone, Copy)]
pub enum ReportFormat {
    Human,
    Json,
    Legacy,
}

#[derive(Clone, Copy, PartialEq, Serialize)]
enum Status {
    #[serde(rename = "PASS")]
    Pass,
    #[serde(rename = "FAIL")]
    Fail,
    #[serde(rename = "NOT RUN")]
    NotRun,
}

#[derive(Serialize)]
struct Check {
    status: Status,
    reason: String,
}

impl Check {
    fn new(status: Status, reason: impl Into<String>) -> Self {
        Self {
            status,
            reason: reason.into(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    version: &'static str,
    binary_digest: Option<Sha256Digest>,
    overall: bool,
    checks: BTreeMap<&'static str, Check>,
}

enum Progress {
    Check(&'static str, Check),
    Digest(Sha256Digest),
}

struct Checks(mpsc::Sender<Progress>);

impl Checks {
    fn check<T>(&self, name: &'static str, success: &str, result: Result<T, impl Into<String>>) -> Option<T> {
        match result {
            Ok(value) => {
                self.send(name, Status::Pass, success);
                Some(value)
            }
            Err(reason) => {
                self.send(name, Status::Fail, reason);
                None
            }
        }
    }

    fn send(&self, name: &'static str, status: Status, reason: impl Into<String>) {
        drop(self.0.send(Progress::Check(name, Check::new(status, reason))));
    }
}

fn read_inventory(path: &Path, inspect_boxes: bool) -> Result<Vec<RecordedBox>, String> {
    let boxes = inventory::load(path)
        .map_err(|error| format!("{error:#}"))?
        .map_or_else(Vec::new, |inventory| inventory.boxes);
    for box_ in boxes.iter().filter(|_| inspect_boxes) {
        lifecycle::decode_box(&box_.record).map_err(|error| {
            format!(
                "box {} has an invalid image or turn record: {error}",
                box_.record.tap_index
            )
        })?;
    }
    Ok(boxes)
}

fn collect(manifest: Option<&Path>, checks: &Checks, legacy: bool) {
    let state = dir::supervisor_dir();
    let layout = Layout::new(PathBuf::from("/proc/self/exe"), state, dir::supervisor_unit_path());
    let digest = checks.check(
        "binary",
        "running binary SHA-256 and version read",
        (|| {
            let digest = update::binary_sha256(&layout.binary)?;
            update::parse_version(VERSION).map_err(|error| {
                format!("the binary has an invalid supervisor version: {error}; reinstall the supervisor")
            })?;
            Ok::<_, String>(digest)
        })(),
    );
    if let Some(digest) = digest.as_ref() {
        drop(checks.0.send(Progress::Digest(digest.clone())));
    }
    checks.check(
        "config",
        "host enrolment and control-plane address loaded",
        load_config(&layout.state)
            .and_then(|config| channel_url(&config.endpoint, &config.host_id))
            .map_err(|failure| failure.detail),
    );
    let runtime = checks.check(
        "runtime",
        "runtime settings loaded",
        load_runtime(&layout.state)
            .and_then(|runtime| box_size(&runtime).map(|_| runtime))
            .map_err(|failure| failure.detail),
    );
    checks.check(
        "host_key",
        "Ed25519 host key pair loaded",
        load_key(&layout.state).map_err(|failure| failure.detail),
    );
    checks.check(
        "channel_state",
        "saved channel ledger loaded",
        load_channel_state(&layout.state).map_err(|failure| failure.detail),
    );
    let boxes = checks.check(
        "inventory",
        "box inventory, images and turns decoded",
        read_inventory(&layout.boxes(), !legacy),
    );
    if !legacy {
        inspect_host(checks, runtime.as_ref(), boxes.as_deref());
        checks.check(
            "handover",
            "legacy handover record decoded",
            read_inventory(&layout.handover(), true),
        );
    }
    checks.check(
        "update_state",
        "saved update records decoded",
        update::validate_state(&layout),
    );
    if let Some(manifest) = manifest {
        if let Some(digest) = digest {
            checks.check(
                MANIFEST,
                "binary digest, version and channel protocol match the manifest",
                check_manifest(manifest, &digest),
            );
        } else {
            checks.send(MANIFEST, Status::NotRun, "the running binary digest is unavailable");
        }
    }
}

fn inspect_host(
    checks: &Checks,
    runtime: Option<&tumpel_api::types::SupervisorRuntime>,
    boxes: Option<&[RecordedBox]>,
) {
    if let Some(runtime) = runtime {
        checks.check(
            "image_cache",
            "cached image manifests decoded",
            images::check_cache(Path::new(&*runtime.image_cache_dir)),
        );
        let held = Held {
            drives: boxes
                .unwrap_or_default()
                .iter()
                .map(|box_| box_.record.session_id.to_string())
                .collect(),
            ..Held::default()
        };
        checks.check(
            "usage",
            "CPU, memory and disk usage read",
            usage::sample(0, Path::new(&*runtime.data_dir), &held).map_err(|error| {
                format!("cannot read host usage: {error:#}; check procfs and dataDir in runtime.json")
            }),
        );
    } else {
        for name in ["image_cache", "usage"] {
            checks.send(name, Status::NotRun, "runtime settings are unavailable");
        }
    }
    match (runtime, boxes) {
        (Some(runtime), Some(boxes)) => {
            let vmm = Firecracker::new(runtime);
            let inspected = (|| -> Result<_, String> {
                let taps = boxes
                    .iter()
                    .map(|box_| u8::try_from(box_.record.tap_index))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())?;
                boxes.iter().try_for_each(|box_| {
                    vmm.observe(box_)
                        .map_err(|error| format!("Inspecting box {} failed. {error}", box_.record.tap_index))
                })?;
                vmm.unclaimed_scopes(&taps)
            })();
            match inspected {
                Ok(taps) if taps.is_empty() => checks.send(
                    "attachment", Status::Pass, "recorded processes, descriptors and Firecracker APIs inspected",
                ),
                Ok(taps) => checks.send(
                    "attachment", Status::Pass,
                    format!("recorded processes, descriptors and Firecracker APIs inspected; scopes without attachment records on TAP devices {taps:?} may be booting or handling setup"),
                ),
                Err(error) => checks.send("attachment", Status::Fail, error),
            }
        }
        _ => checks.send(
            "attachment",
            Status::NotRun,
            "runtime settings or box inventory are unavailable",
        ),
    }
}

fn check_manifest(path: &Path, digest: &Sha256Digest) -> Result<(), String> {
    let architecture = usage::architecture().map_err(|error| error.to_string())?;
    let body = std::fs::read(path).map_err(|error| format!("cannot read the supplied manifest: {error}"))?;
    let release = update::announced_release(&body, "the supplied manifest", &architecture)
        .map_err(|_| "the supplied manifest is invalid or has no supervisor for this architecture")?;
    update::verify_checksum(digest, &release.sha256)?;
    update::verify_version(VERSION, &release, u32::from(ChannelProtocol::VERSION))
}

fn collect_until(manifest: Option<PathBuf>, deadline: Instant, legacy: bool) -> Report {
    let manifest_required = manifest.is_some();
    let (out, wait) = mpsc::channel();
    let started = thread::Builder::new().spawn(move || collect(manifest.as_deref(), &Checks(out), legacy));
    let mut report = Report {
        version: VERSION,
        binary_digest: None,
        overall: false,
        checks: BTreeMap::new(),
    };
    let stopped = if started.is_err() {
        "the report worker could not start"
    } else {
        loop {
            match wait.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(Progress::Check(name, check)) => {
                    report.checks.insert(name, check);
                }
                Ok(Progress::Digest(digest)) => report.binary_digest = Some(digest),
                Err(RecvTimeoutError::Timeout) => break "the report reached its time limit",
                Err(RecvTimeoutError::Disconnected) => break "the report ended before this check finished",
            }
        }
    };
    let mut pending = CHECKS
        .into_iter()
        .filter(|(name, _)| !legacy || !matches!(*name, "image_cache" | "usage" | "attachment" | "handover"))
        .chain(manifest_required.then_some((MANIFEST, "read and verify the supplied manifest")))
        .filter(|(name, _)| !report.checks.contains_key(name));
    if let Some((name, action)) = pending.next() {
        let incomplete = pending
            .map(|(name, _)| (name, Check::new(Status::NotRun, stopped)))
            .collect::<Vec<_>>();
        report
            .checks
            .insert(name, Check::new(Status::Fail, format!("could not {action}: {stopped}")));
        report.checks.extend(incomplete);
    }
    report.overall = report.binary_digest.is_some() && report.checks.values().all(|check| check.status == Status::Pass);
    if !manifest_required {
        report
            .checks
            .insert(MANIFEST, Check::new(Status::NotRun, "no manifest supplied"));
    }
    report
}

pub fn report(format: ReportFormat, manifest: Option<PathBuf>, deadline: Instant) -> ExitCode {
    let report = collect_until(manifest, deadline, matches!(format, ReportFormat::Legacy));
    let stdout = io::stdout().lock();
    let output = match format {
        ReportFormat::Json => write_json(stdout, &report),
        ReportFormat::Human => write_human(stdout, &report),
        ReportFormat::Legacy if report.overall => legacy().and_then(|legacy| write_json(stdout, &legacy)),
        ReportFormat::Legacy => write_human(io::stderr().lock(), &report),
    };
    if let Err(error) = output {
        eprintln!("tumpel-supervisor: writing the report failed: {error}");
        return ExitCode::FAILURE;
    }
    if report.overall {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn write_json(mut out: impl Write, value: &impl Serialize) -> io::Result<()> {
    serde_json::to_writer(&mut out, value)?;
    writeln!(out)
}

fn write_human(mut out: impl Write, report: &Report) -> io::Result<()> {
    let result = if report.overall { "PASS" } else { "FAIL" };
    writeln!(out, "tumpel-supervisor {} {result}", report.version)?;
    if let Some(digest) = &report.binary_digest {
        writeln!(out, "SHA-256 {digest}")?;
    }
    for (name, check) in &report.checks {
        let status = match check.status {
            Status::Pass => "PASS",
            Status::Fail => "FAIL",
            Status::NotRun => "NOT RUN",
        };
        writeln!(out, "{status} {name} - {}", check.reason)?;
    }
    Ok(())
}

fn legacy() -> io::Result<SupervisorCheckReport> {
    let version = |value| NonZeroU32::new(u32::from(value)).ok_or_else(|| io::Error::other("a protocol version is 0"));
    Ok(SupervisorCheckReport {
        supervisor_version: VERSION.parse().map_err(io::Error::other)?,
        channel_protocol_version: version(ChannelProtocol::VERSION)?,
        guest_protocol_version: version(GuestProtocol::VERSION)?,
    })
}
