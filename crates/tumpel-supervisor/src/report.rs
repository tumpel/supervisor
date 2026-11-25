use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::ExitCode;
use tumpel_api::types::{ChannelProtocol, SupervisorHandover};
use tumpel_guest::setup_output::{redacted, strip_ansi};

use crate::run::start;
use crate::update::{BOXES_FILE, HANDOVER_FILE};
use crate::{dir, guest_drive, lifecycle};

pub(crate) const CHANNEL_PROTOCOL_LINE: &str = "PASS channel protocol ";

pub fn report() -> ExitCode {
    match write_report(io::stdout().lock()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("tumpel-supervisor: writing the report failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn write_report(mut out: impl Write) -> io::Result<bool> {
    let state = dir::supervisor_dir();
    let started = start();
    let runtime = started.as_ref().map(|started| &started.runtime);
    let unavailable = |_| "startup failed before the runtime settings could be checked".to_string();
    let mut passed = true;
    for (name, result) in [
        (
            "startup",
            started
                .as_ref()
                .map(|_| "host settings, host key and channel.json loaded")
                .map_err(|failure| format!("{}: {}", failure.kind, failure.detail)),
        ),
        (BOXES_FILE, read_boxes(&state.join(BOXES_FILE))),
        (HANDOVER_FILE, read_boxes(&state.join(HANDOVER_FILE))),
        ("guest drive", guest_drive::check()),
        (
            "image cache",
            runtime.map_err(unavailable).and_then(|runtime| {
                guest_drive::access(Path::new(&*runtime.image_cache_dir)).map(|()| "the cache can hold the guest drive")
            }),
        ),
        (
            "Firecracker",
            runtime
                .map_err(unavailable)
                .and_then(|runtime| check_firecracker(Path::new(&*runtime.firecracker_path))),
        ),
    ] {
        match result {
            Ok(why) => writeln!(out, "PASS {name}: {why}")?,
            Err(why) => {
                passed = false;
                let why = redacted(&strip_ansi(&why)).replace(['\n', '\r'], " ");
                writeln!(out, "FAIL {name}: {why}")?;
            }
        }
    }
    writeln!(out, "{CHANNEL_PROTOCOL_LINE}{}", ChannelProtocol::VERSION)?;
    Ok(passed)
}

fn read_boxes(path: &Path) -> Result<&'static str, String> {
    let Some(record) = dir::read_json::<SupervisorHandover>(path, "SupervisorHandover")
        .map_err(|error| format!("{error:#}; restore a valid box record"))?
    else {
        return Ok("no saved record");
    };
    for handed in &record.boxes {
        lifecycle::decode_box(handed)
            .map_err(|error| format!("box {} has an invalid record: {error}", handed.box_id))?;
    }
    Ok("saved boxes decoded")
}

fn check_firecracker(path: &Path) -> Result<&'static str, String> {
    let fix = "set firecrackerPath in runtime.json to an executable Firecracker binary";
    let metadata =
        std::fs::metadata(path).map_err(|error| format!("cannot read {}: {error}; {fix}", path.display()))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(format!("{} is not an executable file; {fix}", path.display()));
    }
    Ok("configured binary is executable")
}
