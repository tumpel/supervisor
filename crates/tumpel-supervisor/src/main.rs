use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};
use tumpel_api::types::SupervisorFailure;
use tumpel_supervisor::{
    enrol, install, install_telemetry, on_trial, received_descriptors, report, run, supervisor_dir, take_notify_socket,
    Enrolment, ReportFormat, Stopped,
};

#[derive(Parser)]
#[command(
    args_conflicts_with_subcommands = true,
    name = "tumpel-supervisor",
    version = env!("TUMPEL_RELEASE_VERSION"),
    about = "Report whether this host is ready to run tumpel boxes"
)]
struct Cli {
    #[arg(long, conflicts_with_all = ["json", "manifest"], help = "Start host supervision")]
    daemon: bool,
    #[arg(long, help = "Print the report as JSON")]
    json: bool,
    #[arg(
        long,
        value_name = "PATH",
        help = "Verify the running binary against a downloaded release manifest"
    )]
    manifest: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Enrol this machine as a host with a one-time enrolment token.
    Enrol {
        /// The control plane's base URL, as the install command names it.
        #[arg(long)]
        endpoint: String,
        /// A file holding the enrolment token, or - for stdin; the token never goes on the command line.
        #[arg(long)]
        token_file: PathBuf,
        /// The name the host is listed under; defaults to the host name.
        #[arg(long)]
        name: Option<String>,
    },
    /// Prepare this host once: Firecracker, the guest kernel, the TAP pool, the guest filter and the user unit.
    /// With --endpoint and --token-file it also enrols the host and starts the unit.
    Install {
        /// The control plane's base URL to enrol with.
        #[arg(long, requires = "token_file")]
        endpoint: Option<String>,
        /// A file holding the enrolment token, or - for stdin; the token never goes on the command line.
        #[arg(long, requires = "endpoint")]
        token_file: Option<PathBuf>,
    },
    #[command(hide = true)]
    Run {
        #[arg(long, hide = true, conflicts_with = "handover")]
        update_binary: Option<PathBuf>,
        #[arg(long, hide = true)]
        handover: bool,
    },
    #[command(hide = true)]
    Check,
}

const RESTART_INTO_UPDATE: u8 = 75;

fn exit_code(failure: &SupervisorFailure) -> ExitCode {
    match failure {
        SupervisorFailure::Unrecognised(_) => {
            eprintln!("{}", tumpel_channel::UPDATE_REQUIRED);
            ExitCode::FAILURE
        }
        SupervisorFailure::NotEnrolled => ExitCode::from(3),
        SupervisorFailure::ChannelStateLost => ExitCode::from(4),
        SupervisorFailure::HostRevoked => ExitCode::from(5),
        SupervisorFailure::ProtocolError => ExitCode::from(6),
        SupervisorFailure::UsageUnreadable => ExitCode::from(7),
        SupervisorFailure::GuestDriveUnwritable => ExitCode::from(8),
        SupervisorFailure::UpdateNotOnline => ExitCode::from(10),
    }
}

fn with_runtime(task: impl std::future::Future<Output = ExitCode>) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("tumpel-supervisor: starting the async runtime failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(task)
}

const DAEMON_START_SECONDS: u32 = 120;
const STARTUP_TIMED_OUT: libc::c_int = 11;
const REPORT_SECONDS: u32 = 60;

fn daemon(handover: bool, seconds: u32) -> ExitCode {
    if let Err(error) = arm_deadline(seconds, daemon_timed_out) {
        eprintln!("tumpel-supervisor: setting the daemon startup deadline failed: {error}");
        return ExitCode::FAILURE;
    }
    let received = match unsafe { received_descriptors() } {
        Ok(received) => received,
        Err(error) => {
            eprintln!("tumpel-supervisor: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = unsafe { take_notify_socket(handover) } {
        eprintln!("tumpel-supervisor: {error}");
        return ExitCode::FAILURE;
    }
    let telemetry = match install_telemetry() {
        Ok(telemetry) => telemetry,
        Err(detail) => {
            eprintln!("tumpel-supervisor: {detail}");
            return ExitCode::FAILURE;
        }
    };
    with_runtime(async {
        match run(telemetry, handover, received, cancel_deadline).await {
            Ok(Stopped::Revoked | Stopped::Terminated) => ExitCode::SUCCESS,
            Ok(Stopped::Updated) => {
                eprintln!(
                    "tumpel-supervisor: exiting so systemd starts the new binary, which takes the running boxes over"
                );
                ExitCode::from(RESTART_INTO_UPDATE)
            }
            Err(failure) if on_trial(&supervisor_dir()) => {
                eprintln!(
                "tumpel-supervisor: {}: {}: {}; this binary is on its update trial, so systemd restarts the unit and its ExecStartPre puts the previous binary back",
                SupervisorFailure::UpdateNotOnline,
                failure.kind,
                failure.detail
            );
                exit_code(&SupervisorFailure::UpdateNotOnline)
            }
            Err(failure) => {
                eprintln!("tumpel-supervisor: {}: {}", failure.kind, failure.detail);
                exit_code(&failure.kind)
            }
        }
    })
}

extern "C" fn report_timed_out(_: libc::c_int) {
    unsafe { libc::_exit(1) };
}

extern "C" fn daemon_timed_out(_: libc::c_int) {
    let message =
        b"tumpel-supervisor: startup reached its time limit; check state storage and Firecracker before restarting\n";
    let mut stderr = libc::pollfd {
        fd: libc::STDERR_FILENO,
        events: libc::POLLOUT,
        revents: 0,
    };
    unsafe {
        if libc::poll(&raw mut stderr, 1, 0) > 0 && stderr.revents & libc::POLLOUT != 0 {
            libc::write(libc::STDERR_FILENO, message.as_ptr().cast(), message.len());
        }
        libc::_exit(STARTUP_TIMED_OUT);
    }
}

fn arm_deadline(seconds: u32, handler: extern "C" fn(libc::c_int)) -> std::io::Result<()> {
    if unsafe { libc::signal(libc::SIGALRM, handler as *const () as libc::sighandler_t) } == libc::SIG_ERR {
        return Err(std::io::Error::last_os_error());
    }
    unsafe { libc::alarm(seconds) };
    Ok(())
}

fn cancel_deadline() {
    unsafe { libc::alarm(0) };
}

fn show_report(format: ReportFormat, manifest: Option<PathBuf>, seconds: u32) -> ExitCode {
    const OUTPUT_TIME: Duration = Duration::from_millis(100);
    let deadline = Instant::now() + Duration::from_secs(u64::from(seconds)).saturating_sub(OUTPUT_TIME);
    if let Err(error) = arm_deadline(seconds, report_timed_out) {
        eprintln!("tumpel-supervisor: setting the report deadline failed: {error}");
        return ExitCode::FAILURE;
    }
    report(format, manifest, deadline)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Enrol {
            endpoint,
            token_file,
            name,
        }) => with_runtime(async {
            match enrol(&endpoint, &token_file, name).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("tumpel-supervisor: {error:#}");
                    ExitCode::FAILURE
                }
            }
        }),
        Some(Command::Install { endpoint, token_file }) => with_runtime(async {
            let enrolment = endpoint
                .zip(token_file)
                .map(|(endpoint, token_file)| Enrolment { endpoint, token_file });
            match install(enrolment).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("tumpel-supervisor: {error:#}");
                    ExitCode::FAILURE
                }
            }
        }),
        Some(Command::Run { handover, .. }) => daemon(handover, DAEMON_START_SECONDS),
        Some(Command::Check) => show_report(ReportFormat::Legacy, None, REPORT_SECONDS),
        None if cli.daemon => daemon(false, DAEMON_START_SECONDS),
        None => {
            let format = if cli.json {
                ReportFormat::Json
            } else {
                ReportFormat::Human
            };
            show_report(format, cli.manifest, REPORT_SECONDS)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{daemon, show_report, ReportFormat};

    #[test]
    fn deadline_child() {
        if let Some(mode) = std::env::var_os("TUMPEL_TEST_STARTUP_MODE") {
            let result = if mode == "daemon" {
                daemon(false, 1)
            } else {
                show_report(ReportFormat::Json, None, 1)
            };
            std::process::exit(i32::from(result == std::process::ExitCode::FAILURE));
        }
    }

    #[tokio::test]
    async fn a_blocked_host_loader_cannot_hang_startup_or_the_report() {
        let dir = std::env::temp_dir().join(format!("tumpel-startup-deadline-{}", std::process::id()));
        std::fs::create_dir(&dir).expect("test state directory");
        let blocked = dir.join("supervisor.json");
        let path = std::ffi::CString::new(blocked.as_os_str().as_encoded_bytes()).expect("FIFO path");
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        for (mode, code) in [("daemon", super::STARTUP_TIMED_OUT), ("report", 1)] {
            let mut command = tokio::process::Command::new(std::env::current_exe().expect("test binary"));
            let output = tokio::time::timeout(
                Duration::from_secs(5),
                command
                    .args(["--exact", "tests::deadline_child", "--nocapture"])
                    .env_clear()
                    .env("TUMPEL_SUPERVISOR_DIR", &dir)
                    .env("TUMPEL_TEST_STARTUP_MODE", mode)
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .expect("the blocked loader is bounded")
            .expect("the child starts");
            assert_eq!(output.status.code(), Some(code), "{output:?}");
            if mode == "daemon" {
                assert!(String::from_utf8_lossy(&output.stderr).contains("startup reached its time limit"));
            } else {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let json = stdout.lines().find(|line| line.starts_with('{')).expect("JSON report");
                let report: serde_json::Value = serde_json::from_str(json).expect("complete JSON report");
                assert_eq!(report["overall"], false);
                assert_eq!(report["checks"]["config"]["status"], "FAIL");
                assert!(report["checks"]["config"]["reason"]
                    .as_str()
                    .expect("failure reason")
                    .contains("time limit"));
            }
        }
        std::fs::remove_dir_all(dir).expect("remove the FIFO fixture");
    }
}
