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

fn daemon(handover: bool) -> ExitCode {
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
        match run(telemetry, handover, received).await {
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

fn show_report(format: ReportFormat, manifest: Option<PathBuf>) -> ExitCode {
    const SECONDS: u32 = 60;
    const OUTPUT_TIME: Duration = Duration::from_millis(100);
    let deadline = Instant::now() + Duration::from_secs(u64::from(SECONDS)).saturating_sub(OUTPUT_TIME);
    let handler = report_timed_out as *const () as libc::sighandler_t;
    if unsafe { libc::signal(libc::SIGALRM, handler) } == libc::SIG_ERR {
        eprintln!(
            "tumpel-supervisor: setting the report deadline failed: {}",
            std::io::Error::last_os_error()
        );
        return ExitCode::FAILURE;
    }
    unsafe { libc::alarm(SECONDS) };
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
        Some(Command::Run { handover, .. }) => daemon(handover),
        Some(Command::Check) => show_report(ReportFormat::Legacy, None),
        None if cli.daemon => daemon(false),
        None => {
            let format = if cli.json {
                ReportFormat::Json
            } else {
                ReportFormat::Human
            };
            show_report(format, cli.manifest)
        }
    }
}
