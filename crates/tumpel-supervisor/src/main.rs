use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use tumpel_api::types::SupervisorFailure;
use tumpel_supervisor::{
    check, enrol, install, install_telemetry, received_descriptors, report, run, take_notify_socket, Enrolment,
    RunFailure, Stopped,
};

#[derive(Parser)]
#[command(
    name = "tumpel-supervisor",
    version,
    about = "Report host readiness or run tumpel boxes"
)]
struct Cli {
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
    /// Dial out to the control plane and hold the host's channel, replacing this binary when the release names a new one.
    Run {
        #[arg(long, hide = true)]
        update_binary: Option<PathBuf>,
        #[arg(long, hide = true)]
        handover: bool,
    },
    /// Check that this binary starts on this host the way `run` does and reaches the channel's Challenge, then print a `SupervisorCheckReport`.
    Check,
}

const RESTART_INTO_UPDATE: u8 = 75;

fn exit_code(failure: &SupervisorFailure) -> ExitCode {
    match failure {
        SupervisorFailure::NotEnrolled => ExitCode::from(3),
        SupervisorFailure::ChannelStateLost => ExitCode::from(4),
        SupervisorFailure::HostRevoked => ExitCode::from(5),
        SupervisorFailure::ProtocolError => ExitCode::from(6),
        SupervisorFailure::UsageUnreadable => ExitCode::from(7),
        SupervisorFailure::GuestDriveUnwritable => ExitCode::from(8),
        failure => {
            if matches!(failure, SupervisorFailure::Unrecognised(_)) {
                eprintln!("{}", tumpel_channel::UPDATE_REQUIRED);
            }
            ExitCode::FAILURE
        }
    }
}

fn main() -> ExitCode {
    let Some(command) = Cli::parse().command else {
        return report();
    };
    let received = match unsafe { received_descriptors() } {
        Ok(received) => received,
        Err(error) => {
            eprintln!("tumpel-supervisor: {error}");
            return ExitCode::FAILURE;
        }
    };
    let handed = matches!(command, Command::Run { handover: true, .. });
    if let Err(error) = unsafe { take_notify_socket(handed) } {
        eprintln!("tumpel-supervisor: {error}");
        return ExitCode::FAILURE;
    }
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("tumpel-supervisor: starting the async runtime failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    match command {
        Command::Enrol {
            endpoint,
            token_file,
            name,
        } => match runtime.block_on(enrol(&endpoint, &token_file, name)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("tumpel-supervisor: {error:#}");
                ExitCode::FAILURE
            }
        },
        Command::Install { endpoint, token_file } => {
            let enrolment = endpoint
                .zip(token_file)
                .map(|(endpoint, token_file)| Enrolment { endpoint, token_file });
            match runtime.block_on(install(enrolment)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("tumpel-supervisor: {error:#}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Run {
            update_binary: _,
            handover,
        } => {
            let telemetry = match install_telemetry() {
                Ok(telemetry) => telemetry,
                Err(detail) => {
                    eprintln!("tumpel-supervisor: {detail}");
                    return ExitCode::FAILURE;
                }
            };
            match runtime.block_on(run(telemetry, handover, received)) {
                Ok(Stopped::Revoked | Stopped::Terminated) => ExitCode::SUCCESS,
                Ok(Stopped::Updated) => {
                    eprintln!("tumpel-supervisor: exiting so systemd starts the new binary, which takes the running boxes over");
                    ExitCode::from(RESTART_INTO_UPDATE)
                }
                Err(RunFailure::Startup(detail)) => {
                    eprintln!("tumpel-supervisor: startup failed. {detail}. On failure, systemd restarts the supervisor and it takes over the running boxes.");
                    ExitCode::FAILURE
                }
                Err(RunFailure::Supervisor(failure)) => {
                    eprintln!("tumpel-supervisor: {}: {}", failure.kind, failure.detail);
                    exit_code(&failure.kind)
                }
            }
        }
        Command::Check => match runtime.block_on(check()) {
            Ok(report) => match serde_json::to_string(&report) {
                Ok(line) => {
                    println!("{line}");
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("tumpel-supervisor: encoding the SupervisorCheckReport failed: {error}");
                    ExitCode::FAILURE
                }
            },
            Err(error) => {
                eprintln!("tumpel-supervisor: check failed: {error}");
                ExitCode::FAILURE
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use clap::Parser;

    #[test]
    fn the_exec_start_of_a_unit_written_before_the_shell_owned_trial_starts_this_binary() {
        let exec_start =
            "/home/u/.local/bin/tumpel-supervisor run --update-binary /home/u/.local/bin/tumpel-supervisor";
        let cli = Cli::try_parse_from(exec_start.split(' ')).expect("the old ExecStart parses");
        assert!(matches!(cli.command, Some(Command::Run { .. })));
    }
}
