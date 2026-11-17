use clap::{Parser, Subcommand};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::ExitCode;
use tumpel_api::types::SupervisorFailure;
use tumpel_supervisor::{
    check, enrol, install, install_telemetry, on_trial, received_descriptors, run, supervisor_dir, Enrolment, Stopped,
};

#[derive(Parser)]
#[command(
    name = "tumpel-supervisor",
    version,
    about = "Runs tumpel boxes on this host and keeps its channel to the control plane"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
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

fn exit_code(failure: SupervisorFailure) -> ExitCode {
    match failure {
        SupervisorFailure::NotEnrolled => ExitCode::from(3),
        SupervisorFailure::ChannelStateLost => ExitCode::from(4),
        SupervisorFailure::HostRevoked => ExitCode::from(5),
        SupervisorFailure::ProtocolError => ExitCode::from(6),
        SupervisorFailure::UsageUnreadable => ExitCode::from(7),
        SupervisorFailure::GuestDriveUnwritable => ExitCode::from(8),
        SupervisorFailure::UpdateNotOnline => ExitCode::from(10),
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let received = match unsafe { received_descriptors() } {
        Ok(received) => received,
        Err(error) => {
            eprintln!("tumpel-supervisor: {error}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("tumpel-supervisor: starting the async runtime failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    match cli.command {
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
                Ok(Stopped::HandedOver { binary, handover }) => {
                    let error = std::process::Command::new(&binary).args(["run", "--handover"]).exec();
                    eprintln!(
                        "tumpel-supervisor: {}: starting {} to take over {} descriptors of the running boxes failed: {error}; systemd starts the unit again, and its ExecStartPre puts the previous binary back",
                        SupervisorFailure::UpdateNotOnline,
                        binary.display(),
                        handover.descriptors.len()
                    );
                    exit_code(SupervisorFailure::UpdateNotOnline)
                }
                Err(failure) if on_trial(&supervisor_dir()) => {
                    eprintln!(
                    "tumpel-supervisor: {}: {}: {}; this binary is on its update trial, so systemd restarts the unit and its ExecStartPre puts the previous binary back",
                    SupervisorFailure::UpdateNotOnline,
                    failure.kind,
                    failure.detail
                );
                    exit_code(SupervisorFailure::UpdateNotOnline)
                }
                Err(failure) => {
                    eprintln!("tumpel-supervisor: {}: {}", failure.kind, failure.detail);
                    exit_code(failure.kind)
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
        assert!(matches!(cli.command, Command::Run { .. }));
    }
}
