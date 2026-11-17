use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tumpel_api::types::RepositoryCloneFailureCause;

use crate::harness::STDERR_TAIL;
use crate::memory::Confinement;

pub const TOKEN: &str = "GITHUB_TOKEN";
const HELPER_KEY: &str = "credential.https://github.com.helper";
const HELPER: &str =
    "!f() { test \"$1\" = get && printf 'username=x-access-token\\npassword=%s\\n' \"$GITHUB_TOKEN\"; }; f";
const PRE_PUSH: &str = include_str!("repository/pre-push.sh");
const ASKED: &str = "TUMPEL_CLONE_CREDENTIAL_LOG";
const RECORDER: &str = "credential.helper=!f() { printf '%s\\n' \"$1\" >> \"$TUMPEL_CLONE_CREDENTIAL_LOG\"; }; f";
const PREBUILT: &str = ".git/tumpel-prebuilt";
const START_BUDGET: &str = "TUMPEL_PREBUILD_SECONDS";

#[derive(Debug)]
pub struct CloneFailure {
    pub cause: RepositoryCloneFailureCause,
    pub detail: String,
}

fn failure(cause: RepositoryCloneFailureCause, detail: String) -> CloneFailure {
    CloneFailure { cause, detail }
}

fn remove_file(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            Err(format!("removing {}: {error}", path.display()))
        }
        _ => Ok(()),
    }
}

fn refused(log: &str) -> bool {
    let operations: Vec<&str> = log.lines().collect();
    operations.contains(&"get") && !operations.contains(&"store")
}

pub fn git_environment(author_name: &str, author_email: &str, token: Option<&str>) -> Vec<(String, String)> {
    let mut environment: Vec<(String, String)> = [
        ("GIT_TERMINAL_PROMPT", "0"),
        ("GIT_AUTHOR_NAME", author_name),
        ("GIT_AUTHOR_EMAIL", author_email),
        ("GIT_COMMITTER_NAME", author_name),
        ("GIT_COMMITTER_EMAIL", author_email),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value.to_string()))
    .collect();
    if let Some(token) = token {
        environment.extend(
            [
                ("GH_TOKEN", token),
                ("GIT_CONFIG_COUNT", "1"),
                ("GIT_CONFIG_KEY_0", HELPER_KEY),
                ("GIT_CONFIG_VALUE_0", HELPER),
            ]
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.to_string())),
        );
    }
    environment
}

fn guard_pushes(dir: &Path) -> Result<(), CloneFailure> {
    let hooks = dir.join(".git/hooks");
    let hook = hooks.join("pre-push");
    let other = |detail: String| failure(RepositoryCloneFailureCause::Other, detail);
    std::fs::create_dir_all(&hooks).map_err(|error| other(format!("creating {}: {error}", hooks.display())))?;
    std::fs::write(&hook, PRE_PUSH).map_err(|error| other(format!("writing {}: {error}", hook.display())))?;
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755))
        .map_err(|error| other(format!("making {} executable: {error}", hook.display())))
}

pub fn prepare(
    dir: &Path,
    repository: &str,
    environment: &[(String, String)],
    confinement: &Confinement,
    limit: Duration,
    prebuild_commit: Option<&str>,
    start: &Path,
) -> Result<(), CloneFailure> {
    let began = Instant::now();
    if !dir.join(".git").exists() {
        clone(dir, repository, environment, confinement, limit)?;
    }
    let marker = dir.join(PREBUILT);
    let other = |detail: String| failure(RepositoryCloneFailureCause::Other, detail);
    if let Some(commit) = prebuild_commit {
        git(
            dir,
            &["reset", "--quiet", "--hard", commit],
            environment,
            confinement,
            limit,
        )?;
        std::fs::write(&marker, commit).map_err(|error| other(format!("writing {}: {error}", marker.display())))?;
    } else if marker.exists() {
        git(dir, &["pull", "--quiet", "--ff-only"], environment, confinement, limit)?;
        run_start(
            dir,
            start,
            environment,
            confinement,
            limit.saturating_sub(began.elapsed()),
        )?;
        remove_file(&marker).map_err(other)?;
    }
    guard_pushes(dir)
}

fn git(
    dir: &Path,
    args: &[&str],
    environment: &[(String, String)],
    confinement: &Confinement,
    limit: Duration,
) -> Result<(), CloneFailure> {
    let fix = "check that the guest image has git on its PATH and that the repository host answers from the box";
    run(dir, "git", args, fix, environment, confinement, limit)
}

fn run_start(
    dir: &Path,
    start: &Path,
    environment: &[(String, String)],
    confinement: &Confinement,
    limit: Duration,
) -> Result<(), CloneFailure> {
    let program = start.display().to_string();
    let fix = "make postStartCommand in devcontainer.json pass within cloneSeconds; the next turn runs it again";
    let budget = limit.as_secs().saturating_sub(1).max(1).to_string();
    let mut started = environment.to_vec();
    started.push((START_BUDGET.to_string(), budget));
    run(dir, &program, &["start"], fix, &started, confinement, limit).map_err(|failed| match failed.cause {
        RepositoryCloneFailureCause::Other => failure(failed.cause, format!("PostStartFailed: {}", failed.detail)),
        _ => failed,
    })
}

fn run(
    dir: &Path,
    program: &str,
    args: &[&str],
    fix: &str,
    environment: &[(String, String)],
    confinement: &Confinement,
    limit: Duration,
) -> Result<(), CloneFailure> {
    use RepositoryCloneFailureCause::{Other, TimedOut};
    let shown = args.join(" ");
    let mut command = Command::new(program);
    confinement.apply(&mut command);
    let child = command
        .args(args)
        .current_dir(dir)
        .env_clear()
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| failure(Other, format!("starting {program} {shown}: {error}; {fix}")))?;
    let group =
        i32::try_from(child.id()).map_err(|error| failure(Other, format!("{program}'s process id: {error}")))?;
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(child.wait_with_output());
    });
    let Ok(output) = receiver.recv_timeout(limit) else {
        unsafe { libc::kill(-group, libc::SIGKILL) };
        let _ = receiver.recv();
        return Err(failure(
            TimedOut,
            format!(
                "{program} {shown} in {} did not finish within {} s (cloneSeconds of the harness), so {program} was stopped; {fix}",
                dir.display(),
                limit.as_secs()
            ),
        ));
    };
    let output = output.map_err(|error| failure(Other, format!("waiting for {program} {shown}: {error}")))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(failure(
        Other,
        format!(
            "{program} {shown} in {} failed ({}); {program} reported: {}",
            dir.display(),
            output.status,
            tumpel_guest::tail(stderr.trim(), STDERR_TAIL)
        ),
    ))
}

fn clone(
    dir: &Path,
    repository: &str,
    environment: &[(String, String)],
    confinement: &Confinement,
    limit: Duration,
) -> Result<(), CloneFailure> {
    use RepositoryCloneFailureCause::{DirectoryNotEmpty, Other, Refused, TimedOut};
    match std::fs::read_dir(dir).map(|mut entries| entries.next().is_none()) {
        Ok(true) => {}
        Ok(false) => {
            return Err(failure(
                DirectoryNotEmpty,
                format!(
                    "the harness working directory {} holds files but no repository; rebuild the image so the directory starts empty",
                    dir.display()
                ),
            ))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(failure(Other, format!("reading {}: {error}", dir.display()))),
    }
    let other = |detail: String| failure(Other, detail);
    let log = dir.with_file_name(format!(
        ".{}.clone-credentials",
        dir.file_name()
            .map_or_else(|| "workspace".into(), |name| name.to_string_lossy())
    ));
    remove_file(&log).map_err(other)?;
    let mut command = Command::new("git");
    confinement.apply(&mut command);
    let child = command
        .args(["-c", RECORDER, "clone", "--quiet", "--", repository])
        .arg(dir)
        .env_clear()
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .env(ASKED, &log)
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            other(format!(
                "starting git to clone {repository}: {error}; the guest image needs git on its PATH"
            ))
        })?;
    let group = i32::try_from(child.id()).map_err(|error| other(format!("git's process id: {error}")))?;
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = sender.send(child.wait_with_output());
    });
    let waited = receiver.recv_timeout(limit);
    if waited.is_err() {
        unsafe { libc::kill(-group, libc::SIGKILL) };
        let _ = receiver.recv();
    }
    let asked = match std::fs::read_to_string(&log) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(other(format!("reading {}: {error}", log.display()))),
    };
    remove_file(&log).map_err(other)?;
    let Ok(output) = waited else {
        match std::fs::remove_dir_all(dir) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(other(format!(
                    "removing the stopped clone at {}: {error}",
                    dir.display()
                )))
            }
            _ => {}
        }
        return Err(failure(
            TimedOut,
            format!(
                "cloning {repository} did not finish within {} s (cloneSeconds of the harness), so git was stopped; check that the repository host answers from the box",
                limit.as_secs()
            ),
        ));
    };
    let output = output.map_err(|error| other(format!("waiting for git to clone {repository}: {error}")))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let reported = tumpel_guest::tail(stderr.trim(), STDERR_TAIL);
    let token = if environment.iter().any(|(name, _)| name == TOKEN) {
        "with"
    } else {
        "without"
    };
    let cause = if refused(&asked) { Refused } else { Other };
    Err(failure(
        cause,
        format!(
            "cloning {repository} {token} {TOKEN} failed ({}); git reported: {reported}",
            output.status
        ),
    ))
}

#[cfg(test)]
mod tests;
