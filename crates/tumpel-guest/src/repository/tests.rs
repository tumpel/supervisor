use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use tumpel_api::types::RepositoryCloneFailureCause;

use super::{git_environment, CloneFailure, GH_WRAPPER};
use crate::memory::Confinement;

const LIMIT: Duration = Duration::from_mins(1);

fn prepare(
    dir: &Path,
    repository: &str,
    environment: &[(String, String)],
    limit: Duration,
) -> Result<(), CloneFailure> {
    prepare_at(
        dir,
        repository,
        environment,
        limit,
        None,
        Path::new("/nonexistent/prebuild.mjs"),
    )
}

fn prepare_at(
    dir: &Path,
    repository: &str,
    environment: &[(String, String)],
    limit: Duration,
    prebuild_commit: Option<&str>,
    start: &Path,
) -> Result<(), CloneFailure> {
    let confinement = Confinement::Score {
        score: c"/proc/self/oom_score_adj".to_owned(),
    };
    super::prepare(
        dir,
        repository,
        environment,
        &confinement,
        limit,
        prebuild_commit,
        start,
    )
}

fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/guest-tests")
        .join(name);
    match std::fs::remove_dir_all(&dir) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => panic!("clearing {}: {error}", dir.display()),
        _ => {}
    }
    std::fs::create_dir_all(&dir).expect("the scratch directory is created");
    dir
}

fn box_environment(home: &Path, token: Option<&str>) -> Vec<(String, String)> {
    let mut environment = vec![
        ("PATH".to_string(), "/usr/local/bin:/usr/bin:/bin".to_string()),
        ("HOME".to_string(), home.display().to_string()),
        ("GIT_CONFIG_NOSYSTEM".to_string(), "1".to_string()),
    ];
    if let Some(token) = token {
        environment.push(("GITHUB_TOKEN".to_string(), token.to_string()));
    }
    environment.extend(git_environment(
        "Ticket Worker",
        "ticket-worker@acme.test",
        token,
        false,
    ));
    environment
}

fn git(dir: &Path, environment: &[(String, String)], args: &[&str]) -> Output {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env_clear()
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn public_repository(root: &Path, environment: &[(String, String)]) -> PathBuf {
    let source = root.join("source");
    std::fs::create_dir_all(&source).expect("the source directory is created");
    git(&source, environment, &["init", "--quiet", "--initial-branch=trunk"]);
    std::fs::write(source.join("README.md"), "hello\n").expect("a file is written");
    git(&source, environment, &["add", "README.md"]);
    git(&source, environment, &["commit", "--quiet", "-m", "chore: start"]);
    git(&source, environment, &["switch", "--quiet", "-c", "feature"]);
    source
}

#[test]
fn a_public_repository_clones_at_its_default_branch_and_commits_carry_the_persona_identity() {
    let root = scratch("public");
    let environment = box_environment(&root, None);
    let source = public_repository(&root, &environment);
    git(&source, &environment, &["switch", "--quiet", "trunk"]);
    let work = root.join("workspace");
    let url = source.display().to_string();
    prepare(&work, &url, &environment, LIMIT).expect("the public repository clones without a token");
    assert_eq!(text(&git(&work, &environment, &["branch", "--show-current"])), "trunk");
    std::fs::write(work.join("CHANGE.md"), "change\n").expect("a file is written");
    git(&work, &environment, &["add", "CHANGE.md"]);
    git(&work, &environment, &["commit", "--quiet", "-m", "feat: change"]);
    assert_eq!(
        text(&git(
            &work,
            &environment,
            &["log", "-1", "--format=%an <%ae>|%cn <%ce>"]
        )),
        "Ticket Worker <ticket-worker@acme.test>|Ticket Worker <ticket-worker@acme.test>"
    );
}

fn asking_for_credentials() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a local port is bound");
    let port = listener.local_addr().expect("the port is known").port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut reader = BufReader::new(stream.try_clone().expect("the stream is cloned"));
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|read| read > 0) && line != "\r\n" {
                line.clear();
            }
            let _ = stream.write_all(
                b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"acme\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    port
}

#[test]
fn a_clone_the_host_refuses_without_a_token_fails_refused() {
    let root = scratch("private-without-token");
    let url = format!("http://127.0.0.1:{}/acme/app.git", asking_for_credentials());
    let error =
        prepare(&root.join("workspace"), &url, &box_environment(&root, None), LIMIT).expect_err("the clone is refused");
    assert_eq!(error.cause, RepositoryCloneFailureCause::Refused, "{}", error.detail);
    assert!(error.detail.contains("without GITHUB_TOKEN failed"), "{}", error.detail);
    assert!(!root.join(".workspace.clone-credentials").exists());
}

#[test]
fn a_clone_of_a_missing_repository_fails_other_and_carries_what_git_reported() {
    let root = scratch("missing-without-token");
    let missing = root.join("private.git").display().to_string();
    let error =
        prepare(&root.join("workspace"), &missing, &box_environment(&root, None), LIMIT).expect_err("the clone fails");
    assert_eq!(error.cause, RepositoryCloneFailureCause::Other, "{}", error.detail);
    assert!(error.detail.contains("git reported: fatal:"), "{}", error.detail);
}

#[test]
fn a_failed_clone_with_a_token_does_not_show_the_token() {
    let root = scratch("missing-with-token");
    let missing = root.join("private.git").display().to_string();
    let error = prepare(
        &root.join("workspace"),
        &missing,
        &box_environment(&root, Some("ghp-test")),
        LIMIT,
    )
    .expect_err("the clone fails");
    assert!(error.detail.contains(" with GITHUB_TOKEN failed"), "{}", error.detail);
    assert!(!error.detail.contains("ghp-test"), "{}", error.detail);
}

#[test]
fn a_working_directory_with_files_but_no_repository_fails_directory_not_empty() {
    let root = scratch("stray-files");
    let work = root.join("workspace");
    std::fs::create_dir_all(&work).expect("the working directory is made");
    std::fs::write(work.join("notes.txt"), "stray\n").expect("a file is written");
    let missing = root.join("gone.git").display().to_string();
    let error = prepare(&work, &missing, &box_environment(&root, None), LIMIT).expect_err("the clone is refused");
    assert_eq!(
        error.cause,
        RepositoryCloneFailureCause::DirectoryNotEmpty,
        "{}",
        error.detail
    );
}

#[test]
fn a_clone_from_a_host_that_never_answers_stops_at_its_limit_and_leaves_no_partial_clone() {
    let root = scratch("stalled");
    let listener = TcpListener::bind("127.0.0.1:0").expect("a local port is bound");
    let port = listener.local_addr().expect("the port is known").port();
    std::thread::spawn(move || {
        let held: Vec<_> = listener.incoming().collect();
        drop(held);
    });
    let work = root.join("workspace");
    let started = Instant::now();
    let error = prepare(
        &work,
        &format!("git://127.0.0.1:{port}/acme/app.git"),
        &box_environment(&root, None),
        Duration::from_secs(1),
    )
    .expect_err("the stalled clone is stopped");
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
    assert_eq!(error.cause, RepositoryCloneFailureCause::TimedOut, "{}", error.detail);
    assert!(
        error.detail.contains("did not finish within 1 s (cloneSeconds"),
        "{}",
        error.detail
    );
    assert!(!work.exists());
}

fn credential_fill(root: &Path, token: Option<&str>, host: &str) -> String {
    let mut child = Command::new("git")
        .args(["credential", "fill"])
        .current_dir(root)
        .env_clear()
        .envs(box_environment(root, token).iter().map(|(name, value)| (name, value)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("git credential runs");
    child
        .stdin
        .take()
        .expect("git credential reads standard input")
        .write_all(format!("protocol=https\nhost={host}\npath=acme/app.git\n\n").as_bytes())
        .expect("the request is written");
    String::from_utf8_lossy(&child.wait_with_output().expect("git credential ends").stdout).to_string()
}

#[test]
fn the_credential_helper_answers_github_with_the_token_from_the_environment() {
    let root = scratch("credential-helper");
    let answer = credential_fill(&root, Some("ghp-from-env"), "github.com");
    assert!(answer.contains("username=x-access-token\n"), "{answer}");
    assert!(answer.contains("password=ghp-from-env\n"), "{answer}");
    assert!(!credential_fill(&root, Some("ghp-from-env"), "git.example.test").contains("ghp-from-env"));
    assert!(!credential_fill(&root, None, "github.com").contains("password="));
    assert!(!root.join(".git-credentials").exists());
}

#[test]
fn the_credential_helper_answers_each_owner_with_its_own_installation_token() {
    let root = scratch("owner-helper");
    let mut environment = vec![
        ("PATH".to_string(), "/usr/local/bin:/usr/bin:/bin".to_string()),
        ("HOME".to_string(), root.display().to_string()),
        ("GIT_CONFIG_NOSYSTEM".to_string(), "1".to_string()),
        ("GITHUB_TOKEN".to_string(), "ghs-mine".to_string()),
        (
            "TUMPEL_GITHUB_TOKENS".to_string(),
            "mvhenten=ghs-mine tumpel=ghs-org".to_string(),
        ),
    ];
    environment.extend(git_environment(
        "Ticket Worker",
        "ticket-worker@acme.test",
        Some("ghs-mine"),
        true,
    ));
    let fill = |path: &str| {
        let mut child = Command::new("git")
            .args(["credential", "fill"])
            .current_dir(&root)
            .env_clear()
            .envs(environment.iter().map(|(name, value)| (name, value)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("git credential runs");
        child
            .stdin
            .take()
            .expect("git credential reads standard input")
            .write_all(format!("protocol=https\nhost=github.com\npath={path}\n\n").as_bytes())
            .expect("the request is written");
        let output = child.wait_with_output().expect("git credential ends");
        (
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    };
    let (mine, _) = fill("mvhenten/tumpel.git");
    assert!(mine.contains("password=ghs-mine\n"), "{mine}");
    let (org, _) = fill("Tumpel/protocol.git");
    assert!(org.contains("password=ghs-org\n"), "{org}");
    let (stranger, refused) = fill("acme/app.git");
    assert!(!stranger.contains("password="), "{stranger}");
    assert!(
        refused.contains("no GitHub App installation of this tenant covers github.com/acme"),
        "{refused}"
    );
}

fn wrapped_gh(name: &str, gh_repo: Option<&str>, in_checkout: bool, args: &[&str]) -> (String, bool) {
    let root = scratch(name);
    let wrapper = root.join("wrapper");
    let real = root.join("real");
    let checkout = root.join("checkout");
    for dir in [&wrapper, &real, &checkout] {
        std::fs::create_dir_all(dir).expect("a fixture directory is created");
    }
    std::fs::write(wrapper.join("gh"), GH_WRAPPER).expect("the wrapper is written");
    std::fs::write(real.join("gh"), "#!/bin/sh\necho \"$GH_TOKEN $GITHUB_TOKEN\"\n").expect("a fake gh is written");
    for gh in [wrapper.join("gh"), real.join("gh")] {
        std::fs::set_permissions(&gh, std::os::unix::fs::PermissionsExt::from_mode(0o755)).expect("gh is executable");
    }
    let environment = box_environment(&root, None);
    git(&checkout, &environment, &["init", "--quiet"]);
    git(
        &checkout,
        &environment,
        &["remote", "add", "origin", "https://github.com/mvhenten/tumpel"],
    );
    let mut command = Command::new(wrapper.join("gh"));
    command
        .args(args)
        .current_dir(if in_checkout { &checkout } else { &root })
        .env_clear()
        .env(
            "PATH",
            format!("{}:{}:/usr/bin:/bin", wrapper.display(), real.display()),
        )
        .env("HOME", &root)
        .env(
            "GIT_CEILING_DIRECTORIES",
            root.parent().expect("the scratch directory has a parent"),
        )
        .env("GH_TOKEN", "ghs-session")
        .env("GITHUB_TOKEN", "ghs-session")
        .env("TUMPEL_GITHUB_TOKENS", "mvhenten=ghs-mine tumpel=ghs-org");
    if let Some(repo) = gh_repo {
        command.env("GH_REPO", repo);
    }
    let output = command.output().expect("the wrapper runs");
    (
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
        output.status.success(),
    )
}

#[test]
fn gh_in_a_clone_uses_the_token_of_its_origin_owner() {
    let picked = wrapped_gh("gh-origin", None, true, &["pr", "list"]);
    assert_eq!(picked, ("ghs-mine ghs-mine".to_string(), true));
}

#[test]
fn gh_with_a_repo_flag_uses_the_token_of_that_owner() {
    let picked = wrapped_gh("gh-repo-flag", None, true, &["pr", "list", "-R", "tumpel/protocol"]);
    assert_eq!(picked, ("ghs-org ghs-org".to_string(), true));
}

#[test]
fn gh_with_gh_repo_uses_the_token_of_that_owner() {
    let picked = wrapped_gh("gh-gh-repo", Some("tumpel/protocol"), true, &["auth", "token"]);
    assert_eq!(picked, ("ghs-org ghs-org".to_string(), true));
}

#[test]
fn gh_repo_with_an_owner_and_name_argument_uses_the_token_of_that_owner() {
    let picked = wrapped_gh("gh-repo-argument", None, true, &["repo", "view", "Tumpel/protocol"]);
    assert_eq!(picked, ("ghs-org ghs-org".to_string(), true));
}

#[test]
fn gh_with_a_github_url_argument_uses_the_token_of_that_owner() {
    let picked = wrapped_gh(
        "gh-url-argument",
        None,
        true,
        &["pr", "view", "https://github.com/tumpel/protocol/pull/1"],
    );
    assert_eq!(picked, ("ghs-org ghs-org".to_string(), true));
}

#[test]
fn gh_api_on_a_repository_path_beats_gh_repo() {
    let picked = wrapped_gh(
        "gh-api-path",
        Some("mvhenten/tumpel"),
        true,
        &["api", "repos/tumpel/protocol/pulls"],
    );
    assert_eq!(picked, ("ghs-org ghs-org".to_string(), true));
}

#[test]
fn gh_api_with_placeholders_uses_the_token_of_the_origin_owner() {
    let picked = wrapped_gh("gh-api-placeholder", None, true, &["api", "repos/{owner}/{repo}/pulls"]);
    assert_eq!(picked, ("ghs-mine ghs-mine".to_string(), true));
}

#[test]
fn gh_reads_no_target_from_an_option_value() {
    let picked = wrapped_gh(
        "gh-option-value",
        Some("tumpel/protocol"),
        true,
        &["repo", "create", "mine", "--source", "./app", "--private"],
    );
    assert_eq!(picked, ("ghs-org ghs-org".to_string(), true));
    let picked = wrapped_gh(
        "gh-body-url",
        None,
        true,
        &["pr", "create", "--body", "https://github.com/tumpel/protocol/pull/3"],
    );
    assert_eq!(picked, ("ghs-mine ghs-mine".to_string(), true));
}

#[test]
fn gh_without_an_owner_uses_the_session_token() {
    let picked = wrapped_gh("gh-no-owner", None, false, &["api", "user/repos"]);
    assert_eq!(picked, ("ghs-session ghs-session".to_string(), true));
}

#[test]
fn gh_on_an_owner_without_an_installation_uses_the_session_token() {
    let picked = wrapped_gh("gh-stranger", None, true, &["repo", "view", "cli/cli"]);
    assert_eq!(picked, ("ghs-session ghs-session".to_string(), true));
}

fn refused_push(dir: &Path, environment: &[(String, String)], args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env_clear()
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .output()
        .expect("git runs");
    assert!(!output.status.success(), "git {args:?} was not refused");
    String::from_utf8_lossy(&output.stderr).to_string()
}

#[test]
fn the_pre_push_hook_refuses_the_default_branch_and_force_pushes_and_lets_a_new_branch_through() {
    let root = scratch("pre-push");
    let environment = box_environment(&root, None);
    let source = public_repository(&root, &environment);
    let bare = root.join("origin.git");
    git(
        &root,
        &environment,
        &[
            "clone",
            "--quiet",
            "--bare",
            &source.display().to_string(),
            &bare.display().to_string(),
        ],
    );
    git(&bare, &environment, &["symbolic-ref", "HEAD", "refs/heads/trunk"]);
    let work = root.join("workspace");
    prepare(&work, &bare.display().to_string(), &environment, LIMIT).expect("the bare repository clones");
    std::fs::write(work.join("CHANGE.md"), "change\n").expect("a file is written");
    git(&work, &environment, &["add", "CHANGE.md"]);
    git(&work, &environment, &["commit", "--quiet", "-m", "feat: change"]);
    let refusal = refused_push(&work, &environment, &["push", "origin", "HEAD:trunk"]);
    assert!(
        refusal.contains(
            "Tumpel pushes to a branch and opens a draft pull request; the default branch is not pushed from a box"
        ),
        "{refusal}"
    );
    git(
        &work,
        &environment,
        &["push", "--quiet", "origin", "HEAD:refs/heads/tumpel-change"],
    );
    git(
        &work,
        &environment,
        &["commit", "--quiet", "--amend", "-m", "feat: rewritten"],
    );
    let forced = refused_push(
        &work,
        &environment,
        &["push", "--force", "origin", "HEAD:tumpel-change"],
    );
    assert!(forced.contains("Tumpel does not force-push from a box"), "{forced}");
    assert_eq!(
        text(&git(&bare, &environment, &["rev-parse", "trunk"])),
        text(&git(&source, &environment, &["rev-parse", "trunk"]))
    );
}

fn serving_with_token(project_root: &Path, token: &str) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a local port is bound");
    let port = listener.local_addr().expect("the port is known").port();
    let project_root = project_root.to_path_buf();
    let authorization = format!("Basic {}", STANDARD.encode(format!("x-access-token:{token}")));
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut reader = BufReader::new(stream.try_clone().expect("the stream is cloned"));
            let mut request = String::new();
            reader.read_line(&mut request).expect("the request line is read");
            let mut headers = Vec::new();
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|read| read > 0) && line != "\r\n" {
                if let Some((name, value)) = line.trim_end().split_once(": ") {
                    headers.push((name.to_ascii_lowercase(), value.to_string()));
                }
                line.clear();
            }
            let header = |name: &str| {
                headers
                    .iter()
                    .find(|(found, _)| found == name)
                    .map_or(String::new(), |(_, value)| value.clone())
            };
            if header("authorization") != authorization {
                let _ = stream.write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"acme\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                continue;
            }
            let mut body = vec![0; header("content-length").parse().unwrap_or(0)];
            reader.read_exact(&mut body).expect("the request body is read");
            let mut parts = request.split_whitespace();
            let method = parts.next().expect("the request has a method");
            let target = parts.next().expect("the request has a target");
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            let mut backend = Command::new("git")
                .arg("http-backend")
                .env_clear()
                .env("PATH", "/usr/local/bin:/usr/bin:/bin")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_HTTP_EXPORT_ALL", "1")
                .env("GIT_PROJECT_ROOT", &project_root)
                .env("REQUEST_METHOD", method)
                .env("PATH_INFO", path)
                .env("QUERY_STRING", query)
                .env("CONTENT_TYPE", header("content-type"))
                .env("HTTP_CONTENT_ENCODING", header("content-encoding"))
                .env("GIT_PROTOCOL", header("git-protocol"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .expect("git http-backend runs");
            backend
                .stdin
                .take()
                .expect("git http-backend reads standard input")
                .write_all(&body)
                .expect("the request body is passed on");
            let output = backend.wait_with_output().expect("git http-backend ends");
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n");
            let _ = stream.write_all(&output.stdout);
        }
    });
    port
}

#[test]
fn a_prebuild_pins_its_commit_and_the_first_turn_restored_from_it_pulls_the_branch_and_starts_once() {
    let root = scratch("prebuild");
    let port = serving_with_token(&root, "ghp-prebuild");
    let mut environment = box_environment(&root, Some("ghp-prebuild"));
    environment.retain(|(name, _)| name != "GIT_CONFIG_COUNT");
    environment.extend([
        ("GIT_CONFIG_COUNT".to_string(), "2".to_string()),
        (
            "GIT_CONFIG_KEY_1".to_string(),
            format!("credential.http://127.0.0.1:{port}.helper"),
        ),
        ("GIT_CONFIG_VALUE_1".to_string(), super::HELPER.to_string()),
    ]);
    let source = public_repository(&root, &environment);
    git(&source, &environment, &["switch", "--quiet", "trunk"]);
    let recorded = text(&git(&source, &environment, &["rev-parse", "HEAD"]));
    std::fs::write(source.join("README.md"), "later\n").expect("a file is written");
    git(&source, &environment, &["add", "README.md"]);
    git(&source, &environment, &["commit", "--quiet", "-m", "feat: later"]);
    let work = root.join("workspace");
    git(&source, &environment, &["config", "uploadpack.allowFilter", "true"]);
    let url = format!("http://127.0.0.1:{port}/source");
    let start = root.join("prebuild.mjs");
    std::fs::write(&start, "#!/bin/sh\ntest -f NEWEST.md && echo \"$1\" >> ../started\n")
        .expect("the start script is written");
    std::fs::set_permissions(&start, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the start script is executable");
    prepare_at(&work, &url, &environment, LIMIT, Some(&recorded), &start).expect("the prebuild clones at its commit");
    assert!(!root.join("started").exists());
    assert_eq!(text(&git(&work, &environment, &["rev-parse", "HEAD"])), recorded);
    assert_eq!(
        text(&git(
            &work,
            &environment,
            &["config", "remote.origin.partialclonefilter"]
        )),
        "blob:none"
    );
    assert_eq!(
        std::fs::read_to_string(work.join("README.md")).expect("the pinned file is checked out"),
        "hello\n"
    );
    assert_eq!(text(&git(&work, &environment, &["branch", "--show-current"])), "trunk");
    std::fs::write(source.join("NEWEST.md"), "newest\n").expect("a file is written");
    git(&source, &environment, &["add", "NEWEST.md"]);
    git(&source, &environment, &["commit", "--quiet", "-m", "feat: newest"]);
    let newest = text(&git(&source, &environment, &["rev-parse", "HEAD"]));
    prepare_at(&work, &url, &environment, LIMIT, None, &start).expect("the restored box pulls the branch");
    assert_eq!(text(&git(&work, &environment, &["rev-parse", "HEAD"])), newest);
    assert_eq!(
        std::fs::read_to_string(root.join("started")).expect("the start ran"),
        "start\n"
    );
    std::fs::write(work.join("CHANGE.md"), "change\n").expect("a file is written");
    git(&work, &environment, &["add", "CHANGE.md"]);
    git(&work, &environment, &["commit", "--quiet", "-m", "feat: change"]);
    let own = text(&git(&work, &environment, &["rev-parse", "HEAD"]));
    prepare_at(&work, &url, &environment, LIMIT, None, &start).expect("a later turn leaves the branch alone");
    assert_eq!(text(&git(&work, &environment, &["rev-parse", "HEAD"])), own);
    assert_eq!(
        std::fs::read_to_string(root.join("started")).expect("the start ran"),
        "start\n"
    );
}

#[test]
fn a_failing_start_keeps_the_prebuild_marker_so_the_next_turn_starts_again() {
    let root = scratch("prebuild-start-fails");
    let environment = box_environment(&root, None);
    let source = public_repository(&root, &environment);
    git(&source, &environment, &["switch", "--quiet", "trunk"]);
    let recorded = text(&git(&source, &environment, &["rev-parse", "HEAD"]));
    let work = root.join("workspace");
    let url = source.display().to_string();
    let start = root.join("prebuild.mjs");
    std::fs::write(
        &start,
        "#!/bin/sh\necho \"$1\" >> ../started\ntest -f ../passes || { echo broken >&2; exit 4; }\n",
    )
    .expect("the start script is written");
    std::fs::set_permissions(&start, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the start script is executable");
    prepare_at(&work, &url, &environment, LIMIT, Some(&recorded), &start).expect("the prebuild clones at its commit");
    let error = prepare_at(&work, &url, &environment, LIMIT, None, &start).expect_err("the start fails");
    assert_eq!(error.cause, RepositoryCloneFailureCause::Other);
    assert!(error.detail.starts_with("PostStartFailed: "), "{}", error.detail);
    assert!(error.detail.contains("reported: broken"), "{}", error.detail);
    assert!(work.join(".git/tumpel-prebuilt").exists());
    std::fs::write(root.join("passes"), "").expect("a file is written");
    prepare_at(&work, &url, &environment, LIMIT, None, &start).expect("the next turn starts again");
    assert!(!work.join(".git/tumpel-prebuilt").exists());
    prepare_at(&work, &url, &environment, LIMIT, None, &start).expect("a later turn does not start");
    assert_eq!(
        std::fs::read_to_string(root.join("started")).expect("the start ran"),
        "start\nstart\n"
    );
}
