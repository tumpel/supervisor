use std::hash::RandomState;
use std::path::Path;

use serde_json::{json, Value};
use tumpel_api::types::{GuestHarness, GuestTurnStart};

use tumpel_api::types::{GuestHarnessProtocol, GuestTurnEndReason};

use super::{
    answers_no_prompt, ends_turn, environment, home_path, launch, priced_by_anthropic, reuses, skips, turn_arguments,
    turn_environment, without_list_price, HostWatch, Output,
};

fn start(model: &str, variables: &Value, files: &Value) -> GuestTurnStart {
    serde_json::from_value(json!({
        "type": "TurnStart",
        "turnId": "turn-1",
        "model": model,
        "prompt": "fix the build",
        "credential": {
            "credentialHandoverId": "grant-1",
            "expiresAt": "2026-11-14T12:00:00+01:00",
            "variables": variables,
        },
        "files": files,
        "repository": "https://github.com/acme/app.git",
        "gitAuthorName": "Ticket Worker",
        "gitAuthorEmail": "ticket-worker@acme.test",
        "lastLineSeq": 0,
    }))
    .expect("the test turn matches GuestTurnStart")
}

fn key(value: &str) -> Value {
    json!([{ "name": "ANTHROPIC_API_KEY", "value": value }])
}

fn settings(digest: char) -> Value {
    json!([{ "path": ".claude/settings.json", "digest": digest.to_string().repeat(64), "content": "e30=" }])
}

#[test]
fn a_changed_credential_restarts_the_harness_and_an_unchanged_one_does_not() {
    let keys = RandomState::new();
    let running = launch(&keys, &start("claude-sonnet", &key("sk-first"), &json!([])));
    assert!(reuses(
        Some(&running),
        &launch(&keys, &start("claude-sonnet", &key("sk-first"), &json!([])))
    ));
    assert!(!reuses(
        Some(&running),
        &launch(&keys, &start("claude-sonnet", &key("sk-second"), &json!([])))
    ));
    assert!(!reuses(
        Some(&running),
        &launch(&keys, &start("claude-opus", &key("sk-first"), &json!([])))
    ));
    assert!(!reuses(None, &running));
}

#[test]
fn a_changed_file_set_restarts_the_harness_and_the_same_set_in_another_order_does_not() {
    let keys = RandomState::new();
    let both = json!([
        { "path": ".claude/settings.json", "digest": "a".repeat(64), "content": "e30=" },
        { "path": ".claude/CLAUDE.md", "digest": "b".repeat(64), "content": "aGk=" },
    ]);
    let reordered = json!([
        { "path": ".claude/CLAUDE.md", "digest": "b".repeat(64), "content": "aGk=" },
        { "path": ".claude/settings.json", "digest": "a".repeat(64), "content": "e30=" },
    ]);
    let running = launch(&keys, &start("claude-sonnet", &key("sk"), &both));
    assert!(reuses(
        Some(&running),
        &launch(&keys, &start("claude-sonnet", &key("sk"), &reordered))
    ));
    assert!(!reuses(
        Some(&running),
        &launch(&keys, &start("claude-sonnet", &key("sk"), &settings('a')))
    ));
    let one = launch(&keys, &start("claude-sonnet", &key("sk"), &settings('a')));
    assert!(!reuses(
        Some(&one),
        &launch(&keys, &start("claude-sonnet", &key("sk"), &settings('c')))
    ));
}

#[test]
fn the_harness_environment_holds_every_variable_of_the_grant() {
    let turn = start(
        "claude-sonnet",
        &json!([
            { "name": "CLAUDE_CODE_OAUTH_TOKEN", "value": "token" },
            { "name": "GITHUB_TOKEN", "value": "ghp" },
            { "name": "NPM_TOKEN", "value": "npm" },
        ]),
        &json!([]),
    );
    let config: GuestHarness = serde_json::from_value(json!({
        "harness": "ClaudeCode", "harnessVersion": "1", "protocol": "ClaudeStreamJson",
        "command": ["claude"], "workingDirectory": "/root/workspace", "cloneSeconds": 120,
        "environment": { "PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8" },
        "fixedVariables": [
            { "name": "DISABLE_AUTOUPDATER", "value": "1" },
            { "name": "NPM_TOKEN", "value": "fixed" },
        ],
    }))
    .expect("the test config matches GuestHarness");
    let exported = environment(&config, &turn);
    let last = |name: &str| {
        exported
            .iter()
            .rev()
            .find(|(held, _)| held == name)
            .map(|(_, set)| set.as_str())
    };
    assert_eq!(last("DISABLE_AUTOUPDATER"), Some("1"));
    assert_eq!(
        last("NPM_TOKEN"),
        Some("npm"),
        "a grant variable overrides a fixed variable of the same name"
    );
    for (name, value) in [
        ("CLAUDE_CODE_OAUTH_TOKEN", "token"),
        ("GITHUB_TOKEN", "ghp"),
        ("GH_TOKEN", "ghp"),
        ("NPM_TOKEN", "npm"),
        ("HOME", "/root"),
        ("GIT_AUTHOR_NAME", "Ticket Worker"),
        ("GIT_AUTHOR_EMAIL", "ticket-worker@acme.test"),
        ("GIT_COMMITTER_NAME", "Ticket Worker"),
        ("GIT_COMMITTER_EMAIL", "ticket-worker@acme.test"),
    ] {
        assert!(
            exported.iter().any(|(held, set)| held == name && set == value),
            "{name} is missing from the harness environment"
        );
    }
}

#[test]
fn a_persona_file_path_that_climbs_out_of_the_home_is_refused() {
    let home = Path::new("/root");
    assert_eq!(
        home_path(home, ".claude/settings.json"),
        Ok(home.join(".claude/settings.json"))
    );
    assert!(home_path(home, "../etc/passwd").is_err());
    assert!(home_path(home, ".claude/../../etc").is_err());
}

#[test]
fn a_changed_git_identity_restarts_the_harness() {
    let keys = RandomState::new();
    let turn = start("claude-sonnet", &key("sk"), &json!([]));
    let running = launch(&keys, &turn);
    let mut renamed = turn.clone();
    renamed.git_author_name = "Another Worker".parse().expect("a display name");
    assert!(!reuses(Some(&running), &launch(&keys, &renamed)));
    assert!(reuses(Some(&running), &launch(&keys, &turn)));
}

#[test]
fn a_grant_without_a_github_token_exports_no_credential_helper() {
    let exported = turn_environment(&start("claude-sonnet", &key("sk"), &json!([])));
    for name in ["GH_TOKEN", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"] {
        assert!(
            exported.iter().all(|(held, _)| held != name),
            "{name} is exported without a GITHUB_TOKEN"
        );
    }
}

#[test]
fn a_pi_turn_ends_when_pi_settles_or_answers_the_prompt_without_a_run() {
    let pi = GuestHarnessProtocol::PiRpc;
    let settled = r#"{"type":"agent_settled","aborted":false}"#;
    let refused =
        r#"{"type":"response","command":"prompt","success":false,"error":"No API key found for openrouter."}"#;
    let handled = r#"{"type":"response","command":"prompt","success":true,"data":{"disposition":"handled"}}"#;
    for line in [settled, refused, handled] {
        assert!(ends_turn(pi, line), "{line}");
    }
    let started = r#"{"type":"response","command":"prompt","success":true,"data":{"disposition":"started"}}"#;
    let run_ended = r#"{"type":"agent_end","messages":[],"willRetry":true}"#;
    for line in [started, run_ended, r#"{"type":"result"}"#, "not json"] {
        assert!(!ends_turn(pi, line), "{line}");
    }
}

#[test]
fn a_pi_turn_leaves_out_the_streaming_deltas() {
    let pi = GuestHarnessProtocol::PiRpc;
    let delta = r#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"Hi"}}"#;
    let partial = r#"{"type":"tool_execution_update","toolCallId":"call_1","toolName":"bash"}"#;
    assert!(skips(pi, delta));
    assert!(skips(pi, partial));
    assert!(!skips(pi, r#"{"type":"message_end","message":{"role":"assistant"}}"#));
    assert!(skips(
        pi,
        r#"{"type":"response","command":"set_auto_retry","success":true}"#
    ));
    assert!(!skips(
        pi,
        r#"{"type":"response","command":"set_auto_retry","success":false,"error":"Unknown command"}"#
    ));
}

#[test]
fn claude_code_thinking_token_ticks_stay_out_of_the_transcript_and_its_messages_and_result_stay_in() {
    let claude = GuestHarnessProtocol::ClaudeStreamJson;
    let tick = r#"{"type":"system","subtype":"thinking_tokens","estimated_tokens":12791,"estimated_tokens_delta":1,"session_id":"ca9a48ec-b580-43de-a965-00ab062e5bb3","uuid":"bbe76aa8-48df-4e0e-89eb-9b7c18e0e41c"}"#;
    let init = r#"{"type":"system","subtype":"init","cwd":"/root/workspace","session_id":"ca9a48ec-b580-43de-a965-00ab062e5bb3","model":"qwen/qwen3.8-27b"}"#;
    let assistant = r#"{"type":"assistant","message":{"id":"msg_UuSLEyhno5vWjtmGCbMW","type":"message","role":"assistant","content":[{"type":"text","text":"on it"}],"model":"qwen/qwen3.8-27b","usage":{"input_tokens":0,"output_tokens":0},"provider":"PrimeIntellect"},"session_id":"ca9a48ec-b580-43de-a965-00ab062e5bb3"}"#;
    let result = r#"{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.0123,"usage":{"input_tokens":5120,"output_tokens":812}}"#;
    assert!(skips(claude, tick));
    assert!(!skips(claude, init));
    assert!(!skips(claude, assistant));
    assert!(!skips(claude, result));
    assert!(!skips(GuestHarnessProtocol::PiRpc, tick));
}

#[test]
fn a_claude_code_result_keeps_its_tokens_and_loses_its_list_price_unless_anthropic_bills_the_turn() {
    let openrouter = json!([{ "name": "ANTHROPIC_AUTH_TOKEN", "value": "sk-or-test" }]);
    assert!(!priced_by_anthropic(&start(
        "qwen/qwen3.8-27b",
        &openrouter,
        &json!([])
    )));
    assert!(priced_by_anthropic(&start(
        "claude-opus-5-5",
        &key("sk-ant"),
        &json!([])
    )));
    let result = r#"{"type":"result","total_cost_usd":0.858,"usage":{"input_tokens":5120,"output_tokens":812},"modelUsage":{"qwen/qwen3.8-27b":{"inputTokens":5120,"costUSD":0.858}}}"#;
    let kept: Value = serde_json::from_str(&without_list_price(result.to_string())).expect("the line stays JSON");
    assert_eq!(
        kept,
        json!({"type":"result","usage":{"input_tokens":5120,"output_tokens":812},"modelUsage":{"qwen/qwen3.8-27b":{"inputTokens":5120}}})
    );
    let assistant = r#"{"type":"assistant","total_cost_usd":1}"#;
    assert_eq!(without_list_price(assistant.to_string()), assistant);
}

#[test]
fn a_result_for_a_background_task_notification_does_not_end_the_turn_and_the_real_result_does() {
    let notification = json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "num_turns": 0,
        "total_cost_usd": 0,
        "origin": { "kind": "task-notification" },
        "result": "",
    })
    .to_string();
    let reply = json!({
        "type": "result",
        "subtype": "success",
        "is_error": false,
        "num_turns": 15,
        "total_cost_usd": 0.42,
        "result": "done",
    })
    .to_string();
    let protocol = GuestHarnessProtocol::ClaudeStreamJson;
    let ending = [notification, reply]
        .into_iter()
        .filter(|line| !answers_no_prompt(protocol, line))
        .find(|line| ends_turn(protocol, line));
    assert_eq!(ending.as_deref().map(|line| line.contains("\"done\"")), Some(true));
    let failure = json!({
        "type": "result",
        "subtype": "error_during_execution",
        "is_error": true,
        "num_turns": 0,
        "result": "Invalid API key",
    })
    .to_string();
    let failed_notification = json!({
        "type": "result",
        "subtype": "success",
        "is_error": true,
        "num_turns": 0,
        "origin": { "kind": "task-notification" },
    })
    .to_string();
    for line in [failure, failed_notification] {
        assert!(!answers_no_prompt(protocol, &line));
        assert!(ends_turn(protocol, &line));
    }
    assert!(!answers_no_prompt(
        GuestHarnessProtocol::PiRpc,
        r#"{"type":"result","num_turns":0}"#
    ));
}

const CODEX: GuestHarnessProtocol = GuestHarnessProtocol::CodexExecJson;

fn codex_turn(arguments: &Value) -> GuestTurnStart {
    let mut turn = start(
        "gpt-5.5",
        &json!([{ "name": "OPENAI_API_KEY", "value": "sk-test" }]),
        &json!([]),
    );
    turn.credential.arguments = serde_json::from_value(arguments.clone()).expect("arguments are strings");
    turn
}

#[test]
fn a_later_codex_turn_resumes_the_recorded_thread() {
    let turn = codex_turn(&json!(["-c", "model_provider=openrouter"]));
    let thread = "0199a213-81c0-7800-8aa1-bbab2a035a53";
    let resumed = turn_arguments(CODEX, &turn, true, Some(thread)).expect("a Codex credential");
    assert_eq!(
        resumed[2..],
        ["-c", "model_provider=openrouter", "resume", thread].map(String::from)
    );
    let unrecorded = turn_arguments(CODEX, &turn, true, None).expect("a Codex credential");
    assert!(!unrecorded.contains(&"resume".to_string()), "{unrecorded:?}");
}

#[test]
fn a_codex_turn_whose_credential_names_no_provider_does_not_start() {
    let error = turn_arguments(CODEX, &codex_turn(&json!([])), false, None).expect_err("no Codex provider");
    assert!(error.contains("names no Codex model provider"), "{error}");
}

#[test]
fn a_host_that_hangs_up_mid_turn_wakes_that_turn() {
    use std::os::fd::AsFd;
    use std::time::Duration;
    let (host, peer) = std::os::unix::net::UnixStream::pair().expect("a host connection");
    let (sender, output) = std::sync::mpsc::channel();
    let _watching = HostWatch::watch(host.as_fd(), sender, 7, "turn-1".try_into().expect("turn id"))
        .expect("the host connection is watched");
    assert!(
        output.recv_timeout(Duration::from_millis(300)).is_err(),
        "a connected host wakes nothing"
    );
    drop(peer);
    assert!(matches!(
        output.recv_timeout(Duration::from_secs(2)),
        Ok(Output::HostGone(7, _))
    ));
}

fn cancel_frame(turn_id: &str) -> Vec<u8> {
    tumpel_guest::encode(&json!({ "type": "TurnCancel", "turnId": turn_id })).expect("a cancel frame")
}

#[test]
fn a_cancel_of_the_running_turn_wakes_that_turn_and_a_cancel_of_another_does_not() {
    use std::io::Write;
    use std::os::fd::AsFd;
    use std::time::Duration;
    let (host, mut peer) = std::os::unix::net::UnixStream::pair().expect("a host connection");
    let (sender, output) = std::sync::mpsc::channel();
    let _watching = HostWatch::watch(host.as_fd(), sender, 7, "turn-1".try_into().expect("turn id"))
        .expect("the host connection is watched");
    peer.write_all(&cancel_frame("turn-0"))
        .expect("the stale cancel is sent");
    assert!(
        output.recv_timeout(Duration::from_millis(300)).is_err(),
        "a cancel of another turn wakes nothing"
    );
    peer.write_all(&cancel_frame("turn-1")).expect("the cancel is sent");
    assert!(matches!(
        output.recv_timeout(Duration::from_secs(2)),
        Ok(Output::Cancelled(7))
    ));
}

#[test]
fn a_cancel_kills_a_persistent_harness_and_every_process_it_started() {
    use std::io::Write;
    use std::time::Duration;
    let config: GuestHarness = serde_json::from_value(json!({
        "harness": "Shell", "harnessVersion": "1", "protocol": "ShellLines",
        "command": ["sh"], "workingDirectory": "/", "cloneSeconds": 120,
        "environment": { "PATH": "/usr/bin:/bin", "HOME": "/tmp", "LANG": "C.UTF-8" },
        "fixedVariables": [],
    }))
    .expect("the test config matches GuestHarness");
    let memory = crate::memory::Memory {
        confinement: crate::memory::Confinement::Score {
            score: c"/proc/self/oom_score_adj".to_owned(),
        },
        kernel: crate::memory::KernelLog::open(Path::new("/dev/null")).expect("a kernel log"),
        box_mib: 1024,
    };
    let state = super::GuestState::new(config, memory).expect("a guest state");
    let turn = start("m", &json!([]), &json!([]));
    let mut harness = state
        .start_process(std::process::Command::new("sh"), Vec::new(), false, &turn)
        .expect("the harness starts");
    harness
        .stdin
        .write_all(b"sleep 300 & echo $!\n")
        .and_then(|()| harness.stdin.flush())
        .expect("the harness reads its prompt");
    let Ok(Output::Line(tool)) = harness.output.recv_timeout(Duration::from_secs(5)) else {
        panic!("the harness names the process it started");
    };
    let tool: libc::pid_t = tool.parse().expect("a pid");
    assert!(
        std::fs::metadata(format!("/proc/{tool}")).is_ok(),
        "the tool runs before the cancel"
    );

    harness.kill_group().expect("the cancel kills the harness");

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let running = |pid: libc::pid_t| {
        std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| !stat.contains(") Z "))
    };
    while running(tool) {
        assert!(
            std::time::Instant::now() < deadline,
            "the tool {tool} outlived the cancel"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = harness.child.try_wait().expect("the harness is reaped");
    assert_eq!(
        status.and_then(|status| std::os::unix::process::ExitStatusExt::signal(&status)),
        Some(libc::SIGKILL)
    );
}

#[test]
fn a_harness_that_fails_on_a_full_box_disk_fails_box_disk_full_and_a_completed_turn_only_warns() {
    let full = tumpel_guest::disk::Space {
        total_mib: 32_096,
        available_mib: 0,
        free_mib: 12,
    };
    let parts = |ending: super::Ending| match ending {
        super::Ending::Turn {
            reason,
            detail,
            warning,
            ..
        } => (reason, detail, warning),
        super::Ending::Clone(_) => panic!("a turn ending"),
    };
    let advice = "the box disk of 32096 MiB is full, 12 MiB free; give the persona a larger boxDiskMib, up to 262144 MiB, or have it delete build output it no longer needs";
    assert_eq!(
        parts(super::disk_full(
            super::ended(GuestTurnEndReason::HarnessExited, "No space left on device"),
            full,
        )),
        (
            GuestTurnEndReason::BoxDiskFull,
            format!("{advice}; the harness stopped (HarnessExited): No space left on device"),
            None
        )
    );
    assert_eq!(
        parts(super::disk_full(super::ended(GuestTurnEndReason::Completed, ""), full)),
        (GuestTurnEndReason::Completed, String::new(), Some(advice.to_string()))
    );
    assert_eq!(
        parts(super::disk_full(super::ended(GuestTurnEndReason::Cancelled, ""), full)).0,
        GuestTurnEndReason::Cancelled
    );
    let roomy = tumpel_guest::disk::Space { free_mib: 321, ..full };
    assert_eq!(
        parts(super::disk_full(
            super::ended(GuestTurnEndReason::HarnessExited, ""),
            roomy
        )),
        (GuestTurnEndReason::HarnessExited, String::new(), None)
    );
    assert_eq!(
        parts(super::disk_full(super::ended(GuestTurnEndReason::Completed, ""), roomy)).2,
        None
    );
}

#[test]
fn the_box_limits_note_names_the_memory_disk_cpus_and_turn_limit_the_box_got() {
    assert_eq!(
        super::constraints(4096, 32_096, 2, std::num::NonZeroU32::new(3600)),
        "# Box limits\n\n- Memory: 4096 MiB\n- Disk: 32096 MiB\n- CPUs: 2\n- Turn time limit: 3600 seconds\n\nA turn fails BoxDiskFull when the harness fails on a full disk and otherwise completes with a warning; it fails HarnessOutOfMemory when memory runs out and TurnTimedOut past its time limit.\n"
    );
    assert_eq!(
        super::constraints(4096, 32_096, 2, None),
        "# Box limits\n\n- Memory: 4096 MiB\n- Disk: 32096 MiB\n- CPUs: 2\n\nA turn fails BoxDiskFull when the harness fails on a full disk and otherwise completes with a warning; it fails HarnessOutOfMemory when memory runs out and TurnTimedOut past its time limit.\n"
    );
}
