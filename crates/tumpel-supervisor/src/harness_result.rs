use serde_json::Value;
use tumpel_api::types::{GuestHarnessProtocol, GuestTurnEnd, GuestTurnEndReason, Sequence, TurnFailureReason};
use tumpel_guest::tail;

const HARNESS_TEXT_LIMIT: usize = 2048;

pub fn turn_end(
    end: GuestTurnEnd,
    error: Option<(TurnFailureReason, String)>,
) -> Result<Sequence, (TurnFailureReason, String)> {
    match (end.reason, error) {
        (GuestTurnEndReason::HarnessOutOfMemory, _) => Err((TurnFailureReason::HarnessOutOfMemory, end.detail)),
        (GuestTurnEndReason::Cancelled, _) => Err((TurnFailureReason::Cancelled, end.detail)),
        (GuestTurnEndReason::Completed, Some(failure)) => Err(failure),
        (GuestTurnEndReason::HarnessExited, Some((reason, detail))) => Err((
            reason,
            format!("{detail}; the harness stopped ({}): {}", end.reason, end.detail),
        )),
        (GuestTurnEndReason::Completed, None) => Ok(end.last_line_seq),
        (GuestTurnEndReason::HarnessExited | GuestTurnEndReason::HarnessStartFailed, _) => Err((
            TurnFailureReason::HarnessExited,
            format!("the harness stopped ({}): {}", end.reason, end.detail),
        )),
    }
}

const REFUSAL_MARKERS: [&str; 7] = [
    "invalid api key",
    "invalid bearer token",
    "oauth token",
    "authentication_error",
    "please run /login",
    "api error: 401",
    "no api key found",
];

const PI_REFUSAL_STATUSES: [&str; 2] = ["401", "403"];

const OPENCODE_REFUSAL_STATUSES: [u64; 2] = [401, 403];

const CODEX_REFUSAL_STATUSES: [&str; 2] = ["unexpected status 401", "unexpected status 403"];

const CODEX_STARTUP_REFUSALS: [&str; 3] = [
    "personal access token metadata request failed with status 401",
    "personal access token metadata request failed with status 403",
    "failed to decode agent identity JWT",
];

pub fn exit_result(protocol: GuestHarnessProtocol, end: &GuestTurnEnd) -> Option<(TurnFailureReason, String)> {
    if protocol != GuestHarnessProtocol::CodexExecJson || end.reason != GuestTurnEndReason::HarnessExited {
        return None;
    }
    let refusal = end
        .detail
        .lines()
        .find(|line| CODEX_STARTUP_REFUSALS.iter().any(|marker| line.contains(marker)))?;
    Some(classify(refusal.trim(), true))
}

pub fn error_result(protocol: GuestHarnessProtocol, line: &str) -> Option<(TurnFailureReason, String)> {
    let value: Value = serde_json::from_str(line).ok()?;
    match protocol {
        GuestHarnessProtocol::ClaudeStreamJson => claude_error(&value),
        GuestHarnessProtocol::PiRpc => pi_error(&value),
        GuestHarnessProtocol::PromptOnStdin => opencode_error(&value),
        GuestHarnessProtocol::CodexExecJson => codex_error(&value),
        GuestHarnessProtocol::ShellLines => None,
    }
}

fn pi_error(value: &Value) -> Option<(TurnFailureReason, String)> {
    let text = match value.get("type").and_then(Value::as_str)? {
        "response" if value.get("success").and_then(Value::as_bool) == Some(false) => value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("a refused command without an error"),
        "message_end" if value.pointer("/message/stopReason").and_then(Value::as_str) == Some("error") => value
            .pointer("/message/errorMessage")
            .and_then(Value::as_str)
            .unwrap_or("an error message without errorMessage"),
        _ => return None,
    };
    let refused = PI_REFUSAL_STATUSES.iter().any(|status| text.starts_with(status));
    Some(classify(text, refused))
}

fn codex_error(value: &Value) -> Option<(TurnFailureReason, String)> {
    if value.get("type").and_then(Value::as_str) != Some("turn.failed") {
        return None;
    }
    let text = value
        .pointer("/error/message")
        .and_then(Value::as_str)
        .unwrap_or("an error event without a message");
    let refused = CODEX_REFUSAL_STATUSES.iter().any(|status| text.contains(status));
    Some(classify(text, refused))
}

fn opencode_error(value: &Value) -> Option<(TurnFailureReason, String)> {
    if value.get("type").and_then(Value::as_str) != Some("error") {
        return None;
    }
    let error = value.get("error");
    let name = error.and_then(|error| error.get("name")).and_then(Value::as_str);
    let text = error
        .and_then(|error| error.pointer("/data/message"))
        .and_then(Value::as_str)
        .or(name)
        .unwrap_or("an error record with neither a message nor a name");
    let status = error
        .and_then(|error| error.pointer("/data/statusCode"))
        .and_then(Value::as_u64);
    let refused =
        name == Some("ProviderAuthError") || status.is_some_and(|status| OPENCODE_REFUSAL_STATUSES.contains(&status));
    Some(classify(text, refused))
}

fn claude_error(value: &Value) -> Option<(TurnFailureReason, String)> {
    if value.get("type").and_then(Value::as_str) != Some("result")
        || value.get("is_error").and_then(Value::as_bool) != Some(true)
    {
        return None;
    }
    let result = value
        .get("result")
        .and_then(Value::as_str)
        .or_else(|| value.get("subtype").and_then(Value::as_str))
        .unwrap_or("an error result with neither result nor subtype");
    Some(classify(result, false))
}

fn classify(result: &str, refused: bool) -> (TurnFailureReason, String) {
    let lowered = result.to_lowercase();
    if refused || REFUSAL_MARKERS.iter().any(|marker| lowered.contains(marker)) {
        return (
            TurnFailureReason::CredentialRefused,
            format!(
                "the provider refused the credential: {}; renew the persona's credential",
                tail(result, HARNESS_TEXT_LIMIT)
            ),
        );
    }
    (
        TurnFailureReason::HarnessReportedError,
        format!(
            "the harness ended the turn with an error: {}; check the session's model and harness version",
            tail(result, HARNESS_TEXT_LIMIT)
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM: GuestHarnessProtocol = GuestHarnessProtocol::ClaudeStreamJson;

    fn reason_of(protocol: GuestHarnessProtocol, line: &str) -> Option<TurnFailureReason> {
        error_result(protocol, line).map(|(reason, _)| reason)
    }

    #[test]
    fn an_error_result_naming_an_invalid_key_is_a_refusal() {
        let line = r#"{"type":"result","subtype":"success","is_error":true,"result":"Invalid API key · Fix external API key"}"#;
        assert_eq!(
            error_result(STREAM, line),
            Some((
                TurnFailureReason::CredentialRefused,
                "the provider refused the credential: Invalid API key · Fix external API key; renew the persona's credential".to_string()
            ))
        );
    }

    #[test]
    fn an_expired_oauth_token_is_a_refusal() {
        let line = r#"{"type":"result","is_error":true,"result":"OAuth token has expired. Please run /login"}"#;
        assert_eq!(reason_of(STREAM, line), Some(TurnFailureReason::CredentialRefused));
    }

    #[test]
    fn a_provider_401_is_a_refusal() {
        let status = r#"{"type":"result","is_error":true,"result":"API Error: 401 {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"invalid x-api-key\"}}"}"#;
        let kind = r#"{"type":"result","is_error":true,"result":"{\"type\":\"error\",\"error\":{\"type\":\"authentication_error\"}}"}"#;
        for line in [status, kind] {
            assert_eq!(reason_of(STREAM, line), Some(TurnFailureReason::CredentialRefused));
        }
    }

    #[test]
    fn a_server_error_whose_request_id_holds_401_is_not_a_refusal() {
        let line = r#"{"type":"result","is_error":true,"result":"API Error: 500 {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Internal server error\"},\"request_id\":\"req_011CU401abc\"}"}"#;
        assert_eq!(reason_of(STREAM, line), Some(TurnFailureReason::HarnessReportedError));
    }

    #[test]
    fn a_successful_result_or_a_line_that_is_no_result_is_no_error() {
        let ok = r#"{"type":"result","is_error":false,"result":"Invalid API key handling is fixed"}"#;
        let assistant = r#"{"type":"assistant","is_error":true,"result":"Invalid API key"}"#;
        for line in [ok, assistant, "not json"] {
            assert_eq!(error_result(STREAM, line), None);
        }
    }

    #[test]
    fn an_error_result_that_is_not_a_refusal_names_what_the_harness_reported() {
        let line = r#"{"type":"result","subtype":"success","is_error":true,"result":"API Error: 404 {\"type\":\"error\",\"error\":{\"type\":\"not_found_error\",\"message\":\"model: claude-opus-4-1-20250805\"}}"}"#;
        let (reason, detail) = error_result(STREAM, line).expect("an error result");
        assert_eq!(reason, TurnFailureReason::HarnessReportedError);
        assert!(detail.contains("model: claude-opus-4-1-20250805"), "{detail}");
        assert!(
            detail.ends_with("check the session's model and harness version"),
            "{detail}"
        );
    }

    #[test]
    fn an_error_result_without_text_names_its_subtype() {
        let line = r#"{"type":"result","subtype":"error_max_turns","is_error":true}"#;
        let (reason, detail) = error_result(STREAM, line).expect("an error result");
        assert_eq!(reason, TurnFailureReason::HarnessReportedError);
        assert!(detail.contains("error_max_turns"), "{detail}");
    }

    #[test]
    fn an_error_result_with_neither_result_nor_subtype_still_fails_the_turn() {
        let line = r#"{"type":"result","is_error":true}"#;
        let (reason, detail) = error_result(STREAM, line).expect("an error result");
        assert_eq!(reason, TurnFailureReason::HarnessReportedError);
        assert!(
            detail.contains("an error result with neither result nor subtype"),
            "{detail}"
        );
    }

    #[test]
    fn the_harness_text_in_a_detail_is_capped() {
        let text = format!("start{}end", "x".repeat(10_000));
        let line = format!(r#"{{"type":"result","is_error":true,"result":"{text}"}}"#);
        let (_, detail) = error_result(STREAM, &line).expect("an error result");
        assert!(detail.len() < HARNESS_TEXT_LIMIT + 100, "{}", detail.len());
        assert!(detail.contains("xend"), "{detail}");
        assert!(!detail.contains("start"), "{detail}");
    }

    #[test]
    fn a_shell_harness_never_reports_an_error_result() {
        let line = r#"{"type":"result","is_error":true,"result":"Invalid API key"}"#;
        assert_eq!(error_result(GuestHarnessProtocol::ShellLines, line), None);
    }

    const PI: GuestHarnessProtocol = GuestHarnessProtocol::PiRpc;

    #[test]
    fn a_pi_provider_401_or_a_missing_key_is_a_refusal() {
        let status = r#"{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"401: {\"message\":\"Missing Authentication header\",\"code\":401}"}}"#;
        let missing = r#"{"type":"response","command":"prompt","success":false,"error":"No API key found for openrouter.\n\nUse /login to log into a provider"}"#;
        for line in [status, missing] {
            assert_eq!(
                reason_of(PI, line),
                Some(TurnFailureReason::CredentialRefused),
                "{line}"
            );
        }
    }

    #[test]
    fn a_pi_error_that_is_not_a_refusal_names_what_pi_reported() {
        let line = r#"{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"404: No endpoints found for qwen/qwen3-coderx. Request id 401"}}"#;
        let (reason, detail) = error_result(PI, line).expect("an error message");
        assert_eq!(reason, TurnFailureReason::HarnessReportedError);
        assert!(detail.contains("No endpoints found for qwen/qwen3-coderx"), "{detail}");
    }

    #[test]
    fn a_pi_line_that_carries_no_error_is_no_error() {
        let stopped =
            r#"{"type":"message_end","message":{"role":"assistant","stopReason":"stop","errorMessage":"401"}}"#;
        let accepted = r#"{"type":"response","command":"prompt","success":true,"data":{"disposition":"started"}}"#;
        for line in [stopped, accepted] {
            assert_eq!(error_result(PI, line), None, "{line}");
        }
    }

    const OPENCODE: GuestHarnessProtocol = GuestHarnessProtocol::PromptOnStdin;

    #[test]
    fn an_opencode_provider_401_or_a_missing_key_is_a_refusal() {
        let status = r#"{"type":"error","timestamp":1763215200000,"sessionID":"ses_5a1b2c3d4e5f","error":{"name":"APIError","data":{"message":"User not found.","statusCode":401,"isRetryable":false,"responseHeaders":{"content-type":"application/json"},"responseBody":"{\"error\":{\"message\":\"User not found.\",\"code\":401}}","metadata":{}}}}"#;
        let missing = r#"{"type":"error","timestamp":1763215200000,"sessionID":"ses_5a1b2c3d4e5f","error":{"name":"ProviderAuthError","data":{"providerID":"openrouter","message":"OpenRouter API key is missing. Pass it using the apiKey parameter or the OPENROUTER_API_KEY environment variable."}}}"#;
        for line in [status, missing] {
            assert_eq!(
                reason_of(OPENCODE, line),
                Some(TurnFailureReason::CredentialRefused),
                "{line}"
            );
        }
    }

    #[test]
    fn an_opencode_error_that_is_not_a_refusal_names_what_opencode_reported() {
        let line = r#"{"type":"error","timestamp":1763215200000,"sessionID":"ses_5a1b2c3d4e5f","error":{"name":"APIError","data":{"message":"qwen/qwen3-coderx is not a valid model ID","statusCode":400,"isRetryable":false,"metadata":{}}}}"#;
        let (reason, detail) = error_result(OPENCODE, line).expect("an error record");
        assert_eq!(reason, TurnFailureReason::HarnessReportedError);
        assert!(detail.contains("qwen/qwen3-coderx is not a valid model ID"), "{detail}");
    }

    const CODEX: GuestHarnessProtocol = GuestHarnessProtocol::CodexExecJson;

    #[test]
    fn a_codex_provider_401_or_403_is_a_refusal() {
        let unauthorized = r#"{"type":"turn.failed","error":{"message":"unexpected status 401 Unauthorized: Incorrect API key provided: sk-bogus., url: https://api.openai.com/v1/responses"}}"#;
        let forbidden = r#"{"type":"turn.failed","error":{"message":"unexpected status 403 Forbidden: Key limit exceeded, url: https://openrouter.ai/api/v1/responses"}}"#;
        for line in [unauthorized, forbidden] {
            assert_eq!(
                reason_of(CODEX, line),
                Some(TurnFailureReason::CredentialRefused),
                "{line}"
            );
        }
    }

    fn codex_exit(detail: &str) -> GuestTurnEnd {
        serde_json::from_value(serde_json::json!({
            "type": "TurnEnd",
            "turnId": "turn-1",
            "reason": "HarnessExited",
            "detail": detail,
            "lastLineSeq": 0,
        }))
        .expect("the test end matches GuestTurnEnd")
    }

    #[test]
    fn a_codex_access_token_codex_refuses_at_startup_is_a_refusal() {
        let revoked = "the harness exited (exit status: 1) mid-turn; Reading prompt from stdin...\npersonal access token metadata request failed with status 401 Unauthorized\n";
        let not_a_token = "the harness exited (exit status: 1) mid-turn; failed to decode agent identity JWT: invalid credential JSON (Data at line 1 column 53)\n";
        for detail in [revoked, not_a_token] {
            let (reason, message) = exit_result(CODEX, &codex_exit(detail)).expect("a refusal");
            assert_eq!(reason, TurnFailureReason::CredentialRefused, "{detail}");
            assert!(message.ends_with("renew the persona's credential"), "{message}");
        }
    }

    #[test]
    fn a_codex_error_line_before_a_completed_turn_does_not_fail_it() {
        let lines = [
            r#"{"type":"error","message":"stream disconnected before completion: error sending request for url (https://api.openai.com/v1/responses)"}"#,
            r#"{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,"output_tokens":3}}"#,
        ];
        assert_eq!(lines.iter().find_map(|line| error_result(CODEX, line)), None);
    }
}
