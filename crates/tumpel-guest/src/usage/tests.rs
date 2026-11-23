use tumpel_api::types::GuestHarnessProtocol;

use super::{metrics, Cost, Spend, Tokens, Usage};

fn read(protocol: &GuestHarnessProtocol, transcript: &str) -> Option<Usage> {
    let mut spend = Spend::new(protocol);
    for line in transcript.lines() {
        spend.read(line);
    }
    spend.usage()
}

fn tokens(input: u64, output: u64, cache_read: u64, cache_write: u64) -> Tokens {
    Tokens {
        input,
        output,
        cache_read,
        cache_write,
    }
}

fn micro_usd(usage: Usage) -> Option<i64> {
    match usage.cost {
        #[allow(clippy::cast_possible_truncation)]
        Cost::Reported { usd } => Some((usd * 1_000_000.0).round() as i64),
        Cost::NotReported => None,
    }
}

#[test]
fn claude_code_reads_the_usage_since_the_last_result_not_the_restored_running_total() {
    let usage = read(
        &GuestHarnessProtocol::ClaudeStreamJson,
        include_str!("fixtures/claude-code.jsonl"),
    )
    .expect("the result line carries usage");
    assert_eq!(usage.tokens, tokens(2, 3, 19_916, 162));
    assert_eq!(micro_usd(usage), Some(233));
}

#[test]
fn pi_sums_every_assistant_message_end_and_ignores_agent_end() {
    let usage =
        read(&GuestHarnessProtocol::PiRpc, include_str!("fixtures/pi.jsonl")).expect("message_end carries usage");
    assert_eq!(usage.tokens, tokens(4_356, 62, 0, 0));
    assert_eq!(micro_usd(usage), Some(1_866));
}

#[test]
fn opencode_sums_every_step_finish_and_counts_reasoning_as_output() {
    let usage = read(
        &GuestHarnessProtocol::PromptOnStdin,
        include_str!("fixtures/opencode.jsonl"),
    )
    .expect("step_finish carries usage");
    assert_eq!(usage.tokens, tokens(1_821, 173, 5_617, 0));
    assert_eq!(micro_usd(usage), Some(520));
}

#[test]
fn codex_reports_tokens_without_cost_and_splits_cached_input_out_of_input() {
    let usage = read(
        &GuestHarnessProtocol::CodexExecJson,
        include_str!("fixtures/codex.jsonl"),
    )
    .expect("turn.completed carries usage");
    assert_eq!(usage.tokens, tokens(25_790, 6_075, 420_864, 0));
    assert_eq!(usage.cost, Cost::NotReported);
}

#[test]
fn a_codex_turn_is_the_difference_of_the_thread_totals_and_a_shrunk_total_starts_a_new_thread() {
    let first = tokens(100, 10, 50, 0);
    let second = tokens(180, 25, 120, 0);
    assert_eq!(second.since(first), tokens(80, 15, 70, 0));
    assert_eq!(first.since(second), first);
}

#[test]
fn a_claude_result_without_usage_since_the_last_result_reports_nothing() {
    let line = r#"{"type":"result","subtype":"success","total_cost_usd":0.5,"modelUsage":{"m":{"inputTokens":1,"outputTokens":1,"cacheReadInputTokens":0,"cacheCreationInputTokens":0,"costUSD":0.5}}}"#;
    assert_eq!(read(&GuestHarnessProtocol::ClaudeStreamJson, line), None);
}

#[test]
fn a_model_without_a_price_leaves_the_claude_turn_unpriced() {
    let line = r#"{"type":"result","model_usage_since_last_result":{"a":{"inputTokens":1,"outputTokens":2,"cacheReadInputTokens":3,"cacheCreationInputTokens":4,"costUSD":0.1},"b":{"inputTokens":1,"outputTokens":2,"cacheReadInputTokens":3,"cacheCreationInputTokens":4}}}"#;
    let usage = read(&GuestHarnessProtocol::ClaudeStreamJson, line).expect("usage");
    assert_eq!(usage.tokens, tokens(2, 4, 6, 8));
    assert_eq!(usage.cost, Cost::NotReported);
}

fn summary(usage: Usage, priced: bool) -> Vec<(String, Option<String>, f64)> {
    metrics(usage, priced)
        .expect("metrics")
        .into_iter()
        .map(|metric| {
            (
                metric.metric.to_string(),
                metric.dimensions.get("tokenKind").map(ToString::to_string),
                metric.value,
            )
        })
        .collect()
}

#[test]
fn a_priced_turn_records_tokens_per_kind_and_its_cost_in_micro_dollars() {
    let usage = Usage {
        tokens: tokens(1, 2, 3, 4),
        cost: Cost::Reported { usd: 0.012_345_6 },
    };
    assert_eq!(
        summary(usage, true),
        [
            ("turn.tokens".to_string(), Some("Input".to_string()), 1.0),
            ("turn.tokens".to_string(), Some("Output".to_string()), 2.0),
            ("turn.tokens".to_string(), Some("CacheRead".to_string()), 3.0),
            ("turn.tokens".to_string(), Some("CacheWrite".to_string()), 4.0),
            ("turn.cost".to_string(), None, 12_346.0),
        ]
    );
}

#[test]
fn a_turn_without_a_reported_or_trusted_cost_records_tokens_and_no_cost() {
    let unpriced = Usage {
        tokens: tokens(1, 2, 3, 4),
        cost: Cost::Reported { usd: 0.5 },
    };
    let tokens_only = Usage {
        tokens: tokens(1, 2, 3, 4),
        cost: Cost::NotReported,
    };
    for (usage, priced) in [(unpriced, false), (tokens_only, true)] {
        let recorded = summary(usage, priced);
        assert_eq!(recorded.len(), 4);
        assert!(recorded.iter().all(|(metric, ..)| metric == "turn.tokens"));
    }
}
