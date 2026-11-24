use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tumpel_api::types::{GuestHarnessProtocol, GuestMetric, MetricKind, TokenKind};

const TOKENS: (&str, &str, &str) = (
    "turn.tokens",
    "{token}",
    "Tokens the harness reported a turn used, by kind",
);
const COST: (&str, &str, &str) = (
    "turn.cost",
    "uUSD",
    "What the harness reported a turn cost, in micro-US-dollars",
);

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Tokens {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

impl Tokens {
    fn plus(self, other: Self) -> Self {
        Self {
            input: self.input + other.input,
            output: self.output + other.output,
            cache_read: self.cache_read + other.cache_read,
            cache_write: self.cache_write + other.cache_write,
        }
    }

    fn covers(self, other: Self) -> bool {
        self.input >= other.input
            && self.output >= other.output
            && self.cache_read >= other.cache_read
            && self.cache_write >= other.cache_write
    }

    #[must_use]
    pub fn since(self, previous: Self) -> Self {
        if !self.covers(previous) {
            return self;
        }
        Self {
            input: self.input - previous.input,
            output: self.output - previous.output,
            cache_read: self.cache_read - previous.cache_read,
            cache_write: self.cache_write - previous.cache_write,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Cost {
    Reported { usd: f64 },
    NotReported,
}

impl Cost {
    fn plus(self, other: Self) -> Self {
        match (self, other) {
            (Self::Reported { usd: one }, Self::Reported { usd: two }) => Self::Reported { usd: one + two },
            _ => Self::NotReported,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Usage {
    pub tokens: Tokens,
    pub cost: Cost,
}

impl Usage {
    fn plus(self, other: Self) -> Self {
        Self {
            tokens: self.tokens.plus(other.tokens),
            cost: self.cost.plus(other.cost),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaudeModelUsage {
    input_tokens: u64,
    output_tokens: u64,
    cache_read_input_tokens: u64,
    cache_creation_input_tokens: u64,
    #[serde(rename = "costUSD")]
    cost_usd: Option<f64>,
}

#[derive(Deserialize)]
struct ClaudeResult {
    model_usage_since_last_result: HashMap<String, ClaudeModelUsage>,
}

#[derive(Deserialize)]
struct PiCost {
    total: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PiUsage {
    input: u64,
    output: u64,
    cache_read: u64,
    cache_write: u64,
    cost: PiCost,
}

#[derive(Deserialize)]
struct PiMessage {
    role: String,
    usage: PiUsage,
}

#[derive(Deserialize)]
struct PiMessageEnd {
    message: PiMessage,
}

#[derive(Deserialize)]
struct OpenCodeCache {
    read: u64,
    write: u64,
}

#[derive(Deserialize)]
struct OpenCodeTokens {
    input: u64,
    output: u64,
    reasoning: u64,
    cache: OpenCodeCache,
}

#[derive(Deserialize)]
struct OpenCodePart {
    cost: f64,
    tokens: OpenCodeTokens,
}

#[derive(Deserialize)]
struct OpenCodeStepFinish {
    part: OpenCodePart,
}

#[derive(Deserialize)]
struct CodexUsage {
    #[serde(rename = "input_tokens")]
    input: u64,
    #[serde(rename = "cached_input_tokens")]
    cached_input: u64,
    #[serde(rename = "output_tokens")]
    output: u64,
}

#[derive(Deserialize)]
struct CodexTurnCompleted {
    usage: CodexUsage,
}

fn claude(value: Value) -> Option<Usage> {
    let result: ClaudeResult = serde_json::from_value(value).ok()?;
    let models: Vec<ClaudeModelUsage> = result.model_usage_since_last_result.into_values().collect();
    if models.is_empty() {
        return None;
    }
    let cost = models
        .iter()
        .try_fold(0.0, |total, model| Some(total + model.cost_usd?));
    Some(Usage {
        tokens: models.iter().fold(Tokens::default(), |total, model| {
            total.plus(Tokens {
                input: model.input_tokens,
                output: model.output_tokens,
                cache_read: model.cache_read_input_tokens,
                cache_write: model.cache_creation_input_tokens,
            })
        }),
        cost: cost.map_or(Cost::NotReported, |usd| Cost::Reported { usd }),
    })
}

fn pi(value: Value) -> Option<Usage> {
    let end: PiMessageEnd = serde_json::from_value(value).ok()?;
    if end.message.role != "assistant" {
        return None;
    }
    let usage = end.message.usage;
    Some(Usage {
        tokens: Tokens {
            input: usage.input,
            output: usage.output,
            cache_read: usage.cache_read,
            cache_write: usage.cache_write,
        },
        cost: Cost::Reported { usd: usage.cost.total },
    })
}

fn open_code(value: Value) -> Option<Usage> {
    let step: OpenCodeStepFinish = serde_json::from_value(value).ok()?;
    let tokens = step.part.tokens;
    Some(Usage {
        tokens: Tokens {
            input: tokens.input,
            output: tokens.output + tokens.reasoning,
            cache_read: tokens.cache.read,
            cache_write: tokens.cache.write,
        },
        cost: Cost::Reported { usd: step.part.cost },
    })
}

fn codex(value: Value) -> Option<Usage> {
    let usage = serde_json::from_value::<CodexTurnCompleted>(value).ok()?.usage;
    Some(Usage {
        tokens: Tokens {
            input: usage.input.saturating_sub(usage.cached_input),
            output: usage.output,
            cache_read: usage.cached_input,
            cache_write: 0,
        },
        cost: Cost::NotReported,
    })
}

fn reading(protocol: &GuestHarnessProtocol, line: &str) -> Option<Usage> {
    let value: Value = serde_json::from_str(line).ok()?;
    let kind = value.get("type").and_then(Value::as_str)?;
    match (protocol, kind) {
        (GuestHarnessProtocol::ClaudeStreamJson, "result") => claude(value),
        (GuestHarnessProtocol::PiRpc, "message_end") => pi(value),
        (GuestHarnessProtocol::PromptOnStdin, "step_finish") => open_code(value),
        (GuestHarnessProtocol::CodexExecJson, "turn.completed") => codex(value),
        _ => None,
    }
}

pub struct Spend {
    protocol: GuestHarnessProtocol,
    usage: Option<Usage>,
}

impl Spend {
    #[must_use]
    pub fn new(protocol: &GuestHarnessProtocol) -> Self {
        Self {
            protocol: protocol.clone(),
            usage: None,
        }
    }

    pub fn read(&mut self, line: &str) {
        let Some(read) = reading(&self.protocol, line) else {
            return;
        };
        self.usage = Some(match self.usage {
            Some(usage) if self.protocol != GuestHarnessProtocol::CodexExecJson => usage.plus(read),
            _ => read,
        });
    }

    #[must_use]
    pub fn usage(&self) -> Option<Usage> {
        self.usage
    }
}

fn metric(
    (name, unit, description): (&str, &str, &str),
    value: f64,
    dimensions: &[(&str, String)],
) -> Result<GuestMetric, String> {
    Ok(GuestMetric {
        description: description
            .try_into()
            .map_err(|error| format!("{name} description: {error}"))?,
        dimensions: dimensions
            .iter()
            .map(|(key, value)| {
                value
                    .clone()
                    .try_into()
                    .map(|value| ((*key).to_string(), value))
                    .map_err(|error| format!("{name} dimension {key}: {error}"))
            })
            .collect::<Result<_, String>>()?,
        kind: MetricKind::Turn,
        metric: name.try_into().map_err(|error| format!("{name}: {error}"))?,
        unit: unit.try_into().map_err(|error| format!("{name} unit: {error}"))?,
        value,
    })
}

#[allow(clippy::cast_precision_loss)]
pub fn metrics(usage: Usage, priced: bool) -> Result<Vec<GuestMetric>, String> {
    let Usage { tokens, cost } = usage;
    let mut metrics = [
        (TokenKind::Input, tokens.input),
        (TokenKind::Output, tokens.output),
        (TokenKind::CacheRead, tokens.cache_read),
        (TokenKind::CacheWrite, tokens.cache_write),
    ]
    .into_iter()
    .map(|(kind, count)| metric(TOKENS, count as f64, &[("tokenKind", kind.to_string())]))
    .collect::<Result<Vec<_>, _>>()?;
    if let (Cost::Reported { usd }, true) = (cost, priced) {
        metrics.push(metric(COST, (usd * 1_000_000.0).round(), &[])?);
    }
    Ok(metrics)
}

#[cfg(test)]
mod tests;
