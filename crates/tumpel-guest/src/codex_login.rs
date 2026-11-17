use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, SecondsFormat};
use serde::Deserialize;

const DEVICE_PATH: &str = "/codex/device";
const CODE_HEADING: &str = "one-time code";
const CODE_LENGTH: std::ops::RangeInclusive<usize> = 4..=64;

#[derive(Debug, PartialEq, Eq)]
pub struct DevicePrompt {
    pub url: String,
    pub code: String,
}

fn device_url(line: &str) -> Option<String> {
    let at = line.find("https://")?;
    let url = line[at..].split_whitespace().next()?;
    url.ends_with(DEVICE_PATH).then(|| url.to_string())
}

fn user_code(line: &str) -> Option<String> {
    let code = line.trim();
    let valid = CODE_LENGTH.contains(&code.len())
        && code
            .chars()
            .all(|character| character.is_ascii_uppercase() || character.is_ascii_digit() || character == '-');
    valid.then(|| code.to_string())
}

#[must_use]
pub fn device_prompt(text: &str) -> Option<DevicePrompt> {
    let lines: Vec<&str> = text.lines().collect();
    let url = lines.iter().find_map(|line| device_url(line))?;
    let heading = lines.iter().position(|line| line.contains(CODE_HEADING))?;
    let mut after = lines[heading + 1..].iter().map(|line| line.trim());
    let code = after.find(|line| !line.is_empty())?;
    after.next()?;
    Some(DevicePrompt {
        url,
        code: user_code(code)?,
    })
}

#[derive(Deserialize)]
struct AuthFile {
    tokens: Option<AuthTokens>,
}

#[derive(Deserialize)]
struct AuthTokens {
    access_token: Option<String>,
}

#[derive(Deserialize)]
struct Claims {
    exp: Option<i64>,
}

pub fn access_token_expiry(auth_json: &str) -> Result<String, String> {
    let auth: AuthFile =
        serde_json::from_str(auth_json).map_err(|error| format!("auth.json is not the JSON Codex writes: {error}"))?;
    let token = auth
        .tokens
        .and_then(|tokens| tokens.access_token)
        .filter(|token| !token.is_empty())
        .ok_or("auth.json holds no tokens.access_token")?;
    let payload = token
        .split('.')
        .nth(1)
        .ok_or("tokens.access_token in auth.json is not a JWT")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .map_err(|_| "the payload of tokens.access_token in auth.json is not base64url")?;
    let claims: Claims =
        serde_json::from_slice(&bytes).map_err(|_| "the payload of tokens.access_token in auth.json is not JSON")?;
    let exp = claims
        .exp
        .ok_or("tokens.access_token in auth.json carries no exp claim")?;
    Ok(DateTime::from_timestamp(exp, 0)
        .ok_or_else(|| format!("the exp claim {exp} of tokens.access_token is not a time"))?
        .to_rfc3339_opts(SecondsFormat::Secs, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup_output::strip_ansi;

    const PRINTED: &str = "\r\nWelcome to Codex [v\u{1b}[90m0.169.0\u{1b}[0m]\r\n\u{1b}[90mOpenAI's command-line coding agent\u{1b}[0m\r\n\r\nFollow these steps to sign in with ChatGPT using device code authorization:\r\n\r\n1. Open this link in your browser and sign in to your account\r\n   \u{1b}[94mhttps://auth.openai.com/codex/device\u{1b}[0m\r\n\r\n2. Enter this one-time code \u{1b}[90m(expires in 15 minutes)\u{1b}[0m\r\n   \u{1b}[94mK7QD-3MZP9\u{1b}[0m\r\n\r\n\u{1b}[90mDevice codes are a common phishing target. Never share this code.\u{1b}[0m\r\n";

    #[test]
    fn the_device_url_and_code_come_out_of_the_login_output_once_the_code_line_is_whole() {
        let text = strip_ansi(PRINTED);
        assert_eq!(
            device_prompt(&text),
            Some(DevicePrompt {
                url: "https://auth.openai.com/codex/device".to_string(),
                code: "K7QD-3MZP9".to_string(),
            })
        );
        let cut = &text[..text.find("3MZP9").unwrap_or(0) + 2];
        assert_eq!(device_prompt(cut), None, "a code still arriving is not taken");
        assert_eq!(
            device_prompt("Starting local login server on http://localhost:1455\n"),
            None
        );
    }

    #[test]
    fn a_code_line_that_is_not_a_code_is_refused() {
        let text =
            "https://auth.openai.com/codex/device\nEnter this one-time code\n  error: device code request failed\n\n";
        assert_eq!(device_prompt(text), None);
    }

    fn jwt(claims: &str) -> String {
        format!("eyJhbGciOiJSUzI1NiJ9.{}.c2lnbmF0dXJl", URL_SAFE_NO_PAD.encode(claims))
    }

    #[test]
    fn the_expiry_of_the_access_token_comes_out_of_auth_json() {
        let token = jwt(r#"{"sub":"user-1","exp":1795000000}"#);
        let auth = format!(
            r#"{{"OPENAI_API_KEY":null,"tokens":{{"id_token":"x","access_token":"{token}","refresh_token":"r","account_id":"a"}},"last_refresh":"2026-11-16T10:00:00Z"}}"#
        );
        assert_eq!(access_token_expiry(&auth), Ok("2026-11-18T11:06:40Z".to_string()));
    }

    #[test]
    fn auth_json_without_a_usable_access_token_is_refused_without_quoting_it() {
        assert_eq!(
            access_token_expiry(r#"{"OPENAI_API_KEY":"sk-1"}"#),
            Err("auth.json holds no tokens.access_token".to_string())
        );
        assert_eq!(
            access_token_expiry(r#"{"tokens":{"access_token":"opaque-secret"}}"#),
            Err("tokens.access_token in auth.json is not a JWT".to_string())
        );
        let token = jwt(r#"{"sub":"user-1"}"#);
        assert_eq!(
            access_token_expiry(&format!(r#"{{"tokens":{{"access_token":"{token}"}}}}"#)),
            Err("tokens.access_token in auth.json carries no exp claim".to_string())
        );
    }
}
