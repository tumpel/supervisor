pub const TOKEN_PREFIX: &str = "sk-ant-oat";
const REFUSAL: &str = "OAuth error";
const RETRY: &str = "Press Enter to retry";
const ECHO: &str = "Paste code here";
const SECRET_RUN: usize = 32;
const PRINTED_LINES: usize = 8;
const PRINTED_LINE: usize = 400;

#[must_use]
pub fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '\u{1b}' {
            if character == '\r' {
                out.push('\n');
            } else if !character.is_control() || character == '\n' || character == '\t' {
                out.push(character);
            }
            continue;
        }
        match chars.next() {
            Some('[') => {
                for code in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&code) {
                        break;
                    }
                }
            }
            Some(']') => {
                while let Some(code) = chars.next() {
                    if code == '\u{7}' {
                        break;
                    }
                    if code == '\u{1b}' && chars.peek() == Some(&'\\') {
                        chars.next();
                        break;
                    }
                }
            }
            Some(intermediate) if ('\u{20}'..='\u{2f}').contains(&intermediate) => {
                for code in chars.by_ref() {
                    if ('\u{30}'..='\u{7e}').contains(&code) {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    out
}

fn url_char(character: char) -> bool {
    character.is_ascii_graphic() && !matches!(character, '"' | '\'' | '<' | '>' | '`')
}

#[must_use]
pub fn sign_in_url(text: &str) -> Option<String> {
    let mut rest = text;
    while let Some(at) = rest.find("https://") {
        let candidate = &rest[at..];
        let end = candidate
            .find(|character: char| !url_char(character))
            .unwrap_or(candidate.len());
        if end < candidate.len() {
            let url = candidate[..end].trim_end_matches(['.', ',', ')', ']']);
            if url.contains("/oauth/authorize") {
                return Some(url.to_string());
            }
        }
        rest = &candidate[end.max(1)..];
    }
    None
}

fn token_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '-' || character == '_'
}

#[must_use]
pub fn minted_token(text: &str) -> Option<String> {
    let at = text.find(TOKEN_PREFIX)?;
    let candidate = &text[at..];
    let end = candidate.find(|character: char| !token_char(character))?;
    (end > TOKEN_PREFIX.len() + 8).then(|| candidate[..end].to_string())
}

fn oauth_error(text: &str) -> Option<String> {
    let at = text.find(REFUSAL)?;
    let line = &text[at..];
    let end = line.find('\n')?;
    Some(redacted(line[..end].trim_end()))
}

fn shown(text: &str) -> Vec<&str> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('*') && !line.contains(ECHO))
        .collect()
}

fn awaits_retry(text: &str) -> Option<String> {
    let at = text.find(RETRY)?;
    let printed = shown(&text[..at]);
    let shown = printed.get(printed.len().saturating_sub(2)..).unwrap_or_default();
    if shown.is_empty() {
        return Some(format!("Claude Code stopped at \"{RETRY}\" without saying why"));
    }
    Some(redacted(&shown.join(" ")))
}

#[must_use]
pub fn refusal(text: &str) -> Option<String> {
    oauth_error(text).or_else(|| awaits_retry(text))
}

#[must_use]
pub fn printed_lines(text: &str) -> Vec<String> {
    let mut lines = shown(text);
    lines.dedup();
    let last = lines
        .get(lines.len().saturating_sub(PRINTED_LINES)..)
        .unwrap_or_default();
    last.iter()
        .map(|line| crate::tail(&redacted(line), PRINTED_LINE).to_string())
        .collect()
}

#[must_use]
pub fn redacted(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(token_char) {
        out.push_str(&rest[..at]);
        let run = &rest[at..];
        let end = run.find(|character: char| !token_char(character)).unwrap_or(run.len());
        let word = &run[..end];
        if word.starts_with("sk-ant-") {
            out.push_str("[token]");
        } else if word.len() >= SECRET_RUN {
            out.push_str("[redacted]");
        } else {
            out.push_str(word);
        }
        rest = &run[end..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRINTED: &str = "\u{1b}[2K\u{1b}[1GBrowser didn't open? Use the url below to sign in:\r\n\r\n\u{1b}[36mhttps://claude.ai/oauth/authorize?code=true&client_id=9d1c&response_type=code&scope=user%3Ainference&state=abc\u{1b}[39m\r\n\r\nPaste code here if prompted > ";

    #[test]
    fn the_sign_in_url_comes_out_of_the_terminal_output_once_it_is_complete() {
        let text = strip_ansi(PRINTED);
        assert_eq!(
            sign_in_url(&text).as_deref(),
            Some("https://claude.ai/oauth/authorize?code=true&client_id=9d1c&response_type=code&scope=user%3Ainference&state=abc")
        );
        let cut = &text[..text.find("state=").unwrap_or(0) + 7];
        assert_eq!(sign_in_url(cut), None, "a URL still arriving is not taken");
        assert_eq!(sign_in_url("see https://docs.anthropic.com/ for help\n"), None);
    }

    #[test]
    fn the_setup_output_yields_the_token_once_it_is_whole() {
        let printed = strip_ansi("\u{1b}[32m✓ Long-lived authentication token created successfully!\u{1b}[39m\r\n\r\nYour OAuth token (valid for 1 year):\r\n\r\nsk-ant-oat01-Ab_c-98765\r\n\r\nStore this token securely.");
        assert_eq!(minted_token(&printed).as_deref(), Some("sk-ant-oat01-Ab_c-98765"));
        assert_eq!(
            minted_token("token: sk-ant-oat01-AbC_d-1234"),
            None,
            "a token still arriving is not taken"
        );
    }

    #[test]
    fn a_refused_code_is_read_once_its_line_is_whole() {
        let printed = strip_ansi("\u{1b}[31C\u{1b}[1A********************-state\r\n\u{1b}[1C\u{1b}[4A\u{1b}[38;5;211mOAuth error: That code wasn't accepted. It may be incomplete, expired, already used, or from an earlier sign-in attempt. Sign in again to get a new code.\r\u{1b}[2B\u{1b}[39m\u{1b}[K\r\n\u{1b}[1C\u{1b}[1B\u{1b}[38;5;153mPress \u{1b}[1mEnter\u{1b}[22m to retry.");
        assert_eq!(
            refusal(&printed).as_deref(),
            Some("OAuth error: That code wasn't accepted. It may be incomplete, expired, already used, or from an earlier sign-in attempt. Sign in again to get a new code.")
        );
        assert_eq!(
            refusal("OAuth error: That code wasn't"),
            None,
            "a line still arriving is not taken"
        );
        assert_eq!(refusal("Paste code here if prompted > ****"), None);
    }

    #[test]
    fn an_error_that_waits_for_enter_is_read_once_the_prompt_shows() {
        let printed = strip_ansi("Paste code here if prompted > ****************\r\n\u{1b}[38;5;211mCouldn't verify your organization's settings (unavailable_timeout). Retry when the network is available. The token could not be confirmed revoked and was not shown.\u{1b}[39m\r\n\u{1b}[38;5;153mPress \u{1b}[1mEnter\u{1b}[22m to retry.");
        assert_eq!(
            refusal(&printed).as_deref(),
            Some("Couldn't verify your organization's settings (unavailable_timeout). Retry when the network is available. The token could not be confirmed revoked and was not shown.")
        );
        assert_eq!(refusal("Paste code here if prompted > ****\nProcessing"), None);
    }

    #[test]
    fn the_lines_printed_since_the_code_drop_the_echo_repeats_and_secrets() {
        let printed = strip_ansi("Paste code here if prompted > ****************\r\n\u{1b}[2K✻ Exchanging the code…\r\u{1b}[2K✻ Exchanging the code…\r\nfetch failed: token Xy7_kq9Lw2Rr8Tb5Mn3Pv6Zc1Hd4Gf0Ja rejected\r\n");
        assert_eq!(
            printed_lines(&printed),
            vec!["✻ Exchanging the code…", "fetch failed: token [redacted] rejected"]
        );
        assert_eq!(
            printed_lines("Paste code here if prompted > ****\r\n"),
            Vec::<String>::new()
        );
        let many = (1..=12)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(printed_lines(&many).first().map(String::as_str), Some("line 5"));
        let secret = "Qw7_kq9Lw2Rr8Tb5Mn3Pv6Zc1Hd4Gf0JaXy7_kq9L";
        let long = format!("{secret} {}", "ok ".repeat(127));
        let [line] = printed_lines(&long).try_into().unwrap_or_default();
        assert!(
            line.starts_with("[redacted] ok"),
            "a secret cut by the line limit stays hidden: {line}"
        );
    }

    #[test]
    fn a_character_set_switch_in_the_redraw_after_the_code_prints_nothing() {
        let printed = strip_ansi("\u{1b}(B\u{f}\u{1b}[?2004h\r\u{1b}[31C\u{1b}[1A************************************************\r\r\n\u{1b}[32G**************************\r\r\n\r\r\n");
        assert_eq!(printed_lines(&printed), Vec::<String>::new());
    }

    #[test]
    fn a_detail_never_carries_a_token_or_a_code() {
        assert_eq!(redacted("saw sk-ant-oat01-secret_value then"), "saw [token] then");
        assert_eq!(
            redacted("pasted Xy7_kq9Lw2Rr8Tb5Mn3Pv6Zc1Hd4Gf0Ja#st4te-Qw8Ee2Rt6Yu1Io5Pa9Sd3Fg7Hj, it failed"),
            "pasted [redacted]#[redacted], it failed"
        );
        assert_eq!(redacted("OAuth error: access_denied"), "OAuth error: access_denied");
    }
}
