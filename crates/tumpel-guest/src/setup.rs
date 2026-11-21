use std::ffi::CStr;
use std::fs::File;
use std::io::{Read, Write};
use std::num::NonZeroU32;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::memory::Confinement;
use serde_json::json;
use tumpel_api::types::{
    GuestClaudeSetupCode, GuestClaudeSetupFailed, GuestClaudeSetupFailedType, GuestClaudeSetupFailureReason,
    GuestClaudeSetupStart, GuestClaudeSetupStored, GuestClaudeSetupStoredType, GuestClaudeSetupUrl,
    GuestClaudeSetupUrlType, GuestHarness, GuestInput, GuestMessage,
};
use tumpel_guest::codex_login::{access_token_expiry, device_prompt, DevicePrompt};
use tumpel_guest::setup_output::{minted_token, printed_lines, redacted, refusal, sign_in_url, strip_ansi};
use tumpel_guest::{encode, tail, Decoder};

const COLUMNS: u16 = 4000;
const SETTLE: Duration = Duration::from_millis(500);
const ECHO_SETTLE_LIMIT: Duration = Duration::from_secs(3);
const DETAIL_TAIL: usize = 2048;
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const EXITED: &str = "Claude Code exited without printing a token; start the Claude setup again";
const CODEX: &str = "Codex";
const CODEX_LOGIN: [&str; 4] = ["-c", "cli_auth_credentials_store=\"file\"", "login", "--device-auth"];
const CLAUDE_LOGIN: [&str; 1] = ["setup-token"];

struct Terminal {
    child: Child,
    master: File,
    output: Receiver<Vec<u8>>,
    text: String,
}

impl Drop for Terminal {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Terminal {
    fn read(&mut self, wait: Duration) -> Result<bool, ()> {
        match self.output.recv_timeout(wait) {
            Ok(bytes) => {
                self.text.push_str(&strip_ansi(&String::from_utf8_lossy(&bytes)));
                Ok(true)
            }
            Err(RecvTimeoutError::Timeout) => Ok(false),
            Err(RecvTimeoutError::Disconnected) => Err(()),
        }
    }

    fn tail(&self) -> String {
        redacted(tail(&self.text, DETAIL_TAIL))
    }
}

fn last_error(doing: &str) -> String {
    format!("{doing}: {}", std::io::Error::last_os_error())
}

fn open_pty() -> Result<(File, OwnedFd), String> {
    let master = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
    if master < 0 {
        return Err(last_error("opening a pseudo-terminal"));
    }
    let master = unsafe { OwnedFd::from_raw_fd(master) };
    if unsafe { libc::grantpt(master.as_raw_fd()) } != 0 || unsafe { libc::unlockpt(master.as_raw_fd()) } != 0 {
        return Err(last_error("unlocking the pseudo-terminal"));
    }
    let mut name = [0 as libc::c_char; 128];
    if unsafe { libc::ptsname_r(master.as_raw_fd(), name.as_mut_ptr(), name.len()) } != 0 {
        return Err(last_error("naming the pseudo-terminal"));
    }
    let size = libc::winsize {
        ws_row: 50,
        ws_col: COLUMNS,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    if unsafe { libc::ioctl(master.as_raw_fd(), libc::TIOCSWINSZ, &size) } != 0 {
        return Err(last_error("sizing the pseudo-terminal"));
    }
    let path = unsafe { CStr::from_ptr(name.as_ptr()) };
    let slave = unsafe { libc::open(path.as_ptr(), libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
    if slave < 0 {
        return Err(last_error("opening the pseudo-terminal's other end"));
    }
    Ok((File::from(master), unsafe { OwnedFd::from_raw_fd(slave) }))
}

fn spawn(config: &GuestHarness, confinement: &Confinement, arguments: &[&str]) -> Result<Terminal, String> {
    let program = config
        .command
        .first()
        .ok_or("the image's guest.json names no harness command")?;
    let (master, slave) = open_pty()?;
    let duplicate = |fd: &OwnedFd| {
        fd.try_clone()
            .map(Stdio::from)
            .map_err(|error| format!("sharing the pseudo-terminal: {error}"))
    };
    let environment = &config.environment;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .env("PATH", &*environment.path)
        .env("HOME", &*environment.home)
        .env("LANG", &*environment.lang)
        .envs(
            config
                .fixed_variables
                .iter()
                .map(|variable| (variable.name.as_str(), variable.value.as_str())),
        )
        .env("TERM", "xterm-256color")
        .current_dir(&*environment.home)
        .stdin(duplicate(&slave)?)
        .stdout(duplicate(&slave)?)
        .stderr(duplicate(&slave)?);
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 || libc::ioctl(0, libc::TIOCSCTTY, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    confinement.apply(&mut command);
    let child = command
        .spawn()
        .map_err(|error| format!("starting {program} {}: {error}", arguments.join(" ")))?;
    drop(slave);
    let mut reader = master
        .try_clone()
        .map_err(|error| format!("reading the pseudo-terminal: {error}"))?;
    let (sender, output) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buffer = vec![0u8; 16 * 1024];
        while let Ok(read) = reader.read(&mut buffer) {
            if read == 0 || sender.send(buffer[..read].to_vec()).is_err() {
                return;
            }
        }
    });
    Ok(Terminal {
        child,
        master,
        output,
        text: String::new(),
    })
}

fn send(stream: &mut File, message: impl Into<GuestMessage>) -> Result<(), String> {
    let frame = encode(&message.into()).map_err(|error| error.to_string())?;
    stream
        .write_all(&frame)
        .map_err(|error| format!("writing to the host: {error}"))
}

fn fail(
    stream: &mut File,
    start: &GuestClaudeSetupStart,
    reason: GuestClaudeSetupFailureReason,
    detail: String,
) -> Result<(), String> {
    send(
        stream,
        GuestClaudeSetupFailed {
            claude_setup_id: start.claude_setup_id.clone(),
            detail: detail
                .try_into()
                .map_err(|error| format!("the Claude setup failure detail: {error}"))?,
            reason,
            type_: GuestClaudeSetupFailedType::ClaudeSetupFailed,
        },
    )
}

fn await_url(terminal: &mut Terminal) -> Option<String> {
    loop {
        if let Some(url) = sign_in_url(&terminal.text) {
            return Some(url);
        }
        terminal.read(SETTLE).ok()?;
    }
}

fn await_code(stream: &mut File, decoder: &mut Decoder) -> Result<Option<GuestClaudeSetupCode>, String> {
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        if let Some(input) = decoder.take::<GuestInput>().map_err(|error| error.to_string())? {
            if let GuestInput::ClaudeSetupCode(code) = input {
                return Ok(Some(code));
            }
            continue;
        }
        let read = stream
            .read(&mut buffer)
            .map_err(|error| format!("reading from the host: {error}"))?;
        if read == 0 {
            return Ok(None);
        }
        decoder.push(&buffer[..read]);
    }
}

enum Minted {
    Token(String),
    Refused(String),
    TimedOut(Vec<String>),
    Exited,
}

fn await_token(terminal: &mut Terminal, from: usize, budget: Duration) -> Minted {
    let deadline = Instant::now() + budget;
    loop {
        let printed = terminal.text.get(from..).unwrap_or_default();
        if let Some(token) = minted_token(printed) {
            return Minted::Token(token);
        }
        if let Some(line) = refusal(printed) {
            return Minted::Refused(line);
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Minted::TimedOut(printed_lines(printed));
        }
        if terminal.read(SETTLE.min(left)).is_err() {
            return Minted::Exited;
        }
    }
}

fn mint_timed_out(seconds: NonZeroU32, lines: &[String]) -> String {
    let printed = if lines.is_empty() {
        "Claude Code printed nothing after the code".to_string()
    } else {
        format!("Claude Code printed after the code: {}", lines.join(" / "))
    };
    format!("Claude Code minted no token within mintBudgetSeconds {seconds} of the code. {printed}; start the Claude setup again")
}

fn store(start: &GuestClaudeSetupStart, token: &str, expires_at: Option<&str>) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(WRITE_TIMEOUT)
        .build()
        .map_err(|error| format!("building the HTTP client: {error}"))?;
    let answer = client
        .post(start.token_url.as_str())
        .bearer_auth(&*start.token_credential)
        .json(&match expires_at {
            Some(expires_at) => json!({ "tenantId": *start.tenant_id, "value": token, "expiresAt": expires_at }),
            None => json!({ "tenantId": *start.tenant_id, "value": token }),
        })
        .send()
        .map_err(|error| format!("POST {} failed: {}", start.token_url, error.without_url()))?;
    let status = answer.status();
    if status.is_success() {
        return Ok(());
    }
    let body = answer.text().unwrap_or_default();
    Err(format!(
        "POST {} answered {status}: {}",
        start.token_url,
        redacted(tail(&body, DETAIL_TAIL))
    ))
}

fn stored(stream: &mut File, start: &GuestClaudeSetupStart, written: Result<(), String>) -> Result<(), String> {
    match written {
        Ok(()) => send(
            stream,
            GuestClaudeSetupStored {
                claude_setup_id: start.claude_setup_id.clone(),
                type_: GuestClaudeSetupStoredType::ClaudeSetupStored,
            },
        ),
        Err(detail) => fail(
            stream,
            start,
            GuestClaudeSetupFailureReason::TokenNotStored,
            format!("{detail}; the token was discarded with the box, start the Claude setup again"),
        ),
    }
}

fn await_prompt(terminal: &mut Terminal) -> Option<DevicePrompt> {
    loop {
        if let Some(prompt) = device_prompt(&terminal.text) {
            return Some(prompt);
        }
        terminal.read(SETTLE).ok()?;
    }
}

fn auth_file(config: &GuestHarness) -> String {
    config
        .fixed_variables
        .iter()
        .find(|variable| variable.name.as_str() == "CODEX_HOME")
        .map_or_else(
            || format!("{}/.codex/auth.json", *config.environment.home),
            |home| format!("{}/auth.json", home.value.as_str()),
        )
}

fn codex(
    start: &GuestClaudeSetupStart,
    config: &GuestHarness,
    confinement: &Confinement,
    stream: &mut File,
) -> Result<(), String> {
    let mut terminal = match spawn(config, confinement, &CODEX_LOGIN) {
        Ok(terminal) => terminal,
        Err(detail) => return fail(stream, start, GuestClaudeSetupFailureReason::UrlNotPrinted, detail),
    };
    let Some(prompt) = await_prompt(&mut terminal) else {
        let detail = format!(
            "codex login --device-auth exited without printing a sign-in URL and code: {}",
            terminal.tail()
        );
        return fail(stream, start, GuestClaudeSetupFailureReason::UrlNotPrinted, detail);
    };
    let user_code = prompt
        .code
        .try_into()
        .map_err(|error| format!("the one-time code Codex printed: {error}"))?;
    send(
        stream,
        GuestClaudeSetupUrl {
            claude_setup_id: start.claude_setup_id.clone(),
            sign_in_url: prompt
                .url
                .try_into()
                .map_err(|error| format!("the sign-in URL Codex printed: {error}"))?,
            type_: GuestClaudeSetupUrlType::ClaudeSetupUrl,
            user_code: Some(user_code),
        },
    )?;
    while terminal.read(SETTLE).is_ok() {}
    let signed_in = terminal.child.wait().is_ok_and(|status| status.success());
    if !signed_in {
        let detail = format!(
            "codex login --device-auth exited without signing in: {}; start the Codex sign-in again and enter the new code",
            terminal.tail()
        );
        return fail(stream, start, GuestClaudeSetupFailureReason::ClaudeCodeExited, detail);
    }
    drop(terminal);
    let path = auth_file(config);
    let written = std::fs::read_to_string(&path)
        .map_err(|error| format!("Codex signed in but {path} is unreadable: {error}"))
        .and_then(|text| {
            let expires_at = access_token_expiry(&text).map_err(|detail| format!("Codex signed in but {detail}"))?;
            store(start, &text, Some(&expires_at))
        });
    let _ = std::fs::remove_file(&path);
    stored(stream, start, written)
}

pub fn run(
    start: &GuestClaudeSetupStart,
    config: &GuestHarness,
    confinement: &Confinement,
    stream: &mut File,
    decoder: &mut Decoder,
) -> Result<(), String> {
    if config.harness.as_str() == CODEX {
        return codex(start, config, confinement, stream);
    }
    let mut terminal = match spawn(config, confinement, &CLAUDE_LOGIN) {
        Ok(terminal) => terminal,
        Err(detail) => return fail(stream, start, GuestClaudeSetupFailureReason::UrlNotPrinted, detail),
    };
    let Some(url) = await_url(&mut terminal) else {
        let detail = format!(
            "claude setup-token exited without printing a sign-in URL: {}",
            terminal.tail()
        );
        return fail(stream, start, GuestClaudeSetupFailureReason::UrlNotPrinted, detail);
    };
    send(
        stream,
        GuestClaudeSetupUrl {
            claude_setup_id: start.claude_setup_id.clone(),
            sign_in_url: url
                .try_into()
                .map_err(|error| format!("the sign-in URL Claude Code printed: {error}"))?,
            type_: GuestClaudeSetupUrlType::ClaudeSetupUrl,
            user_code: None,
        },
    )?;
    let Some(code) = await_code(stream, decoder)? else {
        return Ok(());
    };
    while terminal.read(Duration::ZERO) == Ok(true) {}
    let from = terminal.text.len();
    terminal
        .master
        .write_all(code.code.as_bytes())
        .map_err(|error| format!("typing the code into claude setup-token: {error}"))?;
    let settled_by = Instant::now() + ECHO_SETTLE_LIMIT;
    while Instant::now() < settled_by && terminal.read(SETTLE) == Ok(true) {}
    terminal
        .master
        .write_all(b"\r")
        .map_err(|error| format!("pressing Enter in claude setup-token: {error}"))?;
    let budget = Duration::from_secs(u64::from(code.mint_budget_seconds.get()));
    let token = match await_token(&mut terminal, from, budget) {
        Minted::Token(token) => token,
        Minted::Refused(line) => {
            let detail = format!("Claude Code refused the code: {line} Start the Claude setup again, sign in at its new sign-in URL and paste the code that page shows");
            return fail(stream, start, GuestClaudeSetupFailureReason::CodeRejected, detail);
        }
        Minted::TimedOut(lines) => {
            let detail = mint_timed_out(code.mint_budget_seconds, &lines);
            return fail(stream, start, GuestClaudeSetupFailureReason::MintTimedOut, detail);
        }
        Minted::Exited => {
            return fail(
                stream,
                start,
                GuestClaudeSetupFailureReason::ClaudeCodeExited,
                EXITED.to_string(),
            )
        }
    };
    drop(terminal);
    let written = store(start, &token, None);
    stored(stream, start, written)
}
