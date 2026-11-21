use std::collections::VecDeque;
use std::fs::File;
use std::hash::{BuildHasher, RandomState};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::num::NonZeroU32;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::{json, Value};
use tumpel_api::types::{
    EntityId, GuestHarness, GuestHarnessProtocol, GuestInput, GuestMessage, GuestMetric, GuestPersonaFile,
    GuestTurnEnd, GuestTurnEndReason, GuestTurnEndType, GuestTurnLine, GuestTurnLineType,
    GuestTurnRepositoryCloneFailed, GuestTurnRepositoryCloneFailedType, GuestTurnRepositoryReady,
    GuestTurnRepositoryReadyType, GuestTurnStart, LongText, Sequence,
};
use tumpel_guest::disk::{self, Space};
use tumpel_guest::{credential_file, encode, Decoder};

use crate::connectors;
use crate::forwarder::Forwarder;
use crate::memory::Memory;
use crate::repository;
use crate::usage::{self, Spend, Tokens, Usage};

const STATE_DIR: &str = "/var/lib/tumpel";
const HOST_POLL_MILLISECONDS: i32 = 200;
const LINE_SEQ_FILE: &str = "/var/lib/tumpel/line-seq";
const STARTED_FILE: &str = "/var/lib/tumpel/harness-started";
const CODEX_THREAD_FILE: &str = "/var/lib/tumpel/codex-thread";
const CODEX_USAGE_FILE: &str = "/var/lib/tumpel/codex-usage.json";
const WRITTEN_FILE: &str = "/var/lib/tumpel/persona-files.json";
const SETUP_PROGRAM: &str = "/opt/tumpel/prebuild.mjs";
const GH_DIR: &str = "/var/lib/tumpel/bin";
const GH_WRAPPER: &str = "/var/lib/tumpel/bin/gh";
const BOX_DISK: &str = "/";
const CONSTRAINTS_DIR: &str = "/etc/claude-code";
const CONSTRAINTS_FILE: &str = "/etc/claude-code/CLAUDE.md";
pub const STDERR_TAIL: usize = 4096;

enum Output {
    Line(String),
    Closed,
    Exited(libc::pid_t),
    HostGone(u64, String),
    Cancelled(u64),
}

struct HostWatch {
    done: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl HostWatch {
    fn watch(host: BorrowedFd<'_>, sender: mpsc::Sender<Output>, turn: u64, turn_id: EntityId) -> Result<Self, String> {
        let host = File::from(
            host.try_clone_to_owned()
                .map_err(|error| format!("watching the host connection: {error}"))?,
        );
        let done = Arc::new(AtomicBool::new(false));
        let watching = Arc::clone(&done);
        let thread = std::thread::spawn(move || {
            let mut poll = libc::pollfd {
                fd: host.as_raw_fd(),
                events: libc::POLLIN | libc::POLLRDHUP,
                revents: 0,
            };
            let mut decoder = Decoder::default();
            let mut buffer = [0u8; 4096];
            while !watching.load(Ordering::Relaxed) {
                if unsafe { libc::poll(&raw mut poll, 1, HOST_POLL_MILLISECONDS) } <= 0 || poll.revents == 0 {
                    continue;
                }
                if watching.load(Ordering::Relaxed) {
                    return;
                }
                let read = match (&host).read(&mut buffer) {
                    Ok(0) => {
                        let _ = sender.send(Output::HostGone(turn, "the host hung up".to_string()));
                        return;
                    }
                    Ok(read) => read,
                    Err(error) => {
                        let _ = sender.send(Output::HostGone(turn, format!("reading from the host failed: {error}")));
                        return;
                    }
                };
                decoder.push(&buffer[..read]);
                loop {
                    match decoder.take::<GuestInput>() {
                        Ok(None) => break,
                        Ok(Some(GuestInput::TurnCancel(cancel))) if cancel.turn_id == turn_id => {
                            let _ = sender.send(Output::Cancelled(turn));
                        }
                        Ok(Some(_)) => {}
                        Err(error) => {
                            let _ = sender.send(Output::HostGone(turn, format!("the host sent {error}")));
                            return;
                        }
                    }
                }
            }
        });
        Ok(Self {
            done,
            thread: Some(thread),
        })
    }
}

impl Drop for HostWatch {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Harness {
    child: Child,
    stdin: Box<dyn Write + Send>,
    output: Receiver<Output>,
    sender: mpsc::Sender<Output>,
    stderr: Arc<Mutex<VecDeque<u8>>>,
    launch: Launch,
}

#[derive(Debug, PartialEq, Eq)]
struct Launch {
    model: String,
    variables: u64,
    files: u64,
    connectors: Vec<(String, String)>,
}

fn launch(keys: &RandomState, start: &GuestTurnStart) -> Launch {
    let mut variables = turn_environment(start);
    variables.sort_unstable();
    let mut files: Vec<(&str, &str)> = start
        .files
        .iter()
        .map(|file| (file.path.as_str(), file.digest.as_str()))
        .collect();
    files.sort_unstable();
    Launch {
        model: start.model.to_string(),
        variables: keys.hash_one(&variables),
        files: keys.hash_one(&files),
        connectors: connectors::servers(&start.credential.connectors),
    }
}

fn image_environment(config: &GuestHarness) -> Vec<(String, String)> {
    let image = &config.environment;
    [("PATH", &*image.path), ("HOME", &*image.home), ("LANG", &*image.lang)]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

fn owner_tokens(start: &GuestTurnStart) -> bool {
    start
        .credential
        .variables
        .iter()
        .any(|variable| variable.name.as_str() == repository::OWNER_TOKENS)
}

fn environment(config: &GuestHarness, start: &GuestTurnStart) -> Vec<(String, String)> {
    let routed = owner_tokens(start);
    image_environment(config)
        .into_iter()
        .map(|(name, value)| match name.as_str() {
            "PATH" if routed => (name, format!("{GH_DIR}:{value}")),
            _ => (name, value),
        })
        .chain(
            config
                .fixed_variables
                .iter()
                .map(|variable| (variable.name.to_string(), variable.value.to_string())),
        )
        .chain(turn_environment(start))
        .chain(connectors::variables(config.protocol, &start.credential.connectors))
        .collect()
}

fn turn_environment(start: &GuestTurnStart) -> Vec<(String, String)> {
    let variables = &start.credential.variables;
    let mut environment = vec![("IS_SANDBOX".to_string(), "1".to_string())];
    environment.extend(
        variables
            .iter()
            .map(|variable| (variable.name.to_string(), variable.value.to_string())),
    );
    let token = variables
        .iter()
        .find(|variable| variable.name.as_str() == repository::TOKEN)
        .map(|variable| variable.value.as_str());
    environment.extend(repository::git_environment(
        &start.git_author_name,
        &start.git_author_email,
        token,
        owner_tokens(start),
    ));
    environment
}

fn home_path(home: &Path, path: &str) -> Result<PathBuf, String> {
    let relative = Path::new(path);
    if !relative
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "persona file {path} is not a plain path under the harness home; the harness catalog lists the accepted paths"
        ));
    }
    Ok(home.join(relative))
}

fn write_files(home: &Path, files: &[GuestPersonaFile], previous: &[String]) -> Result<Vec<String>, String> {
    let mut written = Vec::with_capacity(files.len());
    for file in files {
        let target = home_path(home, &file.path)?;
        let bytes = STANDARD
            .decode(file.content.as_bytes())
            .map_err(|error| format!("persona file {} is not base64: {error}", *file.path))?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|error| format!("creating {}: {error}", parent.display()))?;
        }
        std::fs::write(&target, bytes).map_err(|error| format!("writing {}: {error}", target.display()))?;
        written.push(file.path.to_string());
    }
    for stale in previous.iter().filter(|path| !written.contains(path)) {
        let target = home_path(home, stale)?;
        match std::fs::remove_file(&target) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                return Err(format!("deleting {}: {error}", target.display()))
            }
            _ => {}
        }
    }
    Ok(written)
}

fn read_written() -> Result<Vec<String>, String> {
    match std::fs::read_to_string(WRITTEN_FILE) {
        Ok(text) => {
            serde_json::from_str(&text).map_err(|error| format!("{WRITTEN_FILE} holds no list of paths: {error}"))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(format!("reading {WRITTEN_FILE}: {error}")),
    }
}

fn reuses(running: Option<&Launch>, wanted: &Launch) -> bool {
    running == Some(wanted)
}

impl Harness {
    fn kill_group(&self) -> Result<(), String> {
        let pid = libc::pid_t::try_from(self.child.id())
            .map_err(|error| format!("the harness runs as process {} beyond pid_t: {error}", self.child.id()))?;
        unsafe { libc::kill(-pid, libc::SIGKILL) };
        Ok(())
    }

    fn stderr_tail(&self) -> String {
        let tail = self.stderr.lock().map(|tail| tail.iter().copied().collect::<Vec<u8>>());
        String::from_utf8_lossy(&tail.unwrap_or_default()).trim().to_string()
    }

    fn discard_leftovers(&self, turn_id: &str) {
        let discarded = std::iter::from_fn(|| match self.output.try_recv() {
            Ok(Output::Line(_)) => Some(()),
            _ => None,
        })
        .count();
        if discarded != 0 {
            eprintln!("tumpel-guest: turn {turn_id} discarded {discarded} harness lines written after the previous turn ended");
        }
    }

    fn receive(&self, protocol: GuestHarnessProtocol, turn: u64) -> Result<Output, mpsc::RecvError> {
        loop {
            let output = self.output.recv()?;
            if matches!(output, Output::HostGone(of, _) | Output::Cancelled(of) if of != turn) {
                continue;
            }
            if !matches!(&output, Output::Line(text) if answers_no_prompt(protocol, text)) {
                return Ok(output);
            }
            eprintln!("tumpel-guest: discarded 1 harness result line that answers a background task notification, not the turn's prompt");
        }
    }

    fn stop(mut self) {
        let _ = self.kill_group();
        let _ = self.child.wait();
    }
}

pub struct GuestState {
    config: GuestHarness,
    home: PathBuf,
    keys: RandomState,
    harness: Option<Harness>,
    credential: Option<(PathBuf, String)>,
    last_line_seq: i64,
    markers: u64,
    memory: Memory,
    forwarder: Forwarder,
    caches_masked: bool,
    pi_config_mounted: bool,
}

fn read_line_seq() -> Result<i64, String> {
    match std::fs::read_to_string(LINE_SEQ_FILE) {
        Ok(text) => text
            .trim()
            .parse()
            .map_err(|error| format!("{LINE_SEQ_FILE} holds no line count: {error}")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(format!("reading {LINE_SEQ_FILE}: {error}")),
    }
}

fn spawn_reader(stream: impl Read + Send + 'static, sender: mpsc::Sender<Output>) {
    std::thread::spawn(move || {
        for line in BufReader::new(stream).lines() {
            let Ok(line) = line else { break };
            if sender.send(Output::Line(line)).is_err() {
                return;
            }
        }
        let _ = sender.send(Output::Closed);
    });
}

fn spawn_waiter(pid: libc::pid_t, sender: mpsc::Sender<Output>) {
    std::thread::spawn(move || {
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let exited = unsafe {
            libc::waitid(
                libc::P_PID,
                pid.unsigned_abs(),
                &raw mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        if exited == 0 {
            let _ = sender.send(Output::Exited(pid));
        }
    });
}

fn spawn_tail(mut stream: impl Read + Send + 'static, tail: Arc<Mutex<VecDeque<u8>>>) {
    std::thread::spawn(move || {
        let mut buffer = [0u8; 1024];
        while let Ok(read) = stream.read(&mut buffer) {
            if read == 0 {
                return;
            }
            let Ok(mut tail) = tail.lock() else { return };
            tail.extend(&buffer[..read]);
            let excess = tail.len().saturating_sub(STDERR_TAIL);
            tail.drain(..excess);
        }
    });
}

fn user_message(prompt: &str) -> String {
    json!({
        "type": "user",
        "message": { "role": "user", "content": [{ "type": "text", "text": prompt }] }
    })
    .to_string()
}

fn pi_prompt(prompt: &str) -> String {
    format!(
        "{}\n{}\n",
        json!({ "type": "set_auto_retry", "enabled": false }),
        json!({ "type": "prompt", "message": prompt })
    )
}

const PI_DELTAS: [&str; 2] = ["message_update", "tool_execution_update"];

fn per_turn(protocol: GuestHarnessProtocol) -> bool {
    matches!(
        protocol,
        GuestHarnessProtocol::PromptOnStdin | GuestHarnessProtocol::CodexExecJson
    )
}

fn codex_thread(value: &Value) -> Option<&str> {
    (record_type(value) == Some("thread.started"))
        .then(|| value.get("thread_id").and_then(Value::as_str))
        .flatten()
}

fn record_codex_thread(protocol: GuestHarnessProtocol, line: &str) -> Result<(), String> {
    if protocol != GuestHarnessProtocol::CodexExecJson {
        return Ok(());
    }
    let Some(thread) = serde_json::from_str::<Value>(line)
        .ok()
        .as_ref()
        .and_then(codex_thread)
        .map(str::to_owned)
    else {
        return Ok(());
    };
    std::fs::write(CODEX_THREAD_FILE, thread).map_err(|error| format!("writing {CODEX_THREAD_FILE}: {error}"))
}

fn read_codex_thread() -> Result<Option<String>, String> {
    match std::fs::read_to_string(CODEX_THREAD_FILE) {
        Ok(thread) => Ok(Some(thread)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("reading {CODEX_THREAD_FILE}: {error}")),
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexTotal {
    thread_id: String,
    tokens: Tokens,
}

fn codex_turn_usage(total: Usage) -> Result<Usage, String> {
    let Some(thread_id) = read_codex_thread()? else {
        return Ok(total);
    };
    let previous = match std::fs::read_to_string(CODEX_USAGE_FILE) {
        Ok(text) => Some(
            serde_json::from_str::<CodexTotal>(&text)
                .map_err(|error| format!("{CODEX_USAGE_FILE} holds no Codex token total: {error}"))?,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("reading {CODEX_USAGE_FILE}: {error}")),
    };
    let since = previous
        .filter(|previous| previous.thread_id == thread_id)
        .map_or(total.tokens, |previous| total.tokens.since(previous.tokens));
    let stored = CodexTotal {
        thread_id,
        tokens: total.tokens,
    };
    std::fs::write(
        CODEX_USAGE_FILE,
        serde_json::to_vec(&stored).map_err(|error| error.to_string())?,
    )
    .map_err(|error| format!("writing {CODEX_USAGE_FILE}: {error}"))?;
    Ok(Usage {
        tokens: since,
        cost: total.cost,
    })
}

fn turn_arguments(
    protocol: GuestHarnessProtocol,
    start: &GuestTurnStart,
    started: bool,
    thread: Option<&str>,
) -> Result<Vec<String>, String> {
    let model = start.model.to_string();
    let connectors = connectors::arguments(protocol, &start.credential.connectors);
    Ok(match protocol {
        GuestHarnessProtocol::ShellLines => Vec::new(),
        GuestHarnessProtocol::CodexExecJson => {
            if start.credential.arguments.is_empty() {
                return Err("the turn's credential names no Codex model provider; grant the persona's secret set a credential the Codex catalog entry lists".to_string());
            }
            let mut arguments = vec!["--model".to_string(), model];
            arguments.extend(start.credential.arguments.iter().map(ToString::to_string));
            arguments.extend(connectors);
            if let Some(thread) = thread {
                arguments.extend(["resume".to_string(), thread.to_string()]);
            }
            arguments
        }
        GuestHarnessProtocol::ClaudeStreamJson | GuestHarnessProtocol::PiRpc | GuestHarnessProtocol::PromptOnStdin => {
            let mut arguments = vec!["--model".to_string(), model];
            if started {
                arguments.push("--continue".to_string());
            }
            arguments.extend(connectors);
            arguments
        }
    })
}

fn record_type(value: &Value) -> Option<&str> {
    value.get("type").and_then(Value::as_str)
}

fn pi_retry_off(value: &Value) -> bool {
    value.get("command").and_then(Value::as_str) == Some("set_auto_retry")
        && value.get("success").and_then(Value::as_bool) == Some(true)
}

fn claude_thinking_tokens(value: &Value) -> bool {
    record_type(value) == Some("system") && value.get("subtype").and_then(Value::as_str) == Some("thinking_tokens")
}

fn skips(protocol: GuestHarnessProtocol, line: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    match protocol {
        GuestHarnessProtocol::PiRpc => {
            record_type(&value).is_some_and(|kind| PI_DELTAS.contains(&kind)) || pi_retry_off(&value)
        }
        GuestHarnessProtocol::ClaudeStreamJson => claude_thinking_tokens(&value),
        GuestHarnessProtocol::CodexExecJson => record_type(&value) == Some("item.updated"),
        GuestHarnessProtocol::ShellLines | GuestHarnessProtocol::PromptOnStdin => false,
    }
}

const ANTHROPIC_CREDENTIALS: [&str; 2] = ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"];

fn priced_by_anthropic(start: &GuestTurnStart) -> bool {
    start
        .credential
        .variables
        .iter()
        .any(|variable| ANTHROPIC_CREDENTIALS.contains(&variable.name.as_str()))
}

fn without_list_price(line: String) -> String {
    let Ok(mut value) = serde_json::from_str::<Value>(&line) else {
        return line;
    };
    if record_type(&value) != Some("result") {
        return line;
    }
    let Some(result) = value.as_object_mut() else {
        return line;
    };
    result.remove("total_cost_usd");
    for field in ["modelUsage", "model_usage_since_last_result"] {
        if let Some(models) = result.get_mut(field).and_then(Value::as_object_mut) {
            for usage in models.values_mut().filter_map(Value::as_object_mut) {
                usage.remove("costUSD");
            }
        }
    }
    value.to_string()
}

fn pi_prompt_settled(value: &Value) -> bool {
    value.get("command").and_then(Value::as_str) == Some("prompt")
        && (value.get("success").and_then(Value::as_bool) != Some(true)
            || value.pointer("/data/disposition").and_then(Value::as_str) == Some("handled"))
}

fn ends_turn(protocol: GuestHarnessProtocol, line: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    match (protocol, record_type(&value)) {
        (GuestHarnessProtocol::ClaudeStreamJson, Some("result"))
        | (GuestHarnessProtocol::PiRpc, Some("agent_settled")) => true,
        (GuestHarnessProtocol::PiRpc, Some("response")) => pi_prompt_settled(&value),
        _ => false,
    }
}

fn answers_no_prompt(protocol: GuestHarnessProtocol, line: &str) -> bool {
    if protocol != GuestHarnessProtocol::ClaudeStreamJson {
        return false;
    }
    let Ok(value) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    let failed = value.get("is_error").and_then(Value::as_bool) == Some(true)
        || value
            .get("subtype")
            .and_then(Value::as_str)
            .is_some_and(|subtype| subtype.starts_with("error"));
    record_type(&value) == Some("result")
        && !failed
        && value.pointer("/origin/kind").and_then(Value::as_str) == Some("task-notification")
}

enum Ending {
    Turn {
        reason: GuestTurnEndReason,
        detail: String,
        metrics: Vec<GuestMetric>,
        warning: Option<String>,
    },
    Clone(repository::CloneFailure),
}

fn ended(reason: GuestTurnEndReason, detail: impl Into<String>) -> Ending {
    Ending::Turn {
        reason,
        detail: detail.into(),
        metrics: Vec::new(),
        warning: None,
    }
}

fn constraints(memory_mib: u64, disk_mib: u64, cpus: usize, turn_seconds: Option<NonZeroU32>) -> String {
    let limit = turn_seconds.map_or_else(String::new, |seconds| format!("- Turn time limit: {seconds} seconds\n"));
    format!(
        "# Box limits\n\n- Memory: {memory_mib} MiB\n- Disk: {disk_mib} MiB\n- CPUs: {cpus}\n{limit}\nA turn fails BoxDiskFull when the harness fails on a full disk and otherwise completes with a warning; it fails HarnessOutOfMemory when memory runs out and TurnTimedOut past its time limit.\n"
    )
}

fn write_constraints(memory_mib: u64, turn_seconds: Option<NonZeroU32>) -> Result<(), String> {
    let disk = disk::space(Path::new(BOX_DISK))?;
    let cpus = std::thread::available_parallelism().map_err(|error| format!("counting the box's CPUs: {error}"))?;
    let note = constraints(memory_mib, disk.total_mib, cpus.get(), turn_seconds);
    match std::fs::read_to_string(CONSTRAINTS_FILE) {
        Ok(written) if written == note => return Ok(()),
        Err(error) if error.kind() != ErrorKind::NotFound => {
            return Err(format!("reading {CONSTRAINTS_FILE}: {error}"));
        }
        _ => {}
    }
    std::fs::create_dir_all(CONSTRAINTS_DIR).map_err(|error| format!("creating {CONSTRAINTS_DIR}: {error}"))?;
    std::fs::write(CONSTRAINTS_FILE, note).map_err(|error| format!("writing {CONSTRAINTS_FILE}: {error}"))
}

fn disk_full(mut ending: Ending, disk: Space) -> Ending {
    if !disk.full() {
        return ending;
    }
    let Ending::Turn {
        reason,
        detail,
        warning,
        ..
    } = &mut ending
    else {
        return ending;
    };
    let full = format!(
        "the box disk of {} MiB is full, {} MiB free; give the persona a larger boxDiskMib, up to 262144 MiB, or have it delete build output it no longer needs",
        disk.total_mib, disk.free_mib
    );
    match reason {
        GuestTurnEndReason::Completed => *warning = Some(full),
        GuestTurnEndReason::HarnessExited | GuestTurnEndReason::HarnessStartFailed => {
            *detail = format!("{full}; the harness stopped ({reason}): {detail}");
            *reason = GuestTurnEndReason::BoxDiskFull;
        }
        _ => {}
    }
    ending
}

fn with_spend(ending: Ending, spend: &Spend, protocol: GuestHarnessProtocol, priced: bool) -> Result<Ending, String> {
    let Some(read) = spend.usage() else {
        return Ok(ending);
    };
    let used = if protocol == GuestHarnessProtocol::CodexExecJson {
        codex_turn_usage(read)?
    } else {
        read
    };
    Ok(match ending {
        Ending::Turn {
            reason,
            detail,
            warning,
            ..
        } => Ending::Turn {
            reason,
            detail,
            metrics: usage::metrics(used, priced)?,
            warning,
        },
        clone @ Ending::Clone(_) => clone,
    })
}

impl GuestState {
    pub fn new(config: GuestHarness, memory: Memory) -> Result<Self, String> {
        let home = PathBuf::from(&*config.environment.home);
        Ok(Self {
            config,
            home,
            keys: RandomState::new(),
            harness: None,
            credential: None,
            last_line_seq: read_line_seq()?,
            markers: 0,
            memory,
            forwarder: Forwarder::start()?,
            caches_masked: false,
            pi_config_mounted: false,
        })
    }

    pub fn config(&self) -> &GuestHarness {
        &self.config
    }

    pub fn memory(&self) -> &Memory {
        &self.memory
    }

    pub fn last_line_seq(&self) -> i64 {
        self.last_line_seq
    }

    fn write_credential(&mut self, start: &GuestTurnStart) -> Result<(), String> {
        let Some(file) = &start.credential.credential_file else {
            return Ok(());
        };
        let target = home_path(&self.home, &file.path)?;
        credential_file::write(&target, &file.value)?;
        self.credential = Some((target, file.value.to_string()));
        Ok(())
    }

    fn take_credential(&mut self) -> Result<Option<String>, String> {
        let Some((target, written)) = self.credential.take() else {
            return Ok(None);
        };
        credential_file::take_changed(&target, &written)
    }

    fn spawn(&mut self, start: &GuestTurnStart) -> Result<Harness, String> {
        let written = write_files(&self.home, &start.files, &read_written()?)?;
        let persona_files: Vec<&str> = start.files.iter().map(|file| file.path.as_str()).collect();
        let connectors = &start.credential.connectors;
        connectors::refuse_reserved(self.config.protocol, &self.home, &persona_files, connectors)?;
        if self.config.protocol == GuestHarnessProtocol::PiRpc && !connectors.is_empty() {
            connectors::write_pi_config(&self.home, connectors, self.pi_config_mounted)?;
            self.pi_config_mounted = true;
        }
        std::fs::create_dir_all(STATE_DIR).map_err(|error| format!("creating {STATE_DIR}: {error}"))?;
        std::fs::write(
            WRITTEN_FILE,
            serde_json::to_vec(&written).map_err(|error| error.to_string())?,
        )
        .map_err(|error| format!("writing {WRITTEN_FILE}: {error}"))?;
        std::fs::create_dir_all(GH_DIR).map_err(|error| format!("creating {GH_DIR}: {error}"))?;
        std::fs::write(GH_WRAPPER, repository::GH_WRAPPER).map_err(|error| format!("writing {GH_WRAPPER}: {error}"))?;
        std::fs::set_permissions(GH_WRAPPER, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| format!("making {GH_WRAPPER} executable: {error}"))?;
        let (program, arguments) = self
            .config
            .command
            .split_first()
            .ok_or("guest.json names an empty command")?;
        let per_turn = per_turn(self.config.protocol);
        let mut command = Command::new(program);
        command.args(arguments);
        command.args(turn_arguments(
            self.config.protocol,
            start,
            Path::new(STARTED_FILE).exists(),
            read_codex_thread()?.as_deref(),
        )?);
        let harness = self.start_process(command, environment(&self.config, start), per_turn, start)?;
        std::fs::create_dir_all(STATE_DIR).map_err(|error| format!("creating {STATE_DIR}: {error}"))?;
        std::fs::write(STARTED_FILE, b"").map_err(|error| format!("writing {STARTED_FILE}: {error}"))?;
        Ok(harness)
    }

    fn spawn_setup(&self, start: &GuestTurnStart) -> Result<Harness, String> {
        let mut command = Command::new(SETUP_PROGRAM);
        command.arg("run");
        self.start_process(command, image_environment(&self.config), true, start)
    }

    fn start_process(
        &self,
        mut command: Command,
        variables: Vec<(String, String)>,
        per_turn: bool,
        start: &GuestTurnStart,
    ) -> Result<Harness, String> {
        let program = command.get_program().to_string_lossy().into_owned();
        command.process_group(0);
        self.memory.confinement.apply(&mut command);
        let mut child = command
            .env_clear()
            .envs(variables)
            .current_dir(&*self.config.working_directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("starting {program}: {error}"))?;
        let stdin = Box::new(child.stdin.take().ok_or("the harness has no standard input")?);
        let stdout = child.stdout.take().ok_or("the harness has no standard output")?;
        let stderr = child.stderr.take().ok_or("the harness has no standard error")?;
        let (sender, output) = mpsc::channel();
        if per_turn {
            let pid = libc::pid_t::try_from(child.id())
                .map_err(|error| format!("{program} runs as process {} beyond pid_t: {error}", child.id()))?;
            spawn_waiter(pid, sender.clone());
        }
        spawn_reader(stdout, sender.clone());
        let tail = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL)));
        spawn_tail(stderr, Arc::clone(&tail));
        Ok(Harness {
            child,
            stdin,
            output,
            sender,
            stderr: tail,
            launch: launch(&self.keys, start),
        })
    }

    fn harness_for(&mut self, start: &GuestTurnStart) -> Result<&mut Harness, String> {
        let wanted = launch(&self.keys, start);
        let alive = self
            .harness
            .as_mut()
            .is_some_and(|harness| matches!(harness.child.try_wait(), Ok(None)));
        if per_turn(self.config.protocol)
            || !alive
            || !reuses(self.harness.as_ref().map(|harness| &harness.launch), &wanted)
        {
            if let Some(stale) = self.harness.take() {
                stale.stop();
            }
            let harness = self.spawn(start)?;
            self.harness = Some(harness);
        }
        self.harness
            .as_mut()
            .ok_or_else(|| "the harness did not start".to_string())
    }

    fn setup_for(&mut self, start: &GuestTurnStart) -> Result<&mut Harness, String> {
        if let Some(stale) = self.harness.take() {
            stale.stop();
        }
        let setup = self.spawn_setup(start)?;
        Ok(self.harness.insert(setup))
    }

    pub fn run(&mut self, start: &GuestTurnStart, host: &mut (impl Write + AsFd)) -> Result<(), String> {
        self.last_line_seq = start.last_line_seq.0;
        let first = self.last_line_seq;
        let ending = match self.forwarder.begin(&start.credential.connectors) {
            Ok(()) => self.turn(start, host),
            Err(error) => Ok(ended(GuestTurnEndReason::HarnessStartFailed, error)),
        };
        self.forwarder.end()?;
        let credential_file = self.take_credential()?;
        let ending = match ending {
            Ok(ending) => ending,
            Err(error) => {
                if let Some(harness) = self.harness.take() {
                    harness.stop();
                }
                unsafe { libc::sync() };
                return Err(error);
            }
        };
        if !matches!(
            ending,
            Ending::Turn {
                reason: GuestTurnEndReason::Completed,
                ..
            }
        ) {
            if let Some(harness) = self.harness.take() {
                harness.stop();
            }
        }
        if self.last_line_seq != first {
            std::fs::create_dir_all(STATE_DIR).map_err(|error| format!("creating {STATE_DIR}: {error}"))?;
            std::fs::write(LINE_SEQ_FILE, self.last_line_seq.to_string())
                .map_err(|error| format!("writing {LINE_SEQ_FILE}: {error}"))?;
        }
        unsafe { libc::sync() };
        let end: GuestMessage = match ending {
            Ending::Turn {
                reason,
                detail,
                metrics,
                warning,
            } => GuestTurnEnd {
                credential_file: credential_file
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|error| format!("the credential file the harness left: {error}"))?,
                detail: detail
                    .try_into()
                    .map_err(|error| format!("the turn end detail: {error}"))?,
                last_line_seq: Sequence(self.last_line_seq),
                metrics,
                reason,
                turn_id: start.turn_id.clone(),
                type_: GuestTurnEndType::TurnEnd,
                warning: warning
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|error| format!("the box disk warning: {error}"))?,
            }
            .into(),
            Ending::Clone(failure) => GuestTurnRepositoryCloneFailed {
                cause: failure.cause,
                detail: failure
                    .detail
                    .try_into()
                    .map_err(|error| format!("the repository clone failure detail: {error}"))?,
                turn_id: start.turn_id.clone(),
                type_: GuestTurnRepositoryCloneFailedType::TurnRepositoryCloneFailed,
            }
            .into(),
        };
        send(host, &end)
    }

    fn prepare(
        &self,
        start: &GuestTurnStart,
        host: &mut impl Write,
    ) -> Result<Result<(), repository::CloneFailure>, String> {
        if let Err(failure) = repository::prepare(
            Path::new(&*self.config.working_directory),
            &start.repository,
            &environment(&self.config, start),
            &self.memory.confinement,
            Duration::from_secs(u64::from(self.config.clone_seconds.get())),
            start.prebuild_commit_sha.as_deref().map(String::as_str),
            Path::new(SETUP_PROGRAM),
        ) {
            return Ok(Err(failure));
        }
        let ready: GuestMessage = GuestTurnRepositoryReady {
            turn_id: start.turn_id.clone(),
            type_: GuestTurnRepositoryReadyType::TurnRepositoryReady,
        }
        .into();
        send(host, &ready)?;
        Ok(Ok(()))
    }

    fn launched(&mut self, start: &GuestTurnStart, setup: bool) -> Result<&mut Harness, String> {
        if setup {
            return self.setup_for(start);
        }
        write_constraints(self.memory.box_mib, start.turn_seconds)?;
        self.write_credential(start)?;
        if !self.caches_masked && !start.credential.connectors.is_empty() {
            let credential_file = start.credential.credential_file.as_ref().map(|file| file.path.as_str());
            connectors::mask_oauth_caches(&self.home, credential_file)?;
            self.caches_masked = true;
        }
        self.harness_for(start)
    }

    fn turn(&mut self, start: &GuestTurnStart, host: &mut (impl Write + AsFd)) -> Result<Ending, String> {
        self.markers += 1;
        let turn = self.markers;
        let marker = format!("tumpel-turn-end-{}-{}", std::process::id(), self.markers);
        let setup = start.prebuild_commit_sha.is_some();
        let protocol = if setup {
            GuestHarnessProtocol::PromptOnStdin
        } else {
            self.config.protocol
        };
        let priced = protocol != GuestHarnessProtocol::ClaudeStreamJson || priced_by_anthropic(start);
        let mut last_line_seq = self.last_line_seq;
        if let Err(failure) = self.prepare(start, host)? {
            return Ok(Ending::Clone(failure));
        }
        self.memory.kernel.drain()?;
        let harness = match self.launched(start, setup) {
            Ok(harness) => harness,
            Err(error) => return Ok(ended(GuestTurnEndReason::HarnessStartFailed, error)),
        };
        harness.discard_leftovers(&start.turn_id);
        let _host = HostWatch::watch(host.as_fd(), harness.sender.clone(), turn, start.turn_id.clone())?;
        let input = prompt_input(protocol, setup, &start.prompt, &marker);
        let written = harness
            .stdin
            .write_all(input.as_bytes())
            .and_then(|()| harness.stdin.flush());
        if per_turn(protocol) {
            harness.stdin = Box::new(std::io::sink());
        }
        if let Err(error) = written {
            return Ok(ended(
                GuestTurnEndReason::HarnessExited,
                format!("writing the prompt failed: {error}; {}", harness.stderr_tail()),
            ));
        }
        let mut killed = None;
        let mut spend = Spend::new(protocol);
        let ending = loop {
            let text = match harness.receive(protocol, turn) {
                Ok(Output::Line(text)) => text,
                Ok(Output::Exited(pid)) => {
                    unsafe { libc::kill(-pid, libc::SIGKILL) };
                    continue;
                }
                Ok(Output::HostGone(_, why)) => {
                    self.last_line_seq = last_line_seq;
                    return Err(format!("{why} during turn {}", *start.turn_id));
                }
                Ok(Output::Cancelled(_)) => {
                    harness.kill_group()?;
                    break ended(GuestTurnEndReason::Cancelled, "the host cancelled the turn");
                }
                Ok(Output::Closed) | Err(_) => {
                    let waited = harness.child.wait();
                    if per_turn(protocol) && waited.as_ref().is_ok_and(std::process::ExitStatus::success) {
                        break ended(GuestTurnEndReason::Completed, "");
                    }
                    killed = (waited.as_ref().ok().and_then(ExitStatusExt::signal) == Some(libc::SIGKILL))
                        .then(|| (harness.child.id(), harness.stderr_tail()));
                    let status = waited.map_or_else(|error| error.to_string(), |status| status.to_string());
                    break ended(
                        GuestTurnEndReason::HarnessExited,
                        format!("the harness exited ({status}) mid-turn; {}", harness.stderr_tail()),
                    );
                }
            };
            if protocol == GuestHarnessProtocol::ShellLines && text == marker {
                break ended(GuestTurnEndReason::Completed, "");
            }
            if skips(protocol, &text) {
                continue;
            }
            if let Err(error) = record_codex_thread(protocol, &text) {
                self.last_line_seq = last_line_seq;
                return Err(error);
            }
            last_line_seq += 1;
            if !setup {
                spend.read(&text);
            }
            let finished = ends_turn(protocol, &text);
            let text = if priced { text } else { without_list_price(text) };
            if let Err(error) = send_line(host, &start.turn_id, last_line_seq, text) {
                self.last_line_seq = last_line_seq;
                return Err(error);
            }
            if finished {
                break ended(GuestTurnEndReason::Completed, "");
            }
        };
        self.last_line_seq = last_line_seq;
        with_spend(self.box_ending(ending, killed)?, &spend, protocol, priced)
    }

    fn box_ending(&mut self, ending: Ending, killed: Option<(u32, String)>) -> Result<Ending, String> {
        let ending = self.killed_by_oom(ending, killed)?;
        Ok(disk_full(ending, disk::space(Path::new(BOX_DISK))?))
    }

    fn killed_by_oom(&mut self, ending: Ending, killed: Option<(u32, String)>) -> Result<Ending, String> {
        let Some((pid, tail)) = killed else {
            return Ok(ending);
        };
        let Some(victim) = crate::memory::victim(&self.memory.kernel.drain()?, pid) else {
            return Ok(ending);
        };
        Ok(ended(
            GuestTurnEndReason::HarnessOutOfMemory,
            self.memory.out_of_memory(&victim, &tail),
        ))
    }
}

fn prompt_input(protocol: GuestHarnessProtocol, setup: bool, prompt: &str, marker: &str) -> String {
    match protocol {
        _ if setup => String::new(),
        GuestHarnessProtocol::ClaudeStreamJson => format!("{}\n", user_message(prompt)),
        GuestHarnessProtocol::PiRpc => pi_prompt(prompt),
        GuestHarnessProtocol::ShellLines => format!("{prompt}\nprintf '%s\\n' '{marker}'\n"),
        GuestHarnessProtocol::PromptOnStdin | GuestHarnessProtocol::CodexExecJson => prompt.to_string(),
    }
}

fn send_line(host: &mut impl Write, turn_id: &EntityId, line_seq: i64, text: String) -> Result<(), String> {
    let line = |text: LongText| -> GuestMessage {
        GuestTurnLine {
            line_seq: Sequence(line_seq),
            text,
            turn_id: turn_id.clone(),
            type_: GuestTurnLineType::TurnLine,
        }
        .into()
    };
    let too_long = match LongText::try_from(text) {
        Err(error) => error.to_string(),
        Ok(text) => match encode(&line(text)) {
            Ok(frame) if frame.len() <= LINE_FRAME_BYTES => {
                return host
                    .write_all(&frame)
                    .map_err(|error| format!("sending to the host: {error}"));
            }
            Ok(frame) => format!(
                "its frame of {} bytes leaves no room for its envelope under the control plane's {CONTROL_PLANE_FRAME_BYTES}-byte frame cap",
                frame.len()
            ),
            Err(error) => error.to_string(),
        },
    };
    let notice = LongText::try_from(format!("tumpel: harness line {line_seq} was left out: {too_long}"))
        .map_err(|error| format!("harness line {line_seq}: {error}"))?;
    send(host, &line(notice))
}

const CONTROL_PLANE_FRAME_BYTES: usize = 4_000_000;
const HOST_ENVELOPE_BYTES: usize = 4096;
const LINE_FRAME_BYTES: usize = CONTROL_PLANE_FRAME_BYTES - HOST_ENVELOPE_BYTES;

fn send(host: &mut impl Write, message: &GuestMessage) -> Result<(), String> {
    let frame = encode(message).map_err(|error| error.to_string())?;
    host.write_all(&frame)
        .map_err(|error| format!("sending to the host: {error}"))
}

#[cfg(test)]
mod tests;
