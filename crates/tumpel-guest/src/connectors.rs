use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use serde_json::{json, Map, Value};
use tumpel_api::types::{ChannelDispatchConnector, GuestHarnessProtocol};

use crate::forwarder;

const CACHES: &str = "/run/tumpel/connectors/caches";
const OAUTH_FILES: [&str; 4] = [
    ".claude/.credentials.json",
    ".claude/mcp-needs-auth-cache.json",
    ".codex/.credentials.json",
    ".local/share/opencode/mcp-auth.json",
];
const OAUTH_DIRECTORIES: [&str; 1] = [".codex/secrets"];
pub const OPENCODE_CONFIG: &str = "OPENCODE_CONFIG_CONTENT";

fn servers(connectors: &[ChannelDispatchConnector]) -> Vec<(String, String)> {
    let mut servers: Vec<(String, String)> = connectors
        .iter()
        .map(|connector| {
            (
                format!("tumpel-{}", *connector.connector_id),
                forwarder::route(&connector.connector_id),
            )
        })
        .collect();
    servers.sort_unstable();
    servers
}

fn object(servers: &[(String, String)], entry: impl Fn(&str) -> Value) -> Value {
    Value::Object(
        servers
            .iter()
            .map(|(key, url)| (key.clone(), entry(url)))
            .collect::<Map<String, Value>>(),
    )
}

pub fn arguments(
    protocol: GuestHarnessProtocol,
    connectors: &[ChannelDispatchConnector],
) -> Result<Vec<String>, String> {
    let servers = servers(connectors);
    if servers.is_empty() {
        return Ok(Vec::new());
    }
    Ok(match protocol {
        GuestHarnessProtocol::ClaudeStreamJson => vec![
            "--mcp-config".to_string(),
            json!({ "mcpServers": object(&servers, |url| json!({ "type": "http", "url": url })) }).to_string(),
        ],
        GuestHarnessProtocol::CodexExecJson => servers
            .iter()
            .flat_map(|(key, url)| ["-c".to_string(), format!("mcp_servers.{key}.url={}", json!(url))])
            .collect(),
        GuestHarnessProtocol::PiRpc => {
            return Err(format!(
                "Pi takes no connectors, and this session's persona lists {}; choose a persona without connectors for a Pi session",
                servers.len()
            ))
        }
        GuestHarnessProtocol::PromptOnStdin | GuestHarnessProtocol::ShellLines => Vec::new(),
    })
}

pub fn variables(protocol: GuestHarnessProtocol, connectors: &[ChannelDispatchConnector]) -> Vec<(String, String)> {
    let servers = servers(connectors);
    if servers.is_empty() || protocol != GuestHarnessProtocol::PromptOnStdin {
        return Vec::new();
    }
    let mcp = object(&servers, |url| json!({ "type": "remote", "url": url, "oauth": false }));
    vec![(OPENCODE_CONFIG.to_string(), json!({ "mcp": mcp }).to_string())]
}

pub fn ids(connectors: &[ChannelDispatchConnector]) -> Vec<String> {
    servers(connectors).into_iter().map(|(key, _)| key).collect()
}

fn bind(source: &Path, target: &Path) -> Result<(), String> {
    let path = |path: &Path| {
        CString::new(path.as_os_str().as_bytes())
            .map_err(|error| format!("{} holds a NUL byte: {error}", path.display()))
    };
    let (source_c, target_c) = (path(source)?, path(target)?);
    let mounted = unsafe {
        libc::mount(
            source_c.as_ptr(),
            target_c.as_ptr(),
            std::ptr::null(),
            libc::MS_BIND,
            std::ptr::null(),
        )
    };
    if mounted != 0 {
        return Err(format!(
            "bind-mounting {} over {}: {}",
            source.display(),
            target.display(),
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

fn masked(path: &str) -> bool {
    OAUTH_FILES.contains(&path)
        || OAUTH_DIRECTORIES
            .iter()
            .any(|directory| Path::new(path).starts_with(directory))
}

pub fn mask_oauth_caches(home: &Path, credential_file: Option<&str>) -> Result<(), String> {
    if let Some(path) = credential_file.filter(|path| masked(path)) {
        return Err(format!(
            "the turn's credential file {path} is a native MCP OAuth store the box keeps empty while connectors run; store that credential under another path"
        ));
    }
    std::fs::create_dir_all(CACHES).map_err(|error| format!("creating {CACHES}: {error}"))?;
    for relative in OAUTH_FILES {
        let target = home.join(relative);
        let source = Path::new(CACHES).join(relative.replace('/', "_"));
        let parent = target
            .parent()
            .ok_or_else(|| format!("{} has no parent", target.display()))?;
        std::fs::create_dir_all(parent).map_err(|error| format!("creating {}: {error}", parent.display()))?;
        std::fs::write(&source, b"{}").map_err(|error| format!("creating {}: {error}", source.display()))?;
        if !target.exists() {
            std::fs::write(&target, b"{}").map_err(|error| format!("creating {}: {error}", target.display()))?;
        }
        bind(&source, &target)?;
    }
    for relative in OAUTH_DIRECTORIES {
        let target = home.join(relative);
        let source = Path::new(CACHES).join(relative.replace('/', "_"));
        for directory in [&source, &target] {
            std::fs::create_dir_all(directory).map_err(|error| format!("creating {}: {error}", directory.display()))?;
        }
        bind(&source, &target)?;
    }
    Ok(())
}

fn harness_configs(protocol: GuestHarnessProtocol) -> &'static [&'static str] {
    match protocol {
        GuestHarnessProtocol::ClaudeStreamJson => &[".claude.json"],
        GuestHarnessProtocol::CodexExecJson => &[".codex/config.toml"],
        GuestHarnessProtocol::PromptOnStdin => &[".config/opencode/opencode.json", ".config/opencode/opencode.jsonc"],
        GuestHarnessProtocol::PiRpc | GuestHarnessProtocol::ShellLines => &[],
    }
}

pub fn refuse_reserved(
    protocol: GuestHarnessProtocol,
    home: &Path,
    connectors: &[ChannelDispatchConnector],
) -> Result<(), String> {
    let keys = ids(connectors);
    for relative in harness_configs(protocol) {
        let path = home.join(relative);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "reading {} to check its MCP server names: {error}",
                    path.display()
                ))
            }
        };
        if let Some(key) = keys.iter().find(|key| text.contains(key.as_str())) {
            return Err(format!(
                "{relative} in the persona already names {key}, the name this session's connector takes; rename that MCP server in the persona"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
