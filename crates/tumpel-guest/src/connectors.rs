use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use serde_json::{json, Map, Value};
use tumpel_api::types::{ChannelDispatchConnector, GuestHarnessProtocol};

use crate::forwarder;

const CACHES: &str = "/run/tumpel/connectors/caches";
const OAUTH_FILES: [(&str, &[u8]); 7] = [
    (".claude/.credentials.json", b"{}"),
    (".claude/mcp-needs-auth-cache.json", b"{}"),
    (".codex/.credentials.json", b"{}"),
    (".local/share/opencode/mcp-auth.json", b"{}"),
    (".pi/agent/mcp-auth.json", b"{}"),
    (".pi/agent/mcp.log", b""),
    (".pi/agent/mcp.log.1", b""),
];
const OAUTH_DIRECTORIES: [&str; 1] = [".codex/secrets"];
pub const OPENCODE_CONFIG: &str = "OPENCODE_CONFIG_CONTENT";
const PI_CONFIG: &str = ".pi/agent/mcp.json";
const PI_GENERATED: &str = "/run/tumpel/connectors/pi-mcp.json";

const KEY_LENGTH: usize = 32;

fn key(name: &str) -> String {
    let mut key = String::new();
    for character in name.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            key.push(character.to_ascii_lowercase());
        } else if !key.is_empty() && !key.ends_with('-') {
            key.push('-');
        }
    }
    key.truncate(KEY_LENGTH);
    let key = key.trim_end_matches('-');
    if key.is_empty() { "connector" } else { key }.to_string()
}

pub fn servers(connectors: &[ChannelDispatchConnector]) -> Vec<(String, String)> {
    let mut named: Vec<(String, &str)> = connectors
        .iter()
        .map(|connector| {
            let name = connector
                .name
                .as_ref()
                .map_or(connector.connector_id.as_str(), |name| name.as_str());
            (key(name), connector.connector_id.as_str())
        })
        .collect();
    named.sort_unstable();
    let mut taken = std::collections::HashSet::new();
    let mut servers: Vec<(String, String)> = named
        .into_iter()
        .map(|(base, id)| {
            let (mut key, mut n) = (base.clone(), 1);
            while !taken.insert(key.clone()) {
                n += 1;
                key = format!("{base}-{n}");
            }
            (key, forwarder::route(id))
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

pub fn arguments(protocol: &GuestHarnessProtocol, connectors: &[ChannelDispatchConnector]) -> Vec<String> {
    let servers = servers(connectors);
    if servers.is_empty() {
        return Vec::new();
    }
    match protocol {
        GuestHarnessProtocol::Unrecognised(_) => {
            tumpel_guest::update_required();
            Vec::new()
        }
        GuestHarnessProtocol::ClaudeStreamJson => vec![
            "--mcp-config".to_string(),
            json!({ "mcpServers": object(&servers, |url| json!({ "type": "http", "url": url })) }).to_string(),
        ],
        GuestHarnessProtocol::CodexExecJson => servers
            .iter()
            .flat_map(|(key, url)| ["-c".to_string(), format!("mcp_servers.{key}.url={}", json!(url))])
            .collect(),
        GuestHarnessProtocol::PiRpc | GuestHarnessProtocol::PromptOnStdin | GuestHarnessProtocol::ShellLines => {
            Vec::new()
        }
    }
}

fn pi_config(base: Option<&str>, connectors: &[ChannelDispatchConnector]) -> Result<String, String> {
    let mut config = match base.map(str::trim).filter(|text| !text.is_empty()) {
        None => json!({}),
        Some(text) => serde_json::from_str::<Value>(text).map_err(|error| {
            format!("{PI_CONFIG} is not JSON, so the connectors cannot join its MCP servers: {error}")
        })?,
    };
    let root = config
        .as_object_mut()
        .ok_or_else(|| format!("{PI_CONFIG} is not a JSON object"))?;
    let servers = root
        .entry("mcpServers")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| format!("mcpServers in {PI_CONFIG} is not an object"))?;
    for (key, url) in self::servers(connectors) {
        servers.insert(key, json!({ "url": url }));
    }
    serde_json::to_string_pretty(&config).map_err(|error| format!("writing {PI_CONFIG}: {error}"))
}

pub fn write_pi_config(home: &Path, connectors: &[ChannelDispatchConnector], mounted: bool) -> Result<(), String> {
    let target = home.join(PI_CONFIG);
    let base = match std::fs::read_to_string(&target) {
        Ok(text) => Some(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("reading {}: {error}", target.display())),
    };
    let config = pi_config(base.as_deref(), connectors)?;
    if mounted {
        return std::fs::write(&target, config).map_err(|error| format!("writing {}: {error}", target.display()));
    }
    let generated = Path::new(PI_GENERATED);
    let parent = generated.parent().ok_or("the generated Pi config has no directory")?;
    std::fs::create_dir_all(parent).map_err(|error| format!("creating {}: {error}", parent.display()))?;
    std::fs::write(generated, config).map_err(|error| format!("writing {PI_GENERATED}: {error}"))?;
    if base.is_none() {
        let directory = target.parent().ok_or("the Pi config has no directory")?;
        std::fs::create_dir_all(directory).map_err(|error| format!("creating {}: {error}", directory.display()))?;
        std::fs::write(&target, b"{}").map_err(|error| format!("creating {}: {error}", target.display()))?;
    }
    bind(generated, &target)
}

pub fn variables(protocol: &GuestHarnessProtocol, connectors: &[ChannelDispatchConnector]) -> Vec<(String, String)> {
    let servers = servers(connectors);
    if servers.is_empty() || protocol != &GuestHarnessProtocol::PromptOnStdin {
        return Vec::new();
    }
    let mcp = object(&servers, |url| json!({ "type": "remote", "url": url, "oauth": false }));
    vec![(OPENCODE_CONFIG.to_string(), json!({ "mcp": mcp }).to_string())]
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
    OAUTH_FILES.iter().any(|(file, _)| *file == path)
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
    for (relative, empty) in OAUTH_FILES {
        let target = home.join(relative);
        let source = Path::new(CACHES).join(relative.replace('/', "_"));
        let parent = target
            .parent()
            .ok_or_else(|| format!("{} has no parent", target.display()))?;
        std::fs::create_dir_all(parent).map_err(|error| format!("creating {}: {error}", parent.display()))?;
        std::fs::write(&source, empty).map_err(|error| format!("creating {}: {error}", source.display()))?;
        if !target.exists() {
            std::fs::write(&target, empty).map_err(|error| format!("creating {}: {error}", target.display()))?;
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

fn harness_configs(protocol: &GuestHarnessProtocol) -> &'static [&'static str] {
    match protocol {
        GuestHarnessProtocol::ClaudeStreamJson => &[".claude.json"],
        GuestHarnessProtocol::CodexExecJson => &[".codex/config.toml"],
        GuestHarnessProtocol::PromptOnStdin => &[".config/opencode/opencode.json", ".config/opencode/opencode.jsonc"],
        GuestHarnessProtocol::PiRpc => &[PI_CONFIG],
        GuestHarnessProtocol::ShellLines => &[],
        GuestHarnessProtocol::Unrecognised(_) => {
            tumpel_guest::update_required();
            &[]
        }
    }
}

pub fn refuse_reserved(
    protocol: &GuestHarnessProtocol,
    home: &Path,
    persona_files: &[&str],
    connectors: &[ChannelDispatchConnector],
) -> Result<(), String> {
    let servers = servers(connectors);
    for relative in harness_configs(protocol)
        .iter()
        .filter(|relative| persona_files.contains(relative))
    {
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
        if let Some((key, _)) = servers.iter().find(|(key, _)| {
            [
                format!("\"{key}\""),
                format!("mcp_servers.{key}]"),
                format!("mcp_servers.{key}."),
            ]
            .iter()
            .any(|named| text.contains(named.as_str()))
        }) {
            return Err(format!(
                "{relative} in the persona already names {key}, the name this session's connector takes; rename that MCP server in the persona"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
