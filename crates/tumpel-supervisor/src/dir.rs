use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use tumpel_api::types::{BoxDiskLimit, BoxMemoryLimit, SupervisorChannelState, SupervisorRuntime};
use tumpel_channel::{ChannelError, Store};

pub const CONFIG_FILE: &str = "supervisor.json";
pub const KEY_FILE: &str = "host-key.json";
pub const CHANNEL_FILE: &str = "channel.json";
pub const RUNTIME_FILE: &str = "runtime.json";
pub const SUPERVISOR_UNIT: &str = "tumpel-supervisor.service";

pub fn supervisor_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("TUMPEL_SUPERVISOR_DIR") {
        return PathBuf::from(dir);
    }
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local").join("state")))
        .unwrap_or_default()
        .join("tumpel-supervisor")
}

fn home() -> PathBuf {
    std::env::var_os("HOME").map_or_default(PathBuf::from)
}

fn xdg(variable: &str, fallback: &[&str]) -> PathBuf {
    std::env::var_os(variable).map_or_else(
        || fallback.iter().fold(home(), |path, part| path.join(part)),
        PathBuf::from,
    )
}

pub fn default_runtime() -> Result<SupervisorRuntime> {
    serde_json::from_value(serde_json::json!({
        "firecrackerPath": home().join(".local").join("bin").join("firecracker"),
        "imageCacheDir": xdg("XDG_CACHE_HOME", &[".cache"]).join("tumpel").join("images"),
        "dataDir": xdg("XDG_DATA_HOME", &[".local", "share"]).join("tumpel-supervisor"),
        "boxCapacity": 8,
        "vcpuCount": 2,
        "memoryMib": BoxMemoryLimit::DEFAULT_MIB,
        "overlayMib": BoxDiskLimit::DEFAULT_MIB,
        "layerCacheMib": 8192,
        "budgets": { "bootSeconds": 30, "firstLineSeconds": 120, "snapshotSeconds": 60 },
        "heartbeatIntervalSeconds": 10,
        "updateGraceSeconds": 600,
        "telemetryIntervalSeconds": 30,
    }))
    .context("the default runtime does not match SupervisorRuntime")
}

fn merge_fields(
    defaults: serde_json::Map<String, serde_json::Value>,
    current: &mut serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    defaults
        .into_iter()
        .map(|(key, default)| {
            let value = match (current.remove(&key), default) {
                (Some(serde_json::Value::Object(mut present)), serde_json::Value::Object(nested)) => {
                    serde_json::Value::Object(merge_fields(nested, &mut present))
                }
                (Some(present), _) => present,
                (None, default) => default,
            };
            (key, value)
        })
        .collect()
}

pub fn runtime_with_defaults(path: &Path, current: &serde_json::Value) -> Result<SupervisorRuntime> {
    let serde_json::Value::Object(mut current_fields) = current.clone() else {
        anyhow::bail!("{} is not a JSON object; fix it per SupervisorRuntime in typespec/lib/supervisor.tsp or delete it to get the defaults", path.display());
    };
    let serde_json::Value::Object(defaults) = serde_json::to_value(default_runtime()?)? else {
        anyhow::bail!("the default runtime does not encode as a JSON object");
    };
    serde_json::from_value(serde_json::Value::Object(merge_fields(defaults, &mut current_fields))).with_context(|| {
        format!(
            "{} does not match the SupervisorRuntime schema in typespec",
            path.display()
        )
    })
}

#[must_use]
pub fn supervisor_unit_path() -> PathBuf {
    xdg("XDG_CONFIG_HOME", &[".config"])
        .join("systemd")
        .join("user")
        .join(SUPERVISOR_UNIT)
}

pub fn ensure_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("making {} private to this user", dir.display()))
}

pub fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    let staged = path.with_extension("tmp");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&staged)?;
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o600))?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&staged, path)
}

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let text = serde_json::to_string_pretty(value).with_context(|| format!("encoding {}", path.display()))?;
    write_private(path, &text).with_context(|| format!("writing {}", path.display()))
}

pub fn read_json<T: DeserializeOwned>(path: &Path, schema: &str) -> Result<Option<T>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    serde_json::from_str(&text)
        .map(Some)
        .with_context(|| format!("{} does not match the {schema} schema in typespec", path.display()))
}

pub struct ChannelFile(pub PathBuf);

impl Store for ChannelFile {
    fn save(&mut self, state: &SupervisorChannelState) -> Result<(), ChannelError> {
        let text = serde_json::to_string(state).map_err(|error| ChannelError::Store(error.to_string()))?;
        write_private(&self.0, &text).map_err(|error| ChannelError::Store(format!("{}: {error}", self.0.display())))
    }
}
