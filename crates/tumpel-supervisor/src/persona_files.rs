use std::path::Path;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ring::digest::{digest, SHA256};
use tokio::time::timeout_at;
use tumpel_api::types::{ChannelPersonaFileDownload, GuestPersonaFile};

use crate::bounded::bounded;
use crate::lifecycle::remove_dir;
use crate::lifecycle::setup::then;
use crate::store::Store;

pub const DIR: &str = "persona-files";

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(64), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

fn sha256(bytes: &[u8]) -> String {
    hex(digest(&SHA256, bytes).as_ref())
}

async fn blob(store: &impl Store, dir: &Path, file: &ChannelPersonaFileDownload) -> Result<Vec<u8>, String> {
    let wanted: &str = &file.digest;
    let staged = dir.join(wanted);
    let deadline = crate::lifecycle::instant_at(file.download_expires_at);
    match timeout_at(deadline, store.download(&file.download_url, &staged)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(format!("persona file {} is unreadable: {error}", *file.path)),
        Err(_) => {
            return Err(format!(
                "persona file {} did not download before its URL expired at {}; offer the turn again for a fresh URL",
                *file.path,
                file.download_expires_at.to_rfc3339()
            ))
        }
    }
    let bytes = std::fs::read(&staged).map_err(|error| format!("reading {}: {error}", staged.display()))?;
    let actual = sha256(&bytes);
    if actual != wanted {
        return Err(format!(
            "persona file {} downloaded with digest {actual}, and the offer names {wanted}; upload the file again and register it by its own digest",
            *file.path
        ));
    }
    Ok(bytes)
}

pub async fn fetch(
    store: &impl Store,
    dir: &Path,
    files: &[ChannelPersonaFileDownload],
) -> Result<Vec<GuestPersonaFile>, String> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    std::fs::create_dir_all(dir).map_err(|error| format!("creating {}: {error}", dir.display()))?;
    let mut fetched = Vec::with_capacity(files.len());
    for file in files {
        let content = match blob(store, dir, file)
            .await
            .and_then(|bytes| bounded(&format!("persona file {}", file.path), STANDARD.encode(bytes)))
        {
            Ok(content) => content,
            Err(error) => return Err(then(error, remove_dir(dir))),
        };
        fetched.push(GuestPersonaFile {
            content,
            digest: file.digest.clone(),
            path: file.path.clone(),
        });
    }
    remove_dir(dir)?;
    Ok(fetched)
}
