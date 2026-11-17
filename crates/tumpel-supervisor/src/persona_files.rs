use std::path::Path;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ring::digest::{digest, SHA256};
use tokio::time::timeout_at;
use tumpel_api::types::{ChannelPersonaFileDownload, GuestPersonaFile};

use crate::store::Store;

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

fn cached(file: &Path, wanted: &str) -> Result<Option<Vec<u8>>, String> {
    match std::fs::read(file) {
        Ok(bytes) if sha256(&bytes) == wanted => Ok(Some(bytes)),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("reading {}: {error}", file.display())),
    }
}

async fn blob(store: &impl Store, cache: &Path, file: &ChannelPersonaFileDownload) -> Result<Vec<u8>, String> {
    let wanted: &str = &file.digest;
    let target = cache.join(wanted);
    if let Some(bytes) = cached(&target, wanted)? {
        return Ok(bytes);
    }
    let staged = cache.join(format!("{wanted}.part"));
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
        let _ = std::fs::remove_file(&staged);
        return Err(format!(
            "persona file {} downloaded with digest {actual}, and the offer names {wanted}; upload the file again and register it by its own digest",
            *file.path
        ));
    }
    std::fs::rename(&staged, &target)
        .map_err(|error| format!("moving {} to {}: {error}", staged.display(), target.display()))?;
    Ok(bytes)
}

pub async fn fetch(
    store: &impl Store,
    cache: &Path,
    files: &[ChannelPersonaFileDownload],
) -> Result<Vec<GuestPersonaFile>, String> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    std::fs::create_dir_all(cache).map_err(|error| format!("creating {}: {error}", cache.display()))?;
    let mut fetched = Vec::with_capacity(files.len());
    for file in files {
        let bytes = blob(store, cache, file).await?;
        fetched.push(GuestPersonaFile {
            content: STANDARD.encode(bytes),
            digest: file.digest.clone(),
            path: file.path.clone(),
        });
    }
    Ok(fetched)
}
