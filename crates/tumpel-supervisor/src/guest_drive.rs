use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use ring::digest::{digest, SHA256};

use crate::snapshot::hex;

const WRITE_TIME: Duration = Duration::from_secs(60);

#[cfg(guest_drive)]
const DRIVE: Option<&[u8]> = Some(include_bytes!(env!("TUMPEL_GUEST_DRIVE_PATH")));
#[cfg(not(guest_drive))]
const DRIVE: Option<&[u8]> = None;

fn name(drive: &[u8]) -> String {
    format!("{}.squashfs", hex(digest(&SHA256, drive).as_ref()))
}

pub(crate) fn check(images: &Path) -> Result<&'static str, String> {
    match DRIVE {
        Some(drive) if drive.starts_with(b"hsqs") => check_location(images, &name(drive)).map_err(|error| {
            format!(
                "cannot use the guest drive directory in {}: {error}. Make imageCacheDir in runtime.json writable",
                images.display()
            )
        }),
        Some(_) => Err("the embedded drive is not SquashFS; install a supervisor built by the release workflow".into()),
        None => Ok("this development build has no embedded guest drive"),
    }
}

fn check_location(images: &Path, name: &str) -> io::Result<&'static str> {
    let dir = images.join("guest");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            crate::dir::check_access(images, libc::W_OK | libc::X_OK)?;
            return Ok("the cache can hold the embedded guest drive");
        }
        Err(error) => return Err(error),
    };
    let mut installed = false;
    let mut stale = false;
    for entry in entries {
        if entry?.file_name() == name {
            installed = true;
        } else {
            stale = true;
        }
    }
    let mode = if installed && !stale {
        libc::X_OK
    } else {
        libc::W_OK | libc::X_OK
    };
    crate::dir::check_access(&dir, mode)?;
    Ok("the guest drive directory is ready")
}

fn place(images: &Path, drive: &[u8]) -> Result<PathBuf, String> {
    let dir = images.join("guest");
    let name = name(drive);
    let path = dir.join(&name);
    std::fs::create_dir_all(&dir).map_err(|error| format!("creating {}: {error}", dir.display()))?;
    if !path.exists() {
        let staged = dir.join(format!("{name}.part"));
        std::fs::write(&staged, drive).map_err(|error| format!("writing {}: {error}", staged.display()))?;
        std::fs::rename(&staged, &path).map_err(|error| format!("moving {} into place: {error}", staged.display()))?;
    }
    let entries = std::fs::read_dir(&dir).map_err(|error| format!("listing {}: {error}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("listing {}: {error}", dir.display()))?;
        if entry.file_name() != name.as_str() {
            std::fs::remove_file(entry.path())
                .map_err(|error| format!("deleting the stale guest drive {}: {error}", entry.path().display()))?;
        }
    }
    Ok(path)
}

pub async fn write(images: &Path) -> Result<(), String> {
    let Some(drive) = DRIVE else {
        return Ok(());
    };
    let images = images.to_path_buf();
    let fix = "make imageCacheDir in runtime.json a writable directory with room for the drive";
    match tokio::time::timeout(WRITE_TIME, tokio::task::spawn_blocking(move || place(&images, drive))).await {
        Ok(Ok(placed)) => placed.map(|_| ()).map_err(|error| format!("{error}; {fix}")),
        Ok(Err(error)) => Err(format!("the guest drive writer stopped: {error}")),
        Err(_) => Err(format!(
            "writing the guest drive took longer than {}s; {fix}",
            WRITE_TIME.as_secs()
        )),
    }
}

pub fn path(images: &Path) -> Result<PathBuf, String> {
    static NAME: OnceLock<Option<String>> = OnceLock::new();
    NAME.get_or_init(|| DRIVE.map(name))
        .as_ref()
        .map(|name| images.join("guest").join(name))
        .ok_or_else(|| "this supervisor was built without a guest drive, so it boots no layered box; install a supervisor the release workflow built".to_string())
}

#[cfg(test)]
mod tests;
