use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

const MODE: u32 = 0o600;

pub fn write(target: &Path, contents: &str) -> Result<(), String> {
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|error| format!("creating {}: {error}", parent.display()))?;
    }
    match std::fs::remove_file(target) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(format!("replacing {}: {error}", target.display()))
        }
        _ => {}
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(MODE)
        .open(target)
        .and_then(|mut file| file.write_all(contents.as_bytes()))
        .map_err(|error| format!("writing the credential file {}: {error}", target.display()))
}

pub fn take_changed(target: &Path, written: &str) -> Result<Option<String>, String> {
    let left = match std::fs::read_to_string(target) {
        Ok(left) => left,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("reading the credential file {}: {error}", target.display())),
    };
    std::fs::remove_file(target)
        .map_err(|error| format!("deleting the credential file {}: {error}", target.display()))?;
    Ok((!left.trim().is_empty() && left != written).then_some(left))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/credential-file-tests")
            .join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn the_file_is_written_for_its_owner_only_and_replaces_one_left_behind() {
        let target = scratch("written").join(".codex/auth.json");
        write(&target, "old").expect("first write");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).expect("loosened");
        write(&target, r#"{"tokens":{}}"#).expect("second write");
        assert_eq!(
            std::fs::read_to_string(&target).ok().as_deref(),
            Some(r#"{"tokens":{}}"#)
        );
        let mode = std::fs::metadata(&target).map(|meta| meta.permissions().mode() & 0o777);
        assert_eq!(mode.ok(), Some(0o600));
    }

    #[test]
    fn a_rewritten_file_comes_back_once_and_is_deleted() {
        let target = scratch("rewritten").join("auth.json");
        write(&target, "first").expect("written");
        std::fs::write(&target, "refreshed").expect("the harness rewrites it");
        assert_eq!(take_changed(&target, "first"), Ok(Some("refreshed".to_string())));
        assert!(!target.exists(), "the file does not outlive the turn");
        assert_eq!(take_changed(&target, "first"), Ok(None));
    }

    #[test]
    fn an_unchanged_or_emptied_file_comes_back_as_nothing_and_is_deleted() {
        let target = scratch("unchanged").join("auth.json");
        write(&target, "same").expect("written");
        assert_eq!(take_changed(&target, "same"), Ok(None));
        assert!(!target.exists());
        write(&target, "same").expect("written");
        std::fs::write(&target, "\n").expect("emptied");
        assert_eq!(take_changed(&target, "same"), Ok(None));
        assert!(!target.exists());
    }
}
