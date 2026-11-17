use std::path::PathBuf;

#[path = "release_version.rs"]
mod release_version;

fn main() -> Result<(), String> {
    release_version::release_version()?;
    guest_drive()
}

fn guest_drive() -> Result<(), String> {
    println!("cargo::rustc-check-cfg=cfg(guest_drive)");
    println!("cargo::rerun-if-env-changed=TUMPEL_GUEST_DRIVE");
    let Some(drive) = std::env::var_os("TUMPEL_GUEST_DRIVE").or_else(packaged_drive) else {
        if std::env::var("PROFILE").as_deref() == Ok("release") {
            return Err("a release build of tumpel-supervisor carries the guest drive, TUMPEL_GUEST_DRIVE is unset and the crate holds no guest drive for this target; build it with npm run supervisor:build, or install the prebuilt binary from the GitHub release".to_string());
        }
        return Ok(());
    };
    let named = PathBuf::from(drive);
    let drive = named.canonicalize().map_err(|error| {
        format!(
            "the guest drive TUMPEL_GUEST_DRIVE names, {}, is unreadable: {error}; pack it with npm run guest:drive",
            named.display()
        )
    })?;
    println!("cargo::rerun-if-changed={}", drive.display());
    println!("cargo::rustc-env=TUMPEL_GUEST_DRIVE_PATH={}", drive.display());
    println!("cargo::rustc-cfg=guest_drive");
    Ok(())
}

fn packaged_drive() -> Option<std::ffi::OsString> {
    let dir = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR")?);
    let target = std::env::var("TARGET").ok()?;
    let drive = dir.join("guest-drive").join(format!("{target}.squashfs"));
    drive.is_file().then(|| drive.into_os_string())
}
