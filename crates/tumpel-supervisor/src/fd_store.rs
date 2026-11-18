use std::collections::BTreeMap;
use std::os::fd::{BorrowedFd, FromRawFd, IntoRawFd, OwnedFd};
use std::path::Path;

use sd_notify::NotifyState;
use tumpel_api::types::SupervisorHandover;

pub const GUEST: &str = "guest";
pub const STDOUT: &str = "stdout";
pub const STDERR: &str = "stderr";

fn name(tap: u8, role: &str) -> String {
    format!("box{tap}-{role}")
}

fn systemd() -> Result<(), String> {
    if std::env::var_os("NOTIFY_SOCKET").is_some() {
        return Ok(());
    }
    Err("NOTIFY_SOCKET is unset, so systemd cannot hold the box's descriptors across a restart; run the supervisor from its systemd user unit".to_string())
}

pub fn reach_user_manager() -> Result<(), String> {
    let runtime = std::env::var_os("XDG_RUNTIME_DIR").ok_or_else(|| {
        "XDG_RUNTIME_DIR is unset, so this supervisor cannot reach the systemd user manager that holds the boxes' descriptors; run the supervisor from its systemd user unit".to_string()
    })?;
    std::env::set_var("NOTIFY_SOCKET", Path::new(&runtime).join("systemd").join("notify"));
    Ok(())
}

fn remove(name: &str) -> Result<(), String> {
    sd_notify::notify(&[NotifyState::FdStoreRemove, NotifyState::FdName(name)])
        .map_err(|error| format!("asking systemd to drop {name}: {error}"))
}

pub fn keep(tap: u8, role: &str, fd: BorrowedFd<'_>) -> Result<(), String> {
    systemd()?;
    let name = name(tap, role);
    remove(&name)?;
    sd_notify::notify_with_fds(&[NotifyState::FdStore, NotifyState::FdName(&name)], &[fd])
        .map_err(|error| format!("handing {name} to systemd: {error}"))
}

pub fn forget(tap: u8, role: &str) -> Result<(), String> {
    systemd()?;
    remove(&name(tap, role))
}

pub fn forget_box(tap: u8) -> Result<(), String> {
    [GUEST, STDOUT, STDERR]
        .into_iter()
        .try_for_each(|role| forget(tap, role))
}

/// # Errors
/// Fails when `LISTEN_FDS` and `LISTEN_FDNAMES` disagree or a passed descriptor is not open.
///
/// # Safety
/// Call it before any other thread starts: it removes the `LISTEN_*` variables from the environment.
pub unsafe fn received() -> Result<BTreeMap<String, OwnedFd>, String> {
    let passed = unsafe { sd_notify::listen_fds_with_names_and_unset_env() }
        .map_err(|error| format!("reading the descriptors systemd passed: {error}"))?;
    Ok(passed
        .map(|(fd, name)| (name, unsafe { OwnedFd::from_raw_fd(fd) }))
        .collect())
}

#[must_use]
pub fn restored(mut handover: SupervisorHandover, received: &mut BTreeMap<String, OwnedFd>) -> SupervisorHandover {
    for handed in &mut handover.boxes {
        let Ok(tap) = u8::try_from(handed.tap_index) else {
            continue;
        };
        let mut take = |role| received.remove(&name(tap, role)).map_or(-1, IntoRawFd::into_raw_fd);
        handed.machine.guest_fd = take(GUEST);
        handed.machine.stdout_fd = take(STDOUT);
        handed.machine.stderr_fd = take(STDERR);
    }
    handover
}
