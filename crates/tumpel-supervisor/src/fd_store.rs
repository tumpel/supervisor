use std::collections::BTreeMap;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use sendfd::SendWithFd;
use tumpel_api::types::{SupervisorHandover, SupervisorHandoverBox};

pub const GUEST: &str = "guest";
pub const STDOUT: &str = "stdout";
pub const STDERR: &str = "stderr";

fn name(tap: u8, role: &str) -> String {
    format!("box{tap}-{role}")
}

static NOTIFY_SOCKET: Mutex<Option<PathBuf>> = Mutex::new(None);

fn notify_socket() -> MutexGuard<'static, Option<PathBuf>> {
    NOTIFY_SOCKET.lock().unwrap_or_else(PoisonError::into_inner)
}

/// # Errors
/// Fails when the supervisor took a handover by exec and `XDG_RUNTIME_DIR` is unset.
///
/// # Safety
/// Call it before any other thread starts: it removes `NOTIFY_SOCKET` from the environment.
pub unsafe fn take_notify_socket(handed: bool) -> Result<(), String> {
    let socket = if handed {
        let runtime = std::env::var_os("XDG_RUNTIME_DIR").ok_or_else(|| {
            "XDG_RUNTIME_DIR is unset, so this supervisor cannot reach the systemd user manager that holds the boxes' descriptors; run the supervisor from its systemd user unit".to_string()
        })?;
        Some(Path::new(&runtime).join("systemd").join("notify"))
    } else {
        Some(std::env::var_os("NOTIFY_SOCKET")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or("the systemd descriptor store is unavailable; start the supervisor with systemctl --user start tumpel-supervisor")?)
    };
    std::env::remove_var("NOTIFY_SOCKET");
    *notify_socket() = socket;
    Ok(())
}

fn notify(action: &str, message: &str, fds: &[RawFd]) -> Result<(), String> {
    let socket = notify_socket().clone().ok_or_else(|| {
        "NOTIFY_SOCKET is unset, so systemd cannot hold the box's descriptors across a restart; run the supervisor from its systemd user unit".to_string()
    })?;
    let datagram = UnixDatagram::unbound().map_err(|error| format!("{action}: opening a socket failed: {error}"))?;
    datagram
        .set_write_timeout(Some(Duration::from_secs(5)))
        .and_then(|()| datagram.connect(&socket))
        .and_then(|()| datagram.send_with_fd(message.as_bytes(), fds))
        .map(drop)
        .map_err(|error| format!("{action} at {} failed: {error}", socket.display()))
}

fn remove(name: &str) -> Result<(), String> {
    notify(
        &format!("asking systemd to drop {name}"),
        &format!("FDSTOREREMOVE=1\nFDNAME={name}\n"),
        &[],
    )
}

pub fn keep(tap: u8, role: &str, fd: BorrowedFd<'_>) -> Result<(), String> {
    remove(&name(tap, role))?;
    let name = name(tap, role);
    notify(
        &format!("handing {name} to systemd"),
        &format!("FDSTORE=1\nFDNAME={name}\n"),
        &[fd.as_raw_fd()],
    )
}

pub(crate) struct LegacyFd {
    pub tap: Option<u8>,
    pub role: &'static str,
    pub fd: OwnedFd,
}

pub(crate) fn seal_inherited(fd: RawFd) -> Result<(), String> {
    if fd < 3 {
        return Err(format!("the inherited descriptor {fd} is reserved or missing"));
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(format!(
            "keeping inherited descriptor {fd} out of child processes failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

pub(crate) fn own_inherited(fd: RawFd) -> Result<OwnedFd, String> {
    let mode = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if mode < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, mode | libc::O_NONBLOCK) } < 0 {
        return Err(format!(
            "setting inherited descriptor {fd} to nonblocking failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

pub fn forget(tap: u8, role: &str) -> Result<(), String> {
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

pub(crate) fn restore_box(handed: &mut SupervisorHandoverBox, received: &BTreeMap<String, OwnedFd>) {
    let Ok(tap) = u8::try_from(handed.tap_index) else {
        return;
    };
    let descriptor = |role| received.get(&name(tap, role)).map_or(-1, AsRawFd::as_raw_fd);
    handed.machine.guest_fd = descriptor(GUEST);
    handed.machine.stdout_fd = descriptor(STDOUT);
    handed.machine.stderr_fd = descriptor(STDERR);
}

#[must_use]
pub fn restored(mut handover: SupervisorHandover, received: &BTreeMap<String, OwnedFd>) -> SupervisorHandover {
    for handed in &mut handover.boxes {
        restore_box(handed, received);
    }
    handover
}
