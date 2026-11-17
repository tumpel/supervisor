use std::collections::HashMap;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram};

use tumpel_api::types::{SupervisorHandover, SupervisorHandoverMachine};

const FIRST_RECEIVED: RawFd = 3;
const BARRIER_MILLISECONDS: i32 = 10_000;

fn named(machine: &SupervisorHandoverMachine) -> [(String, RawFd); 3] {
    let pid = machine.process_id;
    [
        (format!("box{pid}-guest"), machine.guest_fd),
        (format!("box{pid}-stdout"), machine.stdout_fd),
        (format!("box{pid}-stderr"), machine.stderr_fd),
    ]
}

fn notify(state: &str, fd: Option<RawFd>) -> Result<(), String> {
    let path = std::env::var("NOTIFY_SOCKET").map_err(|_| {
        "NOTIFY_SOCKET is unset, so systemd cannot keep the boxes' descriptors; run the supervisor from its systemd user unit".to_string()
    })?;
    let address = match path.strip_prefix('@') {
        Some(name) => SocketAddr::from_abstract_name(name),
        None => SocketAddr::from_pathname(&path),
    }
    .map_err(|error| format!("NOTIFY_SOCKET {path:?} is no socket address: {error}"))?;
    let socket = UnixDatagram::unbound().map_err(|error| format!("opening a datagram socket: {error}"))?;
    socket
        .connect_addr(&address)
        .map_err(|error| format!("connecting to systemd at {path}: {error}"))?;
    let mut iov = libc::iovec {
        iov_base: state.as_ptr().cast_mut().cast(),
        iov_len: state.len(),
    };
    let fd_bytes = u32::try_from(std::mem::size_of::<RawFd>()).map_err(|error| error.to_string())?;
    let mut control = vec![0u8; unsafe { libc::CMSG_SPACE(fd_bytes) } as usize];
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &raw mut iov;
    message.msg_iovlen = 1;
    if let Some(fd) = fd {
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = control.len();
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&raw const message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(fd_bytes) as usize;
            libc::CMSG_DATA(header).cast::<RawFd>().write_unaligned(fd);
        }
    }
    if unsafe { libc::sendmsg(socket.as_raw_fd(), &raw const message, 0) } < 0 {
        return Err(format!(
            "telling systemd {:?} failed: {}",
            state.lines().next().unwrap_or_default(),
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

fn barrier() -> Result<(), String> {
    let mut ends = [0; 2];
    if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) } < 0 {
        return Err(format!("opening a pipe: {}", std::io::Error::last_os_error()));
    }
    let mut reader = std::fs::File::from(unsafe { OwnedFd::from_raw_fd(ends[0]) });
    let writer = unsafe { OwnedFd::from_raw_fd(ends[1]) };
    notify("BARRIER=1", Some(writer.as_raw_fd()))?;
    drop(writer);
    let mut poll = libc::pollfd {
        fd: reader.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    if unsafe { libc::poll(&raw mut poll, 1, BARRIER_MILLISECONDS) } <= 0 {
        return Err(format!(
            "systemd did not confirm it holds the boxes' descriptors within {} s",
            BARRIER_MILLISECONDS / 1000
        ));
    }
    reader
        .read(&mut [0u8; 1])
        .map(drop)
        .map_err(|error| format!("waiting for systemd to hold the boxes' descriptors: {error}"))
}

pub fn keep(handover: &SupervisorHandover) -> Result<(), String> {
    for handed in &handover.boxes {
        for (name, fd) in named(&handed.machine) {
            notify(&format!("FDSTOREREMOVE=1\nFDNAME={name}"), None)?;
            notify(&format!("FDSTORE=1\nFDNAME={name}"), Some(fd))?;
        }
    }
    barrier()
}

#[must_use]
pub fn received() -> HashMap<String, RawFd> {
    let pid = std::env::var("LISTEN_PID");
    let names = std::env::var("LISTEN_FDNAMES").unwrap_or_default();
    for key in ["LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES"] {
        std::env::remove_var(key);
    }
    if pid != Ok(std::process::id().to_string()) {
        return HashMap::new();
    }
    names
        .split(':')
        .zip(FIRST_RECEIVED..)
        .map(|(name, fd)| (name.to_string(), fd))
        .collect()
}

#[must_use]
pub fn restored(mut handover: SupervisorHandover, received: &HashMap<String, RawFd>) -> SupervisorHandover {
    for handed in &mut handover.boxes {
        let [guest, stdout, stderr] =
            named(&handed.machine).map(|(name, _)| received.get(&name).copied().unwrap_or(-1));
        handed.machine.guest_fd = guest;
        handed.machine.stdout_fd = stdout;
        handed.machine.stderr_fd = stderr;
    }
    handover
}
