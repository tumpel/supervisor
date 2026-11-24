mod connectors;
mod forwarder;
mod harness;
mod memory;
mod repository;
mod setup;
mod usage;

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use tumpel_api::types::{GuestHarness, GuestHello, GuestHelloType, GuestInput, GuestMessage, GuestProtocol, Sequence};
use tumpel_guest::{encode, Decoder, GUEST_PORT, READY_PORT};

use crate::harness::GuestState;
use crate::memory::Memory;

const CONFIG: &str = "/etc/tumpel/guest.json";
const CMDLINE: &str = "/proc/cmdline";
const LAYERS: &str = "/opt/harness";
const PROC: &str = "/proc";
const CGROUPS: &str = "/sys/fs/cgroup";
const KERNEL_LOG: &str = "/dev/kmsg";

fn vsock() -> Result<OwnedFd, String> {
    let fd = unsafe { libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(format!("opening a vsock socket: {}", std::io::Error::last_os_error()));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn address(cid: u32, port: u32) -> libc::sockaddr_vm {
    let mut address: libc::sockaddr_vm = unsafe { std::mem::zeroed() };
    address.svm_family = libc::sa_family_t::try_from(libc::AF_VSOCK).unwrap_or_default();
    address.svm_cid = cid;
    address.svm_port = port;
    address
}

fn address_size() -> libc::socklen_t {
    libc::socklen_t::try_from(std::mem::size_of::<libc::sockaddr_vm>()).unwrap_or(libc::socklen_t::MAX)
}

fn listen(port: u32) -> Result<OwnedFd, String> {
    use std::os::fd::AsRawFd;
    let socket = vsock()?;
    let local = address(libc::VMADDR_CID_ANY, port);
    if unsafe { libc::bind(socket.as_raw_fd(), (&raw const local).cast(), address_size()) } < 0 {
        return Err(format!(
            "binding vsock port {port}: {}",
            std::io::Error::last_os_error()
        ));
    }
    if unsafe { libc::listen(socket.as_raw_fd(), 4) } < 0 {
        return Err(format!(
            "listening on vsock port {port}: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(socket)
}

fn dial_host(port: u32) -> Result<File, String> {
    use std::os::fd::AsRawFd;
    let socket = vsock()?;
    let host = address(libc::VMADDR_CID_HOST, port);
    if unsafe { libc::connect(socket.as_raw_fd(), (&raw const host).cast(), address_size()) } < 0 {
        return Err(format!(
            "dialling the host's ready port {port}: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(File::from(socket))
}

fn accept(listener: &OwnedFd) -> Result<File, String> {
    use std::os::fd::AsRawFd;
    let fd = unsafe {
        libc::accept4(
            listener.as_raw_fd(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            libc::SOCK_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(format!(
            "accepting on vsock port {GUEST_PORT}: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(File::from(unsafe { OwnedFd::from_raw_fd(fd) }))
}

fn hello(config: &GuestHarness, last_line_seq: i64) -> Result<Vec<u8>, String> {
    let message: GuestMessage = GuestHello {
        harness: config
            .harness
            .to_string()
            .try_into()
            .map_err(|error| format!("harness: {error}"))?,
        harness_version: config
            .harness_version
            .to_string()
            .try_into()
            .map_err(|error| format!("harness version: {error}"))?,
        last_line_seq: Sequence(last_line_seq),
        protocol_version: std::num::NonZeroU32::new(u32::from(GuestProtocol::VERSION)).ok_or("protocol version 0")?,
        type_: GuestHelloType::GuestHello,
    }
    .into();
    encode(&message).map_err(|error| error.to_string())
}

fn connection(mut stream: File, guest: &Mutex<GuestState>) -> Result<(), String> {
    let greeting = {
        let guest = guest.lock().map_err(|_| "the box state was poisoned by a panic")?;
        hello(guest.config(), guest.last_line_seq())?
    };
    stream
        .write_all(&greeting)
        .map_err(|error| format!("sending GuestHello: {error}"))?;
    let mut decoder = Decoder::default();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        while let Some(input) = decoder.take::<GuestInput>().map_err(|error| error.to_string())? {
            match input {
                GuestInput::Unrecognised(_) => tumpel_guest::update_required(),
                GuestInput::TurnStart(start) => {
                    let mut guest = guest.lock().map_err(|_| "the box state was poisoned by a panic")?;
                    guest.run(&start, &mut stream)?;
                }
                GuestInput::ClaudeSetupStart(start) => {
                    let (config, confinement) = {
                        let guest = guest.lock().map_err(|_| "the box state was poisoned by a panic")?;
                        (guest.config().clone(), guest.memory().confinement.clone())
                    };
                    setup::run(&start, &config, &confinement, &mut stream, &mut decoder)?;
                }
                GuestInput::ClaudeSetupCode(_) | GuestInput::TurnCancel(_) => {}
            }
        }
        let read = stream
            .read(&mut buffer)
            .map_err(|error| format!("reading from the host: {error}"))?;
        if read == 0 {
            return Ok(());
        }
        decoder.push(&buffer[..read]);
    }
}

fn read_config(path: &str) -> Result<GuestHarness, String> {
    let text = std::fs::read_to_string(path).map_err(|error| format!("reading {path}: {error}"))?;
    serde_json::from_str(&text)
        .map_err(|error| format!("{path} does not match GuestHarness in supervisor.tsp: {error}"))
}

fn layer_root(cmdline: &str) -> Option<String> {
    cmdline
        .split_whitespace()
        .find_map(|word| word.strip_prefix("tumpel.harness="))
        .map(|harness| format!("{LAYERS}/{harness}"))
}

fn config() -> Result<GuestHarness, String> {
    let cmdline = std::fs::read_to_string(CMDLINE).map_err(|error| format!("reading {CMDLINE}: {error}"))?;
    let Some(root) = layer_root(&cmdline) else {
        return read_config(CONFIG);
    };
    let mut config = read_config(&format!("{root}/tumpel-harness.json"))?;
    config.environment.path = format!("{root}/bin:{}", *config.environment.path)
        .try_into()
        .map_err(|error| format!("the PATH of the harness layer at {root}: {error}"))?;
    Ok(config)
}

fn serve() -> Result<(), String> {
    let memory = Memory::guard(Path::new(PROC), Path::new(CGROUPS), Path::new(KERNEL_LOG))?;
    let guest = GuestState::new(config()?, memory)?;
    let listener = listen(GUEST_PORT)?;
    let announcement = hello(guest.config(), guest.last_line_seq())?;
    let guest = Arc::new(Mutex::new(guest));
    dial_host(READY_PORT)?
        .write_all(&announcement)
        .map_err(|error| format!("announcing on the host's ready port: {error}"))?;
    loop {
        let stream = accept(&listener)?;
        let guest = Arc::clone(&guest);
        std::thread::spawn(move || {
            if let Err(error) = connection(stream, &guest) {
                eprintln!("tumpel-guest: {error}");
            }
        });
    }
}

fn main() -> ExitCode {
    match serve() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tumpel-guest: {error}");
            ExitCode::FAILURE
        }
    }
}
