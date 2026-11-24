use std::collections::BTreeMap;
use std::io::{self, IoSlice, IoSliceMut};
use std::mem::MaybeUninit;
use std::num::NonZeroU32;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixListener as StdListener, UnixStream as StdStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use rustix::net::sockopt::{set_socket_passcred, socket_acceptconn, socket_peercred};
use rustix::net::{
    bind, connect, listen, recvmsg, sendmsg, socket_with, AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage,
    RecvFlags, ReturnFlags, SendAncillaryBuffer, SendAncillaryMessage, SendFlags, SocketAddrUnix, SocketFlags,
    SocketType, UCred,
};
use tokio::io::Interest;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinSet;
use tokio::time::{timeout_at, Instant};

use crate::firecracker::process_identity;
use crate::pool::TAP_POOL;

const NEGOTIATE: Duration = Duration::from_secs(10);
const LISTENER: u8 = 129;
const ACCEPT_LISTENER: u8 = 130;
const DONE: u8 = 131;
const BUSY: u8 = 132;
const REFUSED: u8 = 133;
const BOX_DESCRIPTORS: usize = 3;

pub(crate) type Descriptors = BTreeMap<u8, Arc<[OwnedFd; BOX_DESCRIPTORS]>>;

struct Peer {
    pid: libc::pid_t,
    uid: libc::uid_t,
    start: u64,
}

impl Peer {
    fn check(&self, credentials: UCred) -> Result<()> {
        ensure!(
            credentials.uid.as_raw() == self.uid && credentials.pid.as_raw_nonzero().get() == self.pid,
            "handover peer credentials changed"
        );
        let process = process_identity(self.pid)
            .map_err(anyhow::Error::msg)?
            .context("handover peer exited")?;
        ensure!(process.start == self.start, "handover peer process was replaced");
        Ok(())
    }
}

fn address(state: &Path, uid: libc::uid_t) -> Result<SocketAddrUnix> {
    let state = state.canonicalize().context("finding the supervisor state directory")?;
    let digest = crate::update::sha256_hex(state.as_os_str().as_encoded_bytes());
    let name = format!("tumpel-supervisor-{uid}-{digest}");
    SocketAddrUnix::new_abstract_name(name.as_bytes()).context("naming the local handover endpoint")
}

fn socket() -> Result<OwnedFd> {
    let socket = socket_with(
        AddressFamily::UNIX,
        SocketType::STREAM,
        SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
        None,
    )
    .context("opening a local handover socket")?;
    set_socket_passcred(&socket, true).context("enabling local handover credentials")?;
    Ok(socket)
}

fn transmit(stream: &UnixStream, kind: u8, fds: &[BorrowedFd<'_>]) -> io::Result<()> {
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(BOX_DESCRIPTORS))];
    let mut control = SendAncillaryBuffer::new(&mut space);
    if !fds.is_empty() && !control.push(SendAncillaryMessage::ScmRights(fds)) {
        return Err(io::Error::other("too many descriptors in a handover message"));
    }
    let sent = sendmsg(stream, &[IoSlice::new(&[kind])], &mut control, SendFlags::NOSIGNAL)?;
    if sent != 1 {
        return Err(io::Error::other("short local handover message"));
    }
    Ok(())
}

async fn send(stream: &UnixStream, kind: u8, fds: &[BorrowedFd<'_>]) -> Result<()> {
    stream
        .async_io(Interest::WRITABLE, || transmit(stream, kind, fds))
        .await
        .context("sending a local handover message")
}

struct Message {
    kind: u8,
    fds: Vec<OwnedFd>,
    credentials: UCred,
}

async fn read(stream: &UnixStream) -> Result<Option<Message>> {
    stream
        .async_io(Interest::READABLE, || {
            let mut byte = [0];
            let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(BOX_DESCRIPTORS), ScmCredentials(1))];
            let mut control = RecvAncillaryBuffer::new(&mut space);
            let received = recvmsg(
                stream,
                &mut [IoSliceMut::new(&mut byte)],
                &mut control,
                RecvFlags::CMSG_CLOEXEC,
            )?;
            if received.bytes == 0 {
                return Ok(None);
            }
            if received.flags.contains(ReturnFlags::CTRUNC) {
                return Err(io::Error::other("truncated local handover descriptors"));
            }
            let mut fds = Vec::new();
            let mut credentials = None;
            for ancillary in control.drain() {
                match ancillary {
                    RecvAncillaryMessage::ScmRights(descriptors) => fds.extend(descriptors),
                    RecvAncillaryMessage::ScmCredentials(peer) => credentials = Some(peer),
                    _ => return Err(io::Error::other("unexpected local handover ancillary data")),
                }
            }
            Ok(Some(Message {
                kind: byte[0],
                fds,
                credentials: credentials.ok_or_else(|| io::Error::other("missing local handover credentials"))?,
            }))
        })
        .await
        .context("reading a local handover message")
}

async fn receive(stream: &UnixStream, peer: &Peer) -> Result<Message> {
    let message = read(stream).await?.context("local handover peer disconnected")?;
    peer.check(message.credentials)?;
    Ok(message)
}

async fn expect(stream: &UnixStream, peer: &Peer, kind: u8) -> Result<()> {
    let message = receive(stream, peer).await?;
    ensure!(
        message.kind == kind && message.fds.is_empty(),
        "unexpected local handover request"
    );
    Ok(())
}

pub(crate) struct Endpoint {
    listener: Arc<UnixListener>,
    candidate: PathBuf,
    uid: libc::uid_t,
    pid: libc::pid_t,
}

impl Endpoint {
    pub fn bind(state: &Path, candidate: &Path) -> Result<Self> {
        let uid = unsafe { libc::geteuid() };
        let socket = socket()?;
        bind(&socket, &address(state, uid)?).context("binding the private supervisor handover endpoint")?;
        listen(&socket, 8).context("listening on the private supervisor handover endpoint")?;
        let listener = UnixListener::from_std(StdListener::from(socket))
            .context("registering the private supervisor handover endpoint")?;
        Ok(Self {
            listener: Arc::new(listener),
            candidate: candidate.to_path_buf(),
            uid,
            pid: unsafe { libc::getpid() },
        })
    }

    fn successor(&self, credentials: UCred) -> Result<Peer> {
        ensure!(
            credentials.uid.as_raw() == self.uid,
            "handover successor has a different user"
        );
        let pid = credentials.pid.as_raw_nonzero().get();
        let process = process_identity(pid)
            .map_err(anyhow::Error::msg)?
            .context("handover successor exited")?;
        ensure!(
            process.parent == self.pid,
            "handover successor is not a child of this supervisor"
        );
        let executable = std::fs::metadata(format!("/proc/{pid}/exe")).context("reading the successor executable")?;
        let candidate = std::fs::metadata(&self.candidate).context("reading the staged supervisor executable")?;
        ensure!(
            executable.dev() == candidate.dev() && executable.ino() == candidate.ino(),
            "handover successor is not running the staged supervisor executable"
        );
        Ok(Peer {
            pid,
            uid: self.uid,
            start: process.start,
        })
    }

    pub fn start(self, source: impl Fn() -> Result<Descriptors> + Send + Sync + 'static) -> JoinSet<()> {
        let mut task = JoinSet::new();
        task.spawn(async move {
            if let Err(error) = self.serve(source).await {
                eprintln!("tumpel-supervisor: the local handover endpoint stopped: {error:#}");
            }
        });
        task
    }

    async fn serve(&self, source: impl Fn() -> Result<Descriptors> + Send + Sync + 'static) -> Result<()> {
        let source = Arc::new(source);
        let mut active = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                Some(ended) = active.join_next(), if !active.is_empty() => match ended {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => eprintln!("tumpel-supervisor: local handover failed: {error:#}"),
                    Err(error) => eprintln!("tumpel-supervisor: local handover task stopped: {error}"),
                },
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted.context("accepting a local handover connection")?;
                    let accepted_at = Instant::now();
                    let peer = socket_peercred(&stream)
                        .context("reading successor credentials")
                        .and_then(|credentials| self.successor(credentials));
                    match peer {
                        Ok(peer) if active.is_empty() => {
                            let listener = Arc::clone(&self.listener);
                            let source = Arc::clone(&source);
                            active.spawn(async move {
                                timeout_at(accepted_at + NEGOTIATE, offer(stream, &peer, &listener, &*source))
                                    .await
                                    .context("local handover negotiation exceeded 10 seconds")?
                            });
                        }
                        Ok(_) => refuse(&stream, BUSY),
                        Err(_) => refuse(&stream, REFUSED),
                    }
                }
            }
        }
    }
}

fn refuse(stream: &UnixStream, kind: u8) {
    if let Err(error) = transmit(stream, kind, &[]) {
        eprintln!("tumpel-supervisor: sending the local handover refusal failed: {error}");
    }
}

async fn offer(
    stream: UnixStream,
    peer: &Peer,
    listener: &UnixListener,
    source: &impl Fn() -> Result<Descriptors>,
) -> Result<()> {
    set_socket_passcred(&stream, true).context("enabling successor message credentials")?;
    send(&stream, LISTENER, &[listener.as_fd()]).await?;
    expect(&stream, peer, ACCEPT_LISTENER).await?;
    for (tap, files) in source()? {
        ensure!(tap < TAP_POOL, "the handover inventory names an invalid TAP device");
        send(&stream, tap, &files.each_ref().map(AsFd::as_fd)).await?;
    }
    send(&stream, DONE, &[]).await?;
    ensure!(
        read(&stream).await?.is_none(),
        "the successor sent an unexpected message after transfer"
    );
    Ok(())
}

struct Offer {
    stream: UnixStream,
    peer: Peer,
    listener: OwnedFd,
}

async fn request(state: &Path, parent: libc::pid_t) -> Result<Offer> {
    let uid = unsafe { libc::geteuid() };
    ensure!(
        parent == unsafe { libc::getppid() },
        "the handover predecessor is not the parent process"
    );
    let process = process_identity(parent)
        .map_err(anyhow::Error::msg)?
        .context("the handover predecessor exited")?;
    let peer = Peer {
        pid: parent,
        uid,
        start: process.start,
    };
    let socket = socket()?;
    connect(&socket, &address(state, uid)?).context("connecting to the private supervisor handover endpoint")?;
    let stream = UnixStream::from_std(StdStream::from(socket)).context("registering the local handover connection")?;
    let Message { kind, fds, .. } = receive(&stream, &peer).await?;
    let passed: Result<[OwnedFd; 1], _> = fds.try_into();
    let listener = match (kind, passed) {
        (LISTENER, Ok([listener])) => listener,
        (BUSY, _) => bail!("the supervisor already has a handover successor"),
        (REFUSED, _) => bail!("the supervisor refused the handover successor"),
        _ => bail!("the supervisor did not pass its handover listener"),
    };
    ensure!(
        socket_acceptconn(&listener)?,
        "the handover listener is not a listening socket"
    );
    Ok(Offer { stream, peer, listener })
}

pub struct Transfer {
    pub listener: OwnedFd,
    pub boxes: BTreeMap<u8, [OwnedFd; BOX_DESCRIPTORS]>,
    _connection: UnixStream,
}

async fn collect(offer: Offer) -> Result<Transfer> {
    let Offer { stream, peer, listener } = offer;
    let mut boxes = BTreeMap::new();
    send(&stream, ACCEPT_LISTENER, &[]).await?;
    loop {
        let Message { kind, fds, .. } = receive(&stream, &peer).await?;
        if kind == DONE && fds.is_empty() {
            return Ok(Transfer {
                listener,
                boxes,
                _connection: stream,
            });
        }
        ensure!(
            kind < TAP_POOL && !boxes.contains_key(&kind),
            "invalid handover TAP device"
        );
        boxes.insert(
            kind,
            fds.try_into()
                .map_err(|_| anyhow::anyhow!("a box needs guest, stdout and stderr descriptors"))?,
        );
    }
}

pub async fn receive_handover(state: &Path, parent: NonZeroU32) -> Result<Transfer> {
    let parent = i32::try_from(parent.get()).context("invalid handover predecessor PID")?;
    timeout_at(Instant::now() + NEGOTIATE, async {
        collect(request(state, parent).await?).await
    })
    .await
    .context("local handover negotiation exceeded 10 seconds")?
}

#[cfg(test)]
mod tests;
