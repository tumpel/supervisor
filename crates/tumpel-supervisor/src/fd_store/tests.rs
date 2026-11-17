use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::{UnixDatagram, UnixStream};
use std::path::Path;

use super::{forget_box, keep, GUEST};

fn receive(socket: &UnixDatagram) -> (String, Vec<OwnedFd>) {
    let mut data = [0u8; 256];
    let mut control = [0u8; 64];
    let mut iov = libc::iovec {
        iov_base: data.as_mut_ptr().cast(),
        iov_len: data.len(),
    };
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &raw mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len();
    let read = unsafe { libc::recvmsg(socket.as_raw_fd(), &raw mut message, 0) };
    let read = usize::try_from(read).expect("systemd's socket receives a message");
    let mut fds = Vec::new();
    let mut header = unsafe { libc::CMSG_FIRSTHDR(&raw const message) };
    while !header.is_null() {
        unsafe {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let bytes = (*header).cmsg_len - libc::CMSG_LEN(0) as usize;
                let passed = libc::CMSG_DATA(header);
                for index in 0..bytes / 4 {
                    let mut raw = [0u8; 4];
                    std::ptr::copy_nonoverlapping(passed.add(index * 4), raw.as_mut_ptr(), 4);
                    fds.push(OwnedFd::from_raw_fd(i32::from_ne_bytes(raw)));
                }
            }
            header = libc::CMSG_NXTHDR(&raw const message, header);
        }
    }
    (String::from_utf8(data[..read].to_vec()).expect("a text state"), fds)
}

fn inode(fd: &impl AsFd) -> u64 {
    std::fs::File::from(fd.as_fd().try_clone_to_owned().expect("a copy"))
        .metadata()
        .expect("metadata")
        .ino()
}

#[test]
fn a_box_holds_one_slot_per_role_in_the_store_and_its_stop_empties_them() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/fd-store-tests");
    std::fs::create_dir_all(&dir).expect("the scratch directory is created");
    let path = dir.join("notify.sock");
    if path.exists() {
        std::fs::remove_file(&path).expect("clearing the last run");
    }
    let systemd = UnixDatagram::bind(&path).expect("systemd's socket");
    std::env::set_var("NOTIFY_SOCKET", &path);
    let (guest, _peer) = UnixStream::pair().expect("a guest connection");

    keep(3, GUEST, guest.as_fd()).expect("the guest connection is kept");
    let (removed, none) = receive(&systemd);
    assert_eq!(removed, "FDSTOREREMOVE=1\nFDNAME=box3-guest\n");
    assert!(none.is_empty());
    let (stored, passed) = receive(&systemd);
    assert_eq!(stored, "FDSTORE=1\nFDNAME=box3-guest\n");
    assert_eq!(passed.len(), 1);
    assert_eq!(
        inode(&passed[0]),
        inode(&guest),
        "systemd gets the guest connection itself"
    );

    forget_box(3).expect("the box's slots are emptied");
    let states: Vec<String> = (0..3).map(|_| receive(&systemd).0).collect();
    assert_eq!(
        states,
        [
            "FDSTOREREMOVE=1\nFDNAME=box3-guest\n",
            "FDSTOREREMOVE=1\nFDNAME=box3-stdout\n",
            "FDSTOREREMOVE=1\nFDNAME=box3-stderr\n"
        ]
    );
}
