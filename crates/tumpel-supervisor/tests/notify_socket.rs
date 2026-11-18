use std::os::fd::AsFd;
use std::os::unix::net::{UnixDatagram, UnixStream};

use sendfd::RecvWithFd;
use tumpel_supervisor::{keep_descriptor, take_notify_socket};

#[test]
fn a_child_of_the_supervisor_no_longer_sees_the_notify_socket_and_the_supervisor_still_hands_systemd_a_box_descriptor()
{
    let socket = std::env::temp_dir().join(format!("tumpel-notify-{}.sock", std::process::id()));
    let systemd = UnixDatagram::bind(&socket).expect("a stand-in for systemd");
    std::env::set_var("NOTIFY_SOCKET", &socket);

    unsafe { take_notify_socket(false) }.expect("the supervisor takes the notify socket");
    let child = std::process::Command::new("sh")
        .args(["-c", r#"printf %s "${NOTIFY_SOCKET-unset}""#])
        .output()
        .expect("a child starts");
    let (guest, _peer) = UnixStream::pair().expect("a guest connection");
    keep_descriptor(7, "guest", guest.as_fd()).expect("systemd holds the descriptor");
    let mut removed = [0; 128];
    let mut removed_fds = [0; 1];
    let (removed_len, removed_fd_count) = systemd
        .recv_with_fd(&mut removed, &mut removed_fds)
        .expect("the removal");
    let mut stored = [0; 128];
    let mut stored_fds = [0; 1];
    let (stored_len, stored_fd_count) = systemd.recv_with_fd(&mut stored, &mut stored_fds).expect("the store");

    std::fs::remove_file(&socket).expect("removing the stand-in socket");
    assert_eq!(String::from_utf8_lossy(&child.stdout), "unset");
    assert_eq!(
        (String::from_utf8_lossy(&removed[..removed_len]), removed_fd_count),
        ("FDSTOREREMOVE=1\nFDNAME=box7-guest\n".into(), 0)
    );
    assert_eq!(
        (String::from_utf8_lossy(&stored[..stored_len]), stored_fd_count),
        ("FDSTORE=1\nFDNAME=box7-guest\n".into(), 1)
    );
}
