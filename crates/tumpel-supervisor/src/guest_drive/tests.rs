use std::path::Path;

use ring::digest::{digest, SHA256};

use super::{check_location, place};
use crate::snapshot::hex;

#[test]
fn report_rejects_a_file_where_the_guest_drive_directory_belongs() {
    let images = std::env::temp_dir().join(format!("tumpel-guest-drive-report-{}", std::process::id()));
    std::fs::create_dir(&images).expect("image cache");
    let guest = images.join("guest");
    std::fs::write(&guest, b"obstruction").expect("obstruct the guest directory");

    let error = check_location(&images).expect_err("guest directory is unusable");

    assert_eq!(error.kind(), std::io::ErrorKind::NotADirectory);
    assert_eq!(std::fs::read(&guest).expect("read the obstruction"), b"obstruction");
    std::fs::remove_dir_all(images).expect("remove the fixture");
}

#[test]
fn places_its_own_drive_once_and_deletes_the_others() {
    let images = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/test-guest-drive");
    if images.exists() {
        std::fs::remove_dir_all(&images).expect("clear the fixture");
    }
    std::fs::create_dir_all(images.join("guest")).expect("the guest directory is created");
    std::fs::write(images.join("guest/old.squashfs"), b"old drive").expect("a stale drive is written");
    std::fs::write(images.join("guest/new.squashfs.part"), b"half").expect("a torn write is left");

    let placed = place(&images, b"drive bytes").expect("the drive is placed");
    let again = place(&images, b"drive bytes").expect("placing it again succeeds");

    assert_eq!(placed, again);
    assert_eq!(
        placed,
        images.join(format!(
            "guest/{}.squashfs",
            hex(digest(&SHA256, b"drive bytes").as_ref())
        ))
    );
    assert_eq!(std::fs::read(&placed).expect("the drive is readable"), b"drive bytes");
    let names: Vec<_> = std::fs::read_dir(images.join("guest"))
        .expect("the guest directory is listed")
        .map(|entry| entry.expect("an entry").file_name())
        .collect();
    assert_eq!(names, vec![placed.file_name().expect("a file name").to_owned()]);
}
