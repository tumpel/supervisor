use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

use super::super::tests::image;
use super::{opaque_dirs, opaque_in, Drive, XATTR_MAGIC, XATTR_TRUSTED};

#[test]
fn the_parser_finds_an_opaque_directory_set_by_debugfs() {
    let drive = image(
        "opaque-parser",
        &["upper/a/opaque", "upper/a/plain"],
        &["ea_set /upper/a/opaque trusted.overlay.opaque y"],
    );

    let found = opaque_dirs(&drive, b"/upper").expect("opaque dirs");

    assert_eq!(found.into_iter().collect::<Vec<_>>(), vec![b"a/opaque".to_vec()]);
}

#[test]
fn a_damaged_extended_attribute_block_fails_naming_the_inode_and_block() {
    let drive = image("opaque-bad-block", &["upper/plain"], &["sif /upper/plain file_acl 200"]);
    let block_size = Drive::open(&drive).expect("open").block_size;
    OpenOptions::new()
        .write(true)
        .open(&drive)
        .expect("open for write")
        .write_all_at(b"not an extended attribute block", 200 * block_size)
        .expect("write the bad block");

    let error = opaque_dirs(&drive, b"/upper").expect_err("refused");

    assert!(error.contains("extended attribute block 200 of inode"), "{error}");
}

#[test]
fn a_truncated_extended_attribute_entry_fails() {
    let mut bytes = vec![0; 32];
    bytes[0..4].copy_from_slice(&XATTR_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&[14, XATTR_TRUSTED, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    bytes.extend_from_slice(b"overlay");

    let error = opaque_in(&bytes, 32, 0, "block 7 of inode 12").expect_err("refused");

    assert!(
        error.contains("block 7 of inode 12") && error.contains("cut short"),
        "{error}"
    );
}

#[test]
fn inode_zero_is_refused() {
    let drive = image("opaque-inode-zero", &["upper"], &[]);

    let error = Drive::open(&drive).expect("open").inode(0).expect_err("refused");

    assert!(error.contains("inode 0"), "{error}");
}
