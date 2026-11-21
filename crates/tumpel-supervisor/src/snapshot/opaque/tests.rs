use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::process::Command;

use super::super::tests::{image, sbin, scratch};
use super::super::{Ext4Upper, Upper};
use super::{opaque_dirs, opaque_in, Drive, XATTR_MAGIC, XATTR_TRUSTED};

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    crc
}

fn journal_block(kind: u32, sequence: u32, size: usize) -> Vec<u8> {
    let mut block = vec![0; size];
    block[0..4].copy_from_slice(&0xC03B_3998u32.to_be_bytes());
    block[4..8].copy_from_slice(&kind.to_be_bytes());
    block[8..12].copy_from_slice(&sequence.to_be_bytes());
    block
}

#[test]
fn the_upper_reads_a_journal_transaction_that_wraps_past_the_end_of_the_log() {
    let root = scratch("journal-wrap");
    std::fs::create_dir_all(root.join("tree/upper")).expect("fixture tree");
    std::fs::write(root.join("tree/upper/f"), b"old").expect("fixture file");
    let drive = root.join("drive.ext4");
    let made = Command::new(sbin("mkfs.ext4"))
        .args(["-q", "-F", "-I", "256", "-d"])
        .arg(root.join("tree"))
        .arg(&drive)
        .arg("16M")
        .output()
        .expect("mkfs.ext4 runs");
    assert!(made.status.success(), "{}", String::from_utf8_lossy(&made.stderr));
    let mapped = Command::new(sbin("debugfs"))
        .args(["-R", "bmap /upper/f 0"])
        .arg(&drive)
        .output()
        .expect("debugfs runs");
    let data: u64 = String::from_utf8_lossy(&mapped.stdout)
        .trim()
        .parse()
        .expect("a block number");
    let opened = Drive::open(&drive).expect("open");
    let size = usize::try_from(opened.block_size).expect("block size");
    let journal = opened
        .block_map(8, &opened.inode(8).expect("journal inode"))
        .expect("journal blocks");
    let file = OpenOptions::new().write(true).open(&drive).expect("open for write");
    let write = |logical: u64, bytes: &[u8]| {
        file.write_all_at(bytes, journal[&logical] * opened.block_size)
            .expect("write a journal block");
    };
    let mut superblock = opened.raw_block(journal[&0]).expect("journal superblock");
    let max_len = u64::from(super::be32(&superblock, 0x10));
    let first = u64::from(super::be32(&superblock, 0x14));
    superblock[0x18..0x1C].copy_from_slice(&7u32.to_be_bytes());
    superblock[0x1C..0x20].copy_from_slice(&u32::try_from(max_len - 1).expect("log").to_be_bytes());
    superblock[0xFC..0x100].fill(0);
    let checksum = crc32c(&superblock[..1024]);
    superblock[0xFC..0x100].copy_from_slice(&checksum.to_be_bytes());
    write(0, &superblock);
    let mut descriptor = journal_block(1, 7, size);
    descriptor[12..16].copy_from_slice(&u32::try_from(data).expect("block").to_be_bytes());
    descriptor[16..20].copy_from_slice(&0xAu32.to_be_bytes());
    write(max_len - 1, &descriptor);
    let mut replacement = vec![0; size];
    replacement[..3].copy_from_slice(b"new");
    write(first, &replacement);
    write(first + 1, &journal_block(2, 7, size));

    let upper = Ext4Upper::load(&drive).expect("load");
    let mut read = Vec::new();
    upper
        .open(b"f")
        .expect("open f")
        .read_to_end(&mut read)
        .expect("read f");

    assert_eq!(read, b"new");
}

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
