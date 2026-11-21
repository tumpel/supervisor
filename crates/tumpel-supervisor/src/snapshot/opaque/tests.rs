use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::process::Command;

use super::super::tests::{image, sbin, scratch};
use super::super::{Ext4Upper, Upper};
use super::{be32, crc32c, opaque_in, open_drive, Drive, XATTR_MAGIC, XATTR_TRUSTED};

struct Mounted {
    drive: std::path::PathBuf,
    file: std::fs::File,
    block_size: u64,
    journal: std::collections::HashMap<u64, u64>,
    superblock: Vec<u8>,
    seed: u32,
}

impl Mounted {
    fn new(name: &str, content: &[u8]) -> Self {
        let root = scratch(name);
        std::fs::create_dir_all(root.join("tree/upper")).expect("fixture tree");
        std::fs::write(root.join("tree/upper/f"), content).expect("fixture file");
        let drive = root.join("drive.ext4");
        let made = Command::new(sbin("mkfs.ext4"))
            .args(["-q", "-F", "-I", "256", "-d"])
            .arg(root.join("tree"))
            .arg(&drive)
            .arg("16M")
            .output()
            .expect("mkfs.ext4 runs");
        assert!(made.status.success(), "{}", String::from_utf8_lossy(&made.stderr));
        let marked = Command::new(sbin("debugfs"))
            .args(["-w", "-R", "feature needs_recovery"])
            .arg(&drive)
            .output()
            .expect("debugfs runs");
        assert!(marked.status.success(), "{}", String::from_utf8_lossy(&marked.stderr));
        let opened = Drive::open(&drive).expect("open");
        let journal = opened
            .block_map(8, &opened.inode(8).expect("journal inode"))
            .expect("journal blocks");
        let superblock = opened.raw_block(journal[&0]).expect("journal superblock");
        let seed = crc32c(!0, &[&superblock[0x30..0x40]]);
        let file = OpenOptions::new().write(true).open(&drive).expect("open for write");
        Self {
            drive,
            file,
            block_size: opened.block_size,
            journal,
            superblock,
            seed,
        }
    }

    fn max_len(&self) -> u64 {
        u64::from(be32(&self.superblock, 0x10))
    }

    fn first(&self) -> u64 {
        u64::from(be32(&self.superblock, 0x14))
    }

    fn data_block(&self) -> u64 {
        let mapped = Command::new(sbin("debugfs"))
            .args(["-R", "bmap /upper/f 0"])
            .arg(&self.drive)
            .output()
            .expect("debugfs runs");
        String::from_utf8_lossy(&mapped.stdout)
            .trim()
            .parse()
            .expect("a block number")
    }

    fn write(&self, logical: u64, bytes: &[u8]) {
        self.file
            .write_all_at(bytes, self.journal[&logical] * self.block_size)
            .expect("write a journal block");
    }

    fn seal(&self, block: &mut [u8], at: usize) {
        block[at..at + 4].fill(0);
        let checksum = crc32c(self.seed, &[block]);
        block[at..at + 4].copy_from_slice(&checksum.to_be_bytes());
    }

    fn block(&self, kind: u32, sequence: u32) -> Vec<u8> {
        let mut block = vec![0; usize::try_from(self.block_size).expect("block size")];
        block[0..4].copy_from_slice(&0xC03B_3998u32.to_be_bytes());
        block[4..8].copy_from_slice(&kind.to_be_bytes());
        block[8..12].copy_from_slice(&sequence.to_be_bytes());
        block
    }

    fn start(&mut self, at: u64, sequence: u32) {
        let superblock = &mut self.superblock;
        superblock[0x28..0x2C].copy_from_slice(&0x13u32.to_be_bytes());
        superblock[0x50] = 4;
        superblock[0x18..0x1C].copy_from_slice(&sequence.to_be_bytes());
        superblock[0x1C..0x20].copy_from_slice(&u32::try_from(at).expect("log").to_be_bytes());
        superblock[0xFC..0x100].fill(0);
        let checksum = crc32c(!0, &[&superblock[..1024]]);
        superblock[0xFC..0x100].copy_from_slice(&checksum.to_be_bytes());
        self.write(0, &self.superblock);
    }

    fn descriptor(&self, at: u64, sequence: u32, target: u64, data: &[u8]) {
        let mut block = self.block(1, sequence);
        let mut contents = vec![0; block.len()];
        contents[..data.len()].copy_from_slice(data);
        block[12..16].copy_from_slice(&u32::try_from(target).expect("block").to_be_bytes());
        block[16..20].copy_from_slice(&0xAu32.to_be_bytes());
        let checksum = crc32c(self.seed, &[&sequence.to_be_bytes(), &contents]);
        block[24..28].copy_from_slice(&checksum.to_be_bytes());
        let tail = block.len() - 4;
        self.seal(&mut block, tail);
        self.write(at, &block);
        self.write(if at + 1 >= self.max_len() { self.first() } else { at + 1 }, &contents);
    }

    fn revoke(&self, at: u64, sequence: u32, target: u64) {
        let mut block = self.block(5, sequence);
        block[12..16].copy_from_slice(&24u32.to_be_bytes());
        block[16..24].copy_from_slice(&target.to_be_bytes());
        let tail = block.len() - 4;
        self.seal(&mut block, tail);
        self.write(at, &block);
    }

    fn commit(&self, at: u64, sequence: u32) {
        let mut block = self.block(2, sequence);
        self.seal(&mut block, 16);
        self.write(at, &block);
    }

    fn read(&self) -> Vec<u8> {
        let upper = Ext4Upper::load(&self.drive).expect("load");
        let mut read = Vec::new();
        upper
            .open(b"f")
            .expect("open f")
            .read_to_end(&mut read)
            .expect("read f");
        read
    }
}

#[test]
fn the_upper_reads_a_journal_transaction_that_wraps_past_the_end_of_the_log() {
    let mut mounted = Mounted::new("journal-wrap", b"old");
    let (last, first, data) = (mounted.max_len() - 1, mounted.first(), mounted.data_block());
    mounted.start(last, 7);
    mounted.descriptor(last, 7, data, b"new");
    mounted.commit(first + 1, 7);

    assert_eq!(mounted.read(), b"new");
}

#[test]
fn a_revocation_in_a_later_transaction_cancels_an_earlier_copy_of_the_block() {
    let mut mounted = Mounted::new("journal-revoke", b"old");
    let (first, data) = (mounted.first(), mounted.data_block());
    mounted.start(first, 7);
    mounted.descriptor(first, 7, data, b"new");
    mounted.commit(first + 2, 7);
    mounted.revoke(first + 3, 8, data);
    mounted.commit(first + 4, 8);

    assert_eq!(mounted.read(), b"old");
}

#[test]
fn a_torn_last_transaction_with_a_bad_commit_checksum_is_not_applied() {
    let mut mounted = Mounted::new("journal-torn", b"old");
    let (first, data) = (mounted.first(), mounted.data_block());
    mounted.start(first, 7);
    mounted.descriptor(first, 7, data, b"mid");
    mounted.commit(first + 2, 7);
    mounted.descriptor(first + 3, 8, data, b"new");
    let mut torn = mounted.block(2, 8);
    torn[16..20].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
    mounted.write(first + 5, &torn);

    assert_eq!(mounted.read(), b"mid");
}

#[test]
fn the_parser_finds_an_opaque_directory_set_by_debugfs() {
    let drive = image(
        "opaque-parser",
        &["upper/a/opaque", "upper/a/plain"],
        &["ea_set /upper/a/opaque trusted.overlay.opaque y"],
    );

    let found = open_drive(&drive, b"/upper")
        .map(|(_, opaque)| opaque)
        .expect("opaque dirs");

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

    let error = open_drive(&drive, b"/upper")
        .map(|(_, opaque)| opaque)
        .expect_err("refused");

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
