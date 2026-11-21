use std::collections::{BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use ext4_view::Ext4Read;

const EXT4_MAGIC: u16 = 0xEF53;
const ROOT_INODE: u32 = 2;
const COMPAT_HAS_JOURNAL: u32 = 0x4;
const INCOMPAT_META_BG: u32 = 0x10;
const INCOMPAT_64BIT: u32 = 0x80;
const EXTENTS_FL: u32 = 0x8_0000;
const INLINE_DATA_FL: u32 = 0x1000_0000;
const EXTENT_MAGIC: u16 = 0xF30A;
const XATTR_MAGIC: u32 = 0xEA02_0000;
const XATTR_TRUSTED: u8 = 4;
const OPAQUE_NAME: &[u8] = b"overlay.opaque";
const FILE_TYPE_DIR: u8 = 2;
const JOURNAL_MAGIC: u32 = 0xC03B_3998;
const JOURNAL_DESCRIPTOR: u32 = 1;
const JOURNAL_COMMIT: u32 = 2;
const JOURNAL_REVOKE: u32 = 5;
const JOURNAL_64BIT: u32 = 0x2;
const JOURNAL_CSUM_V3: u32 = 0x10;
const TAG_ESCAPED: u32 = 0x1;
const TAG_SAME_UUID: u32 = 0x2;
const TAG_LAST: u32 = 0x8;

fn le16(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([bytes[at], bytes[at + 1]])
}

fn le32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn be32(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn be64(bytes: &[u8], at: usize) -> u64 {
    (u64::from(be32(bytes, at)) << 32) | u64::from(be32(bytes, at + 4))
}

fn slice(bytes: &[u8], at: usize, len: usize, what: &str) -> Result<Vec<u8>, String> {
    bytes
        .get(at..at + len)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| format!("{what} runs past the end of its block"))
}

struct Drive {
    file: File,
    block_size: u64,
    inode_size: u64,
    inodes_per_group: u64,
    inode_tables: Vec<u64>,
    journal: HashMap<u64, u64>,
    log: HashSet<u64>,
}

impl Drive {
    fn open(path: &Path) -> Result<Self, String> {
        let file = File::open(path).map_err(|error| format!("opening {}: {error}", path.display()))?;
        let mut superblock = vec![0; 1024];
        file.read_exact_at(&mut superblock, 1024)
            .map_err(|error| format!("reading the superblock of {}: {error}", path.display()))?;
        if le16(&superblock, 0x38) != EXT4_MAGIC {
            return Err(format!("{} has no ext4 superblock", path.display()));
        }
        let incompat = le32(&superblock, 0x60);
        if incompat & INCOMPAT_META_BG != 0 {
            return Err(format!(
                "{} uses meta_bg, which the opaque directory reader does not read",
                path.display()
            ));
        }
        let block_size = 1024u64 << le32(&superblock, 0x18);
        let inodes_per_group = u64::from(le32(&superblock, 0x28));
        let groups = u64::from(le32(&superblock, 0x0)).div_ceil(inodes_per_group);
        let desc_size = if incompat & INCOMPAT_64BIT != 0 {
            u64::from(le16(&superblock, 0xFE))
        } else {
            32
        };
        let descriptors_at = (u64::from(le32(&superblock, 0x14)) + 1) * block_size;
        let mut descriptors = vec![0; usize::try_from(groups * desc_size).map_err(|error| error.to_string())?];
        file.read_exact_at(&mut descriptors, descriptors_at)
            .map_err(|error| format!("reading the block group descriptors of {}: {error}", path.display()))?;
        let inode_tables = descriptors
            .chunks_exact(usize::try_from(desc_size).map_err(|error| error.to_string())?)
            .map(|descriptor| {
                let high = if descriptor.len() >= 64 {
                    le32(descriptor, 0x28)
                } else {
                    0
                };
                (u64::from(high) << 32) | u64::from(le32(descriptor, 0x8))
            })
            .collect();
        let mut drive = Self {
            file,
            block_size,
            inode_size: u64::from(le16(&superblock, 0x58)),
            inodes_per_group,
            inode_tables,
            journal: HashMap::new(),
            log: HashSet::new(),
        };
        if le32(&superblock, 0x5C) & COMPAT_HAS_JOURNAL != 0 {
            let journal_inode = le32(&superblock, 0xE0);
            drive.journal = drive.replay(journal_inode)?;
            drive.log = drive.log_blocks(journal_inode)?;
        }
        Ok(drive)
    }

    fn raw_block(&self, physical: u64) -> Result<Vec<u8>, String> {
        let mut block = vec![0; usize::try_from(self.block_size).map_err(|error| error.to_string())?];
        self.file
            .read_exact_at(&mut block, physical * self.block_size)
            .map_err(|error| format!("reading block {physical}: {error}"))?;
        Ok(block)
    }

    fn block(&self, number: u64) -> Result<Vec<u8>, String> {
        self.raw_block(self.journal.get(&number).copied().unwrap_or(number))
    }

    fn inode(&self, number: u32) -> Result<Vec<u8>, String> {
        let index = u64::from(
            number
                .checked_sub(1)
                .ok_or_else(|| "inode 0 is named, which no inode is; the drive's superblock is damaged".to_string())?,
        );
        let table = self
            .inode_tables
            .get(usize::try_from(index / self.inodes_per_group).map_err(|error| error.to_string())?)
            .ok_or_else(|| format!("inode {number} lies outside every block group"))?;
        let offset = (index % self.inodes_per_group) * self.inode_size;
        let block = self.block(table + offset / self.block_size)?;
        let start = usize::try_from(offset % self.block_size).map_err(|error| error.to_string())?;
        let size = usize::try_from(self.inode_size).map_err(|error| error.to_string())?;
        slice(&block, start, size, &format!("inode {number}"))
    }

    fn extents(&self, node: &[u8], number: u32, out: &mut Vec<(u64, u64, u64)>) -> Result<(), String> {
        if node.len() < 12 || le16(node, 0) != EXTENT_MAGIC {
            return Err(format!("inode {number} has a damaged extent tree"));
        }
        let entries = usize::from(le16(node, 2));
        let depth = le16(node, 6);
        for at in (12..12 + entries * 12).step_by(12) {
            if node.len() < at + 12 {
                return Err(format!("inode {number} has a damaged extent tree"));
            }
            if depth == 0 {
                let len = le16(node, at + 4);
                let len = if len > 32768 { len - 32768 } else { len };
                let start = (u64::from(le16(node, at + 6)) << 32) | u64::from(le32(node, at + 8));
                out.push((u64::from(le32(node, at)), u64::from(len), start));
                continue;
            }
            let child = (u64::from(le16(node, at + 8)) << 32) | u64::from(le32(node, at + 4));
            self.extents(&self.block(child)?, number, out)?;
        }
        Ok(())
    }

    fn block_map(&self, number: u32, inode: &[u8]) -> Result<HashMap<u64, u64>, String> {
        let flags = le32(inode, 0x20);
        if flags & INLINE_DATA_FL != 0 || flags & EXTENTS_FL == 0 {
            return Err(format!(
                "inode {number} keeps its data inline or in a block map, which the opaque directory reader does not read"
            ));
        }
        let mut extents = Vec::new();
        self.extents(&inode[0x28..0x28 + 60], number, &mut extents)?;
        Ok(extents
            .into_iter()
            .flat_map(|(logical, len, start)| (0..len).map(move |step| (logical + step, start + step)))
            .collect())
    }

    fn log_blocks(&self, journal_inode: u32) -> Result<HashSet<u64>, String> {
        let inode = self.inode(journal_inode)?;
        Ok(self
            .block_map(journal_inode, &inode)?
            .into_iter()
            .filter(|(logical, _)| *logical != 0)
            .map(|(_, physical)| physical)
            .collect())
    }

    fn replay(&self, journal_inode: u32) -> Result<HashMap<u64, u64>, String> {
        let inode = self.inode(journal_inode)?;
        let map = self.block_map(journal_inode, &inode)?;
        let read = |logical: u64| {
            map.get(&logical)
                .ok_or_else(|| format!("the journal has no block {logical}"))
                .and_then(|physical| self.raw_block(*physical))
        };
        let superblock = read(0)?;
        if be32(&superblock, 0) != JOURNAL_MAGIC {
            return Err("the journal has no superblock".to_string());
        }
        let max_len = u64::from(be32(&superblock, 0x10));
        let first = u64::from(be32(&superblock, 0x14));
        let mut sequence = be32(&superblock, 0x18);
        let mut position = u64::from(be32(&superblock, 0x1C));
        if position == 0 {
            return Ok(HashMap::new());
        }
        let features = be32(&superblock, 0x28);
        if features & JOURNAL_64BIT == 0 || features & JOURNAL_CSUM_V3 == 0 {
            return Err(format!(
                "the journal has features {features:#x}; only 64bit with checksum v3 is read"
            ));
        }
        let next = |position: u64| if position + 1 >= max_len { first } else { position + 1 };
        let tail = usize::try_from(self.block_size).map_err(|error| error.to_string())? - 4;
        let mut pending = Vec::new();
        let mut committed: Vec<(u32, u64, u64)> = Vec::new();
        let mut pending_revokes = Vec::new();
        let mut revoked: HashMap<u64, u32> = HashMap::new();
        loop {
            let block = read(position)?;
            if be32(&block, 0) != JOURNAL_MAGIC || be32(&block, 8) != sequence {
                break;
            }
            match be32(&block, 4) {
                JOURNAL_DESCRIPTOR => {
                    let mut at = 12;
                    while at + 16 <= tail {
                        let flags = be32(&block, at + 4);
                        if flags & TAG_ESCAPED != 0 {
                            return Err("the journal holds an escaped block, which is not read".to_string());
                        }
                        let target = (u64::from(be32(&block, at + 8)) << 32) | u64::from(be32(&block, at));
                        position = next(position);
                        pending.push((target, position));
                        at += if flags & TAG_SAME_UUID == 0 { 32 } else { 16 };
                        if flags & TAG_LAST != 0 {
                            break;
                        }
                    }
                }
                JOURNAL_REVOKE => {
                    let used = usize::try_from(be32(&block, 12)).map_err(|error| error.to_string())?;
                    let mut at = 16;
                    while at + 8 <= used.min(block.len()) {
                        pending_revokes.push(be64(&block, at));
                        at += 8;
                    }
                }
                JOURNAL_COMMIT => {
                    committed.extend(pending.drain(..).map(|(target, at)| (sequence, target, at)));
                    for target in pending_revokes.drain(..) {
                        revoked.insert(target, sequence);
                    }
                    sequence = sequence.wrapping_add(1);
                }
                _ => break,
            }
            position = next(position);
        }
        let mut replayed = HashMap::new();
        for (transaction, target, at) in committed {
            if revoked.get(&target).is_some_and(|revoke| *revoke >= transaction) {
                continue;
            }
            let physical = map.get(&at).ok_or_else(|| format!("the journal has no block {at}"))?;
            replayed.insert(target, *physical);
        }
        Ok(replayed)
    }

    fn children(&self, number: u32) -> Result<Vec<(Vec<u8>, u32)>, String> {
        let inode = self.inode(number)?;
        let map = self.block_map(number, &inode)?;
        let size = (u64::from(le32(&inode, 0x6C)) << 32) | u64::from(le32(&inode, 0x4));
        let mut children = Vec::new();
        for logical in 0..size.div_ceil(self.block_size) {
            let Some(physical) = map.get(&logical) else {
                continue;
            };
            let block = self.block(*physical)?;
            let mut at = 0;
            while at + 8 <= block.len() {
                let child = le32(&block, at);
                let record = usize::from(le16(&block, at + 4));
                let name_len = usize::from(block[at + 6]);
                if record < 8 {
                    return Err(format!("directory inode {number} has a damaged entry"));
                }
                let name = slice(
                    &block,
                    at + 8,
                    name_len,
                    &format!("an entry of directory inode {number}"),
                )?;
                if child != 0 && block[at + 7] == FILE_TYPE_DIR && name != b"." && name != b".." {
                    children.push((name, child));
                }
                at += record;
            }
        }
        Ok(children)
    }

    fn is_opaque(&self, number: u32) -> Result<bool, String> {
        let inode = self.inode(number)?;
        if inode.len() > 0x82 {
            let start = 128 + usize::from(le16(&inode, 0x80));
            if inode.len() >= start + 4
                && le32(&inode, start) == XATTR_MAGIC
                && opaque_in(&inode, start + 4, start + 4, &format!("inode {number}"))?
            {
                return Ok(true);
            }
        }
        let block = (u64::from(le16(&inode, 0x76)) << 32) | u64::from(le32(&inode, 0x68));
        if block == 0 {
            return Ok(false);
        }
        let place = format!("the extended attribute block {block} of inode {number}");
        let attributes = self.block(block)?;
        if le32(&attributes, 0) != XATTR_MAGIC {
            return Err(format!("{place} has no extended attribute header"));
        }
        opaque_in(&attributes, 32, 0, &place)
    }

    fn walk(&self, number: u32, path: &[u8], out: &mut BTreeSet<Vec<u8>>) -> Result<(), String> {
        for (name, child) in self.children(number)? {
            let path = if path.is_empty() {
                name
            } else {
                [path, b"/", &name].concat()
            };
            if self.is_opaque(child)? {
                out.insert(path.clone());
            }
            self.walk(child, &path, out)?;
        }
        Ok(())
    }
}

fn opaque_in(bytes: &[u8], first: usize, values: usize, place: &str) -> Result<bool, String> {
    let mut at = first;
    loop {
        if at + 4 > bytes.len() {
            return Err(format!("{place} has an extended attribute list with no end"));
        }
        if le32(bytes, at) == 0 {
            return Ok(false);
        }
        let truncated = || format!("{place} has an extended attribute entry at byte {at} cut short");
        if at + 16 > bytes.len() {
            return Err(truncated());
        }
        let name_len = usize::from(bytes[at]);
        let name = bytes.get(at + 16..at + 16 + name_len).ok_or_else(truncated)?;
        if bytes[at + 1] == XATTR_TRUSTED && name == OPAQUE_NAME {
            let value_at = values + usize::from(le16(bytes, at + 2));
            let value_len = usize::try_from(le32(bytes, at + 8)).map_err(|error| error.to_string())?;
            let value = bytes
                .get(value_at..value_at.saturating_add(value_len))
                .ok_or_else(|| format!("{place} has an overlay.opaque value outside its bounds"))?;
            if value == b"y" {
                return Ok(true);
            }
        }
        at += (16 + name_len + 3) & !3;
    }
}

pub struct Replayed {
    file: File,
    block_size: u64,
    journal: HashMap<u64, u64>,
    log: HashSet<u64>,
}

impl Replayed {
    pub fn open(path: &Path) -> Result<Self, String> {
        let drive = Drive::open(path)?;
        Ok(Self {
            file: drive.file,
            block_size: drive.block_size,
            journal: drive.journal,
            log: drive.log,
        })
    }
}

impl Ext4Read for Replayed {
    fn read(&mut self, start_byte: u64, dst: &mut [u8]) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let mut at = start_byte;
        let mut filled = 0;
        while filled < dst.len() {
            let block = at / self.block_size;
            let offset = at % self.block_size;
            let len = usize::try_from(self.block_size - offset)?.min(dst.len() - filled);
            let part = &mut dst[filled..filled + len];
            if self.log.contains(&block) {
                part.fill(0);
            } else {
                let physical = self.journal.get(&block).copied().unwrap_or(block);
                self.file.read_exact_at(part, physical * self.block_size + offset)?;
            }
            filled += len;
            at += u64::try_from(len)?;
        }
        Ok(())
    }
}

pub fn opaque_dirs(drive: &Path, root: &[u8]) -> Result<BTreeSet<Vec<u8>>, String> {
    let shown = drive.display();
    let drive = Drive::open(drive)?;
    let mut top = ROOT_INODE;
    for name in root.split(|byte| *byte == b'/').filter(|name| !name.is_empty()) {
        top = drive
            .children(top)?
            .into_iter()
            .find(|(child, _)| child == name)
            .map(|(_, inode)| inode)
            .ok_or_else(|| format!("{shown} has no directory {}", String::from_utf8_lossy(root)))?;
    }
    let mut out = BTreeSet::new();
    drive
        .walk(top, b"", &mut out)
        .map_err(|error| format!("reading the opaque directories of {shown}: {error}"))?;
    Ok(out)
}

#[cfg(test)]
mod tests;
