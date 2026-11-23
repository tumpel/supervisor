use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use ext4_view::{Ext4, FileType};
use fastcdc::v2020::StreamCDC;
use ring::digest::{Context, SHA256};
use tar::{Builder, EntryType, Header};
use tumpel_api::types::{
    ChannelChunkPlacement, SnapshotDirectoryEntry, SnapshotDirectoryEntryKind, SnapshotEntry, SnapshotFileEntry,
    SnapshotFileEntryKind, SnapshotManifest, SnapshotOpaqueDirectoryEntry, SnapshotOpaqueDirectoryEntryKind,
    SnapshotRoot, SnapshotSymlinkEntry, SnapshotSymlinkEntryKind, SnapshotWhiteoutEntry, SnapshotWhiteoutEntryKind,
};

use cache::sha256;
pub use cache::{hex, ChunkCache};
use opaque::open_drive;

pub const MIN_CHUNK_BYTES: usize = 16 * 1024;
pub const AVERAGE_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_CHUNK_BYTES: usize = 256 * 1024;
pub const PACK_BYTES: u64 = 8 * 1024 * 1024;

const UPPER_ON_DRIVE: &str = "/upper";
const OPAQUE_XATTR: &str = "SCHILY.xattr.trusted.overlay.opaque";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    File { size: u64 },
    Dir,
    OpaqueDir,
    Symlink { target: Vec<u8> },
    Whiteout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub kind: Kind,
    pub mode: u32,
    pub uid: u64,
    pub gid: u64,
    pub mtime: u64,
    pub mtime_nanos: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Born {
    pub seconds: i64,
    pub nanos: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub entry: Entry,
    pub born: Born,
}

pub type Scan = BTreeMap<Vec<u8>, Found>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Place {
    pub digest: String,
    pub pack: String,
    pub offset: u64,
    pub length: u32,
}

pub type Places = BTreeMap<String, Place>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored {
    pub entry: Entry,
    pub chunks: Vec<Place>,
}

pub type Manifest = BTreeMap<Vec<u8>, Stored>;

pub type Births = BTreeMap<Vec<u8>, Born>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Base {
    pub manifest: Manifest,
    pub root: Vec<Place>,
    pub births: Births,
}

impl Base {
    #[must_use]
    pub fn placements(&self) -> usize {
        self.manifest.values().map(|stored| stored.chunks.len()).sum()
    }

    #[must_use]
    pub fn places(&self) -> Places {
        self.manifest
            .values()
            .flat_map(|stored| stored.chunks.iter())
            .chain(self.root.iter())
            .map(|place| (place.digest.clone(), place.clone()))
            .collect()
    }
}

pub type Draft = BTreeMap<Vec<u8>, (Entry, Vec<String>)>;

pub struct Cut {
    pub draft: Draft,
    pub births: Births,
}

pub struct PackFile {
    pub digest: String,
    pub path: PathBuf,
    pub size: u64,
    pub chunks: Vec<Place>,
}

pub trait Upper {
    fn scan(&self) -> Result<Scan, String>;
    fn open(&self, path: &[u8]) -> Result<Box<dyn Read + '_>, String>;
}

fn shown(path: &[u8]) -> String {
    String::from_utf8_lossy(path).into_owned()
}

pub fn chunks<R: Read>(reader: R) -> impl Iterator<Item = Result<Vec<u8>, String>> {
    StreamCDC::new(reader, MIN_CHUNK_BYTES, AVERAGE_CHUNK_BYTES, MAX_CHUNK_BYTES).map(|chunk| {
        chunk
            .map(|chunk| chunk.data)
            .map_err(|error| format!("cutting chunks: {error}"))
    })
}

fn store_file(path: &[u8], size: u64, content: &mut dyn Read, cache: &ChunkCache) -> Result<Vec<String>, String> {
    let mut digests = Vec::new();
    let mut read = 0u64;
    for chunk in chunks(content) {
        let chunk = chunk.map_err(|error| format!("reading {}: {error}", shown(path)))?;
        read += chunk.len() as u64;
        digests.push(cache.put(&chunk)?);
    }
    if read != size {
        return Err(format!(
            "read {read} bytes of {}, and its inode says {size}; the drive changed while the box was paused",
            shown(path)
        ));
    }
    Ok(digests)
}

fn reusable(found: &Found, path: &[u8], base: &Base) -> Option<Vec<String>> {
    let stored = base.manifest.get(path)?;
    let unchanged = stored.entry == found.entry && base.births.get(path) == Some(&found.born);
    unchanged.then(|| stored.chunks.iter().map(|place| place.digest.clone()).collect())
}

pub fn cut(upper: &dyn Upper, base: &Base, cache: &ChunkCache) -> Result<Cut, String> {
    let mut draft = Draft::new();
    let mut births = Births::new();
    for (path, found) in upper.scan()? {
        let digests = match (&found.entry.kind, reusable(&found, &path, base)) {
            (Kind::File { .. }, Some(digests)) => digests,
            (Kind::File { size }, None) => store_file(&path, *size, &mut upper.open(&path)?, cache)?,
            _ => Vec::new(),
        };
        births.insert(path.clone(), found.born);
        draft.insert(path, (found.entry, digests));
    }
    Ok(Cut { draft, births })
}

pub fn fresh<'a>(digests: impl IntoIterator<Item = &'a String>, known: &Places) -> Vec<String> {
    let mut seen = BTreeSet::new();
    digests
        .into_iter()
        .filter(|digest| !known.contains_key(*digest) && seen.insert((*digest).clone()))
        .cloned()
        .collect()
}

pub fn draft_digests(draft: &Draft) -> impl Iterator<Item = &String> {
    draft.values().flat_map(|(_, digests)| digests.iter())
}

struct OpenPack {
    file: File,
    path: PathBuf,
    hash: Context,
    size: u64,
    chunks: Vec<(String, u64, u32)>,
}

impl OpenPack {
    fn seal(self) -> Result<PackFile, String> {
        let OpenPack {
            mut file,
            path,
            hash,
            size,
            chunks,
        } = self;
        file.flush()
            .map_err(|error| format!("writing {}: {error}", path.display()))?;
        let digest = hex(hash.finish().as_ref());
        let chunks = chunks
            .into_iter()
            .map(|(chunk, offset, length)| Place {
                digest: chunk,
                pack: digest.clone(),
                offset,
                length,
            })
            .collect();
        Ok(PackFile {
            digest,
            path,
            size,
            chunks,
        })
    }
}

fn open_pack(dir: &Path, index: usize) -> Result<OpenPack, String> {
    let path = dir.join(format!("pack-{index:06}"));
    let file = File::create(&path).map_err(|error| format!("creating {}: {error}", path.display()))?;
    Ok(OpenPack {
        file,
        path,
        hash: Context::new(&SHA256),
        size: 0,
        chunks: Vec::new(),
    })
}

pub fn pack(digests: &[String], cache: &ChunkCache, dir: &Path, packs: &mut Vec<PackFile>) -> Result<(), String> {
    let mut open: Option<OpenPack> = None;
    for digest in digests {
        let file = cache.path(digest);
        let bytes = std::fs::read(&file)
            .map_err(|error| format!("chunk {digest} left the chunk cache {}: {error}", file.display()))?;
        let length = u32::try_from(bytes.len())
            .map_err(|_| format!("chunk {digest} holds {} bytes, more than a pack places", bytes.len()))?;
        let mut current = match open.take() {
            Some(current) if current.size + u64::from(length) > PACK_BYTES => {
                packs.push(current.seal()?);
                open_pack(dir, packs.len())?
            }
            Some(current) => current,
            None => open_pack(dir, packs.len())?,
        };
        current
            .file
            .write_all(&bytes)
            .map_err(|error| format!("writing {}: {error}", current.path.display()))?;
        current.hash.update(&bytes);
        current.chunks.push((digest.clone(), current.size, length));
        current.size += u64::from(length);
        open = Some(current);
    }
    if let Some(last) = open {
        packs.push(last.seal()?);
    }
    Ok(())
}

pub fn seal(draft: Draft, places: &Places) -> Result<Manifest, String> {
    draft
        .into_iter()
        .map(|(path, (entry, digests))| {
            let chunks = digests
                .iter()
                .map(|digest| {
                    places
                        .get(digest)
                        .cloned()
                        .ok_or_else(|| format!("chunk {digest} of {} lies in no pack", shown(&path)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((path, Stored { entry, chunks }))
        })
        .collect()
}

pub fn cut_bytes(bytes: &[u8], cache: &ChunkCache) -> Result<Vec<String>, String> {
    chunks(Cursor::new(bytes))
        .map(|chunk| chunk.and_then(|chunk| cache.put(&chunk)))
        .collect()
}

pub fn assemble(places: &[Place], cache: &ChunkCache) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    for place in places {
        let file = cache.path(&place.digest);
        let mut chunk =
            File::open(&file).map_err(|error| format!("chunk {} is not in the chunk cache: {error}", place.digest))?;
        chunk
            .read_to_end(&mut bytes)
            .map_err(|error| format!("reading {}: {error}", file.display()))?;
    }
    Ok(bytes)
}

pub struct Staged {
    pub base: Base,
    pub packs: Vec<PackFile>,
    pub reused: Vec<String>,
    pub root: PathBuf,
}

fn reused(manifest: &Manifest, root: &[Place], packs: &[PackFile]) -> Vec<String> {
    let written: BTreeSet<&str> = packs.iter().map(|pack| pack.digest.as_str()).collect();
    manifest
        .values()
        .flat_map(|stored| stored.chunks.iter())
        .chain(root)
        .map(|place| place.pack.as_str())
        .filter(|pack| !written.contains(pack))
        .collect::<BTreeSet<&str>>()
        .into_iter()
        .map(str::to_string)
        .collect()
}

pub fn pack_parts<'a, P, R>(placed: &'a [P], reused: &'a [R], per_part: usize) -> Vec<(&'a [P], &'a [R])> {
    let count = placed
        .len()
        .div_ceil(per_part)
        .max(reused.len().div_ceil(per_part))
        .max(1);
    let mut placed = placed.chunks(per_part);
    let mut reused = reused.chunks(per_part);
    (0..count)
        .map(|_| (placed.next().unwrap_or_default(), reused.next().unwrap_or_default()))
        .collect()
}

fn placed(places: &mut Places, packs: &[PackFile]) {
    for place in packs.iter().flat_map(|pack| pack.chunks.iter()) {
        places.insert(place.digest.clone(), place.clone());
    }
}

pub fn stage(
    cut: Cut,
    known: Places,
    held: Places,
    new: &[String],
    cache: &ChunkCache,
    dir: &Path,
) -> Result<Staged, String> {
    let mut places = known;
    places.extend(held);
    let missing: Vec<String> = new
        .iter()
        .filter(|digest| !places.contains_key(*digest))
        .cloned()
        .collect();
    let mut packs = Vec::new();
    pack(&missing, cache, dir, &mut packs)?;
    placed(&mut places, &packs);
    let manifest = seal(cut.draft, &places)?;
    let manifest_digests = cut_bytes(&encode(&manifest)?, cache)?;
    let file_packs = packs.len();
    pack(&fresh(manifest_digests.iter(), &places), cache, dir, &mut packs)?;
    placed(&mut places, &packs[file_packs..]);
    let root = manifest_digests
        .iter()
        .map(|digest| {
            places
                .get(digest)
                .cloned()
                .ok_or_else(|| format!("manifest chunk {digest} lies in no pack"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let root_file = dir.join("root.json");
    std::fs::write(&root_file, encode_root(&root)?)
        .map_err(|error| format!("writing {}: {error}", root_file.display()))?;
    Ok(Staged {
        reused: reused(&manifest, &root, &packs),
        base: Base {
            manifest,
            root,
            births: cut.births,
        },
        packs,
        root: root_file,
    })
}

pub fn lacking<'a>(places: impl IntoIterator<Item = &'a Place>, cache: &ChunkCache) -> Result<Vec<Place>, String> {
    let mut unique = Places::new();
    for place in places {
        unique.entry(place.digest.clone()).or_insert_with(|| place.clone());
    }
    let lacking = cache.lacking(unique.keys())?;
    Ok(lacking
        .into_iter()
        .filter_map(|digest| unique.remove(&digest))
        .collect())
}

pub fn restore_parts(wanted: Vec<Place>, per_part: usize) -> Vec<Vec<Place>> {
    let mut by_pack: BTreeMap<String, Vec<Place>> = BTreeMap::new();
    for place in wanted {
        by_pack.entry(place.pack.clone()).or_default().push(place);
    }
    let mut parts = Vec::new();
    let mut current: Vec<Place> = Vec::new();
    for (_, mut group) in by_pack {
        group.sort_by_key(|place| place.offset);
        if !current.is_empty() && current.len() + group.len() > per_part {
            parts.push(std::mem::take(&mut current));
        }
        for piece in group.chunks(per_part) {
            if current.len() + piece.len() > per_part {
                parts.push(std::mem::take(&mut current));
            }
            current.extend_from_slice(piece);
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Span {
    Whole,
    Range { first: u64, last: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetch {
    pub pack: String,
    pub span: Span,
    pub chunks: Vec<Place>,
}

impl Fetch {
    #[must_use]
    pub fn start(&self) -> u64 {
        match self.span {
            Span::Whole => 0,
            Span::Range { first, .. } => first,
        }
    }
}

pub const RANGE_GAP_BYTES: u64 = 256 * 1024;

pub fn fetches(part: &[Place], sizes: &BTreeMap<String, u64>) -> Result<Vec<Fetch>, String> {
    let mut by_pack: BTreeMap<&str, Vec<&Place>> = BTreeMap::new();
    for place in part {
        by_pack.entry(place.pack.as_str()).or_default().push(place);
    }
    let mut fetches = Vec::new();
    for (pack, mut chunks) in by_pack {
        let Some(size) = sizes.get(pack).copied() else {
            let digests: Vec<&str> = chunks.iter().map(|place| place.digest.as_str()).collect();
            return Err(format!(
                "chunks {} lie in pack {pack}, which the control plane holds for no snapshot of the session",
                digests.join(", ")
            ));
        };
        chunks.sort_by_key(|place| place.offset);
        let lacking: u64 = chunks.iter().map(|place| u64::from(place.length)).sum();
        if lacking * 2 >= size {
            fetches.push(Fetch {
                pack: pack.to_string(),
                span: Span::Whole,
                chunks: chunks.into_iter().cloned().collect(),
            });
            continue;
        }
        let mut run: Vec<Place> = Vec::new();
        let mut first = 0u64;
        let mut end = 0u64;
        for place in chunks {
            if run.is_empty() || place.offset > end + RANGE_GAP_BYTES {
                if !run.is_empty() {
                    fetches.push(ranged(pack, std::mem::take(&mut run), first, end));
                }
                first = place.offset;
            }
            end = end.max(place.offset + u64::from(place.length));
            run.push(place.clone());
        }
        if !run.is_empty() {
            fetches.push(ranged(pack, run, first, end));
        }
    }
    Ok(fetches)
}

fn ranged(pack: &str, chunks: Vec<Place>, first: u64, end: u64) -> Fetch {
    Fetch {
        pack: pack.to_string(),
        span: Span::Range { first, last: end - 1 },
        chunks,
    }
}

pub fn unpack(fetch: &Fetch, bytes: &[u8], cache: &ChunkCache) -> Result<(), String> {
    let start = fetch.start();
    for place in &fetch.chunks {
        let slice = place
            .offset
            .checked_sub(start)
            .and_then(|from| usize::try_from(from).ok())
            .and_then(|from| bytes.get(from..from + place.length as usize))
            .ok_or_else(|| {
                format!(
                    "pack {} ends before chunk {} at offset {}; the pack in storage is damaged, so start a new session",
                    fetch.pack, place.digest, place.offset
                )
            })?;
        cache.adopt(&place.digest, slice)?;
    }
    Ok(())
}

fn text(what: &str, bytes: &[u8]) -> Result<String, String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        format!(
            "the box's upper directory holds the {what} {}, which is not UTF-8; a snapshot stores UTF-8 names, so rename it in the box",
            shown(bytes)
        )
    })
}

fn number<T: TryFrom<U>, U: Copy + std::fmt::Display>(path: &[u8], what: &str, value: U) -> Result<T, String> {
    T::try_from(value).map_err(|_| format!("{} has the {what} {value}, which a snapshot cannot hold", shown(path)))
}

fn wire(what: &str, path: &[u8], error: impl std::fmt::Display) -> String {
    format!("{} does not fit SnapshotManifest's {what}: {error}", shown(path))
}

impl Place {
    pub fn to_wire(&self) -> Result<ChannelChunkPlacement, String> {
        let unfit = |what: &str, error: &dyn std::fmt::Display| {
            format!("chunk {} does not fit ChunkPlacement's {what}: {error}", self.digest)
        };
        Ok(ChannelChunkPlacement {
            digest: self
                .digest
                .clone()
                .try_into()
                .map_err(|error| unfit("digest", &error))?,
            length: std::num::NonZeroU32::new(self.length).ok_or_else(|| unfit("length", &"it is empty"))?,
            offset: i64::try_from(self.offset).map_err(|error| unfit("offset", &error))?,
            pack_digest: self
                .pack
                .clone()
                .try_into()
                .map_err(|error| unfit("packDigest", &error))?,
        })
    }

    pub fn from_wire(placement: &ChannelChunkPlacement) -> Result<Self, String> {
        Ok(Self {
            digest: placement.digest.as_str().to_string(),
            pack: placement.pack_digest.as_str().to_string(),
            offset: u64::try_from(placement.offset).map_err(|_| {
                format!(
                    "chunk {} lies at offset {} of pack {}, before its start",
                    *placement.digest, placement.offset, *placement.pack_digest
                )
            })?,
            length: placement.length.get(),
        })
    }
}

fn encode_entry(path: &[u8], stored: &Stored) -> Result<SnapshotEntry, String> {
    let entry = &stored.entry;
    let name = text("path", path)?;
    let mode: i32 = number(path, "mode", entry.mode)?;
    let uid: i64 = number(path, "uid", entry.uid)?;
    let gid: i64 = number(path, "gid", entry.gid)?;
    let mtime_seconds: i64 = number(path, "modification time", entry.mtime)?;
    let mtime_nanos: i32 = number(path, "modification nanoseconds", entry.mtime_nanos)?;
    Ok(match &entry.kind {
        Kind::File { size } => SnapshotFileEntry {
            chunks: stored
                .chunks
                .iter()
                .map(|place| place.to_wire().map_err(|error| wire("chunk placement", path, error)))
                .collect::<Result<_, _>>()?,
            gid,
            kind: SnapshotFileEntryKind::File,
            mode,
            mtime_nanos,
            mtime_seconds,
            path: name.try_into().map_err(|error| wire("path", path, error))?,
            size_bytes: number(path, "size", *size)?,
            uid,
        }
        .into(),
        Kind::Dir => SnapshotDirectoryEntry {
            gid,
            kind: SnapshotDirectoryEntryKind::Directory,
            mode,
            mtime_nanos,
            mtime_seconds,
            path: name.try_into().map_err(|error| wire("path", path, error))?,
            uid,
        }
        .into(),
        Kind::OpaqueDir => SnapshotOpaqueDirectoryEntry {
            gid,
            kind: SnapshotOpaqueDirectoryEntryKind::OpaqueDirectory,
            mode,
            mtime_nanos,
            mtime_seconds,
            path: name.try_into().map_err(|error| wire("path", path, error))?,
            uid,
        }
        .into(),
        Kind::Symlink { target } => SnapshotSymlinkEntry {
            gid,
            kind: SnapshotSymlinkEntryKind::Symlink,
            mode,
            mtime_nanos,
            mtime_seconds,
            path: name.try_into().map_err(|error| wire("path", path, error))?,
            target: text("link target", target)?
                .try_into()
                .map_err(|error| wire("link target", path, error))?,
            uid,
        }
        .into(),
        Kind::Whiteout => SnapshotWhiteoutEntry {
            gid,
            kind: SnapshotWhiteoutEntryKind::Whiteout,
            mode,
            mtime_nanos,
            mtime_seconds,
            path: name.try_into().map_err(|error| wire("path", path, error))?,
            uid,
        }
        .into(),
    })
}

pub fn encode(manifest: &Manifest) -> Result<Vec<u8>, String> {
    let entries = manifest
        .iter()
        .map(|(path, stored)| encode_entry(path, stored))
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_vec(&SnapshotManifest { entries }).map_err(|error| format!("writing the manifest as JSON: {error}"))
}

struct Common<'a> {
    path: &'a str,
    mode: i32,
    uid: i64,
    gid: i64,
    mtime_seconds: i64,
    mtime_nanos: i32,
}

fn decode_entry(entry: &SnapshotEntry) -> Result<(Vec<u8>, Stored), String> {
    let (common, kind, chunks) = match entry {
        SnapshotEntry::Unrecognised(entry) => {
            return Err(format!(
                "the snapshot manifest holds an entry this supervisor does not know: {entry}"
            ))
        }
        SnapshotEntry::FileEntry(file) => (
            Common {
                path: &file.path,
                mode: file.mode,
                uid: file.uid,
                gid: file.gid,
                mtime_seconds: file.mtime_seconds,
                mtime_nanos: file.mtime_nanos,
            },
            Kind::File {
                size: number(file.path.as_bytes(), "size", file.size_bytes)?,
            },
            file.chunks.iter().map(Place::from_wire).collect::<Result<_, _>>()?,
        ),
        SnapshotEntry::DirectoryEntry(dir) => (
            Common {
                path: &dir.path,
                mode: dir.mode,
                uid: dir.uid,
                gid: dir.gid,
                mtime_seconds: dir.mtime_seconds,
                mtime_nanos: dir.mtime_nanos,
            },
            Kind::Dir,
            Vec::new(),
        ),
        SnapshotEntry::OpaqueDirectoryEntry(dir) => (
            Common {
                path: &dir.path,
                mode: dir.mode,
                uid: dir.uid,
                gid: dir.gid,
                mtime_seconds: dir.mtime_seconds,
                mtime_nanos: dir.mtime_nanos,
            },
            Kind::OpaqueDir,
            Vec::new(),
        ),
        SnapshotEntry::SymlinkEntry(link) => (
            Common {
                path: &link.path,
                mode: link.mode,
                uid: link.uid,
                gid: link.gid,
                mtime_seconds: link.mtime_seconds,
                mtime_nanos: link.mtime_nanos,
            },
            Kind::Symlink {
                target: link.target.as_bytes().to_vec(),
            },
            Vec::new(),
        ),
        SnapshotEntry::WhiteoutEntry(whiteout) => (
            Common {
                path: &whiteout.path,
                mode: whiteout.mode,
                uid: whiteout.uid,
                gid: whiteout.gid,
                mtime_seconds: whiteout.mtime_seconds,
                mtime_nanos: whiteout.mtime_nanos,
            },
            Kind::Whiteout,
            Vec::new(),
        ),
    };
    let path = common.path.as_bytes();
    let entry = Entry {
        kind,
        mode: number(path, "mode", common.mode)?,
        uid: number(path, "uid", common.uid)?,
        gid: number(path, "gid", common.gid)?,
        mtime: number(path, "modification time", common.mtime_seconds)?,
        mtime_nanos: number(path, "modification nanoseconds", common.mtime_nanos)?,
    };
    Ok((path.to_vec(), Stored { entry, chunks }))
}

pub fn decode(bytes: &[u8]) -> Result<Manifest, String> {
    let manifest: SnapshotManifest = serde_json::from_slice(bytes)
        .map_err(|error| format!("the manifest does not match SnapshotManifest in supervisor.tsp: {error}"))?;
    let mut decoded = Manifest::new();
    for entry in &manifest.entries {
        let (path, stored) = decode_entry(entry)?;
        if decoded.insert(path.clone(), stored).is_some() {
            return Err(format!("the manifest lists {} twice", shown(&path)));
        }
    }
    Ok(decoded)
}

pub fn encode_root(manifest: &[Place]) -> Result<Vec<u8>, String> {
    let manifest = manifest.iter().map(Place::to_wire).collect::<Result<Vec<_>, _>>()?;
    serde_json::to_vec(&SnapshotRoot { manifest }).map_err(|error| format!("writing the root as JSON: {error}"))
}

pub fn decode_root(bytes: &[u8]) -> Result<Vec<Place>, String> {
    let root: SnapshotRoot = serde_json::from_slice(bytes)
        .map_err(|error| format!("the root does not match SnapshotRoot in supervisor.tsp: {error}"))?;
    root.manifest.iter().map(Place::from_wire).collect()
}

pub fn verified(digest: &str, bytes: &[u8]) -> Result<(), String> {
    let actual = sha256(bytes);
    if actual == digest {
        return Ok(());
    }
    Err(format!(
        "chunk {digest} read back with digest {actual}; the pack in storage is damaged, so start a new session"
    ))
}

fn append<W: Write>(
    builder: &mut Builder<W>,
    name: &[u8],
    entry: &Entry,
    content: &mut dyn Read,
) -> Result<(), String> {
    let mut header = Header::new_gnu();
    header.set_mode(entry.mode);
    header.set_uid(entry.uid);
    header.set_gid(entry.gid);
    header.set_mtime(entry.mtime);
    header.set_size(0);
    let path = Path::new(OsStr::from_bytes(name));
    let mtime = format!("{}.{:09}", entry.mtime, entry.mtime_nanos);
    let mut extensions: Vec<(&str, &[u8])> = Vec::new();
    if entry.mtime_nanos != 0 {
        extensions.push(("mtime", mtime.as_bytes()));
    }
    if entry.kind == Kind::OpaqueDir {
        extensions.push((OPAQUE_XATTR, b"y".as_slice()));
    }
    if !extensions.is_empty() {
        builder
            .append_pax_extensions(extensions)
            .map_err(|error| format!("writing the attributes of {} into the archive: {error}", shown(name)))?;
    }
    let written = match &entry.kind {
        Kind::File { size } => {
            header.set_entry_type(EntryType::Regular);
            header.set_size(*size);
            builder.append_data(&mut header, path, content.take(*size))
        }
        Kind::Dir | Kind::OpaqueDir => {
            header.set_entry_type(EntryType::Directory);
            builder.append_data(&mut header, path, std::io::empty())
        }
        Kind::Symlink { target } => {
            header.set_entry_type(EntryType::Symlink);
            builder.append_link(&mut header, path, Path::new(OsStr::from_bytes(target)))
        }
        Kind::Whiteout => {
            header.set_entry_type(EntryType::Char);
            header
                .set_device_major(0)
                .and_then(|()| header.set_device_minor(0))
                .map_err(|error| format!("marking {} as a whiteout: {error}", shown(name)))?;
            builder.append_data(&mut header, path, std::io::empty())
        }
    };
    written.map_err(|error| format!("writing {} into the archive: {error}", shown(name)))
}

struct ChunkReader<'a> {
    pending: VecDeque<PathBuf>,
    current: Option<File>,
    cancel: &'a AtomicBool,
}

impl Read for ChunkReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if let Some(current) = self.current.as_mut() {
                let read = current.read(buffer)?;
                if read > 0 || buffer.is_empty() {
                    return Ok(read);
                }
                self.current = None;
            }
            let Some(next) = self.pending.pop_front() else {
                return Ok(0);
            };
            if self.cancel.load(Ordering::Relaxed) {
                return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, CANCELLED));
            }
            self.current = Some(File::open(next)?);
        }
    }
}

fn reassembled<'a>(
    path: &[u8],
    size: u64,
    chunks: &[Place],
    cache: &ChunkCache,
    cancel: &'a AtomicBool,
) -> Result<ChunkReader<'a>, String> {
    let mut total = 0u64;
    let mut pending = VecDeque::with_capacity(chunks.len());
    for place in chunks {
        let file = cache.path(&place.digest);
        let held = std::fs::metadata(&file)
            .map_err(|error| {
                format!(
                    "chunk {} of {} is not in the chunk cache: {error}",
                    place.digest,
                    shown(path)
                )
            })?
            .len();
        if held != u64::from(place.length) {
            return Err(format!(
                "chunk {} of {} holds {held} bytes, and its placement says {}; the stored snapshot is damaged",
                place.digest,
                shown(path),
                place.length
            ));
        }
        total += held;
        pending.push_back(file);
    }
    if total != size {
        return Err(format!(
            "the chunks of {} hold {total} bytes, and the manifest says {size}; the stored snapshot is damaged",
            shown(path)
        ));
    }
    Ok(ChunkReader {
        pending,
        current: None,
        cancel,
    })
}

pub const CANCELLED: &str = "the restore was cancelled";

pub fn write_restore<W: Write>(
    manifest: &Manifest,
    cache: &ChunkCache,
    out: W,
    cancel: &AtomicBool,
) -> Result<(), String> {
    let mut builder = Builder::new(out);
    for (path, stored) in manifest {
        if cancel.load(Ordering::Relaxed) {
            return Err(CANCELLED.to_string());
        }
        let mut content: Box<dyn Read> = match stored.entry.kind {
            Kind::File { size } => Box::new(reassembled(path, size, &stored.chunks, cache, cancel)?),
            _ => Box::new(std::io::empty()),
        };
        append(&mut builder, path, &stored.entry, &mut content)?;
    }
    builder
        .into_inner()
        .and_then(|mut out| out.flush())
        .map_err(|error| format!("finishing the restore archive: {error}"))
}

pub struct Ext4Upper {
    fs: Ext4,
    opaque: BTreeSet<Vec<u8>>,
}

impl Ext4Upper {
    pub fn load(drive: &Path) -> Result<Self, String> {
        let (replayed, opaque) = open_drive(drive, UPPER_ON_DRIVE.as_bytes())?;
        let fs = Ext4::load(Box::new(replayed))
            .map_err(|error| format!("reading the overlay drive {} as ext4: {error}", drive.display()))?;
        Ok(Self { fs, opaque })
    }

    fn on_drive(path: &[u8]) -> Vec<u8> {
        if path.is_empty() {
            return UPPER_ON_DRIVE.as_bytes().to_vec();
        }
        [UPPER_ON_DRIVE.as_bytes(), b"/", path].concat()
    }

    fn walk(&self, dir: &[u8], scan: &mut Scan) -> Result<(), String> {
        let on_drive = Self::on_drive(dir);
        let entries = self
            .fs
            .read_dir(on_drive.as_slice())
            .map_err(|error| format!("listing {}: {error}", shown(&on_drive)))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("listing {}: {error}", shown(&on_drive)))?;
            let name = entry.file_name();
            let name = name.as_ref();
            if name == b"." || name == b".." {
                continue;
            }
            let path = if dir.is_empty() {
                name.to_vec()
            } else {
                [dir, b"/", name].concat()
            };
            let metadata = entry
                .metadata()
                .map_err(|error| format!("reading {}: {error}", shown(&path)))?;
            let kind = match metadata.file_type() {
                FileType::Directory if self.opaque.contains(&path) => Kind::OpaqueDir,
                FileType::Directory => Kind::Dir,
                FileType::Regular => Kind::File { size: metadata.len() },
                FileType::Symlink => Kind::Symlink {
                    target: self
                        .fs
                        .read_link(Self::on_drive(&path).as_slice())
                        .map_err(|error| format!("reading the link {}: {error}", shown(&path)))?
                        .as_ref()
                        .to_vec(),
                },
                FileType::CharacterDevice => Kind::Whiteout,
                _ => continue,
            };
            let is_dir = matches!(kind, Kind::Dir | Kind::OpaqueDir);
            let created = metadata.created().ok_or_else(|| {
                format!(
                    "{} carries no birth time; rebuild the guest image so its drive is formatted with 256-byte inodes",
                    shown(&path)
                )
            })?;
            let born = Born {
                seconds: created.seconds(),
                nanos: created.nanoseconds(),
            };
            scan.insert(
                path.clone(),
                Found {
                    entry: Entry {
                        kind,
                        mode: u32::from(metadata.mode()),
                        uid: u64::from(metadata.uid()),
                        gid: u64::from(metadata.gid()),
                        mtime: u64::try_from(metadata.modified().seconds()).unwrap_or(0),
                        mtime_nanos: metadata.modified().nanoseconds(),
                    },
                    born,
                },
            );
            if is_dir {
                self.walk(&path, scan)?;
            }
        }
        Ok(())
    }
}

impl Upper for Ext4Upper {
    fn scan(&self) -> Result<Scan, String> {
        let mut scan = Scan::new();
        self.walk(b"", &mut scan)?;
        Ok(scan)
    }

    fn open(&self, path: &[u8]) -> Result<Box<dyn Read + '_>, String> {
        let on_drive = Self::on_drive(path);
        self.fs
            .open(on_drive.as_slice())
            .map(|file| Box::new(file) as Box<dyn Read>)
            .map_err(|error| format!("opening {}: {error}", shown(path)))
    }
}

mod cache;
mod opaque;

#[cfg(test)]
mod tests;
