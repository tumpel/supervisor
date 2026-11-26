use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;

use tar::Archive;

use super::cache::sha256;
use super::{
    assemble, cut, decode, decode_root, draft_digests, encode, fetches, fresh, lacking, pack_parts, stage, unpack,
    write_restore, Base, Born, ChunkCache, Entry, Ext4Upper, Found, Kind, Manifest, Scan, Span, Staged, Stored, Upper,
};

pub(super) fn sbin(tool: &str) -> PathBuf {
    ["/usr/sbin", "/sbin"]
        .iter()
        .map(|dir| Path::new(dir).join(tool))
        .find(|path| path.exists())
        .unwrap_or_else(|| PathBuf::from(tool))
}

pub(super) fn scratch(name: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/test-images")
        .join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("clear the fixture");
    }
    root
}

pub(super) fn image(name: &str, dirs: &[&str], debugfs: &[&str]) -> PathBuf {
    let root = scratch(name);
    let tree = root.join("tree");
    for dir in dirs {
        std::fs::create_dir_all(tree.join(dir)).expect("fixture tree");
    }
    let drive = root.join("drive.ext4");
    let made = Command::new(sbin("mkfs.ext4"))
        .args(["-q", "-F", "-I", "256", "-d"])
        .arg(&tree)
        .arg(&drive)
        .arg("16M")
        .output()
        .expect("mkfs.ext4 runs");
    assert!(made.status.success(), "{}", String::from_utf8_lossy(&made.stderr));
    for request in debugfs {
        let done = Command::new(sbin("debugfs"))
            .args(["-w", "-R", request])
            .arg(&drive)
            .output()
            .expect("debugfs runs");
        assert!(done.status.success(), "{}", String::from_utf8_lossy(&done.stderr));
    }
    drive
}

struct Host {
    cache: ChunkCache,
    packs: PathBuf,
}

fn host(name: &str) -> Host {
    let root = scratch(name);
    let cache = ChunkCache::new(root.join("chunks"));
    cache.create().expect("cache dir");
    let packs = root.join("packs");
    std::fs::create_dir_all(&packs).expect("pack dir");
    Host { cache, packs }
}

fn noise(length: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

#[derive(Default)]
struct Tree {
    files: BTreeMap<Vec<u8>, (Found, Vec<u8>)>,
    opened: RefCell<Vec<String>>,
}

impl Tree {
    fn file(mut self, path: &str, bytes: &[u8], mtime: u64) -> Self {
        let found = Found {
            entry: Entry {
                kind: Kind::File {
                    size: bytes.len() as u64,
                },
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                mtime,
                mtime_nanos: 7,
            },
            born: Born { seconds: 5, nanos: 3 },
        };
        self.files.insert(path.as_bytes().to_vec(), (found, bytes.to_vec()));
        self
    }

    fn other(mut self, path: &[u8], kind: Kind) -> Self {
        let found = Found {
            entry: Entry {
                kind,
                mode: 0o755,
                uid: 0,
                gid: 0,
                mtime: 1,
                mtime_nanos: 0,
            },
            born: Born { seconds: 5, nanos: 3 },
        };
        self.files.insert(path.to_vec(), (found, Vec::new()));
        self
    }

    fn opened(&self) -> Vec<String> {
        std::mem::take(&mut *self.opened.borrow_mut())
    }
}

impl Upper for Tree {
    fn scan(&self) -> Result<Scan, String> {
        Ok(self
            .files
            .iter()
            .map(|(path, (found, _))| (path.clone(), found.clone()))
            .collect())
    }

    fn open(&self, path: &[u8]) -> Result<Box<dyn Read + '_>, String> {
        self.opened
            .borrow_mut()
            .push(String::from_utf8_lossy(path).into_owned());
        let (_, bytes) = self.files.get(path).ok_or("no such file")?;
        Ok(Box::new(Cursor::new(bytes.clone())))
    }
}

fn workspace() -> Tree {
    Tree::default()
        .other(b"root", Kind::Dir)
        .file("root/notes.md", b"one", 10)
        .file("root/big.bin", &noise(1_000_000, 1), 10)
        .file("root/empty", b"", 10)
        .other(
            b"root/link",
            Kind::Symlink {
                target: b"notes.md".to_vec(),
            },
        )
        .other(b"etc", Kind::Dir)
        .other(b"etc/motd", Kind::Whiteout)
        .other(b"etc/conf", Kind::OpaqueDir)
}

fn snapshot(tree: &Tree, base: &Base, host: &Host) -> (Staged, Vec<String>) {
    let known = base.places();
    let cut = cut(tree, base, &host.cache).expect("cut");
    let new = fresh(draft_digests(&cut.draft), &known);
    let staged = stage(cut, known, &new, &host.cache, &host.packs).expect("stage");
    (staged, new)
}

fn listing(archive: Vec<u8>) -> BTreeMap<String, String> {
    let mut read = BTreeMap::new();
    for entry in Archive::new(Cursor::new(archive)).entries().expect("entries") {
        let mut entry = entry.expect("entry");
        let name = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
        let opaque = entry
            .pax_extensions()
            .expect("extensions")
            .into_iter()
            .flatten()
            .any(|extension| extension.expect("extension").key_bytes() == b"SCHILY.xattr.trusted.overlay.opaque");
        let header = entry.header().clone();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).expect("bytes");
        let kind = match header.entry_type() {
            tar::EntryType::Char => format!(
                "whiteout {}:{}",
                header.device_major().expect("major").expect("major"),
                header.device_minor().expect("minor").expect("minor")
            ),
            tar::EntryType::Directory if opaque => "opaque dir".to_string(),
            tar::EntryType::Directory => "dir".to_string(),
            tar::EntryType::Symlink => format!(
                "link {}",
                String::from_utf8_lossy(&entry.link_name_bytes().expect("target"))
            ),
            _ => format!("file {} {}", bytes.len(), sha256(&bytes)),
        };
        read.insert(name, kind);
    }
    read
}

#[test]
fn a_manifest_round_trips_through_its_json() {
    let host = host("round-trip");
    let (staged, _) = snapshot(&workspace(), &Base::default(), &host);
    let manifest = staged.base.manifest;

    let decoded = decode(&encode(&manifest).expect("encode")).expect("decode");

    assert_eq!(decoded, manifest);
    assert_eq!(decoded[b"root/empty".as_slice()].chunks, Vec::<super::Place>::new());
    assert!(decoded[b"root/big.bin".as_slice()].chunks.len() > 1);
}

#[test]
fn a_restore_reassembles_each_file_from_its_chunks() {
    let host = host("reassemble");
    let tree = workspace();
    let (staged, _) = snapshot(&tree, &Base::default(), &host);
    let mut archive = Vec::new();

    write_restore(
        &staged.base.manifest,
        &host.cache,
        &mut archive,
        &AtomicBool::new(false),
    )
    .expect("restore");

    let file = |bytes: &[u8]| format!("file {} {}", bytes.len(), sha256(bytes));
    assert_eq!(
        listing(archive),
        BTreeMap::from([
            ("etc".to_string(), "dir".to_string()),
            ("etc/conf".to_string(), "opaque dir".to_string()),
            ("etc/motd".to_string(), "whiteout 0:0".to_string()),
            ("root".to_string(), "dir".to_string()),
            (
                "root/big.bin".to_string(),
                file(&tree.files[b"root/big.bin".as_slice()].1)
            ),
            ("root/empty".to_string(), file(b"")),
            ("root/link".to_string(), "link notes.md".to_string()),
            ("root/notes.md".to_string(), file(b"one")),
        ])
    );
}

#[test]
fn a_later_snapshot_reads_only_changed_files_and_names_only_new_chunks() {
    let host = host("fresh");
    let tree = workspace();
    let (first, new) = snapshot(&tree, &Base::default(), &host);
    let mut all: Vec<String> = first
        .base
        .manifest
        .values()
        .flat_map(|stored| stored.chunks.iter().map(|place| place.digest.clone()))
        .collect();
    all.sort();
    all.dedup();
    let mut named = new.clone();
    named.sort();
    assert_eq!(named, all);
    tree.opened();

    let mut big = tree.files[b"root/big.bin".as_slice()].1.clone();
    big[150_000..150_004].copy_from_slice(b"edit");
    big.splice(700_000..700_000, noise(100, 9));
    let tree = tree.file("root/big.bin", &big, 11);
    let (second, new) = snapshot(&tree, &first.base, &host);

    assert_eq!(tree.opened(), ["root/big.bin"]);
    assert!(!new.is_empty() && new.len() <= 6, "{new:?}");
    assert!(new.iter().all(|digest| host.cache.holds(digest).expect("cache")));
    assert_eq!(
        second.base.manifest[b"root/notes.md".as_slice()],
        first.base.manifest[b"root/notes.md".as_slice()]
    );
}

#[test]
fn packed_chunks_read_back_by_offset_whole_or_by_range() {
    let first = host("pack-first");
    let tree = workspace();
    let (staged, _) = snapshot(&tree, &Base::default(), &first);
    let pack_bytes: BTreeMap<String, Vec<u8>> = staged
        .packs
        .iter()
        .map(|pack| {
            let bytes = std::fs::read(&pack.path).expect("pack");
            assert_eq!(sha256(&bytes), pack.digest);
            assert_eq!(bytes.len() as u64, pack.size);
            (pack.digest.clone(), bytes)
        })
        .collect();
    let sizes: BTreeMap<String, u64> = staged
        .packs
        .iter()
        .map(|pack| (pack.digest.clone(), pack.size))
        .collect();
    let root = decode_root(&std::fs::read(&staged.root).expect("root")).expect("root json");

    let second = host("pack-second");
    let big = &staged.base.manifest[b"root/big.bin".as_slice()].chunks;
    for place in &big[1..] {
        let bytes = &pack_bytes[&place.pack];
        let from = usize::try_from(place.offset).expect("offset");
        second
            .cache
            .adopt(&place.digest, &bytes[from..from + place.length as usize])
            .expect("seed");
    }
    let wanted = lacking(
        staged
            .base
            .manifest
            .values()
            .flat_map(|stored| stored.chunks.iter())
            .chain(root.iter()),
        &second.cache,
    )
    .expect("lacking");
    let planned = fetches(&wanted, &sizes).expect("fetches");
    assert!(planned
        .iter()
        .any(|fetch| matches!(fetch.span, Span::Range { .. }) && fetch.chunks == big[..1]));
    for fetch in &planned {
        let bytes = &pack_bytes[&fetch.pack];
        let read = match fetch.span {
            Span::Whole => bytes.as_slice(),
            Span::Range { first, last } => {
                &bytes[usize::try_from(first).expect("first")..=usize::try_from(last).expect("last")]
            }
        };
        unpack(fetch, read, &second.cache).expect("unpack");
    }

    let manifest = decode(&assemble(&root, &second.cache).expect("assemble")).expect("manifest");
    assert_eq!(manifest, staged.base.manifest);
    let mut from_first = Vec::new();
    let mut from_second = Vec::new();
    write_restore(
        &staged.base.manifest,
        &first.cache,
        &mut from_first,
        &AtomicBool::new(false),
    )
    .expect("first restore");
    write_restore(&manifest, &second.cache, &mut from_second, &AtomicBool::new(false)).expect("second restore");
    assert_eq!(listing(from_second), listing(from_first));
}

#[test]
fn a_path_that_is_not_utf8_fails_naming_it() {
    let mut manifest = Manifest::new();
    manifest.insert(
        b"root/\xff".to_vec(),
        Stored {
            entry: Entry {
                kind: Kind::Dir,
                mode: 0o755,
                uid: 0,
                gid: 0,
                mtime: 1,
                mtime_nanos: 0,
            },
            chunks: Vec::new(),
        },
    );
    let error = encode(&manifest).expect_err("refused");
    assert!(
        error.contains("root/\u{fffd}") && error.contains("not UTF-8"),
        "{error}"
    );
}

#[test]
fn an_opaque_directory_on_the_drive_reaches_the_manifest_as_opaque() {
    let drive = image(
        "opaque-dir",
        &["upper/etc/conf", "upper/etc/plain"],
        &["ea_set /upper/etc/conf trusted.overlay.opaque y"],
    );

    let upper = Ext4Upper::load(&drive).expect("load");
    let cut = cut(&upper, &Base::default(), &host("opaque-dir-chunks").cache).expect("cut");

    let kinds: BTreeMap<String, Kind> = cut
        .draft
        .into_iter()
        .map(|(path, (entry, _))| (String::from_utf8_lossy(&path).into_owned(), entry.kind))
        .collect();
    assert_eq!(
        kinds,
        BTreeMap::from([
            ("etc".to_string(), Kind::Dir),
            ("etc/conf".to_string(), Kind::OpaqueDir),
            ("etc/plain".to_string(), Kind::Dir),
        ])
    );
}

#[test]
fn a_cancelled_restore_stops_before_it_archives_an_entry() {
    let host = host("cancelled-restore");
    let tree = workspace();
    let (staged, _) = snapshot(&tree, &Base::default(), &host);
    let mut archive = Vec::new();

    let restored = write_restore(&staged.base.manifest, &host.cache, &mut archive, &AtomicBool::new(true));

    assert_eq!(restored, Err(super::CANCELLED.to_string()));
    assert_eq!(listing(archive), BTreeMap::new());
}

#[test]
fn pack_parts_send_each_reused_pack_in_exactly_one_part_and_always_send_part_one() {
    let placed: Vec<u32> = (0..5).collect();
    let reused: Vec<String> = (0..7).map(|pack| format!("pack-{pack}")).collect();
    let parts = pack_parts(&placed, &reused, 3);
    assert_eq!(
        parts
            .iter()
            .map(|(placed, reused)| (placed.len(), reused.len()))
            .collect::<Vec<_>>(),
        [(3, 3), (2, 3), (0, 1)]
    );
    let sent: Vec<&String> = parts.iter().flat_map(|(_, reused)| reused.iter()).collect();
    assert_eq!(sent, reused.iter().collect::<Vec<_>>());
    assert_eq!(
        parts
            .iter()
            .flat_map(|(placed, _)| placed.iter())
            .copied()
            .collect::<Vec<_>>(),
        placed
    );

    let none: [u32; 0] = [];
    let unused: [String; 0] = [];
    assert_eq!(pack_parts(&none, &unused, 500).len(), 1);
}
