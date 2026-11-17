use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ring::digest::{digest, SHA256};

static STAGED: AtomicU64 = AtomicU64::new(0);

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

pub fn sha256(bytes: &[u8]) -> String {
    hex(digest(&SHA256, bytes).as_ref())
}

pub struct ChunkCache {
    dir: PathBuf,
}

impl ChunkCache {
    #[must_use]
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn create(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|error| format!("creating the chunk cache {}: {error}", self.dir.display()))
    }

    #[must_use]
    pub fn path(&self, digest: &str) -> PathBuf {
        self.dir.join(digest)
    }

    #[must_use]
    pub fn staged(&self, digest: &str) -> PathBuf {
        let unique = STAGED.fetch_add(1, Ordering::Relaxed);
        self.dir.join(format!(".{digest}.{}.{unique}.part", std::process::id()))
    }

    pub fn holds(&self, digest: &str) -> Result<bool, String> {
        let file = self.path(digest);
        match std::fs::read(&file) {
            Ok(bytes) => Ok(sha256(&bytes) == digest),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(format!("reading {}: {error}", file.display())),
        }
    }

    pub fn lacking<'a>(&self, digests: impl IntoIterator<Item = &'a String>) -> Result<Vec<String>, String> {
        let mut lacking = BTreeSet::new();
        for digest in digests {
            if !self.holds(digest)? {
                lacking.insert(digest.clone());
            }
        }
        Ok(lacking.into_iter().collect())
    }

    fn place(&self, digest: &str, staged: &Path) -> Result<(), String> {
        let target = self.path(digest);
        std::fs::rename(staged, &target)
            .map_err(|error| format!("moving {} to {}: {error}", staged.display(), target.display()))
    }

    fn write(&self, digest: &str, bytes: &[u8]) -> Result<(), String> {
        let staged = self.staged(digest);
        std::fs::write(&staged, bytes).map_err(|error| format!("writing {}: {error}", staged.display()))?;
        self.place(digest, &staged)
    }

    pub fn put(&self, bytes: &[u8]) -> Result<String, String> {
        let digest = sha256(bytes);
        let present = self
            .path(&digest)
            .try_exists()
            .map_err(|error| format!("looking up chunk {digest} in {}: {error}", self.dir.display()))?;
        if !present {
            self.write(&digest, bytes)?;
        }
        Ok(digest)
    }

    pub fn adopt(&self, digest: &str, bytes: &[u8]) -> Result<(), String> {
        super::verified(digest, bytes)?;
        self.write(digest, bytes)
    }
}
