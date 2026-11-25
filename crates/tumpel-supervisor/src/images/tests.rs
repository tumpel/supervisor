use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ring::digest::{digest, SHA256};
use serde_json::json;
use tumpel_api::types::TurnFailureReason;

use super::{adopt, cached, resolve, stage, ImageOrder};
use crate::boot::Holds;
use crate::snapshot::hex;
use crate::store::{DownloadError, Store, UploadError};

fn sha256(bytes: &[u8]) -> String {
    hex(digest(&SHA256, bytes).as_ref())
}

fn cache(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/test-images")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("clear the fixture");
    }
    std::fs::create_dir_all(&dir).expect("the image cache is created");
    dir
}

struct Image {
    rootfs: Vec<u8>,
    manifest: Vec<u8>,
    digest: String,
}

const KERNEL: &[u8] = b"kernel bytes";

fn image(rootfs: &str, harness_version: &str) -> Image {
    let rootfs = rootfs.as_bytes().to_vec();
    let digest = sha256(&rootfs);
    let manifest = serde_json::to_vec(&json!({
        "imageDigest": digest,
        "kernelUrl": "https://example.com/vmlinux",
        "kernelDigest": sha256(KERNEL),
        "baseRootfsUrl": "https://example.com/rootfs.squashfs",
        "baseRootfsDigest": "c".repeat(64),
        "harness": {
            "harness": "Shell", "harnessVersion": harness_version, "protocol": "ShellLines",
            "command": ["/bin/sh", "-s"], "workingDirectory": "/root",
            "environment": { "PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8" }, "fixedVariables": [], "cloneSeconds": 120
        },
        "components": []
    }))
    .expect("manifest");
    Image {
        rootfs,
        manifest,
        digest,
    }
}

fn put(cache: &Path, image: &Image) {
    std::fs::write(cache.join(format!("{}.ext4", image.digest)), &image.rootfs).expect("rootfs");
    std::fs::write(cache.join(format!("{}.json", image.digest)), &image.manifest).expect("manifest");
}

fn order(image: &Image, harness_version: &str) -> ImageOrder {
    order_within(image, harness_version, 60)
}

fn order_within(image: &Image, harness_version: &str, fetch_seconds: u32) -> ImageOrder {
    ImageOrder {
        download: serde_json::from_value(json!({
            "kind": "Full",
            "imageDigest": image.digest,
            "kernelDigest": sha256(KERNEL),
            "manifestDigest": sha256(&image.manifest),
            "imageUrl": "https://r2.example.test/image",
            "kernelUrl": "https://r2.example.test/kernel",
            "manifestUrl": "https://r2.example.test/manifest",
            "downloadExpiresAt": "2026-11-14T12:00:00+01:00",
            "fetchSeconds": fetch_seconds
        }))
        .expect("download"),
        harness: "Shell".to_string(),
        harness_version: harness_version.to_string(),
    }
}

fn listing(cache: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(cache)
        .expect("the cache reads")
        .map(|entry| {
            let path = entry.expect("an entry").path();
            let name = path.file_name().expect("a name").to_string_lossy().to_string();
            (name, std::fs::read(&path).expect("the file reads"))
        })
        .collect()
}

#[test]
fn a_fetched_image_whose_bytes_miss_the_digest_leaves_the_cache_untouched() {
    let cache = cache("wrong-digest");
    let old = image("old rootfs", "1");
    put(&cache, &old);
    let before = listing(&cache);
    let new = image("new rootfs", "1");
    let order = order(&new, "1");
    let staged = stage(&cache, &order).expect("staged");
    std::fs::write(&staged.image, b"tampered rootfs").expect("image");
    if let Some(kernel) = &staged.kernel {
        std::fs::write(kernel, KERNEL).expect("kernel");
    }
    std::fs::write(&staged.manifest, &new.manifest).expect("manifest");

    let refusal = adopt(&cache, &order, &staged, &Holds::default()).expect_err("a wrong digest is refused");

    assert_eq!(refusal.reason, TurnFailureReason::ImageFetchFailed);
    assert!(
        refusal
            .detail
            .contains(&format!("fetching guest image {} for Shell 1", new.digest))
            && refusal.detail.contains(&format!("the offer names {}", new.digest)),
        "{}",
        refusal.detail
    );
    assert_eq!(listing(&cache), before);
}

#[test]
fn a_verified_image_replaces_every_unheld_image_of_its_harness_and_whatever_no_valid_manifest_names() {
    let cache = cache("replace");
    let old = image("old rootfs", "1");
    let other_version = image("other version rootfs", "2");
    let running = image("running rootfs", "3");
    put(&cache, &old);
    put(&cache, &other_version);
    put(&cache, &running);
    let base = sha256(b"base rootfs");
    let base_kernel = sha256(b"base kernel");
    std::fs::write(cache.join(format!("{base}.ext4")), b"base rootfs").expect("base image");
    std::fs::write(cache.join(format!("{base_kernel}.vmlinux")), b"base kernel").expect("base kernel");
    std::fs::write(
        cache.join(format!("{base}.json")),
        serde_json::to_vec(&json!({
            "imageDigest": base,
            "kernelUrl": "https://example.com/vmlinux",
            "kernelDigest": base_kernel,
            "baseRootfsUrl": "https://example.com/rootfs.squashfs",
            "baseRootfsDigest": "c".repeat(64),
            "baseLayoutVersion": 1,
            "components": []
        }))
        .expect("base manifest"),
    )
    .expect("base manifest");
    let orphan = sha256(b"an image whose manifest was removed");
    std::fs::write(cache.join(format!("{orphan}.ext4")), b"orphan").expect("orphan image");
    std::fs::write(cache.join(format!("{}.vmlinux", sha256(b"old kernel"))), b"old kernel").expect("old kernel");
    std::fs::write(cache.join(format!(".{orphan}.ext4.part")), b"partial").expect("partial download");
    std::fs::write(cache.join("notes.txt"), b"kept").expect("an unrelated file");
    let stale = sha256(b"an image built before its manifest named an environment");
    std::fs::write(cache.join(format!("{stale}.ext4")), b"stale").expect("stale image");
    std::fs::write(cache.join(format!("{stale}.json")), b"{\"imageDigest\": \"stale\"}").expect("stale manifest");
    let new = image("new rootfs", "1");
    let order = order(&new, "1");
    let staged = stage(&cache, &order).expect("staged");
    assert_eq!(staged.image, cache.join(format!(".{}.ext4.part", new.digest)));
    std::fs::write(&staged.image, &new.rootfs).expect("image");
    let Some(kernel) = &staged.kernel else {
        panic!("the cache lacks the kernel, so it is fetched");
    };
    std::fs::write(kernel, KERNEL).expect("kernel");
    std::fs::write(&staged.manifest, &new.manifest).expect("manifest");

    let holds = Holds::default();
    let _running = holds.hold(vec![running.digest.clone()]);
    let manifest = adopt(&cache, &order, &staged, &holds).expect("a verified image is adopted");

    assert_eq!(*manifest.image_digest, new.digest);
    let names: Vec<String> = listing(&cache).into_keys().collect();
    let mut expected = vec![
        format!("{}.ext4", new.digest),
        format!("{}.json", new.digest),
        format!("{}.ext4", running.digest),
        format!("{}.json", running.digest),
        format!("{}.vmlinux", sha256(KERNEL)),
        format!("{base}.ext4"),
        format!("{base}.json"),
        format!("{base_kernel}.vmlinux"),
        "notes.txt".to_string(),
    ];
    expected.sort();
    assert_eq!(names, expected);
    assert!(cached(&cache, &new.digest).expect("readable").is_some());
    assert!(
        stage(&cache, &order).expect("staged").kernel.is_none(),
        "a cached kernel is not fetched again"
    );
}

struct Unreachable;

impl Store for Unreachable {
    fn upload(&self, url: &str, _: &Path) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(Err(UploadError::Rejected(format!("PUT {url} is not served"))))
    }

    fn upload_checked(&self, url: &str, _: &Path, _: &str) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(Err(UploadError::Rejected(format!("PUT {url} is not served"))))
    }

    fn download(&self, url: &str, _: &Path) -> impl Future<Output = Result<(), DownloadError>> + Send {
        std::future::ready(Err(DownloadError::Refused(format!("GET {url} is not served"))))
    }

    fn download_range(
        &self,
        url: &str,
        _: &Path,
        _: u64,
        _: u64,
    ) -> impl Future<Output = Result<(), DownloadError>> + Send {
        std::future::ready(Err(DownloadError::Refused(format!("GET {url} is not served"))))
    }
}

#[tokio::test(start_paused = true)]
async fn the_fetch_budget_counts_the_wait_for_another_image_from_turn_image_fetching() {
    let cache = cache("waiting");
    let new = image("new rootfs", "1");
    let order = order_within(&new, "1", 30);
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    let _another = lock.lock().await;
    let started = tokio::time::Instant::now();
    let mut sent = None;

    let refusal = resolve(&Unreachable, &cache, &order, &Holds::default(), &lock, || {
        sent = Some(tokio::time::Instant::now());
    })
    .await
    .expect_err("the wait outlasts the budget");

    assert_eq!(refusal.reason, TurnFailureReason::ImageFetchTimedOut);
    assert!(refusal.detail.contains(&new.digest), "{}", refusal.detail);
    assert_eq!(sent, Some(started));
    assert_eq!(started.elapsed(), std::time::Duration::from_secs(30));
}
