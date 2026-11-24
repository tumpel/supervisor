use std::cell::Cell;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use ring::digest::{digest, SHA256};
use serde_json::json;
use tokio::sync::Mutex;

use super::{resolve, Boot, BootOrder, Caches, Holds};
use crate::layers::LayerCache;
use crate::snapshot::hex;
use crate::store::{DownloadError, Store, UploadError};

fn sha256(bytes: &[u8]) -> String {
    hex(digest(&SHA256, bytes).as_ref())
}

fn images(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/test-boot")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("clear the fixture");
    }
    std::fs::create_dir_all(&dir).expect("the image cache is created");
    dir
}

const KERNEL: &[u8] = b"kernel bytes";
const LAYER_BYTES: usize = 600 * 1024;

#[derive(Default)]
struct Bucket(HashMap<String, Vec<u8>>);

impl Store for Bucket {
    fn upload(&self, url: &str, _: &Path) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(Err(UploadError::Rejected(format!("PUT {url} is not served"))))
    }

    fn upload_checked(&self, url: &str, _: &Path, _: &str) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(Err(UploadError::Rejected(format!("PUT {url} is not served"))))
    }

    fn download(&self, url: &str, file: &Path) -> impl Future<Output = Result<(), DownloadError>> + Send {
        std::future::ready(match self.0.get(url) {
            Some(bytes) => std::fs::write(file, bytes).map_err(|error| DownloadError::Refused(error.to_string())),
            None => Err(DownloadError::Refused(format!("GET {url} answered 404 Not Found"))),
        })
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

struct Release {
    base: String,
    layer: String,
    order: BootOrder,
}

fn publish(bucket: &mut Bucket, base: &str, fill: u8) -> Release {
    let rootfs = base.as_bytes().to_vec();
    let base_digest = sha256(&rootfs);
    let base_manifest = serde_json::to_vec(&json!({
        "imageDigest": base_digest,
        "kernelUrl": "https://example.com/vmlinux",
        "kernelDigest": sha256(KERNEL),
        "baseRootfsUrl": "https://example.com/rootfs.squashfs",
        "baseRootfsDigest": "c".repeat(64),
        "baseLayoutVersion": 1,
        "components": []
    }))
    .expect("base manifest");
    let layer = vec![fill; LAYER_BYTES];
    let layer_digest = sha256(&layer);
    let layer_manifest = serde_json::to_vec(&json!({
        "layerDigest": layer_digest,
        "recipeDigest": "d".repeat(64),
        "architecture": "Amd64",
        "harness": {
            "harness": "Shell", "harnessVersion": "1", "protocol": "ShellLines",
            "command": ["/bin/sh", "-s"], "workingDirectory": "/root",
            "environment": { "PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8" }, "fixedVariables": [], "cloneSeconds": 120
        }
    }))
    .expect("layer manifest");
    let url = |name: &str| format!("https://r2.example.test/{name}");
    let image = serde_json::from_value(json!({
        "kind": "Layered",
        "base": {
            "imageDigest": base_digest,
            "kernelDigest": sha256(KERNEL),
            "manifestDigest": sha256(&base_manifest),
            "imageUrl": url(&base_digest),
            "kernelUrl": url("kernel"),
            "manifestUrl": url(&format!("{base_digest}.json"))
        },
        "layer": {
            "layer": {
                "harness": "Shell",
                "harnessVersion": "1",
                "architecture": "Amd64",
                "layerDigest": layer_digest,
                "manifestDigest": sha256(&layer_manifest),
                "sizeBytes": LAYER_BYTES
            },
            "layerUrl": url(&layer_digest),
            "manifestUrl": url(&format!("{layer_digest}.json"))
        },
        "downloadExpiresAt": "2026-11-16T12:00:00+01:00",
        "fetchSeconds": 60
    }))
    .expect("a layered boot");
    bucket.0.insert(url(&base_digest), rootfs);
    bucket.0.insert(url("kernel"), KERNEL.to_vec());
    bucket.0.insert(url(&format!("{base_digest}.json")), base_manifest);
    bucket.0.insert(url(&layer_digest), layer);
    bucket.0.insert(url(&format!("{layer_digest}.json")), layer_manifest);
    Release {
        base: base_digest,
        layer: layer_digest,
        order: BootOrder::from_offer(image, "Shell".to_string(), "1".to_string()).expect("a known image"),
    }
}

fn names(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .expect("the cache reads")
        .map(|entry| entry.expect("an entry").file_name().to_string_lossy().to_string())
        .collect()
}

#[tokio::test]
async fn a_layered_boot_fetches_base_kernel_and_layer_and_holds_them_while_its_box_runs() {
    let images = images("layered");
    let mut bucket = Bucket::default();
    let first = publish(&mut bucket, "first base", 1);
    let second = publish(&mut bucket, "second base", 2);
    let third = publish(&mut bucket, "third base", 3);
    let layers = LayerCache::new(&images, NonZeroU32::MIN);
    let holds = Holds::default();
    let lock = Mutex::new(());
    let caches = Caches {
        store: &bucket,
        images: &images,
        layers: &layers,
        holds: &holds,
        lock: &lock,
    };
    let fetched = Cell::new(Vec::new());
    let note = |digest: tumpel_api::types::Sha256Digest| {
        let mut seen = fetched.take();
        seen.push(digest.to_string());
        fetched.set(seen);
    };

    let (boot, running) = resolve(&caches, &first.order, note).await.expect("the first box boots");
    let Boot::Layered(layered) = boot else {
        panic!("a layered offer boots layered");
    };
    assert_eq!(*layered.base.image_digest, first.base);
    assert_eq!(
        layered.layer.path,
        images.join(format!("layers/{}.squashfs", first.layer))
    );
    assert_eq!(fetched.take(), vec![first.base.clone()]);
    let (_, again) = resolve(&caches, &first.order, note).await.expect("a cached boot");
    assert!(
        fetched.take().is_empty(),
        "a cached base and layer send no TurnImageFetching"
    );
    drop(again);

    let (_, stopped) = resolve(&caches, &second.order, note)
        .await
        .expect("the second box boots");
    drop(stopped);
    let on_disk = names(&images);
    assert!(on_disk.contains(&format!("{}.ext4", first.base)), "{on_disk:?}");
    assert!(on_disk.contains(&format!("{}.vmlinux", sha256(KERNEL))), "{on_disk:?}");
    let layered = names(&images.join("layers"));
    assert!(layered.contains(&format!("{}.squashfs", first.layer)), "{layered:?}");

    drop(running);
    let (_, last) = resolve(&caches, &third.order, note).await.expect("the third box boots");
    let on_disk = names(&images);
    assert!(!on_disk.contains(&format!("{}.ext4", first.base)), "{on_disk:?}");
    assert!(!on_disk.contains(&format!("{}.ext4", second.base)), "{on_disk:?}");
    assert!(on_disk.contains(&format!("{}.ext4", third.base)), "{on_disk:?}");
    let layered = names(&images.join("layers"));
    assert!(!layered.contains(&format!("{}.squashfs", first.layer)), "{layered:?}");
    assert!(layered.contains(&format!("{}.squashfs", third.layer)), "{layered:?}");
    drop(last);
    assert!(holds.held().is_empty());
}
