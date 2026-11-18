use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::num::NonZeroU32;
use std::path::Path;
use std::time::{Duration, SystemTime};

use ring::digest::{digest, SHA256};
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tumpel_api::types::TurnFailureReason;

use super::{resolve, LayerCache, LayerOrder};
use crate::boot::Holds;
use crate::snapshot::hex;
use crate::store::{Store, UploadError};

fn sha256(bytes: &[u8]) -> String {
    hex(digest(&SHA256, bytes).as_ref())
}

fn cache(name: &str, limit_mib: u32) -> LayerCache {
    let images = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/test-layers")
        .join(name);
    if images.exists() {
        std::fs::remove_dir_all(&images).expect("clear the fixture");
    }
    std::fs::create_dir_all(&images).expect("the image cache is created");
    LayerCache::new(&images, NonZeroU32::new(limit_mib).expect("a non-zero limit"))
}

struct Published {
    layer: Vec<u8>,
    manifest: Vec<u8>,
    order: LayerOrder,
}

fn publish(fill: u8, bytes: usize, harness_version: &str) -> Published {
    let layer = vec![fill; bytes];
    let layer_digest = sha256(&layer);
    let manifest = serde_json::to_vec(&json!({
        "layerDigest": layer_digest,
        "recipeDigest": "d".repeat(64),
        "architecture": "Amd64",
        "harness": {
            "harness": "Shell", "harnessVersion": harness_version, "protocol": "ShellLines",
            "command": ["/bin/sh", "-s"], "workingDirectory": "/root",
            "environment": { "PATH": "/usr/bin:/bin", "HOME": "/root", "LANG": "C.UTF-8" }, "fixedVariables": [], "cloneSeconds": 120
        }
    }))
    .expect("manifest");
    let order = LayerOrder {
        layer: serde_json::from_value(json!({
            "harness": "Shell",
            "harnessVersion": harness_version,
            "architecture": "Amd64",
            "layerDigest": layer_digest,
            "manifestDigest": sha256(&manifest),
            "sizeBytes": bytes
        }))
        .expect("release layer"),
        layer_url: format!("https://r2.example.test/layers/{layer_digest}"),
        manifest_url: format!("https://r2.example.test/layers/{layer_digest}.json"),
        fetch_seconds: NonZeroU32::new(60).expect("a non-zero budget"),
    };
    Published { layer, manifest, order }
}

#[derive(Default)]
struct Bucket(HashMap<String, Vec<u8>>);

impl Bucket {
    fn with(mut self, published: &Published) -> Self {
        self.0
            .insert(published.order.layer_url.clone(), published.layer.clone());
        self.0
            .insert(published.order.manifest_url.clone(), published.manifest.clone());
        self
    }
}

impl Store for Bucket {
    fn upload(&self, url: &str, _: &Path) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(Err(UploadError::Rejected(format!("PUT {url} is not served"))))
    }

    fn upload_checked(&self, url: &str, _: &Path, _: &str) -> impl Future<Output = Result<(), UploadError>> + Send {
        std::future::ready(Err(UploadError::Rejected(format!("PUT {url} is not served"))))
    }

    fn download(&self, url: &str, file: &Path) -> impl Future<Output = Result<(), String>> + Send {
        std::future::ready(match self.0.get(url) {
            Some(bytes) => std::fs::write(file, bytes).map_err(|error| error.to_string()),
            None => Err(format!("GET {url} answered 404 Not Found")),
        })
    }

    fn download_range(&self, url: &str, _: &Path, _: u64, _: u64) -> impl Future<Output = Result<(), String>> + Send {
        std::future::ready(Err(format!("GET {url} is not served")))
    }
}

fn names(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .expect("the layer cache reads")
        .map(|entry| entry.expect("an entry").file_name().to_string_lossy().to_string())
        .collect()
}

fn files(published: &Published) -> [String; 2] {
    let digest = published.order.digest();
    [format!("{digest}.squashfs"), format!("{digest}.json")]
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(60)
}

fn attached_at(cache: &LayerCache, published: &Published, seconds_ago: u64) {
    std::fs::File::open(cache.layer(published.order.digest()))
        .expect("the layer opens")
        .set_modified(SystemTime::now() - Duration::from_secs(seconds_ago))
        .expect("the attach time is set");
}

#[tokio::test]
async fn a_layer_whose_bytes_miss_the_release_digest_is_never_placed() {
    let cache = cache("wrong-digest", 64);
    let published = publish(1, 1024, "1");
    let mut bucket = Bucket::default().with(&published);
    bucket
        .0
        .insert(published.order.layer_url.clone(), b"tampered layer".to_vec());

    let refusal = resolve(
        &bucket,
        &cache,
        &published.order,
        &Holds::default(),
        &Mutex::new(()),
        deadline(),
    )
    .await
    .expect_err("a wrong digest is refused");

    assert_eq!(refusal.reason, TurnFailureReason::ImageFetchFailed);
    assert!(
        refusal.detail.contains(&format!(
            "fetching harness layer {} for Shell 1 on Amd64",
            published.order.digest()
        )) && refusal.detail.contains("the release object is damaged"),
        "{}",
        refusal.detail
    );
    assert_eq!(names(&cache.dir), BTreeSet::new());
}

#[tokio::test]
async fn a_landed_layer_evicts_the_least_recently_attached_layers_no_box_holds_until_the_cache_fits() {
    let cache = cache("evict", 1);
    let bucket = Bucket::default();
    let idle = publish(1, 300 * 1024, "1");
    let held = publish(2, 300 * 1024, "2");
    let reattached = publish(3, 300 * 1024, "3");
    let newest = publish(4, 300 * 1024, "4");
    let bucket = bucket.with(&idle).with(&held).with(&reattached).with(&newest);
    let lock = Mutex::new(());
    for published in [&idle, &held, &reattached] {
        resolve(&bucket, &cache, &published.order, &Holds::default(), &lock, deadline())
            .await
            .expect("a layer under the limit is placed");
    }
    attached_at(&cache, &held, 300);
    attached_at(&cache, &idle, 200);
    attached_at(&cache, &reattached, 100);
    let attached = resolve(
        &Bucket::default(),
        &cache,
        &reattached.order,
        &Holds::default(),
        &lock,
        deadline(),
    )
    .await
    .expect("a cached layer is attached without a download");
    assert_eq!(attached.path, cache.layer(reattached.order.digest()));
    attached_at(&cache, &idle, 50);
    let holds = Holds::default();
    let _held = holds.hold(vec![held.order.digest().to_string()]);

    let landed = resolve(&bucket, &cache, &newest.order, &holds, &lock, deadline())
        .await
        .expect("the new layer is placed");

    assert_eq!(*landed.manifest.layer_digest, newest.order.digest());
    assert_eq!(std::fs::read(&landed.path).expect("the layer reads"), newest.layer);
    let kept: BTreeSet<String> = [&held, &reattached, &newest].into_iter().flat_map(files).collect();
    assert_eq!(names(&cache.dir), kept);
}

#[tokio::test(start_paused = true)]
async fn waiting_on_another_fetch_counts_against_the_layer_budget() {
    let cache = cache("waiting", 64);
    let published = publish(1, 1024, "1");
    let lock = Mutex::new(());
    let _another = lock.lock().await;
    let started = Instant::now();

    let refusal = resolve(
        &Bucket::default().with(&published),
        &cache,
        &published.order,
        &Holds::default(),
        &lock,
        started + Duration::from_secs(30),
    )
    .await
    .expect_err("the wait outlasts the budget");

    assert_eq!(refusal.reason, TurnFailureReason::ImageFetchTimedOut);
    assert!(refusal.detail.contains(published.order.digest()), "{}", refusal.detail);
    assert_eq!(started.elapsed(), Duration::from_secs(30));
}
