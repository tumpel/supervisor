use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::{timeout_at, Instant};
use tumpel_api::types::{HarnessLayerManifest, ReleaseHarnessLayer, TurnFailureReason};

use crate::boot::Holds;
use crate::images::{is_digest, remove, supersede, verify, Refusal};
use crate::store::Store;

#[derive(Clone, Debug)]
pub struct LayerOrder {
    pub layer: ReleaseHarnessLayer,
    pub layer_url: String,
    pub manifest_url: String,
    pub fetch_seconds: NonZeroU32,
}

impl LayerOrder {
    pub(crate) fn digest(&self) -> &str {
        &self.layer.layer_digest
    }

    fn named(&self) -> String {
        format!(
            "harness layer {} for {} {} on {}",
            self.digest(),
            self.layer.harness,
            *self.layer.harness_version,
            self.layer.architecture
        )
    }

    fn failed(&self, detail: impl std::fmt::Display) -> Refusal {
        Refusal {
            reason: TurnFailureReason::ImageFetchFailed,
            detail: format!("fetching {}: {detail}", self.named()),
        }
    }

    fn timed_out(&self) -> Refusal {
        Refusal {
            reason: TurnFailureReason::ImageFetchTimedOut,
            detail: format!(
                "{} was not fetched within the offer's fetchSeconds {} s, counting the wait for another fetch on this host; check the host's bandwidth to the release bucket or raise turnImageFetchBudgetSeconds in the control plane settings",
                self.named(),
                self.fetch_seconds.get()
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LayerCache {
    pub images: PathBuf,
    pub dir: PathBuf,
    pub limit_mib: NonZeroU32,
}

impl LayerCache {
    #[must_use]
    pub fn new(image_cache: &Path, limit_mib: NonZeroU32) -> Self {
        Self {
            images: image_cache.to_path_buf(),
            dir: image_cache.join("layers"),
            limit_mib,
        }
    }

    fn layer(&self, digest: &str) -> PathBuf {
        self.dir.join(format!("{digest}.squashfs"))
    }

    fn manifest(&self, digest: &str) -> PathBuf {
        self.dir.join(format!("{digest}.json"))
    }

    fn staged(&self, name: &str) -> PathBuf {
        self.dir.join(format!(".{name}.part"))
    }

    fn limit_bytes(&self) -> u64 {
        u64::from(self.limit_mib.get()) * 1024 * 1024
    }
}

#[derive(Debug)]
pub struct Layer {
    pub path: PathBuf,
    pub manifest: HarnessLayerManifest,
}

struct Staged {
    layer: PathBuf,
    manifest: PathBuf,
}

impl Staged {
    fn remove(&self) {
        for path in [&self.layer, &self.manifest] {
            if let Err(error) = remove(path) {
                eprintln!("tumpel-supervisor: {error}");
            }
        }
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        self.remove();
    }
}

fn broken(detail: String) -> Refusal {
    Refusal {
        reason: TurnFailureReason::BoxBootFailed,
        detail,
    }
}

pub fn cached(cache: &LayerCache, digest: &str) -> Result<Option<Layer>, Refusal> {
    let manifest_path = cache.manifest(digest);
    let text = match std::fs::read_to_string(&manifest_path) {
        Ok(text) => text,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(broken(format!(
                "reading {}: {error}; set imageCacheDir in runtime.json",
                manifest_path.display()
            )))
        }
    };
    let manifest: HarnessLayerManifest = serde_json::from_str(&text).map_err(|error| {
        broken(format!(
            "{} does not match HarnessLayerManifest in release.tsp: {error}; delete it and the supervisor fetches the layer again",
            manifest_path.display()
        ))
    })?;
    if *manifest.layer_digest != digest {
        return Err(broken(format!(
            "{} names layer {}, not {digest}; delete it and the supervisor fetches the layer the release names",
            manifest_path.display(),
            *manifest.layer_digest
        )));
    }
    let path = cache.layer(digest);
    std::fs::File::open(&path)
        .and_then(|file| file.set_modified(SystemTime::now()))
        .map_err(|error| {
            broken(format!(
                "marking {} attached: {error}; delete {} and the supervisor fetches the layer again",
                path.display(),
                manifest_path.display()
            ))
        })?;
    Ok(Some(Layer { path, manifest }))
}

fn read_manifest(order: &LayerOrder, staged: &Staged) -> Result<HarnessLayerManifest, String> {
    let text = std::fs::read_to_string(&staged.manifest)
        .map_err(|error| format!("reading {}: {error}", staged.manifest.display()))?;
    let manifest: HarnessLayerManifest = serde_json::from_str(&text).map_err(|error| {
        format!("the downloaded manifest does not match HarnessLayerManifest in release.tsp: {error}; publish the release again with npm run release:publish")
    })?;
    let layer = &order.layer;
    let harness = layer.harness.to_string();
    let names = (
        manifest.layer_digest.as_str(),
        manifest.architecture.clone(),
        manifest.harness.harness.as_str(),
        manifest.harness.harness_version.as_str(),
    );
    let wanted = (
        layer.layer_digest.as_str(),
        layer.architecture.clone(),
        harness.as_str(),
        layer.harness_version.as_str(),
    );
    if names == wanted {
        return Ok(manifest);
    }
    Err(format!(
        "the downloaded manifest names layer {} on {} for {} {}, the release names layer {} on {} for {} {}; fix the release manifest and publish it again with npm run release:publish",
        names.0, names.1, names.2, names.3, wanted.0, wanted.1, wanted.2, wanted.3
    ))
}

struct Cached {
    digest: String,
    bytes: u64,
    attached: SystemTime,
}

fn listed(cache: &LayerCache) -> Result<Vec<Cached>, String> {
    let dir = &cache.dir;
    let entries = std::fs::read_dir(dir).map_err(|error| format!("reading {}: {error}", dir.display()))?;
    let mut layers = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("reading {}: {error}", dir.display()))?;
        let name = entry.file_name();
        let Some(digest) = name
            .to_str()
            .and_then(|name| name.strip_suffix(".squashfs"))
            .filter(|digest| is_digest(digest))
        else {
            continue;
        };
        let layer = entry
            .metadata()
            .map_err(|error| format!("reading {}: {error}", entry.path().display()))?;
        let manifest = cache.manifest(digest);
        let manifest_bytes = match std::fs::metadata(&manifest) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == ErrorKind::NotFound => 0,
            Err(error) => return Err(format!("reading {}: {error}", manifest.display())),
        };
        let attached = layer
            .modified()
            .map_err(|error| format!("reading {}: {error}", entry.path().display()))?;
        layers.push(Cached {
            digest: digest.to_string(),
            bytes: layer.len() + manifest_bytes,
            attached,
        });
    }
    Ok(layers)
}

fn evict(cache: &LayerCache, landed: &str, held: &BTreeSet<String>) -> Result<(), String> {
    let mut layers = listed(cache)?;
    let mut total: u64 = layers.iter().map(|layer| layer.bytes).sum();
    layers.retain(|layer| layer.digest != landed && !held.contains(&layer.digest));
    layers.sort_by_key(|layer| layer.attached);
    for layer in layers {
        if total <= cache.limit_bytes() {
            break;
        }
        remove(&cache.manifest(&layer.digest))?;
        remove(&cache.layer(&layer.digest))?;
        total = total.saturating_sub(layer.bytes);
    }
    Ok(())
}

fn place(cache: &LayerCache, order: &LayerOrder, staged: &Staged, holds: &Holds) -> Result<Layer, String> {
    let held = holds.held();
    let digest = order.digest();
    verify(&staged.layer, "harness layer", digest)?;
    verify(&staged.manifest, "harness layer manifest", &order.layer.manifest_digest)?;
    let manifest = read_manifest(order, staged)?;
    let path = cache.layer(digest);
    std::fs::rename(&staged.layer, &path)
        .map_err(|error| format!("moving {} into place: {error}", staged.layer.display()))?;
    std::fs::rename(&staged.manifest, cache.manifest(digest))
        .map_err(|error| format!("moving {} into place: {error}", staged.manifest.display()))?;
    evict(cache, digest, &held)?;
    supersede(&cache.images, &manifest.harness.harness, &held)?;
    Ok(Layer { path, manifest })
}

fn adopt(cache: &LayerCache, order: &LayerOrder, staged: &Staged, holds: &Holds) -> Result<Layer, Refusal> {
    place(cache, order, staged, holds).map_err(|detail| {
        staged.remove();
        order.failed(detail)
    })
}

async fn fetch(
    store: &impl Store,
    cache: &LayerCache,
    order: &LayerOrder,
    holds: &Holds,
    deadline: Instant,
    fetching: OwnedMutexGuard<()>,
) -> Result<Layer, Refusal> {
    std::fs::create_dir_all(&cache.dir)
        .map_err(|error| order.failed(format!("creating the layer cache {}: {error}", cache.dir.display())))?;
    let digest = order.digest();
    let staged = Staged {
        layer: cache.staged(&format!("{digest}.squashfs")),
        manifest: cache.staged(&format!("{digest}.json")),
    };
    let downloads = futures_util::future::try_join(
        store.download(&order.layer_url, &staged.layer),
        store.download(&order.manifest_url, &staged.manifest),
    );
    match timeout_at(deadline, downloads).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            return Err(order.failed(format!("downloading it failed: {error}")));
        }
        Err(_) => {
            return Err(order.timed_out());
        }
    }
    let adopting = (cache.clone(), order.clone(), holds.clone());
    match timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || {
            let _fetching = fetching;
            let (cache, order, holds) = adopting;
            let adopted = adopt(&cache, &order, &staged, &holds);
            drop(staged);
            adopted
        }),
    )
    .await
    {
        Ok(Ok(adopted)) => adopted,
        Ok(Err(error)) => Err(order.failed(format!("verifying it stopped: {error}; restart the supervisor"))),
        Err(_) => Err(order.timed_out()),
    }
}

pub async fn resolve(
    store: &impl Store,
    cache: &LayerCache,
    order: &LayerOrder,
    holds: &Holds,
    lock: &Arc<Mutex<()>>,
    deadline: Instant,
) -> Result<Layer, Refusal> {
    if let Some(layer) = cached(cache, order.digest())? {
        return Ok(layer);
    }
    let Ok(fetching) = timeout_at(deadline, Arc::clone(lock).lock_owned()).await else {
        return Err(order.timed_out());
    };
    if let Some(layer) = cached(cache, order.digest())? {
        return Ok(layer);
    }
    fetch(store, cache, order, holds, deadline, fetching).await
}

#[cfg(test)]
mod tests;
