use std::collections::{BTreeSet, HashSet};
use std::io::{ErrorKind, Read};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ring::digest::{Context, SHA256};
use tokio::sync::Mutex;
use tokio::time::{timeout_at, Instant};
use tumpel_api::types::{
    BaseImageManifest, ChannelBaseImageDownload, ChannelGuestImageDownload, GuestImageManifest, TurnFailureReason,
};

use crate::boot::Holds;
use crate::snapshot::hex;
use crate::store::Store;

#[derive(Debug)]
pub struct Refusal {
    pub reason: TurnFailureReason,
    pub detail: String,
}

#[derive(Clone, Debug)]
pub struct ImageOrder {
    pub download: ChannelGuestImageDownload,
    pub harness: String,
    pub harness_version: String,
}

impl ImageOrder {
    fn digest(&self) -> &str {
        &self.download.image_digest
    }

    fn failed(&self, detail: impl std::fmt::Display) -> Refusal {
        Refusal {
            reason: TurnFailureReason::ImageFetchFailed,
            detail: format!(
                "fetching guest image {} for {} {}: {detail}",
                self.digest(),
                self.harness,
                self.harness_version
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BaseOrder {
    pub download: ChannelBaseImageDownload,
    pub fetch_seconds: NonZeroU32,
}

impl BaseOrder {
    pub(crate) fn digest(&self) -> &str {
        &self.download.image_digest
    }

    fn failed(&self, detail: impl std::fmt::Display) -> Refusal {
        Refusal {
            reason: TurnFailureReason::ImageFetchFailed,
            detail: format!("fetching base image {}: {detail}", self.digest()),
        }
    }

    fn timed_out(&self) -> Refusal {
        Refusal {
            reason: TurnFailureReason::ImageFetchTimedOut,
            detail: format!(
                "base image {} was not fetched within the offer's fetchSeconds {} s, counting the wait for another fetch on this host; check the host's bandwidth to the release bucket or raise turnImageFetchBudgetSeconds in the control plane settings",
                self.digest(),
                self.fetch_seconds.get()
            ),
        }
    }
}

pub struct Staged {
    pub image: PathBuf,
    pub kernel: Option<PathBuf>,
    pub manifest: PathBuf,
}

impl Staged {
    fn paths(&self) -> impl Iterator<Item = &PathBuf> {
        std::iter::once(&self.image)
            .chain(self.kernel.iter())
            .chain(std::iter::once(&self.manifest))
    }

    fn remove(&self) {
        for path in self.paths() {
            if let Err(error) = std::fs::remove_file(path) {
                if error.kind() != ErrorKind::NotFound {
                    eprintln!(
                        "tumpel-supervisor: removing the partial download {}: {error}",
                        path.display()
                    );
                }
            }
        }
    }
}

fn staged_path(cache: &Path, name: &str) -> PathBuf {
    cache.join(format!(".{name}.part"))
}

fn stage_parts(cache: &Path, image_digest: &str, kernel_digest: &str) -> Result<Staged, String> {
    std::fs::create_dir_all(cache).map_err(|error| format!("creating the image cache {}: {error}", cache.display()))?;
    let kernel = cache.join(format!("{kernel_digest}.vmlinux"));
    let kernel_cached = kernel
        .try_exists()
        .map_err(|error| format!("looking up {}: {error}", kernel.display()))?;
    Ok(Staged {
        image: staged_path(cache, &format!("{image_digest}.ext4")),
        kernel: (!kernel_cached).then(|| staged_path(cache, &format!("{kernel_digest}.vmlinux"))),
        manifest: staged_path(cache, &format!("{image_digest}.json")),
    })
}

pub fn stage(cache: &Path, order: &ImageOrder) -> Result<Staged, Refusal> {
    stage_parts(cache, order.digest(), &order.download.kernel_digest).map_err(|detail| order.failed(detail))
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path).map_err(|error| format!("opening {}: {error}", path.display()))?;
    let mut context = Context::new(&SHA256);
    let mut buffer = vec![0_u8; 1 << 20];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("reading {}: {error}", path.display()))?;
        let Some(bytes) = buffer.get(..read) else {
            return Err(format!("reading {} returned more than its buffer", path.display()));
        };
        if bytes.is_empty() {
            return Ok(hex(context.finish().as_ref()));
        }
        context.update(bytes);
    }
}

pub(crate) fn verify(path: &Path, named: &str, expected: &str) -> Result<(), String> {
    let actual = sha256_file(path)?;
    if actual == expected {
        return Ok(());
    }
    Err(format!(
        "the downloaded {named} has sha256 {actual}, the offer names {expected}; the release object is damaged, publish the release again with npm run release:publish"
    ))
}

pub fn cached(cache: &Path, image_digest: &str) -> Result<Option<GuestImageManifest>, Refusal> {
    let path = cache.join(format!("{image_digest}.json"));
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Refusal {
                reason: TurnFailureReason::BoxBootFailed,
                detail: format!("reading {}: {error}; set imageCacheDir in runtime.json", path.display()),
            })
        }
    };
    let manifest: GuestImageManifest = serde_json::from_str(&text).map_err(|error| Refusal {
        reason: TurnFailureReason::BoxBootFailed,
        detail: format!(
            "{} does not match GuestImageManifest in supervisor.tsp: {error}; delete it and the supervisor fetches the image again",
            path.display()
        ),
    })?;
    if *manifest.image_digest != image_digest {
        return Err(Refusal {
            reason: TurnFailureReason::BoxBootFailed,
            detail: format!(
                "{} names image {}, not {image_digest}; delete it and the supervisor fetches the image the release names",
                path.display(),
                *manifest.image_digest
            ),
        });
    }
    Ok(Some(manifest))
}

fn read_manifest(order: &ImageOrder, staged: &Staged) -> Result<GuestImageManifest, String> {
    let text = std::fs::read_to_string(&staged.manifest)
        .map_err(|error| format!("reading {}: {error}", staged.manifest.display()))?;
    let manifest: GuestImageManifest = serde_json::from_str(&text).map_err(|error| {
        format!("the downloaded manifest does not match GuestImageManifest in supervisor.tsp: {error}; publish the release again with npm run release:publish")
    })?;
    let download = &order.download;
    let names = (
        manifest.image_digest.as_str(),
        manifest.kernel_digest.as_str(),
        manifest.harness.harness.as_str(),
        manifest.harness.harness_version.as_str(),
    );
    let wanted = (
        download.image_digest.as_str(),
        download.kernel_digest.as_str(),
        order.harness.as_str(),
        order.harness_version.as_str(),
    );
    if names == wanted {
        return Ok(manifest);
    }
    Err(format!(
        "the downloaded manifest names image {}, kernel {} for {} {}, the offer names image {}, kernel {} for {} {}; fix the release manifest and publish it again with npm run release:publish",
        names.0, names.1, names.2, names.3, wanted.0, wanted.1, wanted.2, wanted.3
    ))
}

#[derive(Default)]
struct Manifests {
    images: Vec<GuestImageManifest>,
    bases: Vec<BaseImageManifest>,
}

fn manifests(cache: &Path) -> Result<Manifests, String> {
    let entries = std::fs::read_dir(cache).map_err(|error| format!("reading {}: {error}", cache.display()))?;
    let mut manifests = Manifests::default();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("reading {}: {error}", cache.display()))?
            .path();
        if path.extension().is_none_or(|extension| extension != "json") || !path.is_file() {
            continue;
        }
        let text = std::fs::read_to_string(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
        if let Ok(manifest) = serde_json::from_str::<GuestImageManifest>(&text) {
            manifests.images.push(manifest);
        } else if let Ok(manifest) = serde_json::from_str::<BaseImageManifest>(&text) {
            manifests.bases.push(manifest);
        } else {
            remove(&path.with_extension("ext4"))?;
            remove(&path)?;
        }
    }
    Ok(manifests)
}

fn rename(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::rename(from, to).map_err(|error| format!("moving {} to {}: {error}", from.display(), to.display()))
}

pub(crate) fn remove(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != ErrorKind::NotFound => Err(format!("removing {}: {error}", path.display())),
        _ => Ok(()),
    }
}

pub(crate) fn is_digest(name: &str) -> bool {
    name.len() == 64 && name.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn unreferenced(name: &str, images: &HashSet<&str>, kernels: &HashSet<&str>) -> bool {
    if let Some(staged) = name.strip_prefix('.').and_then(|name| name.strip_suffix(".part")) {
        return [".ext4", ".vmlinux", ".json"]
            .iter()
            .any(|extension| staged.strip_suffix(extension).is_some_and(is_digest));
    }
    if let Some(image) = name.strip_suffix(".ext4") {
        return is_digest(image) && !images.contains(image);
    }
    name.strip_suffix(".vmlinux")
        .is_some_and(|kernel| is_digest(kernel) && !kernels.contains(kernel))
}

fn drop_image(cache: &Path, image_digest: &str) -> Result<(), String> {
    remove(&cache.join(format!("{image_digest}.ext4")))?;
    remove(&cache.join(format!("{image_digest}.json")))
}

pub(crate) fn supersede(cache: &Path, harness: &str, held: &BTreeSet<String>) -> Result<(), String> {
    let others = manifests(cache)?;
    let (replaced, kept): (Vec<_>, Vec<_>) = others
        .images
        .into_iter()
        .partition(|other| other.harness.harness.as_str() == harness && !held.contains(other.image_digest.as_str()));
    for other in &replaced {
        drop_image(cache, &other.image_digest)?;
    }
    let digests = kept
        .iter()
        .map(|kept| (kept.image_digest.as_str(), kept.kernel_digest.as_str()))
        .chain(
            others
                .bases
                .iter()
                .map(|base| (base.image_digest.as_str(), base.kernel_digest.as_str())),
        );
    sweep(cache, digests)
}

fn prune_bases(
    cache: &Path,
    manifest: &BaseImageManifest,
    others: Manifests,
    held: &BTreeSet<String>,
) -> Result<(), String> {
    let (replaced, kept): (Vec<_>, Vec<_>) = others
        .bases
        .into_iter()
        .partition(|other| other.image_digest != manifest.image_digest && !held.contains(other.image_digest.as_str()));
    for other in &replaced {
        drop_image(cache, &other.image_digest)?;
    }
    let digests = kept
        .iter()
        .chain(std::iter::once(manifest))
        .map(|kept| (kept.image_digest.as_str(), kept.kernel_digest.as_str()))
        .chain(
            others
                .images
                .iter()
                .map(|image| (image.image_digest.as_str(), image.kernel_digest.as_str())),
        );
    sweep(cache, digests)
}

fn sweep<'a>(cache: &Path, digests: impl Iterator<Item = (&'a str, &'a str)>) -> Result<(), String> {
    let (images, kernels): (HashSet<&str>, HashSet<&str>) = digests.unzip();
    let entries = std::fs::read_dir(cache).map_err(|error| format!("reading {}: {error}", cache.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("reading {}: {error}", cache.display()))?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| unreferenced(name, &images, &kernels))
        {
            remove(&entry.path())?;
        }
    }
    Ok(())
}

fn place(cache: &Path, order: &ImageOrder, staged: &Staged, holds: &Holds) -> Result<GuestImageManifest, String> {
    let download = &order.download;
    verify(&staged.image, "image", &download.image_digest)?;
    if let Some(kernel) = &staged.kernel {
        verify(kernel, "kernel", &download.kernel_digest)?;
    }
    verify(&staged.manifest, "manifest", &download.manifest_digest)?;
    let manifest = read_manifest(order, staged)?;
    let digest = order.digest();
    rename(&staged.image, &cache.join(format!("{digest}.ext4")))?;
    if let Some(kernel) = &staged.kernel {
        rename(kernel, &cache.join(format!("{}.vmlinux", *download.kernel_digest)))?;
    }
    rename(&staged.manifest, &cache.join(format!("{digest}.json")))?;
    let mut kept = holds.held();
    kept.insert(digest.to_string());
    supersede(cache, &manifest.harness.harness, &kept)?;
    Ok(manifest)
}

pub fn adopt(cache: &Path, order: &ImageOrder, staged: &Staged, holds: &Holds) -> Result<GuestImageManifest, Refusal> {
    place(cache, order, staged, holds).map_err(|detail| {
        staged.remove();
        order.failed(detail)
    })
}

struct Urls<'a> {
    image: &'a str,
    kernel: &'a str,
    manifest: &'a str,
}

async fn download(store: &impl Store, urls: Urls<'_>, staged: &Staged) -> Result<(), String> {
    let image = store.download(urls.image, &staged.image);
    let manifest = store.download(urls.manifest, &staged.manifest);
    let kernel = async {
        match &staged.kernel {
            Some(kernel) => store.download(urls.kernel, kernel).await,
            None => Ok(()),
        }
    };
    futures_util::future::try_join3(image, manifest, kernel)
        .await
        .map(|_| ())
        .map_err(String::from)
}

fn timed_out(order: &ImageOrder) -> Refusal {
    Refusal {
        reason: TurnFailureReason::ImageFetchTimedOut,
        detail: format!(
            "guest image {} for {} {} was not fetched within the offer's fetchSeconds {} s, counting the wait for another image this host was fetching; check the host's bandwidth to the release bucket or raise turnImageFetchBudgetSeconds in the control plane settings",
            order.digest(),
            order.harness,
            order.harness_version,
            order.download.fetch_seconds.get()
        ),
    }
}

async fn fetch(
    store: &impl Store,
    cache: &Path,
    order: &ImageOrder,
    holds: &Holds,
    deadline: Instant,
) -> Result<GuestImageManifest, Refusal> {
    let staged = stage(cache, order)?;
    let urls = Urls {
        image: &order.download.image_url,
        kernel: &order.download.kernel_url,
        manifest: &order.download.manifest_url,
    };
    match timeout_at(deadline, download(store, urls, &staged)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            staged.remove();
            return Err(order.failed(format!("downloading it failed: {error}")));
        }
        Err(_) => {
            staged.remove();
            return Err(timed_out(order));
        }
    }
    let cache = cache.to_path_buf();
    let adopting = order.clone();
    let holds = holds.clone();
    match timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || adopt(&cache, &adopting, &staged, &holds)),
    )
    .await
    {
        Ok(Ok(adopted)) => adopted,
        Ok(Err(error)) => Err(Refusal {
            reason: TurnFailureReason::ImageFetchFailed,
            detail: format!("verifying a fetched guest image stopped: {error}; restart the supervisor"),
        }),
        Err(_) => Err(timed_out(order)),
    }
}

pub async fn resolve(
    store: &impl Store,
    cache: &Path,
    order: &ImageOrder,
    holds: &Holds,
    lock: &Mutex<()>,
    fetching: impl FnOnce(),
) -> Result<GuestImageManifest, Refusal> {
    if let Some(manifest) = cached(cache, order.digest())? {
        return Ok(manifest);
    }
    let deadline = Instant::now() + Duration::from_secs(u64::from(order.download.fetch_seconds.get()));
    fetching();
    let Ok(_fetching) = timeout_at(deadline, lock.lock()).await else {
        return Err(timed_out(order));
    };
    if let Some(manifest) = cached(cache, order.digest())? {
        return Ok(manifest);
    }
    fetch(store, cache, order, holds, deadline).await
}

pub fn cached_base(cache: &Path, image_digest: &str) -> Result<Option<BaseImageManifest>, Refusal> {
    let path = cache.join(format!("{image_digest}.json"));
    let broken = |detail: String| Refusal {
        reason: TurnFailureReason::BoxBootFailed,
        detail,
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(broken(format!(
                "reading {}: {error}; set imageCacheDir in runtime.json",
                path.display()
            )))
        }
    };
    let manifest: BaseImageManifest = serde_json::from_str(&text).map_err(|error| {
        broken(format!(
            "{} does not match BaseImageManifest in release.tsp: {error}; delete it and the supervisor fetches the base again",
            path.display()
        ))
    })?;
    if *manifest.image_digest != image_digest {
        return Err(broken(format!(
            "{} names base {}, not {image_digest}; delete it and the supervisor fetches the base the release names",
            path.display(),
            *manifest.image_digest
        )));
    }
    Ok(Some(manifest))
}

fn read_base_manifest(order: &BaseOrder, staged: &Staged) -> Result<BaseImageManifest, String> {
    let text = std::fs::read_to_string(&staged.manifest)
        .map_err(|error| format!("reading {}: {error}", staged.manifest.display()))?;
    let manifest: BaseImageManifest = serde_json::from_str(&text).map_err(|error| {
        format!("the downloaded manifest does not match BaseImageManifest in release.tsp: {error}; publish the image release again")
    })?;
    let download = &order.download;
    let names = (manifest.image_digest.as_str(), manifest.kernel_digest.as_str());
    let wanted = (download.image_digest.as_str(), download.kernel_digest.as_str());
    if names == wanted {
        return Ok(manifest);
    }
    Err(format!(
        "the downloaded manifest names base {} with kernel {}, the offer names base {} with kernel {}; fix the image release and publish it again",
        names.0, names.1, wanted.0, wanted.1
    ))
}

fn place_base(cache: &Path, order: &BaseOrder, staged: &Staged, holds: &Holds) -> Result<BaseImageManifest, String> {
    let download = &order.download;
    verify(&staged.image, "base image", &download.image_digest)?;
    if let Some(kernel) = &staged.kernel {
        verify(kernel, "kernel", &download.kernel_digest)?;
    }
    verify(&staged.manifest, "base manifest", &download.manifest_digest)?;
    let manifest = read_base_manifest(order, staged)?;
    let others = manifests(cache)?;
    let digest = order.digest();
    rename(&staged.image, &cache.join(format!("{digest}.ext4")))?;
    if let Some(kernel) = &staged.kernel {
        rename(kernel, &cache.join(format!("{}.vmlinux", *download.kernel_digest)))?;
    }
    rename(&staged.manifest, &cache.join(format!("{digest}.json")))?;
    prune_bases(cache, &manifest, others, &holds.held())?;
    Ok(manifest)
}

pub fn adopt_base(
    cache: &Path,
    order: &BaseOrder,
    staged: &Staged,
    holds: &Holds,
) -> Result<BaseImageManifest, Refusal> {
    place_base(cache, order, staged, holds).map_err(|detail| {
        staged.remove();
        order.failed(detail)
    })
}

pub fn stage_base(cache: &Path, order: &BaseOrder) -> Result<Staged, Refusal> {
    stage_parts(cache, order.digest(), &order.download.kernel_digest).map_err(|detail| order.failed(detail))
}

async fn fetch_base(
    store: &impl Store,
    cache: &Path,
    order: &BaseOrder,
    holds: &Holds,
    deadline: Instant,
) -> Result<BaseImageManifest, Refusal> {
    let staged = stage_base(cache, order)?;
    let urls = Urls {
        image: &order.download.image_url,
        kernel: &order.download.kernel_url,
        manifest: &order.download.manifest_url,
    };
    match timeout_at(deadline, download(store, urls, &staged)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            staged.remove();
            return Err(order.failed(format!("downloading it failed: {error}")));
        }
        Err(_) => {
            staged.remove();
            return Err(order.timed_out());
        }
    }
    let adopting = (cache.to_path_buf(), order.clone(), holds.clone());
    match timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || {
            let (cache, order, holds) = adopting;
            adopt_base(&cache, &order, &staged, &holds)
        }),
    )
    .await
    {
        Ok(Ok(adopted)) => adopted,
        Ok(Err(error)) => Err(order.failed(format!("verifying it stopped: {error}; restart the supervisor"))),
        Err(_) => Err(order.timed_out()),
    }
}

pub async fn resolve_base(
    store: &impl Store,
    cache: &Path,
    order: &BaseOrder,
    holds: &Holds,
    lock: &Mutex<()>,
    deadline: Instant,
) -> Result<BaseImageManifest, Refusal> {
    if let Some(manifest) = cached_base(cache, order.digest())? {
        return Ok(manifest);
    }
    let Ok(_fetching) = timeout_at(deadline, lock.lock()).await else {
        return Err(order.timed_out());
    };
    if let Some(manifest) = cached_base(cache, order.digest())? {
        return Ok(manifest);
    }
    fetch_base(store, cache, order, holds, deadline).await
}

#[cfg(test)]
mod tests;
