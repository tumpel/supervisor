use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;
use tumpel_api::types::{
    BaseImageManifest, ChannelBootImage, GuestHarness, GuestImageManifest, Sha256Digest, SupervisorHandoverFullImage,
    SupervisorHandoverFullImageKind, SupervisorHandoverImage, SupervisorHandoverLayeredImage,
    SupervisorHandoverLayeredImageKind, TurnFailureReason,
};

use crate::images::{self, BaseOrder, ImageOrder, Refusal};
use crate::layers::{self, Layer, LayerCache, LayerOrder};
use crate::store::Store;

#[derive(Clone, Debug)]
pub struct LayeredOrder {
    pub base: BaseOrder,
    pub layer: LayerOrder,
    pub harness: String,
    pub harness_version: String,
}

#[derive(Clone, Debug)]
pub enum BootOrder {
    Full(ImageOrder),
    Layered(LayeredOrder),
}

impl BootOrder {
    #[must_use]
    pub fn harness(&self) -> (&str, &str) {
        match self {
            Self::Full(order) => (&order.harness, &order.harness_version),
            Self::Layered(order) => (&order.harness, &order.harness_version),
        }
    }

    #[must_use]
    pub fn from_offer(image: ChannelBootImage, harness: String, harness_version: String) -> Self {
        match image {
            ChannelBootImage::GuestImageDownload(download) => Self::Full(ImageOrder {
                download,
                harness,
                harness_version,
            }),
            ChannelBootImage::LayeredImageDownload(download) => Self::Layered(LayeredOrder {
                base: BaseOrder {
                    download: download.base,
                    fetch_seconds: download.fetch_seconds,
                },
                layer: LayerOrder {
                    layer: download.layer.layer,
                    layer_url: download.layer.layer_url,
                    manifest_url: download.layer.manifest_url,
                    fetch_seconds: download.fetch_seconds,
                },
                harness,
                harness_version,
            }),
        }
    }
}

#[derive(Debug)]
pub struct Layered {
    pub base: BaseImageManifest,
    pub layer: Layer,
}

#[derive(Debug)]
pub enum Boot {
    Full(GuestImageManifest),
    Layered(Layered),
}

impl Boot {
    #[must_use]
    pub fn harness(&self) -> &GuestHarness {
        match self {
            Self::Full(image) => &image.harness,
            Self::Layered(layered) => &layered.layer.manifest.harness,
        }
    }

    #[must_use]
    pub fn record(&self) -> SupervisorHandoverImage {
        match self {
            Self::Full(manifest) => SupervisorHandoverImage::FullImage(SupervisorHandoverFullImage {
                kind: SupervisorHandoverFullImageKind::Full,
                manifest: manifest.clone(),
            }),
            Self::Layered(layered) => SupervisorHandoverImage::LayeredImage(SupervisorHandoverLayeredImage {
                base: layered.base.clone(),
                kind: SupervisorHandoverLayeredImageKind::Layered,
                layer: layered.layer.manifest.clone(),
                layer_path: layered.layer.path.display().to_string(),
            }),
        }
    }

    #[must_use]
    pub fn from_record(record: SupervisorHandoverImage) -> Self {
        match record {
            SupervisorHandoverImage::FullImage(full) => Self::Full(full.manifest),
            SupervisorHandoverImage::LayeredImage(layered) => Self::Layered(Layered {
                base: layered.base,
                layer: Layer {
                    path: layered.layer_path.into(),
                    manifest: layered.layer,
                },
            }),
        }
    }

    #[must_use]
    pub fn named(&self) -> String {
        match self {
            Self::Full(image) => format!("image {}", *image.image_digest),
            Self::Layered(layered) => format!(
                "base {} with harness layer {}",
                *layered.base.image_digest, *layered.layer.manifest.layer_digest
            ),
        }
    }
}

#[derive(Clone, Default)]
pub struct Holds(Arc<std::sync::Mutex<BTreeMap<String, usize>>>);

pub struct Hold {
    holds: Holds,
    digests: Vec<String>,
}

impl Holds {
    fn hold(&self, digests: Vec<String>) -> Hold {
        let mut held = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for digest in &digests {
            *held.entry(digest.clone()).or_default() += 1;
        }
        Hold {
            holds: self.clone(),
            digests,
        }
    }

    #[must_use]
    pub fn hold_boot(&self, boot: &Boot) -> Hold {
        match boot {
            Boot::Full(_) => self.hold(Vec::new()),
            Boot::Layered(layered) => self.hold(vec![
                layered.base.image_digest.to_string(),
                layered.layer.manifest.layer_digest.to_string(),
            ]),
        }
    }

    #[must_use]
    pub fn held(&self) -> BTreeSet<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut held = self.holds.0.lock().unwrap_or_else(PoisonError::into_inner);
        for digest in &self.digests {
            if let Some(count) = held.get_mut(digest) {
                *count -= 1;
                if *count == 0 {
                    held.remove(digest);
                }
            }
        }
    }
}

pub struct Caches<'a, S: Store> {
    pub store: &'a S,
    pub images: &'a Path,
    pub layers: &'a LayerCache,
    pub holds: &'a Holds,
    pub lock: &'a Mutex<()>,
}

fn broken(detail: String) -> Refusal {
    Refusal {
        reason: TurnFailureReason::BoxBootFailed,
        detail,
    }
}

fn digest(value: &str) -> Result<Sha256Digest, Refusal> {
    value
        .to_string()
        .try_into()
        .map_err(|error| broken(format!("{value} is not a sha256 digest: {error}")))
}

pub async fn resolve<S: Store>(
    caches: &Caches<'_, S>,
    order: &BootOrder,
    fetching: impl FnOnce(Sha256Digest),
) -> Result<(Boot, Hold), Refusal> {
    match order {
        BootOrder::Full(image) => {
            let digest = image.download.image_digest.clone();
            let manifest =
                images::resolve(caches.store, caches.images, image, caches.lock, || fetching(digest)).await?;
            Ok((Boot::Full(manifest), caches.holds.hold(Vec::new())))
        }
        BootOrder::Layered(layered) => {
            let hold = caches.holds.hold(vec![
                layered.base.digest().to_string(),
                layered.layer.layer.layer_digest.to_string(),
            ]);
            let boot = resolve_layered(caches, layered, fetching).await?;
            Ok((Boot::Layered(boot), hold))
        }
    }
}

async fn resolve_layered<S: Store>(
    caches: &Caches<'_, S>,
    order: &LayeredOrder,
    fetching: impl FnOnce(Sha256Digest),
) -> Result<Layered, Refusal> {
    let layer = &order.layer.layer;
    let carried = (layer.harness.to_string(), layer.harness_version.to_string());
    if carried != (order.harness.clone(), order.harness_version.clone()) {
        return Err(broken(format!(
            "the offer names {} {} and its harness layer {} carries {} {}; the control plane picked the wrong layer, update it",
            order.harness, order.harness_version, *layer.layer_digest, carried.0, carried.1
        )));
    }
    let deadline = Instant::now() + Duration::from_secs(u64::from(order.base.fetch_seconds.get()));
    let base = images::cached_base(caches.images, order.base.digest())?;
    let cached_layer = layers::cached(caches.layers, &order.layer)?;
    match (&base, &cached_layer) {
        (None, _) => fetching(digest(order.base.digest())?),
        (Some(_), None) => fetching(layer.layer_digest.clone()),
        (Some(_), Some(_)) => {}
    }
    let base = match base {
        Some(base) => base,
        None => {
            images::resolve_base(
                caches.store,
                caches.images,
                &order.base,
                caches.holds.held(),
                caches.lock,
                deadline,
            )
            .await?
        }
    };
    let layer = match cached_layer {
        Some(layer) => layer,
        None => {
            layers::resolve(
                caches.store,
                caches.layers,
                &order.layer,
                caches.holds.held(),
                caches.lock,
                deadline,
            )
            .await?
        }
    };
    Ok(Layered { base, layer })
}

#[cfg(test)]
mod tests;
