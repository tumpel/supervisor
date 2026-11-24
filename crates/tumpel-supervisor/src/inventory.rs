use std::path::Path;

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use tumpel_api::types::{SupervisorHandover, SupervisorHandoverBox};

use crate::dir;
use crate::firecracker::Observation;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Inventory {
    pub boxes: Vec<RecordedBox>,
    pub handed_at: DateTime<FixedOffset>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct RecordedBox {
    #[serde(flatten)]
    pub record: SupervisorHandoverBox,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<Observation>,
}

impl From<Inventory> for SupervisorHandover {
    fn from(inventory: Inventory) -> Self {
        Self {
            boxes: inventory.boxes.into_iter().map(|box_| box_.record).collect(),
            handed_at: inventory.handed_at,
        }
    }
}

pub(crate) fn load(path: &Path) -> anyhow::Result<Option<Inventory>> {
    dir::read_json(path, "SupervisorHandover")
}
