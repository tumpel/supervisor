use std::path::Path;

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use tumpel_api::types::{SupervisorHandover, SupervisorHandoverBox};

use crate::dir;
use crate::firecracker::Observation;
use crate::lifecycle::Taken;

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

impl From<SupervisorHandover> for Inventory {
    fn from(handover: SupervisorHandover) -> Self {
        Self {
            boxes: handover
                .boxes
                .into_iter()
                .map(|record| RecordedBox {
                    record,
                    observation: None,
                })
                .collect(),
            handed_at: handover.handed_at,
        }
    }
}

impl RecordedBox {
    pub fn same_machine(&self, other: &Self) -> bool {
        self.record.tap_index == other.record.tap_index
            && self.record.session_id == other.record.session_id
            && self.record.box_id == other.record.box_id
            && self.record.machine.process_id == other.record.machine.process_id
    }
}

pub(crate) struct RestoredInventory {
    pub inventory: Inventory,
    pub taken: Taken,
    pub inherited: bool,
}

pub(crate) fn load(path: &Path) -> anyhow::Result<Option<Inventory>> {
    dir::read_json(path, "SupervisorHandover")
}
