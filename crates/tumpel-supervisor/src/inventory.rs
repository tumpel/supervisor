use std::collections::HashSet;
use std::path::Path;

use anyhow::Context;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use tumpel_api::types::{SupervisorHandover, SupervisorHandoverBox};

use crate::dir;
use crate::firecracker::Observation;
use crate::lifecycle::Taken;
use crate::update::Layout;

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
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub reconnect: bool,
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
                    reconnect: false,
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

pub(crate) struct StartupBoxes {
    pub inventory: Inventory,
    pub handed_count: usize,
    pub inherited: bool,
    pub preserve_handover: bool,
}

impl StartupBoxes {
    pub fn from_record(inventory: Inventory, taken: Taken) -> Self {
        let handed_count = if matches!(taken, Taken::Handed) {
            inventory.boxes.len()
        } else {
            0
        };
        Self {
            inventory,
            handed_count,
            inherited: false,
            preserve_handover: false,
        }
    }

    pub fn select(layout: &Layout, handed: bool, has_descriptors: bool) -> anyhow::Result<Option<Self>> {
        let standing = load(&layout.boxes()).with_context(|| {
            format!(
                "restore the saved box inventory at {} before restarting the supervisor",
                layout.boxes().display()
            )
        })?;
        let mut preserve_handover = false;
        if handed || has_descriptors {
            let handover = match load(&layout.handover()) {
                Ok(handover) => handover,
                Err(error) if standing.is_some() => {
                    eprintln!("tumpel-supervisor: {error:#}; restoring the standing inventory and keeping the invalid handover for diagnosis");
                    preserve_handover = true;
                    None
                }
                Err(error) => return Err(error),
            };
            if let Some(mut handover) = handover.filter(|handover| {
                standing
                    .as_ref()
                    .is_none_or(|saved| handed || saved.handed_at <= handover.handed_at)
            }) {
                let handed_count = handover.boxes.len();
                if let Some(standing) = standing {
                    for saved in standing.boxes {
                        let current = handover.boxes[..handed_count].iter_mut().find(|box_| {
                            box_.record.tap_index == saved.record.tap_index
                                || box_.record.session_id == saved.record.session_id
                        });
                        if let Some(current) = current {
                            if current.same_machine(&saved) {
                                current.observation = saved.observation;
                                if saved.reconnect {
                                    current.record.state = saved.record.state;
                                    current.record.dirty = saved.record.dirty;
                                    current.reconnect = true;
                                }
                            } else {
                                eprintln!(
                                    "tumpel-supervisor: the handover record for session {} on TAP {} supersedes a different saved box {} of session {} on TAP {}",
                                    current.record.session_id, current.record.tap_index,
                                    saved.record.box_id, saved.record.session_id, saved.record.tap_index
                                );
                            }
                        } else {
                            handover.boxes.push(saved);
                        }
                    }
                }
                return Ok(Some(Self {
                    inventory: handover,
                    handed_count,
                    inherited: !has_descriptors,
                    preserve_handover,
                }));
            }
        }
        Ok(standing.map(|inventory| Self {
            inventory,
            handed_count: 0,
            inherited: false,
            preserve_handover,
        }))
    }
}

pub(crate) fn load(path: &Path) -> anyhow::Result<Option<Inventory>> {
    let inventory: Option<Inventory> = dir::read_json(path, "SupervisorHandover")?;
    if let Some(inventory) = inventory.as_ref() {
        validate(inventory.boxes.iter().map(|box_| &box_.record)).map_err(anyhow::Error::msg)?;
    }
    Ok(inventory)
}

pub(crate) fn validate<'a>(records: impl IntoIterator<Item = &'a SupervisorHandoverBox>) -> Result<(), String> {
    let mut taps = HashSet::new();
    let mut sessions = HashSet::new();
    for record in records {
        if !(0..i32::from(crate::pool::TAP_POOL)).contains(&record.tap_index)
            || !taps.insert(record.tap_index)
            || !sessions.insert(&record.session_id)
        {
            return Err("the box inventory has an invalid or duplicate TAP index or a duplicate session".to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Layout, StartupBoxes};

    #[test]
    fn partial_and_malformed_handovers_leave_standing_boxes_available_for_recovery() {
        let state = std::env::temp_dir().join(format!("tumpel-startup-records-{}", std::process::id()));
        std::fs::create_dir(&state).expect("state");
        let layout = Layout::new(state.join("binary"), state.clone(), state.join("unit"));
        let box_ = |index: u8| {
            json!({
                "boxId":format!("box-{index}"),"boxRetention":"Warm","dirty":true,"idleMilliseconds":1000,
                "image":{"kind":"FutureImage"},
                "machine":{"apiSocket":"/api.sock","vsock":"/guest.sock","guestBuffer":"","consoleTail":"",
                    "guestFd":-1,"stdoutFd":-1,"stderrFd":-1,"processId":100+u32::from(index)},
                "queued":[],"sessionId":format!("session-{index}"),"tapIndex":index,"state":{"kind":"Warm"}
            })
        };
        let standing = json!({"handedAt":"2026-11-16T09:30:00Z","boxes":[box_(0),box_(1)]});
        let handed = json!({"handedAt":"2026-11-16T09:30:01Z","boxes":[box_(0)]});
        std::fs::write(layout.boxes(), standing.to_string()).expect("standing inventory");
        std::fs::write(layout.handover(), handed.to_string()).expect("partial handover");
        let selected = StartupBoxes::select(&layout, false, true)
            .expect("valid selection")
            .expect("boxes");
        assert_eq!(selected.handed_count, 1);
        assert_eq!(selected.inventory.boxes.len(), 2);
        assert_eq!(selected.inventory.boxes[1].record.tap_index, 1);

        std::fs::write(layout.handover(), b"{").expect("malformed handover");
        let selected = StartupBoxes::select(&layout, false, true)
            .expect("standing recovery")
            .expect("boxes");
        assert_eq!(selected.handed_count, 0);
        assert_eq!(selected.inventory.boxes.len(), 2);
        assert!(selected.preserve_handover);
        assert_eq!(std::fs::read(layout.handover()).expect("diagnostic record"), b"{");
        std::fs::remove_dir_all(state).expect("remove fixture");
    }
}
