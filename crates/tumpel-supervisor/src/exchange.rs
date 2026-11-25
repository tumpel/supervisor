use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{oneshot, watch};
use tumpel_api::types::{
    ChannelRestoreDownloads, ChannelSnapshotChunksHeld, ChannelSnapshotPackUploads, ChannelTurnClaimed,
    ChannelTurnWithdrawn, ChunkExchange, EntityId,
};

use tumpel_channel::{Ask, Key};

pub enum Answer {
    ChunksHeld(ChannelSnapshotChunksHeld),
    PackUploads(ChannelSnapshotPackUploads),
    Downloads(ChannelRestoreDownloads),
    Claimed(ChannelTurnClaimed),
    Withdrawn(ChannelTurnWithdrawn),
    Refused(Key, String),
}

impl Answer {
    fn key(&self) -> Key {
        match self {
            Self::ChunksHeld(answer) => Key {
                ask: Ask::SnapshotChunks,
                id: answer.snapshot_id.clone(),
                part: answer.part,
            },
            Self::PackUploads(answer) => Key {
                ask: Ask::SnapshotPacks,
                id: answer.snapshot_id.clone(),
                part: answer.part,
            },
            Self::Downloads(answer) => Key {
                ask: Ask::RestoreChunks,
                id: answer.turn_id.clone(),
                part: answer.part,
            },
            Self::Claimed(answer) => Key::claim(&answer.turn_id),
            Self::Withdrawn(answer) => Key::claim(&answer.turn_id),
            Self::Refused(key, _) => key.clone(),
        }
    }
}

pub const DIGESTS_PER_PART: usize = ChunkExchange::DIGESTS_PER_PART as usize;

pub fn part_number(index: usize) -> Result<NonZeroU32, String> {
    u32::try_from(index + 1)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| format!("part {} is past what a u32 counts", index + 1))
}

pub struct Waiting {
    pub key: Key,
    pub answer: oneshot::Receiver<Answer>,
}

#[derive(Clone, Default)]
pub struct Answers(Arc<Mutex<HashMap<Key, oneshot::Sender<Answer>>>>);

impl Answers {
    pub fn expect(&self, key: Key) -> Waiting {
        let (sender, answer) = oneshot::channel();
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key.clone(), sender);
        Waiting { key, answer }
    }

    pub fn forget(&self, key: &Key) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).remove(key);
    }

    pub fn deliver(&self, answer: Answer) {
        let key = answer.key();
        let waiting = self.0.lock().unwrap_or_else(PoisonError::into_inner).remove(&key);
        let delivered = waiting.is_some_and(|sender| sender.send(answer).is_ok());
        if !delivered {
            eprintln!("tumpel-supervisor: the answer to {key} arrived with nothing waiting for it; dropped it");
        }
    }
}

#[derive(Clone, Default)]
pub struct Cancels(Arc<watch::Sender<HashSet<EntityId>>>);

impl Cancels {
    pub fn cancel(&self, turn_id: EntityId) {
        self.0.send_modify(|cancelled| {
            cancelled.insert(turn_id);
        });
    }

    pub async fn cancelled(&self, turn_id: &EntityId) {
        let _ = self
            .0
            .subscribe()
            .wait_for(|cancelled| cancelled.contains(turn_id))
            .await;
    }

    pub fn contains(&self, turn_id: &EntityId) -> bool {
        self.0.borrow().contains(turn_id)
    }

    pub fn forget(&self, turn_id: &EntityId) {
        self.0.send_if_modified(|cancelled| cancelled.remove(turn_id));
    }
}
