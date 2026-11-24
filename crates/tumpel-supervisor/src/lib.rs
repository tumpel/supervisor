mod boot;
mod bounded;
mod check;
mod dir;
mod enrol;
mod exchange;
mod fd_store;
mod firecracker;
mod guest_drive;
mod harness_result;
mod images;
mod install;
mod inventory;
pub mod layers;
mod lifecycle;
mod local_handover;
mod persona_files;
mod pool;
mod run;
mod snapshot;
mod store;
mod telemetry;
mod update;
mod usage;

pub use boot::BootOrder;
pub use check::{report, ReportFormat};
pub use dir::supervisor_dir;
pub use enrol::enrol;
pub use exchange::{Answer, Answers};
pub use fd_store::{
    keep as keep_descriptor, received as received_descriptors, restored as restored_descriptors, take_notify_socket,
};
pub use firecracker::{Firecracker, FirecrackerBox, Observation, VsockGuest};
pub use images::{ImageOrder, Refusal};
pub use install::{install, Enrolment};
pub use lifecycle::{
    Attached, BoxTurn, Budgets, Command, Guest, IdleGate, Input, Lifecycle, Machine, Settings, SnapshotOrder, Taken,
    Vmm,
};
pub use local_handover::{receive_handover, Transfer};
pub use pool::{Disk, PoolExhausted, Size, Tap, TapPool};
pub use run::{run, Failure, Stopped, DAEMON_START_SECONDS};
pub use snapshot::{Entry, Ext4Upper, Kind, Manifest, Scan, Upper};
pub use store::{DownloadError, HttpStore, Store, UploadError};
pub use telemetry::{install as install_telemetry, Telemetry};
pub use update::on_trial;

#[cfg(test)]
mod test_logs;
