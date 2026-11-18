use std::num::{NonZeroU32, NonZeroU64};
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use tumpel_api::types::{HostArchitecture, HostUsage};

use crate::firecracker::OVERLAY_DRIVE;
use crate::pool::{Disk, Held};

const MEMINFO: &str = "/proc/meminfo";
const LOADAVG: &str = "/proc/loadavg";
const STAT: &str = "/proc/stat";

pub fn architecture() -> Result<HostArchitecture> {
    match std::env::consts::ARCH {
        "x86_64" => Ok(HostArchitecture::Amd64),
        "aarch64" => Ok(HostArchitecture::Arm64),
        other => bail!("this machine is {other}; tumpel hosts run on x86_64 or aarch64"),
    }
}

pub fn cpu_count() -> Result<NonZeroU32> {
    let count = std::thread::available_parallelism().context("counting the host's CPUs failed")?;
    NonZeroU32::new(u32::try_from(count.get()).unwrap_or(u32::MAX)).ok_or_else(|| anyhow!("the host reports no CPUs"))
}

fn read_meminfo() -> Result<String> {
    std::fs::read_to_string(MEMINFO).with_context(|| format!("reading {MEMINFO} for the host's memory failed"))
}

fn meminfo_kib(meminfo: &str, field: &str) -> Result<u64> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .ok_or_else(|| anyhow!("{MEMINFO} has no {field} line in kB"))
}

pub fn memory_bytes() -> Result<NonZeroU64> {
    NonZeroU64::new(meminfo_kib(&read_meminfo()?, "MemTotal")? * 1024)
        .ok_or_else(|| anyhow!("{MEMINFO} reports no memory"))
}

fn written_mib(sessions: &Path, drives: &[String]) -> Result<u64> {
    let mut blocks = 0;
    for drive in drives {
        let drive = sessions.join(drive).join(OVERLAY_DRIVE);
        match std::fs::metadata(&drive) {
            Ok(metadata) => blocks += metadata.blocks(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reading what the box disk {} holds failed", drive.display()))
            }
        }
    }
    Ok(blocks * 512 / (1024 * 1024))
}

pub fn prepare_data_dir(data_dir: &Path) -> Result<Disk> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(data_dir)
        .with_context(|| {
            format!(
                "creating the data directory {} failed; set dataDir in runtime.json to a directory this user can write",
                data_dir.display()
            )
        })?;
    disk(data_dir, &[])
}

pub fn disk(data_dir: &Path, drives: &[String]) -> Result<Disk> {
    let space = tumpel_guest::disk::space(data_dir).map_err(|error| anyhow!(error))?;
    Ok(Disk {
        total_mib: space.total_mib,
        free_mib: space.available_mib,
        written_mib: written_mib(&data_dir.join("sessions"), drives)?,
    })
}

fn load_averages(loadavg: &str) -> Result<[f64; 3]> {
    let loads: Vec<f64> = loadavg
        .split_whitespace()
        .take(3)
        .filter_map(|field| field.parse::<f64>().ok())
        .filter(|load| load.is_finite() && *load >= 0.0)
        .collect();
    <[f64; 3]>::try_from(loads).map_err(|_| {
        anyhow!("{LOADAVG} does not start with the one, five and fifteen minute load averages: {loadavg:?}")
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuTimes {
    busy: u64,
    total: u64,
}

impl CpuTimes {
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn busy_since(self, before: Self) -> Option<f64> {
        let total = self.total.checked_sub(before.total).filter(|total| *total > 0)?;
        let busy = self.busy.checked_sub(before.busy)?;
        Some(busy as f64 / total as f64)
    }
}

fn cpu_times(stat: &str) -> Result<CpuTimes> {
    let ticks: Vec<u64> = stat
        .lines()
        .find_map(|line| line.strip_prefix("cpu "))
        .map_or_else(Vec::new, |rest| {
            rest.split_whitespace()
                .take(8)
                .filter_map(|field| field.parse().ok())
                .collect()
        });
    let [user, nice, system, idle, iowait, irq, softirq, steal] =
        <[u64; 8]>::try_from(ticks).map_err(|_| anyhow!("{STAT} has no cpu line with eight tick counts"))?;
    let busy = user + nice + system + irq + softirq + steal;
    Ok(CpuTimes {
        busy,
        total: busy + idle + iowait,
    })
}

pub struct HostStats {
    pub usage: HostUsage,
    pub memory_total_mib: u64,
    pub loads: [f64; 3],
    pub cpu: CpuTimes,
}

fn stats(boxes_warm: u32, disk: Disk, held: &Held, procs: [&str; 3]) -> Result<HostStats> {
    let [meminfo, loadavg, stat] = procs;
    let loads = load_averages(loadavg)?;
    Ok(HostStats {
        usage: HostUsage {
            boxes_warm: i32::try_from(boxes_warm).unwrap_or(i32::MAX),
            load_average: loads[0],
            memory_available_mib: i64::try_from(meminfo_kib(meminfo, "MemAvailable")? / 1024).unwrap_or(i64::MAX),
            disk_budget_mib: Some(i64::try_from(disk.budget_mib()).unwrap_or(i64::MAX)),
            disk_held_mib: Some(i64::try_from(held.disk_held_mib).unwrap_or(i64::MAX)),
            memory_budget_mib: Some(i64::try_from(held.memory_budget_mib).unwrap_or(i64::MAX)),
            memory_held_mib: Some(i64::try_from(held.memory_held_mib).unwrap_or(i64::MAX)),
        },
        memory_total_mib: meminfo_kib(meminfo, "MemTotal")? / 1024,
        loads,
        cpu: cpu_times(stat)?,
    })
}

pub fn sample(boxes_warm: u32, data_dir: &Path, held: &Held) -> Result<HostStats> {
    let loadavg =
        std::fs::read_to_string(LOADAVG).with_context(|| format!("reading {LOADAVG} for the host's load failed"))?;
    let stat =
        std::fs::read_to_string(STAT).with_context(|| format!("reading {STAT} for the host's CPU time failed"))?;
    let disk = disk(data_dir, &held.drives)?;
    stats(boxes_warm, disk, held, [&read_meminfo()?, &loadavg, &stat])
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{cpu_times, disk, prepare_data_dir, stats, written_mib, Disk};
    use crate::pool::Held;

    #[test]
    fn a_freshly_enrolled_host_without_a_data_directory_creates_it_and_reads_its_disk() {
        let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/usage-tests/fresh/tumpel-supervisor");
        if let Some(parent) = data.parent().filter(|parent| parent.exists()) {
            std::fs::remove_dir_all(parent).expect("clearing the last run");
        }
        assert!(
            disk(&data, &[]).is_err(),
            "a host that never booted a box has no data directory yet"
        );
        let prepared = prepare_data_dir(&data).expect("the supervisor creates its data directory at start");
        assert!(prepared.total_mib > 0);
        assert_eq!(
            std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&data).expect("the directory").permissions())
                & 0o777,
            0o700
        );
        assert!(disk(&data, &[]).is_ok(), "the heartbeat reads its disk");
    }

    #[test]
    fn written_disk_counts_only_the_drives_of_boxes_the_pool_holds() {
        let sessions = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/usage-tests/sessions");
        if sessions.exists() {
            std::fs::remove_dir_all(&sessions).expect("clearing the last run");
        }
        for session in ["held", "orphan"] {
            std::fs::create_dir_all(sessions.join(session)).expect("a session directory");
            std::fs::write(sessions.join(session).join("overlay.ext4"), vec![1u8; 3 * 1024 * 1024]).expect("a drive");
        }
        assert_eq!(
            written_mib(&sessions, &["held".to_string(), "booting".to_string()]).expect("written"),
            3
        );
    }

    const STAT: &str = "cpu  100 0 50 800 50 0 0 0 0 0\ncpu0 100 0 50 800 50 0 0 0 0 0\n";
    const LATER: &str = "cpu  160 0 70 880 70 0 0 0 0 0\ncpu0 160 0 70 880 70 0 0 0 0 0\n";

    #[test]
    fn stats_read_memory_in_mib_and_the_load_averages_from_captured_proc_files() {
        let disk = Disk {
            total_mib: 100_000,
            free_mib: 80_000,
            written_mib: 0,
        };
        let held = Held {
            drives: Vec::new(),
            disk_held_mib: 32_768,
            memory_budget_mib: 33_792,
            memory_held_mib: 6144,
        };
        let sampled = stats(
            0,
            disk,
            &held,
            [include_str!("usage/meminfo"), include_str!("usage/loadavg"), STAT],
        )
        .expect("stats");
        assert_eq!(
            (sampled.usage.disk_budget_mib, sampled.usage.disk_held_mib),
            (Some(70_000), Some(32_768))
        );
        assert_eq!(
            (sampled.usage.memory_budget_mib, sampled.usage.memory_held_mib),
            (Some(33_792), Some(6144))
        );
        assert_eq!(sampled.usage.memory_available_mib, 12_973_128 / 1024);
        assert_eq!(sampled.memory_total_mib, 16_375_664 / 1024);
        assert!((sampled.usage.load_average - 10.01).abs() < f64::EPSILON);
        assert_eq!(sampled.loads, [10.01, 6.83, 4.95]);
    }

    #[test]
    fn cpu_busy_is_the_share_of_ticks_outside_idle_and_iowait_between_two_readings() {
        let before = cpu_times(STAT).expect("before");
        let after = cpu_times(LATER).expect("after");
        assert_eq!(after.busy_since(before), Some(80.0 / 180.0));
        assert_eq!(before.busy_since(before), None);
    }
}
