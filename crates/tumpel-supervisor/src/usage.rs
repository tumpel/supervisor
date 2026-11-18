use std::num::{NonZeroU32, NonZeroU64};

use anyhow::{anyhow, bail, Context, Result};
use tumpel_api::types::{HostArchitecture, HostUsage};

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

fn field_kib(text: &str, field: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))
        .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
}

fn meminfo_kib(meminfo: &str, field: &str) -> Result<u64> {
    field_kib(meminfo, field).ok_or_else(|| anyhow!("{MEMINFO} has no {field} line in kB"))
}

fn resident_kib(status: &str) -> u64 {
    field_kib(status, "VmRSS").unwrap_or(0)
}

pub fn resident_mib(pid: std::num::NonZeroU32) -> Result<u64> {
    let path = format!("/proc/{pid}/status");
    match std::fs::read_to_string(&path) {
        Ok(status) => Ok(resident_kib(&status) / 1024),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).with_context(|| format!("reading {path} for a box's resident memory failed")),
    }
}

pub fn memory_bytes() -> Result<NonZeroU64> {
    NonZeroU64::new(meminfo_kib(&read_meminfo()?, "MemTotal")? * 1024)
        .ok_or_else(|| anyhow!("{MEMINFO} reports no memory"))
}

pub fn memory_total_and_available_mib() -> Result<(u64, u64)> {
    let meminfo = read_meminfo()?;
    Ok((
        meminfo_kib(&meminfo, "MemTotal")? / 1024,
        meminfo_kib(&meminfo, "MemAvailable")? / 1024,
    ))
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

fn stats(boxes_warm: u32, meminfo: &str, loadavg: &str, stat: &str) -> Result<HostStats> {
    let loads = load_averages(loadavg)?;
    Ok(HostStats {
        usage: HostUsage {
            boxes_warm: i32::try_from(boxes_warm).unwrap_or(i32::MAX),
            load_average: loads[0],
            memory_available_mib: i64::try_from(meminfo_kib(meminfo, "MemAvailable")? / 1024).unwrap_or(i64::MAX),
        },
        memory_total_mib: meminfo_kib(meminfo, "MemTotal")? / 1024,
        loads,
        cpu: cpu_times(stat)?,
    })
}

pub fn sample(boxes_warm: u32) -> Result<HostStats> {
    let loadavg =
        std::fs::read_to_string(LOADAVG).with_context(|| format!("reading {LOADAVG} for the host's load failed"))?;
    let stat =
        std::fs::read_to_string(STAT).with_context(|| format!("reading {STAT} for the host's CPU time failed"))?;
    stats(boxes_warm, &read_meminfo()?, &loadavg, &stat)
}

#[cfg(test)]
mod tests {
    use super::{cpu_times, resident_kib, stats};

    const STAT: &str = "cpu  100 0 50 800 50 0 0 0 0 0\ncpu0 100 0 50 800 50 0 0 0 0 0\n";
    const LATER: &str = "cpu  160 0 70 880 70 0 0 0 0 0\ncpu0 160 0 70 880 70 0 0 0 0 0\n";

    #[test]
    fn stats_read_memory_in_mib_and_the_load_averages_from_captured_proc_files() {
        let sampled = stats(0, include_str!("usage/meminfo"), include_str!("usage/loadavg"), STAT).expect("stats");
        assert_eq!(sampled.usage.memory_available_mib, 12_973_128 / 1024);
        assert_eq!(sampled.memory_total_mib, 16_375_664 / 1024);
        assert!((sampled.usage.load_average - 10.01).abs() < f64::EPSILON);
        assert_eq!(sampled.loads, [10.01, 6.83, 4.95]);
    }

    #[test]
    fn resident_memory_is_vm_rss_and_a_zombie_without_memory_holds_none() {
        let running = "Name:\tfirecracker\nState:\tS (sleeping)\nVmPeak:\t 4300000 kB\nVmRSS:\t 3670016 kB\nRssAnon:\t 3600000 kB\n";
        assert_eq!(resident_kib(running), 3_670_016);
        assert_eq!(resident_kib("Name:\tfirecracker\nState:\tZ (zombie)\nThreads:\t1\n"), 0);
    }

    #[test]
    fn cpu_busy_is_the_share_of_ticks_outside_idle_and_iowait_between_two_readings() {
        let before = cpu_times(STAT).expect("before");
        let after = cpu_times(LATER).expect("after");
        assert_eq!(after.busy_since(before), Some(80.0 / 180.0));
        assert_eq!(before.busy_since(before), None);
    }
}
