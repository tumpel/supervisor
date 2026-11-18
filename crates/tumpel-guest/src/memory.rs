use std::ffi::{CStr, CString};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Command;

pub const AGENT_SCORE: &str = "-1000";
pub const HARNESS_SCORE: &[u8] = b"500";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Confinement {
    Group { score: CString, procs: CString },
    Score { score: CString },
}

fn c_path(path: &Path) -> Result<CString, String> {
    CString::new(path.as_os_str().as_bytes()).map_err(|error| format!("{} holds a NUL byte: {error}", path.display()))
}

fn write(path: &Path, value: &str) -> Result<(), String> {
    std::fs::write(path, value).map_err(|error| format!("writing {value} to {}: {error}", path.display()))
}

fn create(dir: &Path) -> Result<(), String> {
    match std::fs::create_dir(dir) {
        Err(error) if error.kind() != ErrorKind::AlreadyExists => {
            Err(format!("creating the cgroup {}: {error}", dir.display()))
        }
        _ => Ok(()),
    }
}

fn unified_cgroup(proc_root: &Path) -> Result<Option<String>, String> {
    let path = proc_root.join("self/cgroup");
    let text = std::fs::read_to_string(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    Ok(text
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(|group| group.trim_start_matches('/').to_string()))
}

fn has_memory_controller(group: &Path) -> Result<bool, String> {
    let path = group.join("cgroup.controllers");
    match std::fs::read_to_string(&path) {
        Ok(text) => Ok(text.split_whitespace().any(|controller| controller == "memory")),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("reading {}: {error}", path.display())),
    }
}

pub fn protect(proc_root: &Path, cgroup_root: &Path, pid: u32) -> Result<Confinement, String> {
    let own_score = proc_root.join("self/oom_score_adj");
    write(&own_score, AGENT_SCORE)?;
    let score = c_path(&own_score)?;
    let Some(group) = unified_cgroup(proc_root)? else {
        return Ok(Confinement::Score { score });
    };
    let own = cgroup_root.join(group);
    if !has_memory_controller(&own)? {
        return Ok(Confinement::Score { score });
    }
    let agent = own.join("agent");
    let harness = own.join("harness");
    create(&agent)?;
    create(&harness)?;
    write(&agent.join("cgroup.procs"), &pid.to_string())?;
    write(&own.join("cgroup.subtree_control"), "+memory")?;
    Ok(Confinement::Group {
        score,
        procs: c_path(&harness.join("cgroup.procs"))?,
    })
}

fn write_raw(path: &CStr, value: &[u8]) -> std::io::Result<()> {
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_TRUNC | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let written = unsafe { libc::write(fd, value.as_ptr().cast(), value.len()) };
    let error = std::io::Error::last_os_error();
    unsafe { libc::close(fd) };
    if usize::try_from(written).ok() != Some(value.len()) {
        return Err(error);
    }
    Ok(())
}

impl Confinement {
    pub fn apply(&self, command: &mut Command) {
        let confinement = self.clone();
        unsafe {
            command.pre_exec(move || match &confinement {
                Self::Group { score, procs } => {
                    write_raw(procs, b"0")?;
                    write_raw(score, HARNESS_SCORE)
                }
                Self::Score { score } => write_raw(score, HARNESS_SCORE),
            });
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Victim {
    pub process: String,
    pub line: String,
}

fn record_text(record: &str) -> &str {
    let message = record.split_once(';').map_or(record, |(_, message)| message);
    message.split_once('\n').map_or(message, |(line, _)| line).trim_end()
}

fn killed_process(line: &str) -> Option<(u32, &str)> {
    let (cause, killed) = line.split_once(": Killed process ")?;
    if !cause.ends_with("out of memory") && !cause.ends_with("Out of memory") {
        return None;
    }
    let (pid, rest) = killed.split_once(" (")?;
    let pid = pid.parse::<u32>().ok()?;
    rest.split_once(") total-vm:")
        .map(|(process, _)| process)
        .or_else(|| rest.rsplit_once(')').map(|(process, _)| process))
        .map(|process| (pid, process))
}

pub fn victim(records: &[String], harness_pid: u32) -> Option<Victim> {
    records.iter().map(|record| record_text(record)).find_map(|line| {
        killed_process(line)
            .filter(|(pid, _)| *pid == harness_pid)
            .map(|(_, process)| Victim {
                process: process.to_string(),
                line: line.to_string(),
            })
    })
}

pub fn memory_mib(proc_root: &Path) -> Result<u64, String> {
    let path = proc_root.join("meminfo");
    let text = std::fs::read_to_string(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    text.lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|value| value.trim().strip_suffix("kB"))
        .and_then(|kib| kib.trim().parse::<u64>().ok())
        .map(|kib| kib / 1024)
        .ok_or_else(|| format!("{} has no MemTotal line in kB", path.display()))
}

pub struct KernelLog {
    file: File,
    path: String,
}

impl KernelLog {
    pub fn open(path: &Path) -> Result<Self, String> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .map_err(|error| format!("opening the kernel log {}: {error}", path.display()))?;
        Ok(Self {
            file,
            path: path.display().to_string(),
        })
    }

    pub fn drain(&mut self) -> Result<Vec<String>, String> {
        let mut records = Vec::new();
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            match self.file.read(&mut buffer) {
                Ok(0) => return Ok(records),
                Ok(read) => records.push(String::from_utf8_lossy(&buffer[..read]).into_owned()),
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(records),
                Err(error) if error.kind() == ErrorKind::BrokenPipe => {}
                Err(error) => return Err(format!("reading the kernel log {}: {error}", self.path)),
            }
        }
    }
}

pub struct Memory {
    pub confinement: Confinement,
    pub kernel: KernelLog,
    pub box_mib: u64,
}

impl Memory {
    pub fn guard(proc_root: &Path, cgroup_root: &Path, kernel_log: &Path) -> Result<Self, String> {
        Ok(Self {
            confinement: protect(proc_root, cgroup_root, std::process::id())?,
            kernel: KernelLog::open(kernel_log)?,
            box_mib: memory_mib(proc_root)?,
        })
    }

    pub fn out_of_memory(&self, victim: &Victim, stderr_tail: &str) -> String {
        format!(
            "the box ran out of its {} MiB of memory and the kernel killed the harness {} ({}); give the persona more boxMemoryMib or have it run lighter builds; {stderr_tail}",
            self.box_mib, victim.process, victim.line
        )
    }
}

#[cfg(test)]
mod tests;
