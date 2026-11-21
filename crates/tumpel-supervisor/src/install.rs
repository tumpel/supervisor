use anyhow::{anyhow, bail, Context, Result};
use ring::digest::{digest, SHA256};
use std::fmt::Write as _;
use std::io::ErrorKind;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tumpel_api::types::SupervisorRuntime;

use crate::dir::{self, CONFIG_FILE, RUNTIME_FILE, SUPERVISOR_UNIT};
use crate::enrol;
use crate::pool::TAP_POOL;
use crate::update::{Layout, ROLLBACK_SCRIPT};

const FIRECRACKER_ARCHIVE_URL: &str =
    "https://github.com/firecracker-microvm/firecracker/releases/download/v1.18.0/firecracker-v1.18.0-x86_64.tgz";
const FIRECRACKER_ARCHIVE_DIGEST: &str = "1ac8eec866dd5eea14efecf6323e22b23eea24054545338a5b34bc5bca6623cd";
const FIRECRACKER_MEMBER: &str = "release-v1.18.0-x86_64/firecracker-v1.18.0-x86_64";
const FIRECRACKER_DIGEST: &str = "b33f46cd6d88834cf6a4f8d184f08f219809c636236bffd286d233a60602a2ba";
const NETWORK_DIR: &str = "/etc/systemd/network";
const FILTER_FILE: &str = "/etc/tumpel-guest-filter.nft";
const FILTER_UNIT_FILE: &str = "/etc/systemd/system/tumpel-guest-filter.service";
const FILTER_UNIT: &str = "tumpel-guest-filter.service";
const TAP_UNIT_FILE: &str = "/etc/systemd/system/tumpel-tap-pool.service";
const TAP_UNIT: &str = "tumpel-tap-pool.service";
const NETWORK_MANAGER_CONF: &str = "/etc/NetworkManager/conf.d/90-tumpel-unmanaged.conf";
const NETWORK_MANAGER_TEXT: &str = "[keyfile]\nunmanaged-devices=interface-name:tumpel*\n";
const WAIT_ONLINE: &str = "/usr/lib/systemd/systemd-networkd-wait-online";

pub struct Enrolment {
    pub endpoint: String,
    pub token_file: PathBuf,
}

const FILTER_UNIT_TEXT: &str = "[Unit]
Description=Keep tumpel guests off private networks
DefaultDependencies=no
Before=network-pre.target
Wants=network-pre.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/sbin/nft -f /etc/tumpel-guest-filter.nft
ExecStop=/usr/sbin/nft delete table inet tumpel_guest

[Install]
WantedBy=sysinit.target
";

struct Account {
    name: String,
    uid: u32,
    home: PathBuf,
}

fn sha256(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

fn has_digest(path: &Path, expected: &str) -> Result<bool> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(sha256(&bytes) == expected),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

async fn fetch(url: &str, expected: &str) -> Result<Vec<u8>> {
    let response = reqwest::get(url).await.with_context(|| format!("downloading {url}"))?;
    if !response.status().is_success() {
        bail!(
            "downloading {url} answered {}; check the pinned URL in install.rs",
            response.status()
        );
    }
    let bytes = response.bytes().await.with_context(|| format!("downloading {url}"))?;
    let actual = sha256(&bytes);
    if actual != expected {
        bail!("{url} has digest {actual}, install.rs pins {expected}; refusing a changed artifact");
    }
    Ok(bytes.to_vec())
}

fn place(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", path.display()))?;
    std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    let staged = path.with_extension("part");
    std::fs::write(&staged, bytes).with_context(|| format!("writing {}", staged.display()))?;
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting the mode of {}", staged.display()))?;
    std::fs::rename(&staged, path).with_context(|| format!("moving {} into place", path.display()))
}

fn output(program: &str, args: &[&str]) -> Result<String> {
    let result = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("starting {program}"))?;
    if !result.status.success() {
        bail!(
            "{program} {} failed ({}): {}",
            args.join(" "),
            result.status,
            String::from_utf8_lossy(&result.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&result.stdout).trim().to_string())
}

fn status(program: &str, args: &[&str]) -> Result<()> {
    let result = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("starting {program}"))?;
    if !result.success() {
        bail!("{program} {} failed ({result})", args.join(" "));
    }
    Ok(())
}

fn as_root(args: &[&str]) -> Result<()> {
    let result = Command::new("sudo")
        .args(args)
        .status()
        .with_context(|| format!("starting {} as root", args.join(" ")))?;
    if result.success() {
        return Ok(());
    }
    let still_admin = Command::new("sudo")
        .args(["-n", "true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|probe| probe.success());
    if !still_admin {
        bail!(
            "sudo refused {}; install needs admin rights for its one-time host setup, so rerun it from a terminal or with passwordless sudo",
            args.join(" ")
        );
    }
    bail!("{} exited with {result} as root", args.join(" "))
}

fn invocation() -> Result<Vec<String>> {
    std::env::args_os()
        .map(|arg| {
            arg.into_string().map_err(|arg| {
                anyhow!(
                    "argument {} is not UTF-8; rerun install with plain arguments",
                    arg.display()
                )
            })
        })
        .collect()
}

fn admin_rights(account: &Account, token_on_stdin: bool) -> Result<()> {
    let ready = Command::new("sudo")
        .args(["-n", "true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("starting sudo; install needs it for its one-time host setup")?
        .success();
    if ready {
        return Ok(());
    }
    if std::fs::File::open("/dev/tty").is_ok() {
        return status("sudo", &["-v"]).context("sudo refused; install needs admin rights for its one-time host setup");
    }
    if token_on_stdin {
        bail!(
            "install needs admin rights for its one-time host setup, and sudo wants a password with no terminal to ask on; \
             let {name} use sudo without a password (add the line '{name} ALL=(ALL) NOPASSWD: ALL' with sudo visudo -f /etc/sudoers.d/{name}) \
             and rerun install, or save the token to a file on this host and run install from a terminal with --token-file <path>",
            name = account.name
        );
    }
    let command = invocation()?.join(" ");
    bail!(
        "install needs admin rights for its one-time host setup, and sudo wants a password with no terminal to ask on; \
         run it from a terminal (ssh -t <host> {command}) or let {name} use sudo without a password \
         (add the line '{name} ALL=(ALL) NOPASSWD: ALL' with sudo visudo -f /etc/sudoers.d/{name}) and rerun install",
        name = account.name
    )
}

fn account() -> Result<Account> {
    let uid = std::fs::metadata("/proc/self")
        .context("reading /proc/self for this user's id")?
        .uid();
    if uid == 0 {
        bail!("install runs as root; run it as the user the supervisor runs as, and it asks for admin rights itself");
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is not set; run install from the supervisor user's login shell"))?;
    Ok(Account {
        name: output("id", &["-un"])?,
        uid,
        home,
    })
}

fn put_root_file(staging: &Path, dest: &str, text: &str) -> Result<bool> {
    match std::fs::read_to_string(dest) {
        Ok(current) if current == text => return Ok(false),
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("reading {dest}")),
    }
    let name = Path::new(dest)
        .file_name()
        .ok_or_else(|| anyhow!("{dest} has no file name"))?;
    let staged = staging.join(name);
    std::fs::write(&staged, text).with_context(|| format!("writing {}", staged.display()))?;
    let source = staged.to_string_lossy().to_string();
    as_root(&["install", "-m", "0644", "-o", "root", "-g", "root", &source, dest])?;
    std::fs::remove_file(&staged).with_context(|| format!("removing {}", staged.display()))?;
    Ok(true)
}

fn load_runtime() -> Result<SupervisorRuntime> {
    let dir = dir::supervisor_dir();
    let path = dir.join(RUNTIME_FILE);
    if let Some(current) = dir::read_json::<serde_json::Value>(&path, "JSON")? {
        return migrate_runtime(&path, &current);
    }
    let runtime = dir::default_runtime()?;
    dir::ensure_dir(&dir)?;
    dir::write_json(&dir.join(RUNTIME_FILE), &runtime)?;
    eprintln!(
        "wrote the default {}; the supervisor runs as many boxes as this host's memory holds",
        dir.join(RUNTIME_FILE).display()
    );
    Ok(runtime)
}

fn migrate_runtime(path: &Path, current: &serde_json::Value) -> Result<SupervisorRuntime> {
    let runtime = dir::runtime_with_defaults(path, current)?;
    if &serde_json::to_value(&runtime)? != current {
        dir::write_json(path, &runtime)?;
        eprintln!(
            "updated {} to the current SupervisorRuntime: added missing fields with their defaults and dropped unknown ones",
            path.display()
        );
    }
    Ok(runtime)
}

fn check_kvm(account: &Account) -> Result<()> {
    let Err(error) = std::fs::OpenOptions::new().read(true).write(true).open("/dev/kvm") else {
        return Ok(());
    };
    if error.kind() == ErrorKind::NotFound {
        bail!("/dev/kvm is missing; enable KVM on this machine (nested virtualisation on a VM) and rerun install");
    }
    if error.kind() != ErrorKind::PermissionDenied {
        return Err(error).context("opening /dev/kvm");
    }
    let groups = output("id", &["-nG", &account.name])?;
    if !groups.split_whitespace().any(|group| group == "kvm") {
        as_root(&["usermod", "-aG", "kvm", &account.name])?;
    }
    as_root(&["systemctl", "try-restart", &format!("user@{}.service", account.uid)])?;
    let usable = Command::new("sg")
        .args(["kvm", "-c", "test -r /dev/kvm -a -w /dev/kvm"])
        .status()
        .context("starting sg to check /dev/kvm under group kvm")?
        .success();
    if !usable {
        bail!(
            "{} is in group kvm and /dev/kvm still refuses it; check that ls -l /dev/kvm shows group kvm with mode 0660 and rerun install",
            account.name
        );
    }
    eprintln!(
        "{} joined group kvm; the supervisor gets it from the restarted user manager",
        account.name
    );
    Ok(())
}

async fn install_firecracker(runtime: &SupervisorRuntime, staging: &Path) -> Result<()> {
    let path = PathBuf::from(&*runtime.firecracker_path);
    if !has_digest(&path, FIRECRACKER_DIGEST)? {
        let archive = staging.join("firecracker.tgz");
        std::fs::write(
            &archive,
            fetch(FIRECRACKER_ARCHIVE_URL, FIRECRACKER_ARCHIVE_DIGEST).await?,
        )
        .with_context(|| format!("writing {}", archive.display()))?;
        let result = Command::new("tar")
            .arg("-xzOf")
            .arg(&archive)
            .arg(FIRECRACKER_MEMBER)
            .output()
            .context("starting tar to unpack Firecracker")?;
        std::fs::remove_file(&archive).with_context(|| format!("removing {}", archive.display()))?;
        if !result.status.success() {
            bail!(
                "unpacking {FIRECRACKER_MEMBER} failed ({}): {}",
                result.status,
                String::from_utf8_lossy(&result.stderr).trim()
            );
        }
        let actual = sha256(&result.stdout);
        if actual != FIRECRACKER_DIGEST {
            bail!("{FIRECRACKER_MEMBER} has digest {actual}, install.rs pins {FIRECRACKER_DIGEST}");
        }
        place(&path, &result.stdout, 0o755)?;
    }
    eprintln!("firecracker v1.18.0 at {}", path.display());
    Ok(())
}

const FILTER_TEXT: &str = "table inet tumpel_guest
delete table inet tumpel_guest
table inet tumpel_guest {
\tset private4 {
\t\ttype ipv4_addr
\t\tflags interval
\t\telements = { 10.0.0.0/8, 100.64.0.0/10, 169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16 }
\t}

\tchain forward {
\t\ttype filter hook forward priority filter; policy accept;
\t\tiifname \"tumpel*\" ip saddr != 172.16.0.0/16 counter drop
\t\tiifname \"tumpel*\" ip daddr @private4 counter drop
\t}

\tchain postrouting {
\t\ttype nat hook postrouting priority srcnat; policy accept;
\t\tip saddr 172.16.0.0/16 oifname != \"tumpel*\" masquerade
\t}

\tchain input {
\t\ttype filter hook input priority filter; policy accept;
\t\tiifname \"tumpel*\" counter drop
\t}
}
";

fn install_filter(staging: &Path) -> Result<()> {
    let rules = put_root_file(staging, FILTER_FILE, FILTER_TEXT)?;
    let unit = put_root_file(staging, FILTER_UNIT_FILE, FILTER_UNIT_TEXT)?;
    if unit {
        as_root(&["systemctl", "daemon-reload"])?;
    }
    as_root(&["systemctl", "enable", "--quiet", FILTER_UNIT])?;
    let active = Command::new("systemctl")
        .args(["is-active", "--quiet", FILTER_UNIT])
        .status()
        .context("starting systemctl is-active")?
        .success();
    if rules || unit || !active {
        as_root(&["systemctl", "restart", FILTER_UNIT]).with_context(|| {
            format!("nft load failed: {FILTER_UNIT} could not load {FILTER_FILE}; check journalctl -u {FILTER_UNIT}")
        })?;
    }
    eprintln!("guest filter {FILTER_FILE} loaded for 172.16.0.0/16 by {FILTER_UNIT}");
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct HostNetwork {
    networkd: bool,
    network_manager: bool,
    nmcli: bool,
}

fn host_network(root: &Path, networkd: bool) -> HostNetwork {
    HostNetwork {
        networkd,
        network_manager: root.join("usr/sbin/NetworkManager").exists() || root.join("usr/bin/NetworkManager").exists(),
        nmcli: root.join("usr/bin/nmcli").exists(),
    }
}

fn probe_host_network() -> Result<HostNetwork> {
    let networkd = Command::new("systemctl")
        .args(["is-active", "--quiet", "systemd-networkd.service"])
        .status()
        .context("starting systemctl is-active systemd-networkd")?
        .success();
    Ok(host_network(Path::new("/"), networkd))
}

fn tap_unit_text(boxes: u32, user: &str) -> String {
    let mut unit = String::from(
        "[Unit]
Description=Make the tumpel TAP pool
After=tumpel-guest-filter.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=sysctl -q -w net.ipv4.ip_forward=1
",
    );
    for k in 0..boxes {
        let _ = write!(
            unit,
            "ExecStart=/bin/sh -c 'ip link show dev tumpel{k} >/dev/null 2>&1 || ip tuntap add dev tumpel{k} mode tap user {user}'
ExecStart=ip addr replace 172.16.{k}.1/30 dev tumpel{k}
ExecStart=ip link set dev tumpel{k} up
"
        );
    }
    unit.push_str("\n[Install]\nWantedBy=multi-user.target\n");
    unit
}

fn tap_index(file: &str) -> Option<u32> {
    let rest = file.strip_prefix("50-tumpel")?;
    let index = rest.strip_suffix(".netdev").or_else(|| rest.strip_suffix(".network"))?;
    index.parse().ok()
}

fn link_index(device: &str) -> Option<u32> {
    device.strip_prefix("tumpel")?.parse().ok()
}

fn link_exists(device: &str) -> bool {
    Path::new("/sys/class/net").join(device).exists()
}

fn network_files() -> Result<Vec<(String, u32)>> {
    let entries = match std::fs::read_dir(NETWORK_DIR) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("reading {NETWORK_DIR}")),
    };
    let mut files = Vec::new();
    for entry in entries {
        let name = entry
            .with_context(|| format!("reading {NETWORK_DIR}"))?
            .file_name()
            .to_string_lossy()
            .to_string();
        if let Some(k) = tap_index(&name) {
            files.push((name, k));
        }
    }
    Ok(files)
}

fn install_network_manager_conf(network: &HostNetwork, staging: &Path) -> Result<()> {
    if !network.network_manager {
        return Ok(());
    }
    if !Path::new("/etc/NetworkManager/conf.d").is_dir() {
        as_root(&["mkdir", "-p", "/etc/NetworkManager/conf.d"])?;
    }
    let changed = put_root_file(staging, NETWORK_MANAGER_CONF, NETWORK_MANAGER_TEXT)?;
    if changed && network.nmcli {
        as_root(&["nmcli", "general", "reload"]).context(
            "NetworkManager reload failed: nmcli general reload did not apply the tumpel* unmanaged rule; check journalctl -u NetworkManager",
        )?;
    }
    eprintln!("NetworkManager leaves tumpel* devices alone ({NETWORK_MANAGER_CONF})");
    Ok(())
}

fn networkd_pool(boxes: u32, account: &Account, staging: &Path) -> Result<()> {
    let mut changed = false;
    for k in 0..boxes {
        let device = format!("tumpel{k}");
        let netdev = format!("[NetDev]\nName={device}\nKind=tap\n\n[Tap]\nUser={}\n", account.name);
        let network = format!(
            "[Match]\nName={device}\n\n[Network]\nAddress=172.16.{k}.1/30\nIPMasquerade=ipv4\nConfigureWithoutCarrier=yes\n"
        );
        if put_root_file(staging, &format!("{NETWORK_DIR}/50-{device}.netdev"), &netdev)? && link_exists(&device) {
            as_root(&["networkctl", "delete", &device])?;
        }
        changed |= put_root_file(staging, &format!("{NETWORK_DIR}/50-{device}.network"), &network)?;
        changed |= !link_exists(&device);
    }
    for (name, k) in network_files()? {
        if k < boxes {
            continue;
        }
        as_root(&["rm", "-f", &format!("{NETWORK_DIR}/{name}")])?;
        let device = format!("tumpel{k}");
        if link_exists(&device) {
            as_root(&["networkctl", "delete", &device])?;
        }
        changed = true;
    }
    if changed {
        as_root(&["networkctl", "reload"])
            .context("networkd reload failed: networkctl reload did not apply the TAP pool; check journalctl -u systemd-networkd")?;
    }
    let mut wait = vec!["--timeout=30".to_string()];
    wait.extend((0..boxes).map(|k| format!("--interface=tumpel{k}:no-carrier")));
    let wait: Vec<&str> = wait.iter().map(String::as_str).collect();
    status(WAIT_ONLINE, &wait).with_context(|| {
        format!(
            "the TAP pool did not come up within 30 s; check networkctl status tumpel0 through tumpel{}",
            boxes - 1
        )
    })
}

fn unit_pool(boxes: u32, account: &Account, staging: &Path) -> Result<()> {
    for (name, _) in network_files()? {
        as_root(&["rm", "-f", &format!("{NETWORK_DIR}/{name}")])?;
    }
    let unit = put_root_file(staging, TAP_UNIT_FILE, &tap_unit_text(boxes, &account.name))?;
    if unit {
        as_root(&["systemctl", "daemon-reload"])?;
    }
    as_root(&["systemctl", "enable", "--quiet", TAP_UNIT])?;
    let entries = std::fs::read_dir("/sys/class/net").context("reading /sys/class/net")?;
    for entry in entries {
        let device = entry
            .context("reading /sys/class/net")?
            .file_name()
            .to_string_lossy()
            .to_string();
        if link_index(&device).is_some_and(|k| k >= boxes) {
            as_root(&["ip", "link", "delete", "dev", &device])?;
        }
    }
    let active = Command::new("systemctl")
        .args(["is-active", "--quiet", TAP_UNIT])
        .status()
        .context("starting systemctl is-active")?
        .success();
    let missing = (0..boxes).any(|k| !link_exists(&format!("tumpel{k}")));
    if unit || missing || !active {
        as_root(&["systemctl", "restart", TAP_UNIT]).with_context(|| {
            format!("tuntap unit failed: {TAP_UNIT} did not make the TAP pool; check journalctl -u {TAP_UNIT}")
        })?;
    }
    Ok(())
}

fn install_tap_pool(boxes: u32, account: &Account, staging: &Path) -> Result<()> {
    let network = probe_host_network()?;
    install_network_manager_conf(&network, staging)?;
    if network.networkd {
        networkd_pool(boxes, account, staging)?;
    } else {
        unit_pool(boxes, account, staging)?;
    }
    for k in 0..boxes {
        let owner_file = format!("/sys/class/net/tumpel{k}/owner");
        let owner = std::fs::read_to_string(&owner_file).with_context(|| format!("reading {owner_file}"))?;
        if owner.trim() != account.uid.to_string() {
            bail!(
                "tumpel{k} is owned by uid {}, not {} ({}); delete that device and rerun install",
                owner.trim(),
                account.name,
                account.uid
            );
        }
    }
    eprintln!(
        "TAP pool tumpel0..tumpel{} owned by {}, gateways 172.16.<k>.1/30, made by {}",
        boxes - 1,
        account.name,
        if network.networkd { "systemd-networkd" } else { TAP_UNIT }
    );
    Ok(())
}

pub(crate) fn supervisor_unit_text(binary: &Path) -> String {
    let previous = Layout::new(binary.to_path_buf(), PathBuf::new(), PathBuf::new()).previous;
    format!(
        "[Unit]
Description=tumpel supervisor

[Service]
ExecStartPre=/bin/sh -c '{rollback}' {binary} {previous}
ExecStart={binary} run
ExecStopPost=/bin/sh -c 'if [ \"$$EXIT_CODE\" = exited ] && [ \"$$EXIT_STATUS\" = 6 ]; then sleep 60; fi'
Restart=on-failure
RestartSec=5
RestartPreventExitStatus=3 5 11 12
NotifyAccess=main
FileDescriptorStoreMax={descriptors}
FileDescriptorStorePreserve=yes
TimeoutStopSec=10min

[Install]
WantedBy=default.target
",
        rollback = ROLLBACK_SCRIPT.replace('$', "$$"),
        previous = previous.display(),
        binary = binary.display(),
        descriptors = 3 * u32::from(TAP_POOL)
    )
}

fn install_unit(account: &Account) -> Result<()> {
    if !Path::new("/var/lib/systemd/linger").join(&account.name).exists() {
        as_root(&["loginctl", "enable-linger", &account.name])?;
    }
    let binary = account.home.join(".local").join("bin").join("tumpel-supervisor");
    let current = std::env::current_exe().context("finding this binary")?;
    let same = std::fs::canonicalize(&binary).is_ok_and(|installed| installed == current);
    if !same {
        let bytes = std::fs::read(&current).with_context(|| format!("reading {}", current.display()))?;
        place(&binary, &bytes, 0o755)?;
    }
    let config_home = std::env::var_os("XDG_CONFIG_HOME").map_or_else(|| account.home.join(".config"), PathBuf::from);
    let unit_path = config_home.join("systemd").join("user").join(SUPERVISOR_UNIT);
    let unit = supervisor_unit_text(&binary);
    if std::fs::read_to_string(&unit_path).ok().as_deref() != Some(unit.as_str()) {
        place(&unit_path, unit.as_bytes(), 0o644)?;
    }
    status("systemctl", &["--user", "daemon-reload"])?;
    status("systemctl", &["--user", "enable", "--quiet", SUPERVISOR_UNIT])?;
    Ok(())
}

fn start_unit() -> Result<()> {
    if dir::supervisor_dir().join(CONFIG_FILE).exists() {
        status("systemctl", &["--user", "restart", SUPERVISOR_UNIT])?;
        eprintln!("{SUPERVISOR_UNIT} enabled and restarted");
        return Ok(());
    }
    eprintln!(
        "{SUPERVISOR_UNIT} enabled; enrol and start it with tumpel-supervisor install --endpoint <url> --token-file <path|->"
    );
    Ok(())
}

/// # Errors
/// Fails when a download does not match its pinned digest, a privileged step is refused, KVM is not
/// usable by this user, a host network step (nft load, `NetworkManager` reload, networkd reload or the TAP
/// unit) fails, or the enrolment is refused.
pub async fn install(enrolment: Option<Enrolment>) -> Result<()> {
    if enrolment.is_some() {
        enrol::ensure_unenrolled()?;
    }
    if std::env::consts::ARCH != "x86_64" {
        bail!(
            "this machine is {}; install pins Firecracker for x86_64 only",
            std::env::consts::ARCH
        );
    }
    let account = account()?;
    let token_on_stdin = enrolment
        .as_ref()
        .is_some_and(|enrolment| enrolment.token_file == Path::new("-"));
    admin_rights(&account, token_on_stdin)?;
    check_kvm(&account)?;
    let token = enrolment
        .as_ref()
        .map(|enrolment| enrol::read_token(&enrolment.token_file))
        .transpose()?;
    let runtime = load_runtime()?;
    let staging = dir::supervisor_dir().join("install");
    dir::ensure_dir(&staging)?;
    install_firecracker(&runtime, &staging).await?;
    install_filter(&staging)?;
    install_tap_pool(u32::from(TAP_POOL), &account, &staging)?;
    install_unit(&account)?;
    std::fs::remove_dir(&staging).with_context(|| format!("removing {}", staging.display()))?;
    if let (Some(enrolment), Some(token)) = (enrolment, token) {
        enrol::enrol_with_token(&enrolment.endpoint, token, None).await?;
    }
    start_unit()
}

#[cfg(test)]
mod tests;
