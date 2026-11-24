use serde_json::json;

use super::{
    host_network, link_index, migrate_runtime, supervisor_unit_text, tap_index, tap_unit_text, HostNetwork,
    FILTER_TEXT, NETWORK_MANAGER_TEXT,
};

#[test]
fn the_supervisor_unit_starts_run_without_a_rollback_line() {
    let unit = supervisor_unit_text(std::path::Path::new("/opt/tumpel/bin/tumpel-supervisor"));
    assert!(unit
        .lines()
        .any(|line| line == "ExecStart=/opt/tumpel/bin/tumpel-supervisor run"));
    assert!(!unit.contains("ExecStartPre="));
}

#[test]
fn an_old_budgets_object_gets_the_missing_field_and_keeps_its_values() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/install-tests");
    std::fs::create_dir_all(&dir).expect("the scratch directory is created");
    let path = dir.join("runtime.json");
    let current = json!({
        "firecrackerPath": "/opt/firecracker",
        "imageCacheDir": "/var/cache/tumpel",
        "dataDir": "/var/lib/tumpel",
        "boxCapacity": 4,
        "warmWindowSeconds": 600,
        "vcpuCount": 2,
        "memoryMib": 1024,
        "overlayMib": 4096,
        "budgets": { "bootSeconds": 45, "turnSeconds": 900, "snapshotSeconds": 90 },
    });
    let runtime = migrate_runtime(&path, &current).expect("the old runtime migrates");
    let written: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("runtime.json is written"))
            .expect("runtime.json is JSON");
    assert_eq!(runtime.budgets.boot_seconds.get(), 45);
    assert_eq!(runtime.budgets.snapshot_seconds.get(), 90);
    assert_eq!(runtime.budgets.first_line_seconds.get(), 120);
    assert_eq!(runtime.box_capacity.get(), 4);
    assert_eq!(written["budgets"]["firstLineSeconds"], 120);
    assert!(written["budgets"].get("turnSeconds").is_none());
}

fn fixture_root(name: &str, files: &[&str]) -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/install-tests")
        .join(name);
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("the old fixture is removed");
    }
    for file in files {
        let path = root.join(file);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("the fixture directory is created");
        std::fs::write(&path, "").expect("the fixture file is written");
    }
    root
}

#[test]
fn a_desktop_with_network_manager_and_no_networkd_takes_the_unit_path_and_unmanages_tumpel_devices() {
    let root = fixture_root("desktop", &["usr/sbin/NetworkManager", "usr/bin/nmcli"]);
    assert_eq!(
        host_network(&root, false),
        HostNetwork {
            networkd: false,
            network_manager: true,
            nmcli: true
        }
    );
    assert_eq!(
        NETWORK_MANAGER_TEXT,
        "[keyfile]\nunmanaged-devices=interface-name:tumpel*\n"
    );
}

#[test]
fn a_server_with_networkd_and_no_network_manager_keeps_the_networkd_path() {
    let root = fixture_root("server", &["usr/lib/systemd/systemd-networkd"]);
    assert_eq!(
        host_network(&root, true),
        HostNetwork {
            networkd: true,
            network_manager: false,
            nmcli: false
        }
    );
}

#[test]
fn the_tap_unit_makes_each_device_once_for_the_supervisor_user_with_its_gateway() {
    let unit = tap_unit_text(2, "builder");
    assert!(unit.contains("Type=oneshot\nRemainAfterExit=yes\nExecStart=sysctl -q -w net.ipv4.ip_forward=1\n"));
    assert!(unit.contains(
        "ExecStart=/bin/sh -c 'ip link show dev tumpel1 >/dev/null 2>&1 || ip tuntap add dev tumpel1 mode tap user builder'\n"
    ));
    assert!(
        unit.contains("ExecStart=ip addr replace 172.16.1.1/30 dev tumpel1\nExecStart=ip link set dev tumpel1 up\n")
    );
    assert!(!unit.contains("tumpel2"));
    assert!(unit.ends_with("[Install]\nWantedBy=multi-user.target\n"));
}

#[test]
fn the_filter_masquerades_guest_traffic_leaving_the_host() {
    assert!(FILTER_TEXT.contains("type nat hook postrouting priority srcnat; policy accept;"));
    assert!(FILTER_TEXT.contains("ip saddr 172.16.0.0/16 oifname != \"tumpel*\" masquerade"));
}

#[test]
fn only_numbered_tumpel_links_and_networkd_files_count_as_pool_members() {
    assert_eq!(link_index("tumpel12"), Some(12));
    assert_eq!(link_index("tumpelx"), None);
    assert_eq!(link_index("eth0"), None);
    assert_eq!(tap_index("50-tumpel3.netdev"), Some(3));
    assert_eq!(tap_index("50-tumpel3.network"), Some(3));
    assert_eq!(tap_index("10-eth0.network"), None);
}
