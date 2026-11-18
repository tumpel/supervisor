use std::path::{Path, PathBuf};
use std::process::Command;

use super::{c_path, protect, victim, Confinement, Victim};

const SERVICE: &str = "system.slice/tumpel-guest.service";

fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/guest-tests")
        .join(name);
    match std::fs::remove_dir_all(&dir) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => panic!("clearing {}: {error}", dir.display()),
        _ => {}
    }
    std::fs::create_dir_all(&dir).expect("the scratch directory is created");
    dir
}

fn fake_roots(name: &str, cgroup: &str, controllers: &str) -> (PathBuf, PathBuf) {
    let root = scratch(name);
    let proc_root = root.join("proc");
    let cgroup_root = root.join("cgroup");
    std::fs::create_dir_all(proc_root.join("self")).expect("the fake proc root is created");
    std::fs::write(proc_root.join("self/oom_score_adj"), "0").expect("the fake score is written");
    std::fs::write(proc_root.join("self/cgroup"), cgroup).expect("the fake cgroup is written");
    std::fs::create_dir_all(cgroup_root.join(SERVICE)).expect("the fake service cgroup is created");
    std::fs::write(cgroup_root.join(SERVICE).join("cgroup.controllers"), controllers)
        .expect("the fake controllers are written");
    (proc_root, cgroup_root)
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("reading {}: {error}", path.display()))
}

#[test]
fn the_agent_leaves_the_oom_killer_and_the_harness_gets_its_own_group_where_only_the_picked_process_dies() {
    let (proc_root, cgroup_root) = fake_roots("memory-group", &format!("0::/{SERVICE}\n"), "cpu memory pids\n");
    let confinement = protect(&proc_root, &cgroup_root, 4242).expect("the guest is protected");
    let own = cgroup_root.join(SERVICE);
    assert_eq!(read(&proc_root.join("self/oom_score_adj")), "-1000");
    assert_eq!(read(&own.join("agent/cgroup.procs")), "4242");
    assert_eq!(read(&own.join("cgroup.subtree_control")), "+memory");
    assert_eq!(
        confinement,
        Confinement::Group {
            score: c_path(&proc_root.join("self/oom_score_adj")).expect("a path"),
            procs: c_path(&own.join("harness/cgroup.procs")).expect("a path"),
        }
    );
}

#[test]
fn without_a_memory_controller_the_harness_only_gets_a_high_oom_score() {
    for (name, cgroup, controllers) in [
        ("memory-no-controller", format!("0::/{SERVICE}\n"), "cpu pids\n"),
        (
            "memory-no-unified",
            format!("4:memory:/{SERVICE}\n"),
            "cpu memory pids\n",
        ),
    ] {
        let (proc_root, cgroup_root) = fake_roots(name, &cgroup, controllers);
        let confinement = protect(&proc_root, &cgroup_root, 4242).expect("the guest is protected");
        assert_eq!(read(&proc_root.join("self/oom_score_adj")), "-1000", "{name}");
        assert!(!cgroup_root.join(SERVICE).join("harness").exists(), "{name}");
        assert_eq!(
            confinement,
            Confinement::Score {
                score: c_path(&proc_root.join("self/oom_score_adj")).expect("a path"),
            },
            "{name}"
        );
    }
}

#[test]
fn a_confined_child_joins_the_harness_group_with_score_500_before_it_runs() {
    let root = scratch("memory-child");
    let score = root.join("oom_score_adj");
    let procs = root.join("cgroup.procs");
    std::fs::write(&score, "-1000").expect("the fake score is written");
    std::fs::write(&procs, "").expect("the fake procs file is written");
    let confinement = Confinement::Group {
        score: c_path(&score).expect("a path"),
        procs: c_path(&procs).expect("a path"),
    };
    let mut command = Command::new("true");
    confinement.apply(&mut command);
    let status = command.status().expect("true runs");
    assert!(status.success());
    assert_eq!(read(&score), "500");
    assert_eq!(read(&procs), "0");
}

#[test]
fn the_oom_kill_of_the_harness_names_the_harness_even_after_an_earlier_kill_of_a_build() {
    let records = [
        "6,1500,8312000,-;rustc invoked oom-killer: gfp_mask=0x140cca(GFP_HIGHUSER_MOVABLE|__GFP_COMP), order=0, oom_score_adj=500\n".to_string(),
        "3,1520,8312450,-;Out of memory: Killed process 1234 (rustc) total-vm:812344kB, anon-rss:546012kB, file-rss:0kB, shmem-rss:0kB, UID:0 pgtables:1400kB oom_score_adj:500\n SUBSYSTEM=memory\n".to_string(),
        "3,1521,8312455,-;Memory cgroup out of memory: Killed process 1100 (claude) total-vm:1200000kB, anon-rss:300000kB, file-rss:0kB, shmem-rss:0kB, UID:0 pgtables:900kB oom_score_adj:500\n".to_string(),
    ];
    assert_eq!(
        victim(&records, 1100),
        Some(Victim {
            process: "claude".to_string(),
            line: "Memory cgroup out of memory: Killed process 1100 (claude) total-vm:1200000kB, anon-rss:300000kB, file-rss:0kB, shmem-rss:0kB, UID:0 pgtables:900kB oom_score_adj:500".to_string(),
        })
    );
    assert_eq!(victim(&records[..2], 1100), None);
}

#[test]
fn a_kernel_log_without_an_oom_kill_names_no_victim() {
    let records = [
        "6,1,100,-;Killed process 99 (sleep) by a signal\n".to_string(),
        "4,2,200,-;Out of memory: Killed process nope (rustc) total-vm:1kB\n".to_string(),
        "6,3,300,-;virtio_blk virtio1: [vda] 4194304 512-byte logical blocks\n".to_string(),
    ];
    assert_eq!(victim(&records, 99), None);
}
