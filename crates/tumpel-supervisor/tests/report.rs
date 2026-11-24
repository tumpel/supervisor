#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::mem::offset_of;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixDatagram;
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};
    use std::time::Duration;

    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use futures_util::{SinkExt, StreamExt};
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde_json::{json, Value};
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;
    use tumpel_api::types::{ChannelProtocol, GuestProtocol, SupervisorCheckReport};

    const BINARY: &str = env!("CARGO_BIN_EXE_tumpel-supervisor");
    const VERSION: &str = env!("TUMPEL_RELEASE_VERSION");

    struct Host {
        root: PathBuf,
        notify: UnixDatagram,
    }

    impl Host {
        fn new(name: &str, endpoint: &str) -> Self {
            let root = std::env::temp_dir().join(format!("tumpel-report-{}-{name}", std::process::id()));
            fs::create_dir(&root).expect("new test host");
            for dir in ["state", "data", "images", "config/systemd/user", "runtime/systemd"] {
                fs::create_dir_all(root.join(dir)).expect("fixture directory");
            }
            let notify =
                UnixDatagram::bind(root.join("runtime/systemd/notify")).expect("the service notification socket");
            notify.set_nonblocking(true).expect("nonblocking notifications");

            let host = Self { root, notify };
            host.state(
                "supervisor.json",
                &json!({"endpoint": endpoint, "hostId":"host-1", "tenantId":"tenant-1"}),
            );
            let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("fixture key");
            let key = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("fixture pair");
            host.state(
                "host-key.json",
                &json!({
                    "privateKey": STANDARD.encode(pkcs8.as_ref()),
                    "publicKey": STANDARD.encode(key.public_key().as_ref()),
                }),
            );
            host.state("channel.json", &json!({"received":0, "sent":0, "unacked":[]}));
            host.state(
                "runtime.json",
                &json!({
                    "dataDir": host.root.join("data"),
                    "imageCacheDir": host.root.join("images"),
                    "heartbeatIntervalSeconds": 1,
                }),
            );
            host
        }

        fn state(&self, name: &str, value: &Value) {
            fs::write(self.root.join("state").join(name), value.to_string()).expect("fixture state");
        }

        fn command(&self, args: &[&str]) -> Command {
            let mut command = Command::new(BINARY);
            command
                .args(args)
                .env_clear()
                .env("TUMPEL_SUPERVISOR_DIR", self.root.join("state"))
                .env("XDG_CONFIG_HOME", self.root.join("config"))
                .env("XDG_RUNTIME_DIR", self.root.join("runtime"))
                .env("NOTIFY_SOCKET", self.root.join("runtime/systemd/notify"))
                .env("PATH", &self.root);
            command
        }

        fn audited_report(&self, args: &[&str]) -> Output {
            let mut command = self.command(args);
            deny_writes_and_connections(&mut command);
            command.output().expect("the report exits")
        }
    }

    impl Drop for Host {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).expect("remove the test host");
        }
    }

    fn write_and_connection_filter() -> Vec<libc::sock_filter> {
        use libc::{
            sock_filter, BPF_ABS, BPF_JEQ, BPF_JGT, BPF_JMP, BPF_JSET, BPF_LD, BPF_RET, SECCOMP_RET_ALLOW,
            SECCOMP_RET_KILL_PROCESS,
        };

        let insn = |code: u32, jt, jf, k| sock_filter {
            code: u16::try_from(code).expect("BPF instruction"),
            jt,
            jf,
            k,
        };
        let deny = insn(BPF_RET, 0, 0, SECCOMP_RET_KILL_PROCESS);
        let mut filter = vec![insn(BPF_LD | BPF_ABS, 0, 0, 0)];
        for syscall in [
            libc::SYS_connect,
            libc::SYS_bind,
            libc::SYS_listen,
            libc::SYS_sendto,
            libc::SYS_sendmsg,
            libc::SYS_mkdirat,
            libc::SYS_unlinkat,
            libc::SYS_renameat,
            libc::SYS_renameat2,
            libc::SYS_linkat,
            libc::SYS_symlinkat,
            libc::SYS_mknodat,
            libc::SYS_fchmod,
            libc::SYS_fchmodat,
            libc::SYS_fchownat,
            libc::SYS_truncate,
            libc::SYS_ftruncate,
            libc::SYS_utimensat,
            libc::SYS_openat2,
        ] {
            filter.push(insn(
                BPF_JMP | BPF_JEQ,
                0,
                1,
                u32::try_from(syscall).expect("syscall number"),
            ));
            filter.push(deny);
        }
        #[cfg(target_arch = "x86_64")]
        for syscall in [
            libc::SYS_creat,
            libc::SYS_mkdir,
            libc::SYS_rmdir,
            libc::SYS_unlink,
            libc::SYS_rename,
            libc::SYS_link,
            libc::SYS_symlink,
            libc::SYS_mknod,
            libc::SYS_chmod,
            libc::SYS_chown,
            libc::SYS_utime,
        ] {
            filter.push(insn(
                BPF_JMP | BPF_JEQ,
                0,
                1,
                u32::try_from(syscall).expect("syscall number"),
            ));
            filter.push(deny);
        }
        let args = u32::try_from(offset_of!(libc::seccomp_data, args)).expect("BPF offset");
        let write_flags =
            libc::O_ACCMODE | libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND | (libc::O_TMPFILE & !libc::O_DIRECTORY);
        let mut check_arg = |syscall, offset, op, value| {
            filter.extend([
                insn(BPF_JMP | BPF_JEQ, 0, 3, u32::try_from(syscall).expect("syscall number")),
                insn(BPF_LD | BPF_ABS, 0, 0, args + offset),
                insn(BPF_JMP | op, 0, 1, value),
                deny,
                insn(BPF_LD | BPF_ABS, 0, 0, 0),
            ]);
        };
        check_arg(
            libc::SYS_openat,
            16,
            BPF_JSET,
            u32::try_from(write_flags).expect("file flags"),
        );
        #[cfg(target_arch = "x86_64")]
        check_arg(
            libc::SYS_open,
            8,
            BPF_JSET,
            u32::try_from(write_flags).expect("file flags"),
        );
        check_arg(libc::SYS_write, 0, BPF_JGT, 2);
        check_arg(libc::SYS_writev, 0, BPF_JGT, 2);
        filter.push(insn(BPF_RET, 0, 0, SECCOMP_RET_ALLOW));
        filter
    }

    fn deny_writes_and_connections(command: &mut Command) {
        let mut filter = write_and_connection_filter();
        let len = u16::try_from(filter.len()).expect("BPF program length");
        unsafe {
            command.pre_exec(move || {
                let program = libc::sock_fprog {
                    len,
                    filter: filter.as_mut_ptr(),
                };
                if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) < 0
                    || libc::prctl(
                        libc::PR_SET_SECCOMP,
                        libc::SECCOMP_MODE_FILTER,
                        &raw const program,
                        0,
                        0,
                    ) < 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    #[test]
    fn a_healthy_report_does_not_write_connect_or_notify_and_both_formats_agree() {
        let host = Host::new("healthy", "http://127.0.0.1:9");
        let human = host.audited_report(&[]);
        assert!(human.status.success(), "{human:?}");
        let human = String::from_utf8(human.stdout).expect("human report");
        let machine = host.audited_report(&["--json"]);
        assert!(machine.status.success(), "{machine:?}");
        let machine: Value = serde_json::from_slice(&machine.stdout).expect("JSON report");

        assert_eq!(machine["overall"], true);
        assert_eq!(machine["version"], VERSION);
        assert!(human.contains(&format!("tumpel-supervisor {VERSION} PASS\n")));
        let digest = machine["binaryDigest"].as_str().expect("binary digest");
        let expected = ring::digest::digest(&ring::digest::SHA256, &fs::read(BINARY).expect("the running binary"));
        let expected = expected.as_ref().iter().fold(String::new(), |mut hex, byte| {
            use std::fmt::Write;
            write!(hex, "{byte:02x}").expect("write hex");
            hex
        });
        assert_eq!(digest, expected);
        assert!(human.contains(&format!("SHA-256 {digest}\n")));
        let checks = machine["checks"].as_object().expect("named checks");
        assert_eq!(checks.len(), 11);
        assert_eq!(checks["manifest"]["status"], "NOT RUN");
        for (name, result) in checks {
            let result = format!(
                "{} {name} - {}\n",
                result["status"].as_str().expect("status"),
                result["reason"].as_str().expect("reason")
            );
            assert!(human.contains(&result), "{result}");
        }

        let legacy = host.audited_report(&["check"]);
        assert!(legacy.status.success(), "{legacy:?}");
        let legacy: SupervisorCheckReport = serde_json::from_slice(&legacy.stdout).expect("legacy response");
        assert_eq!(legacy.supervisor_version.as_str(), VERSION);
        assert_eq!(
            legacy.channel_protocol_version.get(),
            u32::from(ChannelProtocol::VERSION)
        );
        assert_eq!(legacy.guest_protocol_version.get(), u32::from(GuestProtocol::VERSION));
        assert!(!host.root.join("state/boxes.json").exists());
        assert_eq!(
            host.notify.recv(&mut [0; 64]).expect_err("no readiness").kind(),
            io::ErrorKind::WouldBlock
        );

        let manifest = host.root.join("manifest.json");
        let arch = if cfg!(target_arch = "aarch64") {
            "Arm64"
        } else {
            "Amd64"
        };
        let mut release = json!({"releaseVersion":"0.1.0","channelProtocolVersion":ChannelProtocol::VERSION,
        "supervisors":[{"architecture":arch,"sha256":digest,"supervisorVersion":VERSION}]});
        let manifest_args = ["--json", "--manifest", manifest.to_str().expect("manifest path")];
        fs::write(&manifest, release.to_string()).expect("manifest");
        let verified = host.audited_report(&manifest_args);
        assert!(verified.status.success(), "{verified:?}");
        let verified: Value = serde_json::from_slice(&verified.stdout).expect("verified report");
        assert_eq!(verified["checks"]["manifest"]["status"], "PASS");
        release["supervisors"][0]["supervisorVersion"] = json!("999.0.0");
        fs::write(&manifest, release.to_string()).expect("manifest with wrong version");
        assert_eq!(host.audited_report(&manifest_args).status.code(), Some(1));
        release["supervisors"][0]["sha256"] = json!("0".repeat(64));
        fs::write(&manifest, release.to_string()).expect("manifest with wrong digest");
        assert_eq!(host.audited_report(&manifest_args).status.code(), Some(1));
    }

    #[test]
    fn invalid_arguments_and_local_state_exit_with_distinct_failures() {
        let host = Host::new("invalid", "http://127.0.0.1:9");
        for args in [
            &["--daemon", "--json"][..],
            &["--manifest"],
            &["--json", "run"],
            &["run", "--update-binary", BINARY, "--handover"],
            &["--unknown"],
        ] {
            assert_eq!(host.audited_report(args).status.code(), Some(2), "{args:?}");
        }
        host.state("channel.json", &json!({"received":0, "sent":-1, "unacked":[]}));
        host.state("update-trial.json", &json!({"privateKey":"fixture-credential"}));
        host.state(
            "supervisor.json",
            &json!({
                "endpoint":"https://user:fixture-credential@invalid host", "hostId":"host-1", "tenantId":"tenant-1"
            }),
        );
        let invalid_image = host.root.join("images/broken.json");
        let image = host.root.join("images/broken.ext4");
        fs::write(&invalid_image, "{").expect("invalid manifest");
        fs::write(&image, "fixture image").expect("image alongside the invalid manifest");
        let output = host.audited_report(&["--json"]);
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        let stdout = String::from_utf8(output.stdout).expect("report");
        assert!(!stdout.contains("fixture-credential"));
        let parsed: Value = serde_json::from_str(&stdout).expect("JSON report");
        assert_eq!(parsed["overall"], false);
        assert_eq!(parsed["checks"]["channel_state"]["status"], "FAIL");
        assert_eq!(parsed["checks"]["update_state"]["status"], "FAIL");
        assert_eq!(parsed["checks"]["config"]["status"], "FAIL");
        assert_eq!(parsed["checks"]["image_cache"]["status"], "FAIL");
        assert_eq!(parsed["checks"]["runtime"]["status"], "PASS");
        assert!(invalid_image.exists());
        assert!(image.exists());
    }

    #[tokio::test]
    async fn the_old_and_installed_unit_and_legacy_aliases_start_supervision() {
        let old = format!("[Service]\nExecStart={BINARY} run --update-binary {BINARY}\n");
        let mut new = String::new();
        for (index, alias) in ["old", "new", "run", "handover"].into_iter().enumerate() {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("control-plane boundary");
            let endpoint = format!("http://{}", listener.local_addr().expect("listener address"));
            let host = Host::new(alias, &endpoint);
            let systemctl = host.root.join("systemctl");
            fs::write(&systemctl, "#!/bin/sh\ntest \"$*\" = '--user daemon-reload'\n").expect("service operation");
            fs::set_permissions(&systemctl, fs::Permissions::from_mode(0o755)).expect("executable service operation");
            let unit = host.root.join("config/systemd/user/tumpel-supervisor.service");
            let argv: Vec<&str> = match index {
                0 | 1 => {
                    let installed = if index == 0 { &old } else { &new };
                    fs::write(&unit, installed).expect("installed unit");
                    installed
                        .lines()
                        .find_map(|line| line.strip_prefix("ExecStart="))
                        .expect("ExecStart")
                        .split_whitespace()
                        .skip(1)
                        .collect()
                }
                2 => vec!["run"],
                _ => {
                    fs::write(&unit, &old).expect("unit for legacy handover");
                    vec!["run", "--handover"]
                }
            };
            let mut command = tokio::process::Command::from(host.command(&argv));
            let mut daemon = command.kill_on_drop(true).spawn().expect("daemon executable");
            timeout(Duration::from_secs(10), async {
                let (stream, _) = listener.accept().await.expect("the daemon connects");
                let mut ws = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("channel WebSocket");
                ws.send(Message::text(
                    r#"{"type":"Challenge","nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}"#,
                ))
                .await
                .expect("challenge");
                let hello = ws
                    .next()
                    .await
                    .expect("hello")
                    .expect("channel message")
                    .into_text()
                    .expect("hello text");
                let hello: Value = serde_json::from_str(&hello).expect("hello JSON");
                assert_eq!(hello["type"], "Hello");
                ws.send(Message::text(
                    json!({"type":"Welcome","hostId":"host-1","protocolVersion":ChannelProtocol::VERSION}).to_string(),
                ))
                .await
                .expect("welcome");
                let heartbeat = ws
                    .next()
                    .await
                    .expect("heartbeat")
                    .expect("channel message")
                    .into_text()
                    .expect("heartbeat text");
                let heartbeat: Value = serde_json::from_str(&heartbeat).expect("heartbeat JSON");
                assert_eq!(heartbeat["type"], "Heartbeat");
            })
            .await
            .expect("daemon starts and reports its heartbeat");

            let mut notification = [0; 64];
            let received = host.notify.recv(&mut notification).expect("daemon readiness");
            assert_eq!(&notification[..received], b"READY=1");
            assert!(daemon.try_wait().expect("daemon status").is_none());
            daemon.kill().await.expect("stop the test daemon");
            if index == 0 {
                new = fs::read_to_string(&unit).expect("rewritten unit");
                assert!(new.contains("Type=notify\n"));
                assert!(new.contains("KillMode=control-group\n"));
                assert!(new.contains(&format!(
                    "ExecStart={} --daemon\n",
                    Path::new(BINARY).canonicalize().expect("binary").display()
                )));
            }
        }
    }

    #[test]
    fn a_record_with_wrong_socket_paths_fails_the_attachment_check() {
        let host = Host::new("attachment", "http://127.0.0.1:9");
        let image = json!({
            "imageDigest": "a".repeat(64),
            "kernelUrl": "https://example.test/kernel",
            "kernelDigest": "b".repeat(64),
            "baseRootfsUrl": "https://example.test/rootfs.squashfs",
            "baseRootfsDigest": "c".repeat(64),
            "harness": {
                "harness":"Shell", "harnessVersion":"1", "protocol":"ShellLines", "command":["/bin/sh","-s"],
                "workingDirectory":"/root", "environment":{"HOME":"/root","PATH":"/usr/bin:/bin","LANG":"C.UTF-8"},
                "fixedVariables":[], "cloneSeconds":120
            },
            "components": []
        });
        fs::write(
            host.root.join("images").join(format!("{}.json", "a".repeat(64))),
            image.to_string(),
        )
        .expect("cached image manifest");
        host.state(
            "boxes.json",
            &json!({
                "handedAt":"2026-11-16T09:30:00Z",
                "boxes":[{
                    "boxId":"box-1", "boxRetention":"Warm", "dirty":true, "idleMilliseconds":0,
                    "image":{"kind":"Full","manifest":image},
                    "machine":{
                        "apiSocket":"/wrong/api.sock","vsock":"/wrong/v.sock","guestBuffer":"",
                        "consoleTail":"","guestFd":-1,"stdoutFd":-1,"stderrFd":-1,"processId":std::process::id()
                    },
                    "queued":[], "sessionId":"session-1", "tapIndex":0,
                    "state":{"kind":"Turn","turnId":"turn-1","turnMilliseconds":1234,"firstLineMilliseconds":0,
                        "repositoryReady":true,"seenLine":true}
                }]
            }),
        );
        let result = host.audited_report(&["--json"]);
        assert_eq!(result.status.code(), Some(1), "{result:?}");
        let report: Value = serde_json::from_slice(&result.stdout).expect("JSON report");
        assert_eq!(report["checks"]["inventory"]["status"], "PASS", "{report}");
        assert_eq!(report["checks"]["image_cache"]["status"], "PASS");
        assert_eq!(report["checks"]["attachment"]["status"], "FAIL");
        assert!(report["checks"]["attachment"]["reason"]
            .as_str()
            .expect("attachment reason")
            .contains("unexpected socket paths"));
    }
}
