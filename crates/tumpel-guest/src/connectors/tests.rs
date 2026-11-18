use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use tumpel_api::types::{ChannelDispatchConnector, GuestHarnessProtocol};

use super::{arguments, mask_oauth_caches, refuse_reserved, variables, OPENCODE_CONFIG};

fn connector(id: &str) -> ChannelDispatchConnector {
    serde_json::from_value(json!({
        "connectorId": id,
        "url": "https://mcp.example.com/mcp",
        "tokenUrl": format!("https://tumpel.example.com/v1/turns/T1/connectors/{id}/token"),
        "tokenCredential": "c".repeat(43),
    }))
    .expect("the connector reads")
}

#[test]
fn each_harness_sees_the_connectors_as_loopback_mcp_servers_and_no_secret() {
    let connectors = [connector("B2"), connector("A1")];
    let claude = arguments(GuestHarnessProtocol::ClaudeStreamJson, &connectors).expect("Claude Code takes connectors");
    assert_eq!(claude[0], "--mcp-config");
    let config: Value = serde_json::from_str(&claude[1]).expect("the MCP config is JSON");
    assert_eq!(
        config,
        json!({ "mcpServers": {
            "tumpel-A1": { "type": "http", "url": "http://127.0.0.1:47160/connectors/A1" },
            "tumpel-B2": { "type": "http", "url": "http://127.0.0.1:47160/connectors/B2" },
        }})
    );
    assert_eq!(
        arguments(GuestHarnessProtocol::CodexExecJson, &connectors).expect("Codex takes connectors"),
        [
            "-c",
            r#"mcp_servers.tumpel-A1.url="http://127.0.0.1:47160/connectors/A1""#,
            "-c",
            r#"mcp_servers.tumpel-B2.url="http://127.0.0.1:47160/connectors/B2""#,
        ]
    );
    let opencode = variables(GuestHarnessProtocol::PromptOnStdin, &connectors);
    assert_eq!(opencode.len(), 1);
    assert_eq!(opencode[0].0, OPENCODE_CONFIG);
    let config: Value = serde_json::from_str(&opencode[0].1).expect("the OpenCode config is JSON");
    assert_eq!(
        config["mcp"]["tumpel-A1"],
        json!({ "type": "remote", "url": "http://127.0.0.1:47160/connectors/A1", "oauth": false })
    );
    let every = [claude.join(" "), opencode[0].1.clone()].join(" ");
    assert!(
        !every.contains("ccccccccc") && !every.contains("mcp.example.com"),
        "{every}"
    );
    assert_eq!(
        variables(GuestHarnessProtocol::ClaudeStreamJson, &connectors),
        Vec::<(String, String)>::new()
    );
}

#[test]
fn a_pi_session_with_connectors_is_refused_and_one_without_starts() {
    let refused = arguments(GuestHarnessProtocol::PiRpc, &[connector("A1")]).expect_err("Pi takes no connectors");
    assert!(refused.contains("persona without connectors"), "{refused}");
    assert_eq!(arguments(GuestHarnessProtocol::PiRpc, &[]), Ok(Vec::new()));
}

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

#[test]
fn a_persona_mcp_server_that_takes_a_connector_name_refuses_the_turn() {
    let home = scratch("reserved-mcp-names");
    std::fs::create_dir_all(home.join(".codex")).expect("the Codex home is created");
    std::fs::write(
        home.join(".codex/config.toml"),
        "[mcp_servers.\"tumpel-A1\"]\nurl = \"https://other.example.com/mcp\"\n",
    )
    .expect("the persona config is written");
    let refused = refuse_reserved(GuestHarnessProtocol::CodexExecJson, &home, &[connector("A1")])
        .expect_err("the persona takes the connector's name");
    assert!(
        refused.contains(".codex/config.toml") && refused.contains("tumpel-A1"),
        "{refused}"
    );
    assert_eq!(
        refuse_reserved(GuestHarnessProtocol::CodexExecJson, &home, &[connector("B2")]),
        Ok(())
    );
    assert_eq!(
        refuse_reserved(GuestHarnessProtocol::ClaudeStreamJson, &home, &[connector("A1")]),
        Ok(())
    );
}

#[test]
fn a_credential_file_at_a_masked_oauth_store_is_refused_before_anything_is_mounted() {
    let home = scratch("masked-credential-file");
    let refused = mask_oauth_caches(&home, Some(".codex/secrets/mcp_oauth.age")).expect_err("the store stays empty");
    assert!(refused.contains("another path"), "{refused}");
    assert!(!home.join(".codex").exists());
}
