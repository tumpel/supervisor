use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use tumpel_api::types::{ChannelDispatchConnector, GuestHarnessProtocol};

use super::{arguments, mask_oauth_caches, pi_config, refuse_reserved, servers, variables, OPENCODE_CONFIG};

fn connector(id: &str, name: &str) -> ChannelDispatchConnector {
    serde_json::from_value(json!({
        "connectorId": id,
        "name": name,
        "url": "https://mcp.example.com/mcp",
        "tokenUrl": format!("https://tumpel.example.com/v1/turns/T1/connectors/{id}/token"),
        "tokenCredential": "c".repeat(43),
    }))
    .expect("the connector reads")
}

#[test]
fn each_harness_sees_the_connectors_as_loopback_mcp_servers_and_no_secret() {
    let connectors = [connector("B2", "Linear"), connector("A1", "GitMCP Docs")];
    let claude = arguments(&GuestHarnessProtocol::ClaudeStreamJson, &connectors);
    assert_eq!(claude[0], "--mcp-config");
    let config: Value = serde_json::from_str(&claude[1]).expect("the MCP config is JSON");
    assert_eq!(
        config,
        json!({ "mcpServers": {
            "gitmcp-docs": { "type": "http", "url": "http://127.0.0.1:47160/connectors/A1" },
            "linear": { "type": "http", "url": "http://127.0.0.1:47160/connectors/B2" },
        }})
    );
    assert_eq!(
        arguments(&GuestHarnessProtocol::CodexExecJson, &connectors),
        [
            "-c",
            r#"mcp_servers.gitmcp-docs.url="http://127.0.0.1:47160/connectors/A1""#,
            "-c",
            r#"mcp_servers.linear.url="http://127.0.0.1:47160/connectors/B2""#,
        ]
    );
    let opencode = variables(&GuestHarnessProtocol::PromptOnStdin, &connectors);
    assert_eq!(opencode.len(), 1);
    assert_eq!(opencode[0].0, OPENCODE_CONFIG);
    let config: Value = serde_json::from_str(&opencode[0].1).expect("the OpenCode config is JSON");
    assert_eq!(
        config["mcp"]["gitmcp-docs"],
        json!({ "type": "remote", "url": "http://127.0.0.1:47160/connectors/A1", "oauth": false })
    );
    let every = [claude.join(" "), opencode[0].1.clone()].join(" ");
    assert!(
        !every.contains("ccccccccc") && !every.contains("mcp.example.com"),
        "{every}"
    );
    assert_eq!(
        variables(&GuestHarnessProtocol::ClaudeStreamJson, &connectors),
        Vec::<(String, String)>::new()
    );
}

#[test]
fn pi_sees_the_connectors_beside_the_persona_mcp_servers_in_its_runtime_mcp_json() {
    let connectors = [connector("B2", "Linear"), connector("A1", "GitMCP Docs")];
    let persona =
        r#"{ "autoEnableCodemode": false, "mcpServers": { "docs": { "url": "https://docs.example.com/mcp" } } }"#;
    let config: Value = serde_json::from_str(&pi_config(Some(persona), &connectors).expect("the config merges"))
        .expect("the Pi config is JSON");
    assert_eq!(
        config,
        json!({
            "autoEnableCodemode": false,
            "mcpServers": {
                "docs": { "url": "https://docs.example.com/mcp" },
                "gitmcp-docs": { "url": "http://127.0.0.1:47160/connectors/A1" },
                "linear": { "url": "http://127.0.0.1:47160/connectors/B2" },
            }
        })
    );
    let fresh: Value =
        serde_json::from_str(&pi_config(None, &connectors[1..]).expect("a config without a persona one"))
            .expect("the Pi config is JSON");
    assert_eq!(
        fresh,
        json!({ "mcpServers": { "gitmcp-docs": { "url": "http://127.0.0.1:47160/connectors/A1" } } })
    );
    let broken = pi_config(Some("{ mcpServers: "), &connectors).expect_err("a persona config that is not JSON");
    assert!(broken.contains(".pi/agent/mcp.json"), "{broken}");
    assert_eq!(
        arguments(&GuestHarnessProtocol::PiRpc, &connectors),
        Vec::<String>::new()
    );
}

#[test]
fn a_connector_name_becomes_a_server_name_every_harness_accepts_and_a_clash_takes_a_suffix() {
    let unnamed: ChannelDispatchConnector = serde_json::from_value(json!({
        "connectorId": "C3",
        "url": "https://mcp.example.com/mcp",
        "tokenUrl": "https://tumpel.example.com/token",
        "tokenCredential": "c".repeat(43),
    }))
    .expect("a connector from a control plane that sends no name reads");
    let keys: Vec<String> = servers(&[
        connector("B2", "Docs"),
        connector("A1", "docs!"),
        connector("D4", "Ünïcødé ✨ Server"),
        connector("E5", "✨"),
        connector("F6", &"x".repeat(40)),
        unnamed,
    ])
    .into_iter()
    .map(|(key, _)| key)
    .collect();
    assert_eq!(
        keys,
        [
            "c3",
            "connector",
            "docs",
            "docs-2",
            "n-c-d-server",
            "x".repeat(32).as_str()
        ]
    );
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
        "[mcp_servers.linear]\nurl = \"https://other.example.com/mcp\"\n",
    )
    .expect("the persona config is written");
    let persona = [".codex/config.toml"];
    let refused = refuse_reserved(
        &GuestHarnessProtocol::CodexExecJson,
        &home,
        &persona,
        &[connector("A1", "Linear")],
    )
    .expect_err("the persona takes the connector's name");
    assert!(
        refused.contains(".codex/config.toml") && refused.contains("linear"),
        "{refused}"
    );
    assert_eq!(
        refuse_reserved(
            &GuestHarnessProtocol::CodexExecJson,
            &home,
            &persona,
            &[connector("B2", "Linear Docs")]
        ),
        Ok(())
    );
    assert_eq!(
        refuse_reserved(
            &GuestHarnessProtocol::CodexExecJson,
            &home,
            &[],
            &[connector("A1", "Linear")]
        ),
        Ok(()),
        "a config the persona does not ship, such as one the box generated, is not the persona's"
    );
    std::fs::create_dir_all(home.join(".pi/agent")).expect("the Pi agent directory is created");
    std::fs::write(
        home.join(".pi/agent/mcp.json"),
        r#"{"mcpServers":{"linear":{"url":"https://x"}}}"#,
    )
    .expect("the persona Pi config is written");
    let pi = refuse_reserved(
        &GuestHarnessProtocol::PiRpc,
        &home,
        &[".pi/agent/mcp.json"],
        &[connector("A1", "Linear")],
    )
    .expect_err("the Pi persona takes the connector's name");
    assert!(pi.contains(".pi/agent/mcp.json"), "{pi}");
    assert_eq!(
        refuse_reserved(
            &GuestHarnessProtocol::ClaudeStreamJson,
            &home,
            &persona,
            &[connector("A1", "Linear")]
        ),
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
