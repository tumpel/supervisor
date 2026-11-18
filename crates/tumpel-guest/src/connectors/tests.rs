use serde_json::{json, Value};
use tumpel_api::types::{ChannelDispatchConnector, GuestHarnessProtocol};

use super::{arguments, variables, OPENCODE_CONFIG};

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
