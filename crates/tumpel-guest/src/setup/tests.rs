use std::ffi::CString;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use tumpel_api::types::{GuestClaudeSetupStart, GuestHarness, GuestMessage};
use tumpel_guest::{encode, Decoder};

use super::run;
use crate::memory::Confinement;

const PROMPT: &str = "printf '\\nPlease visit the following URL to authorize the application:\\n\\nhttps://accounts.google.com/o/oauth2/v2/auth?code_challenge=x1Y2&state=s7&client_id=681255809395.apps.googleusercontent.com\\n\\nEnter the authorization code: '\nread code\n";

fn home(name: &str, script: &str) -> PathBuf {
    let home = std::env::temp_dir().join(format!("tumpel-setup-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).expect("the test home is created");
    let gemini = home.join("gemini");
    std::fs::write(&gemini, format!("#!/bin/sh\n{script}")).expect("the fake gemini is written");
    std::fs::set_permissions(&gemini, std::fs::Permissions::from_mode(0o755)).expect("the fake gemini is executable");
    home
}

fn token_url() -> (String, std::thread::JoinHandle<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("the token URL binds");
    let port = listener.local_addr().expect("the token URL has an address").port();
    let posted = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("the token URL accepts");
        let mut reader = BufReader::new(stream);
        let mut length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("the request header reads");
            if line.trim().is_empty() {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse().expect("the content length is a number");
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).expect("the request body reads");
        reader
            .get_mut()
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}")
            .expect("the token URL answers");
        serde_json::from_slice(&body).expect("the request body is JSON")
    });
    (format!("http://127.0.0.1:{port}/token"), posted)
}

fn setup(home: &Path, token_url: &str, mint_budget_seconds: u32) -> Vec<Value> {
    let config: GuestHarness = serde_json::from_value(json!({
        "harness": "Gemini", "harnessVersion": "0.71.1", "protocol": "GeminiStreamJson",
        "command": [home.join("gemini")], "workingDirectory": home, "cloneSeconds": 120,
        "environment": { "PATH": "/usr/bin:/bin", "HOME": home, "LANG": "C.UTF-8" },
        "fixedVariables": [{ "name": "GOOGLE_GENAI_USE_GCA", "value": "true" }],
    }))
    .expect("the test config matches GuestHarness");
    let start: GuestClaudeSetupStart = serde_json::from_value(json!({
        "type": "ClaudeSetupStart", "claudeSetupId": "setup-1", "tenantId": "tenant-1",
        "tokenUrl": token_url, "tokenCredential": "c".repeat(43),
    }))
    .expect("the test start matches GuestClaudeSetupStart");
    let score = home.join("oom_score_adj");
    std::fs::write(&score, "0").expect("the score file is written");
    let confinement = Confinement::Score {
        score: CString::new(score.to_string_lossy().as_bytes()).expect("the score path has no NUL"),
    };
    let (guest, mut host) = UnixStream::pair().expect("the host channel opens");
    let code = json!({ "type": "ClaudeSetupCode", "claudeSetupId": "setup-1", "code": "4/0Ab-code", "mintBudgetSeconds": mint_budget_seconds });
    host.write_all(&encode(&code).expect("the code encodes"))
        .expect("the host sends the code");
    let mut stream = File::from(OwnedFd::from(guest));
    run(&start, &config, &confinement, &mut stream, &mut Decoder::default()).expect("the setup runs");
    drop(stream);
    let mut wire = Vec::new();
    host.read_to_end(&mut wire).expect("the host reads the guest's frames");
    let mut decoder = Decoder::default();
    decoder.push(&wire);
    let mut sent = Vec::new();
    while let Some(message) = decoder.take::<GuestMessage>().expect("the frames decode") {
        let mut message = serde_json::to_value(message).expect("the frame encodes");
        if let Some(fields) = message.as_object_mut() {
            fields.remove("claudeSetupId");
        }
        sent.push(message);
    }
    sent
}

#[test]
fn a_gemini_sign_in_types_the_code_and_posts_oauth_creds_without_an_expiry() {
    let home = home(
        "signed-in",
        &format!("printf '%s\\n' \"NO_BROWSER=$NO_BROWSER\" \"$(cat \"$HOME/.gemini/settings.json\")\" > \"$HOME/seen\"\n{PROMPT}printf '%s\\n' \"$code\" >> \"$HOME/seen\"\nprintf '%s' '{{\"access_token\":\"ya29.a\",\"refresh_token\":\"1//r\",\"expiry_date\":1795000000000}}' > \"$HOME/.gemini/oauth_creds.json\"\nexec sleep 60\n"),
    );
    let (url, posted) = token_url();
    let sent = setup(&home, &url, 30);
    assert_eq!(
        sent,
        vec![
            json!({ "type": "ClaudeSetupUrl", "signInUrl": "https://accounts.google.com/o/oauth2/v2/auth?code_challenge=x1Y2&state=s7&client_id=681255809395.apps.googleusercontent.com" }),
            json!({ "type": "ClaudeSetupStored" }),
        ]
    );
    assert_eq!(
        std::fs::read_to_string(home.join("seen")).expect("the fake gemini wrote what it saw"),
        "NO_BROWSER=true\n{\"security\":{\"auth\":{\"selectedType\":\"oauth-personal\"}}}\n4/0Ab-code\n"
    );
    assert_eq!(
        posted.join().expect("the token URL saw the post"),
        json!({ "tenantId": "tenant-1", "value": "{\"access_token\":\"ya29.a\",\"refresh_token\":\"1//r\",\"expiry_date\":1795000000000}" })
    );
}

#[test]
fn oauth_creds_without_a_refresh_token_fail_the_setup() {
    let home = home(
        "no-refresh-token",
        &format!("{PROMPT}printf '%s' '{{\"access_token\":\"ya29.a\"}}' > \"$HOME/.gemini/oauth_creds.json\"\nexec sleep 60\n"),
    );
    let sent = setup(&home, "http://127.0.0.1:9/token", 1);
    assert_eq!(sent[1]["type"], "ClaudeSetupFailed");
    assert_eq!(sent[1]["reason"], "MintTimedOut");
}

#[test]
fn a_rejected_code_fails_the_setup_and_stops_gemini() {
    let home = home(
        "rejected",
        &format!("echo $$ > \"$HOME/pid\"\n{PROMPT}echo 'Failed to authenticate with authorization code: invalid_grant'\nexec sleep 60\n"),
    );
    let sent = setup(&home, "http://127.0.0.1:9/token", 30);
    assert_eq!(sent[1]["type"], "ClaudeSetupFailed");
    assert_eq!(sent[1]["reason"], "CodeRejected");
    let pid = std::fs::read_to_string(home.join("pid")).expect("the fake gemini wrote its pid");
    assert!(
        !Path::new(&format!("/proc/{}", pid.trim())).exists(),
        "gemini is stopped"
    );
}
