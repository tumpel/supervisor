use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use serde_json::json;
use tumpel_api::types::ChannelDispatchConnector;

use super::{destination, fresh, parse_head, route, Forwarder, Token, PORT};

const CONNECTOR: &str = "01HZY8Q6V0M1C2D3E4F5G6H7J8";

fn upstream(answers: Vec<(&'static str, String)>) -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("the fake upstream binds");
    let port = listener.local_addr().expect("the fake upstream has an address").port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("the fake upstream accepts");
            let request = read_request(&mut stream);
            let path = request.split_whitespace().nth(1).unwrap_or_default().to_string();
            recorded.lock().expect("the record lock").push(request);
            let answer = answers.iter().find(|(prefix, _)| path.starts_with(prefix)).map_or_else(
                || "HTTP/1.1 404 Not Found\r\nconnection: close\r\ncontent-length: 0\r\n\r\n".to_string(),
                |(_, answer)| answer.clone(),
            );
            stream.write_all(answer.as_bytes()).expect("the fake upstream answers");
        }
    });
    (port, seen)
}

fn request_complete(bytes: &[u8]) -> bool {
    let text = String::from_utf8_lossy(bytes);
    let Some((head, body)) = text.split_once("\r\n\r\n") else {
        return false;
    };
    let length = head
        .lines()
        .find_map(|line| {
            line.to_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>())
        })
        .map_or(0, |length| length.expect("a numeric content-length"));
    body.len() >= length
}

fn connector(upstream: u16) -> ChannelDispatchConnector {
    serde_json::from_value(json!({
        "connectorId": CONNECTOR,
        "url": format!("http://127.0.0.1:{upstream}/mcp"),
        "tokenUrl": format!("http://127.0.0.1:{upstream}/token"),
        "tokenCredential": "c".repeat(43),
    }))
    .expect("the connector reads")
}

fn call(port: u16, extra: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("the forwarder accepts");
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
    write!(
        stream,
        "POST /connectors/{CONNECTOR} HTTP/1.1\r\nhost: 127.0.0.1:{port}\r\ncontent-type: application/json\r\nmcp-session-id: s-1\r\n{extra}content-length: {}\r\n\r\n{body}",
        body.len()
    )
    .expect("the request writes");
    let mut answer = String::new();
    stream.read_to_string(&mut answer).expect("the answer reads");
    answer
}

fn token_answer(token: &str) -> String {
    let body =
        json!({ "accessToken": token, "expiresAt": (Utc::now() + TimeDelta::hours(1)).to_rfc3339() }).to_string();
    format!(
        "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

#[test]
fn a_token_is_fetched_again_once_fewer_than_30_seconds_remain() {
    let now = Utc::now();
    let expiring = |seconds| Token {
        value: "t".to_string(),
        expires_at: Some((now + TimeDelta::seconds(seconds)).fixed_offset()),
    };
    assert!(fresh(&expiring(31), now));
    assert!(!fresh(&expiring(30), now));
    assert!(fresh(
        &Token {
            value: "t".to_string(),
            expires_at: None
        },
        now
    ));
}

#[test]
fn a_browser_and_a_call_outside_a_turn_are_refused() {
    let (_forwarder, forwarder_port) = Forwarder::listen(0, Duration::from_secs(10)).expect("the forwarder starts");
    let browser = call(forwarder_port, "origin: http://127.0.0.1:3000\r\n");
    assert!(browser.starts_with("HTTP/1.1 403"), "{browser}");
    let idle = call(forwarder_port, "");
    assert!(idle.starts_with("HTTP/1.1 409"), "{idle}");
    assert!(idle.contains("ConnectorTurnNotRunning"), "{idle}");
}

#[test]
fn a_call_carries_the_fetched_token_upstream_and_streams_the_answer_back() {
    let (port, seen) = upstream(vec![
        ("/token", token_answer("access-1")),
        (
            "/mcp",
            "HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: text/event-stream\r\nmcp-session-id: s-1\r\ncontent-length: 23\r\n\r\ndata: {\"result\":\"ok\"}\n\n".to_string(),
        ),
    ]);
    let (forwarder, forwarder_port) = Forwarder::listen(0, Duration::from_secs(10)).expect("the forwarder starts");
    forwarder.begin(&[connector(port)]).expect("the turn begins");
    let first = call(forwarder_port, "");
    let second = call(forwarder_port, "");
    assert!(first.starts_with("HTTP/1.1 200"), "{first}");
    assert!(first.contains("content-type: text/event-stream"), "{first}");
    assert!(first.contains("data: {\"result\":\"ok\"}"), "{first}");
    assert!(second.starts_with("HTTP/1.1 200"), "{second}");
    let seen = seen.lock().expect("the record lock");
    let fetches: Vec<&String> = seen
        .iter()
        .filter(|request| request.starts_with("POST /token"))
        .collect();
    assert_eq!(fetches.len(), 1, "{seen:?}");
    assert!(
        fetches[0].contains(&format!("authorization: Bearer {}", "c".repeat(43))),
        "{seen:?}"
    );
    let calls: Vec<&String> = seen.iter().filter(|request| request.starts_with("POST /mcp")).collect();
    assert_eq!(calls.len(), 2, "{seen:?}");
    assert!(calls[0].contains("authorization: Bearer access-1"), "{seen:?}");
    assert!(calls[0].contains("mcp-session-id: s-1"), "{seen:?}");
    assert!(!calls[0].contains(&format!("127.0.0.1:{forwarder_port}")), "{seen:?}");
}

#[test]
fn an_upstream_refusal_fails_the_call_without_the_oauth_challenge() {
    let (port, _) = upstream(vec![
        ("/token", token_answer("access-1")),
        (
            "/mcp",
            "HTTP/1.1 401 Unauthorized\r\nconnection: close\r\nwww-authenticate: Bearer resource_metadata=\"https://x/.well-known\"\r\ncontent-length: 0\r\n\r\n".to_string(),
        ),
    ]);
    let (forwarder, forwarder_port) = Forwarder::listen(0, Duration::from_secs(10)).expect("the forwarder starts");
    forwarder.begin(&[connector(port)]).expect("the turn begins");
    let refused = call(forwarder_port, "");
    assert!(refused.starts_with("HTTP/1.1 502"), "{refused}");
    assert!(refused.contains("ConnectorAuthRefused"), "{refused}");
    assert!(!refused.to_lowercase().contains("www-authenticate"), "{refused}");
    forwarder.end().expect("the turn ends");
    assert!(call(forwarder_port, "").contains("ConnectorTurnNotRunning"));
}

fn read_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 8192];
    while !request_complete(&bytes) {
        let read = stream.read(&mut buffer).expect("the fake upstream reads");
        assert!(read > 0, "the request ended early");
        bytes.extend_from_slice(&buffer[..read]);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn token_server(listener: &TcpListener) {
    let mut stream = listener
        .incoming()
        .next()
        .expect("a token fetch")
        .expect("the token fetch connects");
    read_request(&mut stream);
    stream
        .write_all(token_answer("access-1").as_bytes())
        .expect("the token answers");
}

#[test]
fn an_upstream_that_sends_no_first_byte_within_the_budget_fails_the_call_unreachable() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("the fake upstream binds");
    let port = listener.local_addr().expect("the fake upstream has an address").port();
    let (keep, kept) = mpsc::channel::<TcpStream>();
    std::thread::spawn(move || {
        token_server(&listener);
        let mut stream = listener.incoming().next().expect("a call").expect("the call connects");
        read_request(&mut stream);
        keep.send(stream).expect("the silent upstream holds its connection");
    });
    let (forwarder, forwarder_port) = Forwarder::listen(0, Duration::from_millis(300)).expect("the forwarder starts");
    forwarder.begin(&[connector(port)]).expect("the turn begins");
    let answer = call(forwarder_port, "");
    assert!(answer.starts_with("HTTP/1.1 502"), "{answer}");
    assert!(answer.contains("ConnectorUnreachable"), "{answer}");
    drop(kept);
}

#[test]
fn a_harness_that_hangs_up_mid_stream_closes_the_upstream_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("the fake upstream binds");
    let port = listener.local_addr().expect("the fake upstream has an address").port();
    let (closed, upstream_closed) = mpsc::channel();
    std::thread::spawn(move || {
        token_server(&listener);
        let mut stream = listener.incoming().next().expect("a call").expect("the call connects");
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n6\r\ndata:\n\r\n")
            .expect("the stream starts");
        let ended = loop {
            std::thread::sleep(Duration::from_millis(20));
            if let Err(error) = stream.write_all(b"2\r\n:\n\r\n").and_then(|()| stream.flush()) {
                break error;
            }
        };
        closed
            .send(ended.kind())
            .expect("the upstream reports its stream closed");
    });
    let (forwarder, forwarder_port) = Forwarder::listen(0, Duration::from_secs(10)).expect("the forwarder starts");
    forwarder.begin(&[connector(port)]).expect("the turn begins");
    let mut harness = TcpStream::connect(("127.0.0.1", forwarder_port)).expect("the forwarder accepts");
    write!(
        harness,
        "GET /connectors/{CONNECTOR} HTTP/1.1\r\nhost: 127.0.0.1:{forwarder_port}\r\naccept: text/event-stream\r\n\r\n"
    )
    .expect("the request writes");
    let mut first = [0u8; 64];
    let read = harness.read(&mut first).expect("the stream answers");
    assert!(String::from_utf8_lossy(&first[..read]).starts_with("HTTP/1.1 200"));
    drop(harness);
    let kind = upstream_closed
        .recv_timeout(Duration::from_secs(5))
        .expect("the upstream stream closes once the harness hangs up");
    assert!(
        matches!(
            kind,
            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
        ),
        "{kind:?}"
    );
}

#[test]
fn only_a_public_https_destination_takes_a_credential() {
    assert!(destination("https://mcp.example.com/mcp", "connector URL", "A1").is_ok());
    for refused in [
        "http://mcp.example.com/mcp",
        "https://10.1.2.3/mcp",
        "https://169.254.169.254/token",
        "https://[fd00::1]/mcp",
        "ftp://mcp.example.com/mcp",
    ] {
        let error = destination(refused, "connector URL", "A1").expect_err(refused);
        assert!(error.contains("public HTTPS"), "{error}");
    }
}

#[test]
fn an_upstream_error_or_foreign_redirect_reaches_the_harness_only_as_a_problem() {
    let (port, _) = upstream(vec![
        ("/token", token_answer("access-1")),
        (
            "/mcp",
            "HTTP/1.1 500 Internal Server Error\r\nconnection: close\r\nx-provider-trace: abc\r\ncontent-type: text/html\r\ncontent-length: 23\r\n\r\n<pre>stack trace</pre>\n".to_string(),
        ),
    ]);
    let (forwarder, forwarder_port) = Forwarder::listen(0, Duration::from_secs(10)).expect("the forwarder starts");
    forwarder.begin(&[connector(port)]).expect("the turn begins");
    let failed = call(forwarder_port, "");
    assert!(failed.starts_with("HTTP/1.1 500"), "{failed}");
    assert!(failed.contains("ConnectorUnreachable"), "{failed}");
    assert!(
        !failed.contains("stack trace") && !failed.contains("x-provider-trace"),
        "{failed}"
    );
    forwarder.end().expect("the turn ends");

    let (port, _) = upstream(vec![
        ("/token", token_answer("access-1")),
        (
            "/mcp",
            "HTTP/1.1 307 Temporary Redirect\r\nconnection: close\r\nlocation: https://elsewhere.example.com/mcp\r\ncontent-length: 0\r\n\r\n".to_string(),
        ),
    ]);
    forwarder.begin(&[connector(port)]).expect("the next turn begins");
    let redirected = call(forwarder_port, "");
    assert!(redirected.starts_with("HTTP/1.1 502"), "{redirected}");
    assert!(!redirected.contains("elsewhere.example.com"), "{redirected}");
}

#[test]
fn a_turn_end_cancels_its_calls_and_a_late_token_never_reaches_the_next_turn() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("the fake upstream binds");
    let port = listener.local_addr().expect("the fake upstream has an address").port();
    let (fetched, first_fetch) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    std::thread::spawn(move || {
        let mut slow = listener
            .incoming()
            .next()
            .expect("turn A's fetch")
            .expect("it connects");
        read_request(&mut slow);
        fetched.send(()).expect("the first fetch is reported");
        let mut fast = listener
            .incoming()
            .next()
            .expect("turn B's fetch")
            .expect("it connects");
        read_request(&mut fast);
        fast.write_all(token_answer("access-B").as_bytes())
            .expect("turn B's token answers");
        let mut call = listener.incoming().next().expect("turn B's call").expect("it connects");
        recorded.lock().expect("the record lock").push(read_request(&mut call));
        call.write_all(
            b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{}",
        )
        .expect("turn B's call answers");
        released.recv().expect("the test releases turn A's fetch");
        if let Err(error) = slow.write_all(token_answer("access-A").as_bytes()) {
            assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ),
                "{error}"
            );
        }
    });
    let (forwarder, forwarder_port) = Forwarder::listen(0, Duration::from_secs(10)).expect("the forwarder starts");
    forwarder.begin(&[connector(port)]).expect("turn A begins");
    let turn_a = std::thread::spawn(move || call(forwarder_port, ""));
    first_fetch
        .recv_timeout(Duration::from_secs(5))
        .expect("turn A fetches a token");
    forwarder.end().expect("turn A ends at once, its call cancelled");
    assert!(!turn_a.join().expect("turn A's caller").starts_with("HTTP/1.1 200"));
    forwarder.begin(&[connector(port)]).expect("turn B begins");
    let turn_b = call(forwarder_port, "");
    release.send(()).expect("turn A's fetch is released");
    assert!(turn_b.starts_with("HTTP/1.1 200"), "{turn_b}");
    let seen = seen.lock().expect("the record lock");
    assert!(seen[0].contains("authorization: Bearer access-B"), "{seen:?}");
}

#[test]
fn the_route_written_for_a_uuid_connector_parses_back_to_its_id() {
    let id = "3f2b8c1e-9d4a-4e7b-a1c6-5f0e2d9b7a84";
    let url = route(id);
    let path = url
        .strip_prefix(&format!("http://127.0.0.1:{PORT}"))
        .expect("the route is on the forwarder's port");
    let request = format!("POST {path} HTTP/1.1\r\nhost: 127.0.0.1:{PORT}\r\n\r\n");
    let parsed = parse_head(request.as_bytes(), PORT).ok().map(|head| head.connector_id);
    assert_eq!(parsed.as_deref(), Some(id));
}
