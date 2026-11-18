use chrono::{DateTime, FixedOffset, TimeDelta, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tumpel_api::types::{ChannelDispatchConnector, ConnectorAccessToken, Problem, ProblemCode};

pub const PORT: u16 = 47160;
const UPSTREAM_BUDGET: Duration = Duration::from_secs(10);
const TOKEN_BUDGET: Duration = Duration::from_secs(15);
const EARLY_SECONDS: i64 = 30;
const HEAD_LIMIT: usize = 64 * 1024;
const BODY_LIMIT: usize = 16 * 1024 * 1024;
const PREFIX: &str = "/connectors/";
const DROPPED_REQUEST: [&str; 13] = [
    "host",
    "origin",
    "authorization",
    "content-length",
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];
const DROPPED_RESPONSE: [&str; 9] = [
    "content-length",
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "www-authenticate",
];

#[derive(Clone, Debug, PartialEq, Eq)]
struct Token {
    value: String,
    expires_at: Option<DateTime<FixedOffset>>,
}

struct Route {
    url: String,
    token_url: String,
    credential: String,
    token: Option<Token>,
}

type Turn = Arc<Mutex<Option<HashMap<String, Route>>>>;

pub fn route(connector_id: &str) -> String {
    format!("http://127.0.0.1:{PORT}{PREFIX}{connector_id}")
}

#[derive(Clone)]
pub struct Forwarder {
    turn: Turn,
}

impl Forwarder {
    pub fn start() -> Result<Self, String> {
        Ok(Self::listen(PORT, UPSTREAM_BUDGET)?.0)
    }

    fn listen(port: u16, budget: Duration) -> Result<(Self, u16), String> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", port))
            .map_err(|error| format!("binding the connector forwarder on 127.0.0.1:{port}: {error}"))?;
        let port = listener
            .local_addr()
            .map_err(|error| format!("reading the connector forwarder address: {error}"))?
            .port();
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("making the connector forwarder listener non-blocking: {error}"))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("starting the connector forwarder runtime: {error}"))?;
        let client = reqwest::Client::builder()
            .connect_timeout(budget)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("building the connector forwarder client: {error}"))?;
        let forwarder = Self {
            turn: Arc::new(Mutex::new(None)),
        };
        let turn = Arc::clone(&forwarder.turn);
        std::thread::spawn(move || {
            runtime.block_on(async move {
                let listener = match TcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(error) => return eprintln!("tumpel-guest: connector forwarder listener: {error}"),
                };
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            tokio::spawn(serve(stream, Arc::clone(&turn), client.clone(), port, budget));
                        }
                        Err(error) => eprintln!("tumpel-guest: connector forwarder accept: {error}"),
                    }
                }
            });
        });
        Ok((forwarder, port))
    }

    pub fn begin(&self, connectors: &[ChannelDispatchConnector]) -> Result<(), String> {
        let routes = connectors
            .iter()
            .map(|connector| {
                (
                    connector.connector_id.to_string(),
                    Route {
                        url: connector.url.clone(),
                        token_url: connector.token_url.clone(),
                        credential: connector.token_credential.to_string(),
                        token: None,
                    },
                )
            })
            .collect();
        *self
            .turn
            .lock()
            .map_err(|_| "the connector forwarder state was poisoned")? = Some(routes);
        Ok(())
    }

    pub fn end(&self) -> Result<(), String> {
        *self
            .turn
            .lock()
            .map_err(|_| "the connector forwarder state was poisoned")? = None;
        Ok(())
    }
}

struct Refusal {
    status: u16,
    problem: Problem,
}

enum Failure {
    Refused(Refusal),
    Client(std::io::Error),
}

impl From<Refusal> for Failure {
    fn from(refusal: Refusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<std::io::Error> for Failure {
    fn from(error: std::io::Error) -> Self {
        Self::Client(error)
    }
}

fn refusal(status: u16, code: ProblemCode, title: &str, detail: String, fix: &str) -> Refusal {
    Refusal {
        status,
        problem: Problem {
            code,
            title: title.to_string(),
            detail,
            fix: fix.to_string(),
        },
    }
}

fn fresh(token: &Token, now: DateTime<Utc>) -> bool {
    token
        .expires_at
        .is_none_or(|expires_at| expires_at.with_timezone(&Utc) - now > TimeDelta::seconds(EARLY_SECONDS))
}

fn local_host(value: &str, port: u16) -> bool {
    value == format!("127.0.0.1:{port}") || value == format!("localhost:{port}")
}

fn connector_id(path: &str) -> Option<&str> {
    path.strip_prefix(PREFIX)
        .filter(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_alphanumeric()))
}

fn forwarded(name: &str, dropped: &[&str]) -> bool {
    !dropped.iter().any(|header| name.eq_ignore_ascii_case(header))
}

struct Head {
    method: reqwest::Method,
    connector_id: String,
    headers: Vec<(String, Vec<u8>)>,
    content_length: usize,
}

fn parse_head(bytes: &[u8], port: u16) -> Result<Head, Refusal> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    let invalid = |detail: String| {
        refusal(
            400,
            ProblemCode::InvalidRequest,
            "Malformed connector request",
            detail,
            "Send an HTTP/1.1 MCP request to the connector's loopback route.",
        )
    };
    match request.parse(bytes) {
        Ok(httparse::Status::Complete(_)) => {}
        Ok(httparse::Status::Partial) => return Err(invalid("the request head ended early".to_string())),
        Err(error) => return Err(invalid(format!("the request head does not parse: {error}"))),
    }
    let header = |name: &str| {
        request
            .headers
            .iter()
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| String::from_utf8_lossy(header.value).into_owned())
    };
    if !header("host").is_some_and(|host| local_host(&host, port)) || header("origin").is_some() {
        return Err(refusal(
            403,
            ProblemCode::Forbidden,
            "Connector route refused",
            "the request came with a foreign Host or a browser Origin".to_string(),
            "Only the harness reaches a connector, through the URL in its generated MCP config.",
        ));
    }
    if header("transfer-encoding").is_some() {
        return Err(invalid("a chunked request body is not supported".to_string()));
    }
    let method = match request.method {
        Some("POST") => reqwest::Method::POST,
        Some("GET") => reqwest::Method::GET,
        Some("DELETE") => reqwest::Method::DELETE,
        other => return Err(invalid(format!("the method {other:?} is not POST, GET or DELETE"))),
    };
    let path = request
        .path
        .ok_or_else(|| invalid("the request has no path".to_string()))?;
    let Some(connector_id) = connector_id(path) else {
        return Err(refusal(
            404,
            ProblemCode::NotFound,
            "No such connector route",
            format!("{path} is not a connector route"),
            "Use the URL in the generated MCP config.",
        ));
    };
    let content_length = match header("content-length") {
        None => 0,
        Some(value) => match value.trim().parse::<usize>() {
            Ok(length) if length <= BODY_LIMIT => length,
            Ok(_) | Err(_) => {
                return Err(invalid(format!(
                    "the Content-Length {value} is not a length up to {BODY_LIMIT}"
                )))
            }
        },
    };
    Ok(Head {
        method,
        connector_id: connector_id.to_string(),
        headers: request
            .headers
            .iter()
            .filter(|header| forwarded(header.name, &DROPPED_REQUEST))
            .map(|header| (header.name.to_string(), header.value.to_vec()))
            .collect(),
        content_length,
    })
}

async fn read_request(stream: &mut TcpStream, port: u16) -> Result<(Head, Vec<u8>), Failure> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let end = loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
        let read = stream.read(&mut chunk).await?;
        if read == 0 || buffer.len() > HEAD_LIMIT {
            return Err(Failure::Refused(refusal(
                400,
                ProblemCode::InvalidRequest,
                "Malformed connector request",
                "the request head ended early or passed 64 KiB".to_string(),
                "Send an HTTP/1.1 MCP request to the connector's loopback route.",
            )));
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let head = parse_head(&buffer[..end], port)?;
    let mut body = buffer.split_off(end);
    while body.len() < head.content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(head.content_length);
    Ok((head, body))
}

fn not_running(connector_id: &str) -> Refusal {
    refusal(
        409,
        ProblemCode::ConnectorTurnNotRunning,
        "No turn is running",
        format!("connector {connector_id} serves requests only while a turn runs"),
        "Call the connector during a turn.",
    )
}

async fn fetch_token(
    client: &reqwest::Client,
    connector_id: &str,
    token_url: &str,
    credential: &str,
) -> Result<Token, Refusal> {
    let failed = |detail: String| {
        refusal(
            503,
            ProblemCode::ConnectorRefreshFailed,
            "Connector token unavailable",
            format!("connector {connector_id}: {detail}"),
            "Try the tool call again; if it keeps failing, an Owner reconnects the connector.",
        )
    };
    let response = client
        .post(token_url)
        .bearer_auth(credential)
        .timeout(TOKEN_BUDGET)
        .send()
        .await
        .map_err(|error| failed(format!("the token fetch failed: {error}")))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|error| failed(format!("reading the token answer failed: {error}")))?;
    if !status.is_success() {
        return Err(match serde_json::from_slice::<Problem>(&body) {
            Ok(problem) => Refusal {
                status: status.as_u16(),
                problem,
            },
            Err(_) => failed(format!("the control plane answered {status}")),
        });
    }
    let token: ConnectorAccessToken = serde_json::from_slice(&body)
        .map_err(|error| failed(format!("the token answer does not match ConnectorAccessToken: {error}")))?;
    let token = Token {
        value: token.access_token.to_string(),
        expires_at: token.expires_at,
    };
    if token
        .expires_at
        .is_some_and(|expires_at| expires_at.with_timezone(&Utc) <= Utc::now())
    {
        return Err(failed("the control plane answered an expired token".to_string()));
    }
    Ok(token)
}

async fn token(turn: &Turn, client: &reqwest::Client, connector_id: &str) -> Result<(String, String), Refusal> {
    let poisoned = || {
        refusal(
            500,
            ProblemCode::ConnectorRefreshFailed,
            "Connector forwarder failed",
            "the forwarder state was poisoned by a panic".to_string(),
            "Start a new turn.",
        )
    };
    let (url, token_url, credential) = {
        let turn = turn.lock().map_err(|_| poisoned())?;
        let routes = turn.as_ref().ok_or_else(|| not_running(connector_id))?;
        let route = routes.get(connector_id).ok_or_else(|| {
            refusal(
                404,
                ProblemCode::NotFound,
                "Connector not in this session",
                format!("connector {connector_id} is not one of this session's connectors"),
                "Use a connector the session's persona lists.",
            )
        })?;
        if let Some(token) = route.token.as_ref().filter(|token| fresh(token, Utc::now())) {
            return Ok((route.url.clone(), token.value.clone()));
        }
        (route.url.clone(), route.token_url.clone(), route.credential.clone())
    };
    let fetched = fetch_token(client, connector_id, &token_url, &credential).await?;
    let value = fetched.value.clone();
    let mut turn = turn.lock().map_err(|_| poisoned())?;
    let route = turn
        .as_mut()
        .and_then(|routes| routes.get_mut(connector_id))
        .ok_or_else(|| not_running(connector_id))?;
    route.token = Some(fetched);
    Ok((url, value))
}

async fn write_refusal(stream: &mut TcpStream, refusal: &Refusal) -> std::io::Result<()> {
    let body = serde_json::to_vec(&refusal.problem)?;
    let head = format!(
        "HTTP/1.1 {} {}\r\ncontent-type: application/problem+json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        refusal.status,
        match reqwest::StatusCode::from_u16(refusal.status) {
            Ok(status) => status.canonical_reason().unwrap_or(""),
            Err(_) => "",
        },
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

async fn serve(mut stream: TcpStream, turn: Turn, client: reqwest::Client, port: u16, budget: Duration) {
    let written = match forward(&mut stream, &turn, &client, port, budget).await {
        Ok(()) => Ok(()),
        Err(Failure::Refused(refused)) => write_refusal(&mut stream, &refused).await,
        Err(Failure::Client(error)) => Err(error),
    };
    if let Err(error) = written {
        eprintln!("tumpel-guest: connector forwarder connection: {error}");
    }
}

async fn forward(
    stream: &mut TcpStream,
    turn: &Turn,
    client: &reqwest::Client,
    port: u16,
    budget: Duration,
) -> Result<(), Failure> {
    let (head, body) = read_request(stream, port).await?;
    let id = head.connector_id.as_str();
    let (url, access_token) = token(turn, client, id).await?;
    let unreachable = |detail: String| {
        refusal(
            502,
            ProblemCode::ConnectorUnreachable,
            "Connector unreachable",
            format!("connector {id} at {url}: {detail}"),
            "Try the tool call again later; the turn goes on.",
        )
    };
    let mut request = client.request(head.method, &url).bearer_auth(&access_token).body(body);
    for (name, value) in head.headers {
        request = request.header(name, value);
    }
    let mut response = match tokio::time::timeout(budget, request.send()).await {
        Err(_) => return Err(unreachable(format!("no answer within {} seconds", budget.as_secs_f32())).into()),
        Ok(Err(error)) => return Err(unreachable(error.to_string()).into()),
        Ok(Ok(response)) => response,
    };
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return Err(refusal(
            502,
            ProblemCode::ConnectorAuthRefused,
            "Connector refused the credential",
            format!("connector {id} at {url} answered {status}"),
            "An Owner reconnects the connector; the turn goes on.",
        )
        .into());
    }
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    )
    .into_bytes();
    for (name, value) in response.headers() {
        if !forwarded(name.as_str(), &DROPPED_RESPONSE) {
            continue;
        }
        if name == reqwest::header::LOCATION {
            if value.as_bytes() == url.as_bytes() {
                head.extend_from_slice(format!("location: http://127.0.0.1:{port}{PREFIX}{id}\r\n").as_bytes());
            }
            continue;
        }
        head.extend_from_slice(name.as_str().as_bytes());
        head.extend_from_slice(b": ");
        head.extend_from_slice(value.as_bytes());
        head.extend_from_slice(b"\r\n");
    }
    head.extend_from_slice(b"transfer-encoding: chunked\r\nconnection: close\r\n\r\n");
    stream.write_all(&head).await?;
    let (mut reader, mut writer) = stream.split();
    let mut probe = [0u8; 1];
    loop {
        tokio::select! {
            closed = reader.read(&mut probe) => {
                if closed? == 0 {
                    return Ok(());
                }
            }
            chunk = response.chunk() => match chunk {
                Ok(Some(chunk)) => {
                    writer.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await?;
                    writer.write_all(&chunk).await?;
                    writer.write_all(b"\r\n").await?;
                    writer.flush().await?;
                }
                Ok(None) => break,
                Err(error) => {
                    eprintln!("tumpel-guest: connector {id} ended its stream: {error}");
                    break;
                }
            },
        }
    }
    writer.write_all(b"0\r\n\r\n").await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
