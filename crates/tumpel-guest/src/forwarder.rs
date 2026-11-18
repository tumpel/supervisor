use chrono::{DateTime, FixedOffset, TimeDelta, Utc};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tumpel_api::types::{ChannelDispatchConnector, ConnectorAccessToken, Problem, ProblemCode};

pub const PORT: u16 = 47160;
const UPSTREAM_BUDGET: Duration = Duration::from_secs(10);
const TOKEN_BUDGET: Duration = Duration::from_secs(15);
const DRAIN_BUDGET: Duration = Duration::from_secs(5);
const POISONED: &str = "the connector forwarder state was poisoned by a panic";
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

struct Live {
    generation: u64,
    routes: HashMap<String, Route>,
    ended: watch::Sender<bool>,
}

#[derive(Default)]
struct State {
    generation: u64,
    live: Option<Live>,
    active: usize,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    drained: Condvar,
}

type Turn = Arc<Shared>;

pub fn route(connector_id: &str) -> String {
    format!("http://127.0.0.1:{PORT}{PREFIX}{connector_id}")
}

fn loopback(host: &str) -> bool {
    host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn private(host: &str) -> bool {
    match host.trim_start_matches('[').trim_end_matches(']').parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified(),
        Ok(IpAddr::V6(ip)) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || (ip.segments()[0] & 0xfe00) == 0xfc00
                || (ip.segments()[0] & 0xffc0) == 0xfe80
        }
        Err(_) => host == "localhost" || host.ends_with(".localhost"),
    }
}

fn destination(raw: &str, what: &str, connector_id: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(raw)
        .map_err(|error| format!("connector {connector_id}: the {what} {raw} is not a URL: {error}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| format!("connector {connector_id}: the {what} {raw} names no host"))?;
    let test_loopback = cfg!(test) && loopback(host);
    match url.scheme() {
        "https" if !private(host) || test_loopback => Ok(raw.to_string()),
        "http" if test_loopback => Ok(raw.to_string()),
        _ => Err(format!(
            "connector {connector_id}: the {what} {raw} is not a public HTTPS address, so the turn's credential would leave over the box network in the clear or to a private host"
        )),
    }
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
            turn: Arc::new(Shared::default()),
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
        let mut routes = HashMap::new();
        for connector in connectors {
            let id = connector.connector_id.to_string();
            let route = Route {
                url: destination(&connector.url, "connector URL", &id)?,
                token_url: destination(&connector.token_url, "token URL", &id)?,
                credential: connector.token_credential.to_string(),
                token: None,
            };
            routes.insert(id, route);
        }
        let mut state = self.turn.state.lock().map_err(|_| POISONED)?;
        if state.live.is_some() {
            return Err("the connector forwarder began a turn while another one still runs".to_string());
        }
        state.generation += 1;
        state.live = Some(Live {
            generation: state.generation,
            routes,
            ended: watch::channel(false).0,
        });
        Ok(())
    }

    pub fn end(&self) -> Result<(), String> {
        let mut state = self.turn.state.lock().map_err(|_| POISONED)?;
        if let Some(live) = state.live.take() {
            live.ended.send_replace(true);
        }
        let (state, waited) = self
            .turn
            .drained
            .wait_timeout_while(state, DRAIN_BUDGET, |state| state.active > 0)
            .map_err(|_| POISONED)?;
        if waited.timed_out() {
            return Err(format!(
                "{} connector calls of the ended turn did not stop within {} s",
                state.active,
                DRAIN_BUDGET.as_secs()
            ));
        }
        Ok(())
    }
}

struct Admission {
    turn: Turn,
    generation: u64,
    ended: watch::Receiver<bool>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        if let Ok(mut state) = self.turn.state.lock() {
            state.active -= 1;
        }
        self.turn.drained.notify_all();
    }
}

async fn turn_ended(mut ended: watch::Receiver<bool>) {
    while !*ended.borrow_and_update() {
        if ended.changed().await.is_err() {
            return;
        }
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
    path.strip_prefix(PREFIX).filter(|id| !id.is_empty())
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

fn poisoned() -> Refusal {
    refusal(
        500,
        ProblemCode::ConnectorRefreshFailed,
        "Connector forwarder failed",
        POISONED.to_string(),
        "Start a new turn.",
    )
}

struct Admitted {
    admission: Admission,
    url: String,
    token_url: String,
    credential: String,
    token: Option<String>,
}

fn admit(turn: &Turn, connector_id: &str) -> Result<Admitted, Refusal> {
    let mut state = turn.state.lock().map_err(|_| poisoned())?;
    let live = state.live.as_ref().ok_or_else(|| not_running(connector_id))?;
    let route = live.routes.get(connector_id).ok_or_else(|| {
        refusal(
            404,
            ProblemCode::NotFound,
            "Connector not in this session",
            format!("connector {connector_id} is not one of this session's connectors"),
            "Use a connector the session's persona lists.",
        )
    })?;
    let admitted = Admitted {
        admission: Admission {
            turn: Arc::clone(turn),
            generation: live.generation,
            ended: live.ended.subscribe(),
        },
        url: route.url.clone(),
        token_url: route.token_url.clone(),
        credential: route.credential.clone(),
        token: route
            .token
            .as_ref()
            .filter(|token| fresh(token, Utc::now()))
            .map(|token| token.value.clone()),
    };
    state.active += 1;
    Ok(admitted)
}

async fn token(admitted: &Admitted, client: &reqwest::Client, connector_id: &str) -> Result<String, Refusal> {
    if let Some(token) = &admitted.token {
        return Ok(token.clone());
    }
    let fetched = fetch_token(client, connector_id, &admitted.token_url, &admitted.credential).await?;
    let value = fetched.value.clone();
    let mut state = admitted.admission.turn.state.lock().map_err(|_| poisoned())?;
    let route = state
        .live
        .as_mut()
        .filter(|live| live.generation == admitted.admission.generation)
        .and_then(|live| live.routes.get_mut(connector_id))
        .ok_or_else(|| not_running(connector_id))?;
    route.token = Some(fetched);
    Ok(value)
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
    stream.flush().await?;
    stream.shutdown().await?;
    let mut unread = [0u8; 8192];
    let drained = tokio::time::timeout(DRAIN_BUDGET, async {
        while stream.read(&mut unread).await? > 0 {}
        Ok::<(), std::io::Error>(())
    })
    .await;
    match drained {
        Ok(read) => read,
        Err(_) => Ok(()),
    }
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
    let admitted = admit(turn, &head.connector_id)?;
    let ended = admitted.admission.ended.clone();
    tokio::select! {
        biased;
        () = turn_ended(ended) => Ok(()),
        proxied = proxy(stream, &admitted, client, port, budget, head, body) => proxied,
    }
}

fn upstream_failure(status: reqwest::StatusCode, id: &str, url: &str) -> Refusal {
    let detail = format!("connector {id} at {url} answered {status}");
    match status {
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => refusal(
            502,
            ProblemCode::ConnectorAuthRefused,
            "Connector refused the credential",
            detail,
            "An Owner reconnects the connector; the turn goes on.",
        ),
        status if status.is_server_error() => refusal(
            status.as_u16(),
            ProblemCode::ConnectorUnreachable,
            "Connector failed",
            detail,
            "Try the tool call again later; the turn goes on.",
        ),
        status => refusal(
            status.as_u16(),
            ProblemCode::InvalidRequest,
            "Connector refused the request",
            detail,
            "Change the tool call; the turn goes on.",
        ),
    }
}

async fn proxy(
    stream: &mut TcpStream,
    admitted: &Admitted,
    client: &reqwest::Client,
    port: u16,
    budget: Duration,
    head: Head,
    body: Vec<u8>,
) -> Result<(), Failure> {
    let id = head.connector_id.as_str();
    let url = admitted.url.as_str();
    let access_token = token(admitted, client, id).await?;
    let unreachable = |detail: String| {
        refusal(
            502,
            ProblemCode::ConnectorUnreachable,
            "Connector unreachable",
            format!("connector {id} at {url}: {detail}"),
            "Try the tool call again later; the turn goes on.",
        )
    };
    let mut request = client.request(head.method, url).bearer_auth(&access_token).body(body);
    for (name, value) in head.headers {
        request = request.header(name, value);
    }
    let mut response = match tokio::time::timeout(budget, request.send()).await {
        Err(_) => return Err(unreachable(format!("no answer within {} seconds", budget.as_secs_f32())).into()),
        Ok(Err(error)) => return Err(unreachable(error.to_string()).into()),
        Ok(Ok(response)) => response,
    };
    let status = response.status();
    if status.is_redirection() {
        let location = response.headers().get(reqwest::header::LOCATION);
        if location.is_some_and(|location| location.as_bytes() == url.as_bytes()) {
            let head = format!(
                "HTTP/1.1 {} {}\r\nlocation: http://127.0.0.1:{port}{PREFIX}{id}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                status.as_u16(),
                status.canonical_reason().unwrap_or("")
            );
            stream.write_all(head.as_bytes()).await?;
            stream.flush().await?;
            return Ok(());
        }
        return Err(unreachable(format!(
            "answered {status} with a redirect to another address, which the forwarder never follows"
        ))
        .into());
    }
    if !status.is_success() {
        return Err(upstream_failure(status, id, url).into());
    }
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    )
    .into_bytes();
    for (name, value) in response.headers() {
        if !forwarded(name.as_str(), &DROPPED_RESPONSE) || name == reqwest::header::LOCATION {
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
