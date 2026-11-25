use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::path::Path;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use reqwest::header::{HeaderName, HeaderValue, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use tokio::io::AsyncWriteExt;

pub enum UploadError {
    Rejected(String),
    Unreadable(String),
}

pub enum DownloadError {
    Transient(String),
    Expired(String),
    Refused(String),
}

impl fmt::Display for DownloadError {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(detail) | Self::Expired(detail) | Self::Refused(detail) => out.write_str(detail),
        }
    }
}

impl From<DownloadError> for String {
    fn from(error: DownloadError) -> Self {
        error.to_string()
    }
}

pub const DOWNLOAD_ATTEMPTS: u32 = 10;
pub const ATTEMPTS_PER_URL: u32 = 4;
const FIRST_WAIT: Duration = Duration::from_millis(250);
const LAST_WAIT: Duration = Duration::from_secs(8);

#[must_use]
pub fn backoff(attempt: u32, jitter: u16) -> Duration {
    let wait = FIRST_WAIT
        .saturating_mul(1 << attempt.saturating_sub(1).min(16))
        .min(LAST_WAIT);
    let half = wait / 2;
    half + half.mul_f64(f64::from(jitter) / f64::from(u16::MAX))
}

pub async fn wait(attempt: u32) {
    tokio::time::sleep(backoff(attempt, jitter())).await;
}

#[must_use]
pub fn jitter() -> u16 {
    let [low, high, ..] = RandomState::new().hash_one(()).to_le_bytes();
    u16::from_le_bytes([low, high])
}

pub trait Store: Send + Sync + 'static {
    fn upload(&self, url: &str, file: &Path) -> impl Future<Output = Result<(), UploadError>> + Send;
    fn upload_checked(
        &self,
        url: &str,
        file: &Path,
        sha256: &str,
    ) -> impl Future<Output = Result<(), UploadError>> + Send;
    fn download(&self, url: &str, file: &Path) -> impl Future<Output = Result<(), DownloadError>> + Send;
    fn download_range(
        &self,
        url: &str,
        file: &Path,
        first: u64,
        last: u64,
    ) -> impl Future<Output = Result<(), DownloadError>> + Send;
}

pub struct HttpStore(reqwest::Client);

impl HttpStore {
    pub fn new() -> Result<Self, String> {
        reqwest::Client::builder()
            .build()
            .map(Self)
            .map_err(|error| format!("building the snapshot HTTP client: {error}"))
    }

    async fn put(&self, url: &str, file: &Path, headers: Vec<(HeaderName, HeaderValue)>) -> Result<(), UploadError> {
        let opened = tokio::fs::File::open(file)
            .await
            .map_err(|error| UploadError::Unreadable(format!("opening {}: {error}", file.display())))?;
        let size = opened
            .metadata()
            .await
            .map_err(|error| UploadError::Unreadable(format!("sizing {}: {error}", file.display())))?
            .len();
        let mut request = self.0.put(url).header(CONTENT_LENGTH, size);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request
            .body(reqwest::Body::from(opened))
            .send()
            .await
            .map_err(|error| UploadError::Rejected(format!("PUT {}: {}", without_query(url), error.without_url())))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = response.text().await.unwrap_or_default();
        Err(UploadError::Rejected(format!(
            "PUT {} answered {status}: {}",
            without_query(url),
            body.chars().take(300).collect::<String>()
        )))
    }

    async fn get(
        &self,
        url: &str,
        file: &Path,
        headers: Vec<(HeaderName, HeaderValue)>,
        accepts: fn(StatusCode) -> bool,
    ) -> Result<(), DownloadError> {
        let mut request = self.0.get(url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let mut response = request.send().await.map_err(|error| {
            DownloadError::Transient(format!("GET {}: {}", without_query(url), error.without_url()))
        })?;
        let status = response.status();
        if !accepts(status) {
            let detail = format!("GET {} answered {status}", without_query(url));
            return Err(match status {
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => DownloadError::Expired(detail),
                _ if status.is_server_error() => DownloadError::Transient(detail),
                _ => DownloadError::Refused(detail),
            });
        }
        let mut out = std::fs::File::create(file)
            .map(tokio::fs::File::from_std)
            .map_err(|error| DownloadError::Refused(format!("creating {}: {error}", file.display())))?;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| DownloadError::Transient(format!("GET {}: {}", without_query(url), error.without_url())))?
        {
            out.write_all(&chunk)
                .await
                .map_err(|error| DownloadError::Refused(format!("writing {}: {error}", file.display())))?;
        }
        out.flush()
            .await
            .map_err(|error| DownloadError::Refused(format!("writing {}: {error}", file.display())))
    }
}

fn without_query(url: &str) -> &str {
    url.split('?').next().unwrap_or(url)
}

pub fn checksum(sha256: &str) -> Result<String, String> {
    let bytes = (0..sha256.len())
        .step_by(2)
        .map(|at| {
            sha256
                .get(at..at + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| format!("{sha256} is no hex SHA-256 digest"))
        })
        .collect::<Result<Vec<u8>, String>>()?;
    if bytes.len() != 32 {
        return Err(format!("{sha256} is no hex SHA-256 digest"));
    }
    Ok(STANDARD.encode(bytes))
}

impl Store for HttpStore {
    async fn upload(&self, url: &str, file: &Path) -> Result<(), UploadError> {
        self.put(url, file, Vec::new()).await
    }

    async fn upload_checked(&self, url: &str, file: &Path, sha256: &str) -> Result<(), UploadError> {
        let value = checksum(sha256)
            .and_then(|value| HeaderValue::from_str(&value).map_err(|error| error.to_string()))
            .map_err(UploadError::Unreadable)?;
        self.put(
            url,
            file,
            vec![(HeaderName::from_static("x-amz-checksum-sha256"), value)],
        )
        .await
    }

    async fn download(&self, url: &str, file: &Path) -> Result<(), DownloadError> {
        self.get(url, file, Vec::new(), |status| status.is_success()).await
    }

    async fn download_range(&self, url: &str, file: &Path, first: u64, last: u64) -> Result<(), DownloadError> {
        let range = HeaderValue::from_str(&format!("bytes={first}-{last}"))
            .map_err(|error| DownloadError::Refused(error.to_string()))?;
        self.get(url, file, vec![(RANGE, range)], |status| {
            status == StatusCode::PARTIAL_CONTENT
        })
        .await
    }
}
