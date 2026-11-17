use std::future::Future;
use std::path::Path;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use reqwest::header::{HeaderName, HeaderValue, CONTENT_LENGTH, RANGE};
use reqwest::StatusCode;
use tokio::io::AsyncWriteExt;

pub enum UploadError {
    Rejected(String),
    Unreadable(String),
}

pub trait Store: Send + Sync + 'static {
    fn upload(&self, url: &str, file: &Path) -> impl Future<Output = Result<(), UploadError>> + Send;
    fn upload_checked(
        &self,
        url: &str,
        file: &Path,
        sha256: &str,
    ) -> impl Future<Output = Result<(), UploadError>> + Send;
    fn download(&self, url: &str, file: &Path) -> impl Future<Output = Result<(), String>> + Send;
    fn download_range(
        &self,
        url: &str,
        file: &Path,
        first: u64,
        last: u64,
    ) -> impl Future<Output = Result<(), String>> + Send;
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
    ) -> Result<(), String> {
        let mut request = self.0.get(url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let mut response = request
            .send()
            .await
            .map_err(|error| format!("GET {}: {}", without_query(url), error.without_url()))?;
        let status = response.status();
        if !accepts(status) {
            return Err(format!("GET {} answered {status}", without_query(url)));
        }
        let mut out = tokio::fs::File::create(file)
            .await
            .map_err(|error| format!("creating {}: {error}", file.display()))?;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| format!("GET {}: {}", without_query(url), error.without_url()))?
        {
            out.write_all(&chunk)
                .await
                .map_err(|error| format!("writing {}: {error}", file.display()))?;
        }
        out.flush()
            .await
            .map_err(|error| format!("writing {}: {error}", file.display()))
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

    async fn download(&self, url: &str, file: &Path) -> Result<(), String> {
        self.get(url, file, Vec::new(), |status| status.is_success()).await
    }

    async fn download_range(&self, url: &str, file: &Path, first: u64, last: u64) -> Result<(), String> {
        let range = HeaderValue::from_str(&format!("bytes={first}-{last}")).map_err(|error| error.to_string())?;
        self.get(url, file, vec![(RANGE, range)], |status| {
            status == StatusCode::PARTIAL_CONTENT
        })
        .await
    }
}
