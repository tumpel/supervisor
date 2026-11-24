use anyhow::{anyhow, bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use std::path::Path;
use tumpel_api::types::{HostEnrol, SupervisorConfig, SupervisorHostKey, TenantApiHostRecord};
use tumpel_api::{Client, Error};

use crate::bounded::bounded;
use crate::dir::{self, CHANNEL_FILE, CONFIG_FILE, KEY_FILE, RUNTIME_FILE};
use crate::usage;

fn host_name(name: Option<String>) -> String {
    name.or_else(|| hostname::get().ok().and_then(|name| name.into_string().ok()))
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "tumpel host".to_string())
}

pub fn read_token(source: &Path) -> Result<String> {
    let token = if source == Path::new("-") {
        std::io::read_to_string(std::io::stdin()).context("reading the enrolment token from stdin")?
    } else {
        std::fs::read_to_string(source)
            .with_context(|| format!("reading the enrolment token from {}", source.display()))?
    };
    let token = token.trim().to_string();
    if token.is_empty() {
        if source == Path::new("-") {
            bail!("stdin holds no enrolment token; pipe the minted token in");
        }
        bail!(
            "{} holds no enrolment token; save the minted token in it",
            source.display()
        );
    }
    Ok(token)
}

pub fn ensure_unenrolled() -> Result<()> {
    let dir = dir::supervisor_dir();
    if dir.join(CONFIG_FILE).exists() {
        bail!(
            "{} already holds an enrolment; delete that directory to enrol this machine again",
            dir.display()
        );
    }
    Ok(())
}

pub async fn enrol_with_token(endpoint: &str, token: String, name: Option<String>) -> Result<TenantApiHostRecord> {
    let endpoint = endpoint.trim_end_matches('/');
    let dir = dir::supervisor_dir();
    ensure_unenrolled()?;
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow!("generating the host's Ed25519 key pair failed"))?;
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())
        .map_err(|_| anyhow!("reading back the generated host key failed"))?;
    let public_key = STANDARD.encode(pair.public_key().as_ref());
    let name = host_name(name);
    let body = HostEnrol {
        architecture: usage::architecture()?,
        cpu_count: usage::cpu_count()?,
        enrolment_token: token
            .try_into()
            .map_err(|error| anyhow!("the enrolment token does not fit the contract: {error}"))?,
        memory_bytes: usage::memory_bytes()?,
        name: name
            .clone()
            .try_into()
            .map_err(|error| anyhow!("--name {name:?} is not a host name: {error}"))?,
        public_key: public_key
            .clone()
            .try_into()
            .map_err(|error| anyhow!("the public key does not fit the contract: {error}"))?,
        supervisor_version: env!("TUMPEL_RELEASE_VERSION")
            .try_into()
            .map_err(|error| anyhow!("the supervisor version does not fit the contract: {error}"))?,
    };
    let client = Client::new(endpoint).context("building the HTTP client")?;
    let host = match client.hosts_enrol(&body).await {
        Ok(host) => host.into_inner(),
        Err(Error::ErrorResponse(problem)) => {
            bail!("{}: {}. {}", problem.title, problem.detail, problem.fix)
        }
        Err(error) => return Err(error).with_context(|| format!("enrolling with {endpoint}/v1/hosts/enrol")),
    };
    dir::ensure_dir(&dir)?;
    dir::write_json(
        &dir.join(KEY_FILE),
        &SupervisorHostKey {
            private_key: STANDARD
                .encode(pkcs8.as_ref())
                .try_into()
                .map_err(|error| anyhow!("encoding the private key: {error}"))?,
            public_key: public_key
                .try_into()
                .map_err(|error| anyhow!("encoding the public key: {error}"))?,
        },
    )?;
    dir::write_json(&dir.join(CHANNEL_FILE), &tumpel_channel::empty_state())?;
    if !dir.join(RUNTIME_FILE).exists() {
        dir::write_json(&dir.join(RUNTIME_FILE), &dir::default_runtime()?)?;
    }
    dir::write_json(
        &dir.join(CONFIG_FILE),
        &SupervisorConfig {
            endpoint: bounded("the control plane endpoint", endpoint).map_err(anyhow::Error::msg)?,
            host_id: host.host_id.clone(),
            tenant_id: host.tenant_id.clone(),
        },
    )?;
    println!("{}", serde_json::to_string_pretty(&host)?);
    eprintln!(
        "enrolled host {} as {}; wrote {}",
        *host.host_id,
        *host.name,
        dir.display()
    );
    Ok(host)
}

/// # Errors
/// Fails when the machine is already enrolled, the token is unreadable or refused, or the
/// enrolment cannot be written.
pub async fn enrol(endpoint: &str, token_file: &Path, name: Option<String>) -> Result<()> {
    ensure_unenrolled()?;
    let token = read_token(token_file)?;
    enrol_with_token(endpoint, token, name).await?;
    eprintln!("start it with tumpel-supervisor run");
    Ok(())
}
