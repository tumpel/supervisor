use futures_util::{SinkExt, StreamExt};
use std::num::NonZeroU32;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tumpel_api::types::{ChannelControlEnvelope, ChannelProtocol, GuestProtocol, SupervisorCheckReport};

use crate::run::start;

const CHANNEL_DIAL: Duration = Duration::from_secs(20);
const CHALLENGE: Duration = Duration::from_secs(10);

fn version(value: u16) -> Result<NonZeroU32, String> {
    NonZeroU32::new(u32::from(value)).ok_or_else(|| "a protocol version is 0".to_string())
}

async fn challenge(url: &str) -> Result<(), String> {
    let (mut stream, _) = timeout(CHANNEL_DIAL, tokio_tungstenite::connect_async(url))
        .await
        .map_err(|_| format!("dialing {url} ran past {} s", CHANNEL_DIAL.as_secs()))?
        .map_err(|error| format!("dialing {url} failed: {error}"))?;
    timeout(CHALLENGE, async {
        loop {
            let text = match stream.next().await {
                Some(Ok(Message::Text(text))) => text,
                Some(Ok(other)) => return Err(format!("{url} sent {other:?} in place of a Challenge")),
                Some(Err(error)) => return Err(format!("reading the Challenge from {url} failed: {error}")),
                None => return Err(format!("{url} closed before its Challenge")),
            };
            let envelope: ChannelControlEnvelope =
                serde_json::from_str(text.as_str()).map_err(|error| format!("{url} sent no Challenge: {error}"))?;
            match envelope {
                ChannelControlEnvelope::Challenge(_) => return Ok(()),
                ChannelControlEnvelope::Unrecognised(_) => tumpel_channel::update_required(),
                _ => return Err(format!("{url} sent no Challenge; update the supervisor")),
            }
        }
    })
    .await
    .map_err(|_| format!("{url} sent no Challenge within {} s", CHALLENGE.as_secs()))??;
    let _ = stream.send(Message::Close(None)).await;
    Ok(())
}

#[cfg(test)]
mod tests;

pub async fn check() -> Result<SupervisorCheckReport, String> {
    let started = start().map_err(|failure| format!("{}: {}", failure.kind, failure.detail))?;
    eprintln!(
        "supervisor {} starts as host {}",
        *started.running, *started.config.host_id
    );
    challenge(&started.url).await?;
    eprintln!("{} sent its Challenge; closed before Hello", started.url);
    Ok(SupervisorCheckReport {
        supervisor_version: env!("TUMPEL_RELEASE_VERSION")
            .parse()
            .map_err(|error| format!("the supervisor version is empty: {error}"))?,
        channel_protocol_version: version(ChannelProtocol::VERSION)?,
        guest_protocol_version: version(GuestProtocol::VERSION)?,
    })
}
