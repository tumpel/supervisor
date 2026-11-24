use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::test(start_paused = true)]
async fn an_unknown_challenge_never_counts_as_readiness_and_a_known_challenge_with_extra_fields_does() {
    let logs = crate::test_logs::UpdateLogs::default();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let url = format!("ws://{}", listener.local_addr().expect("address"));
    let server = async {
        let (stream, _) = listener.accept().await.expect("connection");
        let mut socket = tokio_tungstenite::accept_async(stream).await.expect("websocket");
        socket
            .send(Message::text(
                r#"{"type":"FutureChallenge","nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","extra":true}"#,
            ))
            .await
            .expect("future challenge");
        tokio::time::sleep(Duration::from_secs(1)).await;
        socket
            .send(Message::text(
                r#"{"type":"Challenge","nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","extra":true}"#,
            ))
            .await
            .expect("known challenge");
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    };
    let (result, ()) = tokio::join!(super::challenge(&url), server);
    result.expect("the known challenge proves readiness");
    assert_eq!(*logs.0.lock().expect("logs"), [tumpel_channel::UPDATE_REQUIRED]);
}

#[tokio::test(start_paused = true)]
async fn unfamiliar_challenges_do_not_extend_the_readiness_deadline() {
    let logs = crate::test_logs::UpdateLogs::default();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let url = format!("ws://{}", listener.local_addr().expect("address"));
    let server = async {
        let (stream, _) = listener.accept().await.expect("connection");
        let mut socket = tokio_tungstenite::accept_async(stream).await.expect("websocket");
        for _ in 0..3 {
            socket
                .send(Message::text(r#"{"type":"FutureChallenge","extra":true}"#))
                .await
                .expect("future challenge");
            tokio::time::sleep(Duration::from_secs(4)).await;
        }
    };
    let (result, ()) = tokio::join!(super::challenge(&url), server);
    assert!(result
        .expect_err("no readiness without a known challenge")
        .contains("within 10 s"));
    assert_eq!(*logs.0.lock().expect("logs"), [tumpel_channel::UPDATE_REQUIRED; 3]);
}
