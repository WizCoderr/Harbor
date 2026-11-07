use std::time::Duration;

use harbor_agent::{ServeOptions, IpcClient};
use serde_json::json;

#[tokio::test]
async fn socket_answers_status_and_shuts_down() {
    std::env::set_var("HARBOR_DISABLE_OS_TIMER", "1");
    std::env::set_var("HARBOR_DISABLE_CLIPBOARD", "1");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let serve_path = path.clone();
    let server = tokio::spawn(async move {
        harbor_agent::serve(serve_path, ServeOptions { until_idle: false, memory_secrets: true }).await
    });
    let mut client = None;
    for _ in 0..40 {
        if let Ok(connected) = IpcClient::connect(&path).await {
            client = Some(connected);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let client = client.expect("Harbor agent did not open its socket");
    let status = client.call("agent.status", json!({})).await.unwrap();
    assert_eq!(status["ok"], true);
    client.call("agent.shutdown", json!({})).await.unwrap();
    let finished = tokio::time::timeout(Duration::from_secs(3), server).await;
    assert!(finished.is_ok(), "agent did not shut down");
}
