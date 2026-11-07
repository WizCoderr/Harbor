use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use harbor_agent::{default_data_dir, IpcClient};
use serde_json::Value;
use tauri::{Emitter, Manager, State};
use tokio::sync::Mutex;

struct Agent {
    client: Mutex<Option<Arc<IpcClient>>>,
}

#[tauri::command]
async fn agent_call(state: State<'_, Agent>, method: String, params: Value) -> Result<Value, String> {
    let client = wait_for_client(&state).await?;
    client.call(&method, params).await.map_err(|err| err.to_string())
}

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .manage(Agent { client: Mutex::new(None) })
        .invoke_handler(tauri::generate_handler![agent_call])
        .setup(|app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                match ensure_agent().await {
                    Ok(client) => {
                        if let Some(state) = handle.try_state::<Agent>() {
                            *state.client.lock().await = Some(Arc::clone(&client));
                        }
                        let mut events = client.subscribe();
                        while let Ok(event) = events.recv().await {
                            let _ = handle.emit("harbor://event", event);
                        }
                    }
                    Err(err) => {
                        let _ = handle.emit("harbor://event", serde_json::json!({"event": "notification", "title": "Harbor agent did not start", "body": err}));
                    }
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("Harbor window failed");
}

async fn wait_for_client(state: &Agent) -> Result<Arc<IpcClient>, String> {
    for _ in 0..80 {
        if let Some(client) = state.client.lock().await.clone() {
            return Ok(client);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("Harbor agent is not available.".into())
}

async fn ensure_agent() -> Result<Arc<IpcClient>, String> {
    let data_dir = default_data_dir();
    if let Ok(client) = IpcClient::connect(&data_dir).await {
        return Ok(Arc::new(client));
    }
    let binary = agent_binary();
    Command::new(&binary)
        .arg("--data-dir")
        .arg(&data_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|err| format!("Could not start {}: {err}", binary.display()))?;
    for _ in 0..50 {
        if let Ok(client) = IpcClient::connect(&data_dir).await {
            return Ok(Arc::new(client));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Harbor agent did not open its socket.".into())
}

fn agent_binary() -> PathBuf {
    if let Ok(path) = std::env::var("HARBOR_AGENT") {
        return PathBuf::from(path);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join(if cfg!(windows) { "harbor-agent.exe" } else { "harbor-agent" });
            if sibling.exists() {
                return sibling;
            }
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../target/debug/harbor-agent")
}
