//! Headless Harbor agent. It owns the download engine, the local socket, and scheduled wakeups.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
#[cfg(not(target_os = "android"))]
use harbor_core::endpoint_id;
use harbor_core::{EXTENSION_ID, FIREFOX_EXTENSION_ID, NATIVE_HOST_NAME};
use harbor_engine::{EngineConfig, EngineEvent, EngineHandle, MemorySecrets, Secrets};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

pub struct ServeOptions {
    pub until_idle: bool,
    pub memory_secrets: bool,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self { until_idle: false, memory_secrets: false }
    }
}

pub fn run() -> Result<()> {
    tracing_subscriber();
    let mut args = std::env::args().skip(1);
    let mut data_dir = None;
    let mut run_due = false;
    let mut native = false;
    let mut install = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = args.next().map(PathBuf::from),
            "--run-due" => run_due = true,
            "--native-messaging" => native = true,
            "--install-host" => install = true,
            "--help" | "-h" => {
                println!("harbor-agent [--data-dir PATH] [--run-due] [--native-messaging] [--install-host]");
                return Ok(());
            }
            other => return Err(anyhow!("Unknown argument: {other}")),
        }
    }
    let data_dir = data_dir.unwrap_or_else(default_data_dir);
    if install {
        return install_host(&data_dir);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    if native {
        return runtime.block_on(native_messaging(&data_dir));
    }
    runtime.block_on(async {
        if run_due && agent_is_running(&data_dir).await {
            let client = IpcClient::connect(&data_dir).await?;
            client.call("scheduler.tick", json!({})).await?;
            return Ok(());
        }
        serve(data_dir, ServeOptions { until_idle: run_due, memory_secrets: std::env::var("HARBOR_MEMORY_SECRETS").ok().as_deref() == Some("1") }).await
    })
}

pub fn default_data_dir() -> PathBuf {
    dirs::data_dir().unwrap_or_else(|| PathBuf::from(".")).join("Harbor")
}

pub fn socket_path(data_dir: &Path) -> PathBuf {
    #[cfg(target_os = "android")]
    {
        return data_dir.join("harbor.sock");
    }
    #[cfg(not(target_os = "android"))]
    {
        let id = endpoint_id(&data_dir.to_string_lossy());
        #[cfg(unix)]
        {
            let user = std::env::var("USER").unwrap_or_else(|_| "user".into());
            PathBuf::from(format!("/tmp/harbor-{user}-{id}.sock"))
        }
        #[cfg(windows)]
        {
            PathBuf::from(format!(r"\\.\pipe\harbor-{id}"))
        }
    }
}

pub async fn serve(data_dir: PathBuf, options: ServeOptions) -> Result<()> {
    std::fs::create_dir_all(&data_dir)?;
    let lock_path = data_dir.join("agent.lock");
    let mut lock = fslock::LockFile::open(&lock_path).with_context(|| format!("open {}", lock_path.display()))?;
    if !lock.try_lock().context("lock Harbor agent")? {
        return Err(anyhow!("Harbor is already running"));
    }
    let secrets: Arc<dyn Secrets> = if options.memory_secrets {
        Arc::new(MemorySecrets::default())
    } else {
        platform_secrets(&data_dir)
    };
    let engine = EngineHandle::open(data_dir.join("harbor.db"), EngineConfig::default(), secrets).map_err(anyhow::Error::msg)?;
    engine.boot().map_err(anyhow::Error::msg)?;
    engine.apply_runtime_settings().map_err(anyhow::Error::msg)?;
    let shutdown = CancellationToken::new();
    let (events, _) = broadcast::channel::<String>(512);
    let engine_events = engine.subscribe();
    let event_sender = events.clone();
    tokio::spawn(async move {
        let mut engine_events = engine_events;
        loop {
            match engine_events.recv().await {
                Ok(event) => {
                    if let EngineEvent::Notification { title, body } = &event {
                        show_notification(title, body);
                    }
                    if let Ok(line) = serde_json::to_string(&event) {
                        let _ = event_sender.send(line);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    #[cfg(not(target_os = "android"))]
    {
        let clipboard_engine = engine.clone();
        let clipboard_stop = shutdown.clone();
        if std::env::var("HARBOR_DISABLE_CLIPBOARD").ok().as_deref() != Some("1") {
            tokio::spawn(async move { clipboard_loop(clipboard_engine, clipboard_stop).await });
        }
    }
    let idle_engine = engine.clone();
    let idle_stop = shutdown.clone();
    if options.until_idle {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = idle_engine.scheduler_tick();
            loop {
                tokio::time::sleep(Duration::from_millis(400)).await;
                let running = idle_engine.dispatch("queues.list", json!({})).await.ok()
                    .and_then(|v| v.as_array().map(|queues| queues.iter().any(|q| q["status"] == "running")))
                    .unwrap_or(false);
                if idle_engine.active_jobs() == 0 && !running {
                    idle_stop.cancel();
                    break;
                }
            }
        });
    } else {
        let tick_engine = engine.clone();
        let tick_stop = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = tick_stop.cancelled() => break,
                    _ = interval.tick() => {
                        if let Ok(started) = tick_engine.scheduler_tick() {
                            if !started.is_empty() {
                                let _ = register_wakeup(&tick_engine);
                            }
                        }
                    }
                }
            }
        });
    }
    accept_loop(engine, events, shutdown, &data_dir).await?;
    let _ = lock.unlock();
    Ok(())
}

async fn accept_loop(engine: EngineHandle, events: broadcast::Sender<String>, shutdown: CancellationToken, data_dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::net::UnixListener;
        let path = socket_path(data_dir);
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    spawn_connection(stream, engine.clone(), events.clone(), shutdown.clone(), data_dir.to_path_buf());
                }
            }
        }
        let _ = std::fs::remove_file(&path);
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ServerOptions;
        let path = socket_path(data_dir);
        loop {
            let server = ServerOptions::new().first_pipe_instance(false).create(path.as_os_str())?;
            tokio::select! {
                _ = shutdown.cancelled() => break,
                connected = server.connect() => {
                    connected?;
                    spawn_connection(server, engine.clone(), events.clone(), shutdown.clone(), data_dir.to_path_buf());
                }
            }
        }
    }
    Ok(())
}

fn spawn_connection<S>(stream: S, engine: EngineHandle, events: broadcast::Sender<String>, shutdown: CancellationToken, data_dir: PathBuf)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(err) = connection(stream, engine, events, shutdown, data_dir).await {
            tracing::debug!(error = %err, "client connection closed");
        }
    });
}

async fn connection<S>(stream: S, engine: EngineHandle, events: broadcast::Sender<String>, shutdown: CancellationToken, data_dir: PathBuf) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();
    let mut incoming = events.subscribe();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            line = lines.next_line() => {
                let Some(line) = line? else { break };
                if line.trim().is_empty() { continue }
                let response = dispatch_line(&engine, &line, &shutdown, &data_dir).await;
                writer.write_all(response.as_bytes()).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
            }
            event = incoming.recv() => {
                if let Ok(event) = event {
                    writer.write_all(event.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await?;
                }
            }
        }
    }
    Ok(())
}

async fn dispatch_line(engine: &EngineHandle, line: &str, shutdown: &CancellationToken, data_dir: &Path) -> String {
    let request: Value = match serde_json::from_str(line) {
        Ok(value) => value,
        Err(err) => return json!({"error": err.to_string()}).to_string(),
    };
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request.get("method").and_then(|value| value.as_str()).unwrap_or("");
    if method == "agent.shutdown" {
        shutdown.cancel();
        return json!({"id": id, "result": {"ok": true}}).to_string();
    }
    let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
    tracing::debug!(method, "command");
    let response = match engine.dispatch(method, params).await {
        Ok(result) => json!({"id": id, "result": result}),
        Err(err) => json!({"id": id, "error": err}),
    };
    if matches!(method, "schedules.save" | "schedules.delete" | "scheduler.tick") {
        let _ = register_wakeup(engine);
        let _ = data_dir;
    }
    response.to_string()
}

pub struct IpcClient {
    writer: Mutex<ClientWriter>,
    pending: Arc<std::sync::Mutex<std::collections::HashMap<String, tokio::sync::oneshot::Sender<Value>>>>,
    events: broadcast::Sender<Value>,
}

#[cfg(unix)]
type ClientWriter = tokio::net::unix::OwnedWriteHalf;

#[cfg(windows)]
type ClientWriter = tokio::net::windows::named_pipe::NamedPipeClient;

impl IpcClient {
    pub async fn connect(data_dir: &Path) -> Result<Self> {
        #[cfg(unix)]
        let stream = tokio::net::UnixStream::connect(socket_path(data_dir)).await.context("connect to Harbor")?;
        #[cfg(windows)]
        let stream = tokio::net::windows::named_pipe::ClientOptions::new()
            .open(socket_path(data_dir).as_os_str())
            .context("connect to Harbor")?;
        #[cfg(unix)]
        let (reader, writer) = stream.into_split();
        #[cfg(windows)]
        let (reader, writer) = tokio::io::split(stream);
        let pending = Arc::new(std::sync::Mutex::new(std::collections::HashMap::<String, tokio::sync::oneshot::Sender<Value>>::new()));
        let (events, _) = broadcast::channel(256);
        let pending_task = pending.clone();
        let events_task = events.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(value) = serde_json::from_str::<Value>(&line) else { continue };
                if let Some(id) = value.get("id").and_then(|id| id.as_str()) {
                    if let Some(sender) = pending_task.lock().expect("pending").remove(id) {
                        let _ = sender.send(value);
                    }
                } else {
                    let _ = events_task.send(value);
                }
            }
        });
        Ok(Self { writer: Mutex::new(writer), pending, events })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pending.lock().expect("pending").insert(id.clone(), sender);
        let line = json!({"id": id, "method": method, "params": params}).to_string();
        {
            let mut writer = self.writer.lock().await;
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
        }
        let response = tokio::time::timeout(Duration::from_secs(120), receiver).await.context("Harbor did not answer")??;
        self.pending.lock().expect("pending").remove(&id);
        if let Some(err) = response.get("error") {
            return Err(anyhow!(err.to_string()));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }
}

pub fn read_native_message(reader: &mut impl Read) -> Result<Value> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 1024 * 1024 {
        return Err(anyhow!("native message is too large"));
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body)?;
    Ok(serde_json::from_slice(&body)?)
}

pub fn write_native_message(writer: &mut impl Write, value: &Value) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    writer.write_all(&(body.len() as u32).to_le_bytes())?;
    writer.write_all(&body)?;
    writer.flush()?;
    Ok(())
}

async fn native_messaging(data_dir: &Path) -> Result<()> {
    if !agent_is_running(data_dir).await {
        let exe = std::env::current_exe()?;
        std::process::Command::new(exe).arg("--data-dir").arg(data_dir).stdin(std::process::Stdio::null()).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn()?;
        let start = std::time::Instant::now();
        while !agent_is_running(data_dir).await {
            if start.elapsed() > Duration::from_secs(8) {
                return Err(anyhow!("Harbor agent did not start"));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let client = IpcClient::connect(data_dir).await?;
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut input = stdin.lock();
    loop {
        let message = match read_native_message(&mut input) {
            Ok(message) => message,
            Err(err) if err.downcast_ref::<std::io::Error>().is_some_and(|io| io.kind() == std::io::ErrorKind::UnexpectedEof) => break,
            Err(err) => return Err(err),
        };
        let response = client.call("native.handle", message).await.unwrap_or_else(|err| json!({"ok": false, "error": err.to_string()}));
        write_native_message(&mut stdout, &response)?;
    }
    Ok(())
}

async fn agent_is_running(data_dir: &Path) -> bool {
    IpcClient::connect(data_dir).await.is_ok()
}

#[cfg(not(target_os = "android"))]
async fn clipboard_loop(engine: EngineHandle, shutdown: CancellationToken) {
    let mut clipboard = match arboard::Clipboard::new() {
        Ok(clipboard) => clipboard,
        Err(_) => return,
    };
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
        let enabled = engine.dispatch("settings.get", json!({})).await.ok().and_then(|v| v.get("clipboard_watch").and_then(|v| v.as_bool())).unwrap_or(false);
        if !enabled { continue; }
        if let Ok(text) = clipboard.get_text() {
            engine.note_clipboard(&text);
        }
    }
}

fn show_notification(title: &str, body: &str) {
    #[cfg(not(target_os = "android"))]
    {
        let _ = notify_rust::Notification::new().summary(title).body(body).show();
    }
    #[cfg(target_os = "android")]
    {
        let _ = (title, body);
    }
}

fn platform_secrets(data_dir: &Path) -> Arc<dyn Secrets> {
    #[cfg(not(target_os = "android"))]
    {
        let _ = data_dir;
        Arc::new(KeyringSecrets)
    }
    #[cfg(target_os = "android")]
    {
        Arc::new(FileSecrets::new(data_dir.join("secrets.json")))
    }
}

#[cfg(not(target_os = "android"))]
struct KeyringSecrets;

#[cfg(not(target_os = "android"))]
impl Secrets for KeyringSecrets {
    fn put(&self, account: &str, secret: &str) -> anyhow::Result<()> {
        let entry = keyring::Entry::new("Harbor", account)?;
        entry.set_password(secret)?;
        Ok(())
    }
    fn get(&self, account: &str) -> anyhow::Result<Option<String>> {
        let entry = keyring::Entry::new("Harbor", account)?;
        match entry.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(err) => Err(anyhow!(err)),
        }
    }
    fn delete(&self, account: &str) -> anyhow::Result<()> {
        let entry = keyring::Entry::new("Harbor", account)?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }
}

#[cfg(target_os = "android")]
struct FileSecrets {
    path: PathBuf,
}

#[cfg(target_os = "android")]
impl FileSecrets {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn load(&self) -> anyhow::Result<std::collections::HashMap<String, String>> {
        if !self.path.exists() {
            return Ok(std::collections::HashMap::new());
        }
        let text = std::fs::read_to_string(&self.path)?;
        Ok(serde_json::from_str(&text).unwrap_or_default())
    }

    fn save(&self, values: &std::collections::HashMap<String, String>) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, serde_json::to_vec(values)?)?;
        Ok(())
    }
}

#[cfg(target_os = "android")]
impl Secrets for FileSecrets {
    fn put(&self, account: &str, secret: &str) -> anyhow::Result<()> {
        let mut values = self.load()?;
        values.insert(account.to_string(), secret.to_string());
        self.save(&values)
    }
    fn get(&self, account: &str) -> anyhow::Result<Option<String>> {
        Ok(self.load()?.get(account).cloned())
    }
    fn delete(&self, account: &str) -> anyhow::Result<()> {
        let mut values = self.load()?;
        values.remove(account);
        self.save(&values)
    }
}

pub fn install_host(data_dir: &Path) -> Result<()> {
    let _ = data_dir;
    let exe = std::env::current_exe()?;
    let exe = exe.to_string_lossy().replace('\\', "\\\\");
    let chrome = json!({
        "name": NATIVE_HOST_NAME,
        "description": "Harbor download manager",
        "path": exe,
        "type": "stdio",
        "allowed_origins": [format!("chrome-extension://{EXTENSION_ID}/")]
    });
    let firefox = json!({
        "name": NATIVE_HOST_NAME,
        "description": "Harbor download manager",
        "path": exe,
        "type": "stdio",
        "allowed_extensions": [FIREFOX_EXTENSION_ID]
    });
    for dir in chrome_host_dirs() {
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(format!("{NATIVE_HOST_NAME}.json")), serde_json::to_vec_pretty(&chrome)?)?;
    }
    for dir in firefox_host_dirs() {
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(format!("{NATIVE_HOST_NAME}.json")), serde_json::to_vec_pretty(&firefox)?)?;
    }
    println!("Installed the Harbor native messaging host for Chrome, Edge, Brave, and Firefox.");
    println!("Load the unpacked extension, then run this command again if the agent path changes.");
    Ok(())
}

fn chrome_host_dirs() -> Vec<PathBuf> {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    #[cfg(target_os = "macos")]
    {
        let base = home.join("Library/Application Support");
        return vec![
            base.join("Google/Chrome/NativeMessagingHosts"),
            base.join("Microsoft Edge/NativeMessagingHosts"),
            base.join("BraveSoftware/Brave-Browser/NativeMessagingHosts"),
            base.join("Chromium/NativeMessagingHosts"),
        ];
    }
    #[cfg(target_os = "linux")]
    {
        let config = dirs::config_dir().unwrap_or(home.join(".config"));
        return vec![
            config.join("google-chrome/NativeMessagingHosts"),
            config.join("microsoft-edge/NativeMessagingHosts"),
            config.join("BraveSoftware/Brave-Browser/NativeMessagingHosts"),
            config.join("chromium/NativeMessagingHosts"),
        ];
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = home;
        Vec::new()
    }
}

fn firefox_host_dirs() -> Vec<PathBuf> {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    #[cfg(target_os = "macos")]
    {
        return vec![home.join("Library/Application Support/Mozilla/NativeMessagingHosts")];
    }
    #[cfg(target_os = "linux")]
    {
        return vec![home.join(".mozilla/native-messaging-hosts")];
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = home;
        Vec::new()
    }
}

fn register_wakeup(engine: &EngineHandle) -> Result<()> {
    if std::env::var("HARBOR_DISABLE_OS_TIMER").ok().as_deref() == Some("1") {
        return Ok(());
    }
    let when = engine.next_wakeup().map_err(anyhow::Error::msg)?;
    let exe = std::env::current_exe()?;
    let data_dir = engine.database_path().parent().unwrap_or(Path::new(".")).to_path_buf();
    #[cfg(target_os = "macos")]
    return register_launchd(when, &exe, &data_dir);
    #[cfg(target_os = "linux")]
    return register_systemd(when, &exe, &data_dir);
    #[cfg(windows)]
    return register_task(when, &exe, &data_dir);
    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    {
        let _ = (when, exe, data_dir);
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn register_launchd(when: Option<i64>, exe: &Path, data_dir: &Path) -> Result<()> {
    use chrono::{Datelike, Timelike};
    let home = dirs::home_dir().context("home directory")?;
    let plist = home.join("Library/LaunchAgents/com.harbor.download.wakeup.plist");
    let uid = std::process::Command::new("id").arg("-u").output().ok().and_then(|o| String::from_utf8(o.stdout).ok()).map(|s| s.trim().to_string()).unwrap_or_else(|| "501".into());
    let domain = format!("gui/{uid}");
    let _ = std::process::Command::new("launchctl").args(["bootout", &domain, plist.to_str().unwrap_or("")]).status();
    let Some(when) = when else {
        let _ = std::fs::remove_file(&plist);
        return Ok(());
    };
    let local = chrono::DateTime::from_timestamp_millis(when).context("schedule time")?.with_timezone(&chrono::Local);
    std::fs::create_dir_all(plist.parent().unwrap())?;
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>com.harbor.download.wakeup</string>
<key>ProgramArguments</key><array>
<string>{}</string><string>--run-due</string><string>--data-dir</string><string>{}</string>
</array>
<key>StartCalendarInterval</key><dict>
<key>Month</key><integer>{}</integer>
<key>Day</key><integer>{}</integer>
<key>Hour</key><integer>{}</integer>
<key>Minute</key><integer>{}</integer>
</dict>
</dict></plist>
"#,
        xml_escape(&exe.display().to_string()),
        xml_escape(&data_dir.display().to_string()),
        local.month(),
        local.day(),
        local.hour(),
        local.minute()
    );
    std::fs::write(&plist, body)?;
    let _ = std::process::Command::new("launchctl").args(["bootstrap", &domain, plist.to_str().unwrap_or("")]).status();
    Ok(())
}

#[cfg(target_os = "linux")]
fn register_systemd(when: Option<i64>, exe: &Path, data_dir: &Path) -> Result<()> {
    let config = dirs::config_dir().context("config dir")?.join("systemd/user");
    std::fs::create_dir_all(&config)?;
    let service = config.join("harbor-wakeup.service");
    let timer = config.join("harbor-wakeup.timer");
    let Some(when) = when else {
        let _ = std::process::Command::new("systemctl").args(["--user", "disable", "--now", "harbor-wakeup.timer"]).status();
        let _ = std::fs::remove_file(&service);
        let _ = std::fs::remove_file(&timer);
        return Ok(());
    };
    let local = chrono::DateTime::from_timestamp_millis(when).context("schedule time")?.with_timezone(&chrono::Local);
    std::fs::write(&service, format!("[Unit]\nDescription=Harbor scheduled downloads\n[Service]\nType=oneshot\nExecStart=\"{}\" --run-due --data-dir \"{}\"\n", exe.display(), data_dir.display()))?;
    std::fs::write(&timer, format!("[Unit]\nDescription=Harbor wakeup\n[Timer]\nOnCalendar={} {:02}:{:02}:00\nPersistent=true\n[Install]\nWantedBy=timers.target\n", local.format("%Y-%m-%d"), local.hour(), local.minute()))?;
    let _ = std::process::Command::new("systemctl").args(["--user", "daemon-reload"]).status();
    let _ = std::process::Command::new("systemctl").args(["--user", "enable", "--now", "harbor-wakeup.timer"]).status();
    Ok(())
}

#[cfg(windows)]
fn register_task(when: Option<i64>, exe: &Path, data_dir: &Path) -> Result<()> {
    let _ = std::process::Command::new("schtasks").args(["/Delete", "/TN", "HarborWake", "/F"]).status();
    let Some(when) = when else { return Ok(()) };
    let local = chrono::DateTime::from_timestamp_millis(when).context("schedule time")?.with_timezone(&chrono::Local);
    let start = format!("{:02}:{:02}", local.hour(), local.minute());
    let date = local.format("%m/%d/%Y").to_string();
    let command = format!("\"{}\" --run-due --data-dir \"{}\"", exe.display(), data_dir.display());
    let _ = std::process::Command::new("schtasks").args(["/Create", "/F", "/TN", "HarborWake", "/SC", "ONCE", "/ST", &start, "/SD", &date, "/TR", &command]).status();
    Ok(())
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> String {
    value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn tracing_subscriber() {
    let _ = tracing_subscriber_init();
}

fn tracing_subscriber_init() -> Result<()> {
    // Keep the default logger quiet unless RUST_LOG is set. Avoid a hard dependency on the fmt subscriber.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_frames_round_trip() {
        let value = json!({"action": "ping"});
        let mut buf = Vec::new();
        write_native_message(&mut buf, &value).unwrap();
        let decoded = read_native_message(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded["action"], "ping");
        assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize, buf.len() - 4);
    }
}
