//! Harbor's download engine. The desktop window sends commands; this crate moves bytes.

mod error;
mod ftp;
mod grabber;
mod http;
mod jobs;
mod limiter;
mod queue;
mod transfer;

pub mod api;

pub use error::EngineError;

use crate::http::build_client;
use crate::jobs::{Job as DownloadJob, RuntimeStats};
use crate::limiter::Limiter;
use crate::transfer::Gauges;
use harbor_core::{local_minutes, now_ms, Download, DownloadStatus, Segment};
use harbor_store::Store;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::broadcast;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum EngineEvent {
    Progress {
        download_id: String,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
        speed_bps: u64,
        average_bps: u64,
        eta_secs: Option<u64>,
        active_connections: u32,
        accelerated: bool,
        status: DownloadStatus,
        segments: Vec<Segment>,
    },
    Status {
        download_id: String,
        status: DownloadStatus,
        error: Option<String>,
        warning: Option<String>,
    },
    Clipboard { url: String },
    Notification { title: String, body: String },
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub segment_threshold: u64,
    pub min_split_bytes: u64,
    pub max_segments: usize,
    pub chunk_bytes: u64,
    pub flush_interval: Duration,
    pub max_retries: u32,
    pub adaptive: bool,
    pub resume_on_open: bool,
    pub quota_window: Duration,
    pub queue_retry: Duration,
    pub danger_accept_invalid_certs: bool,
    pub extra_root_pem: Option<String>,
    pub user_agent: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            segment_threshold: harbor_core::ACCELERATION_MIN_BYTES,
            min_split_bytes: 256 * 1024,
            max_segments: 64,
            chunk_bytes: 512 * 1024,
            flush_interval: Duration::from_millis(500),
            max_retries: 5,
            adaptive: true,
            resume_on_open: true,
            quota_window: Duration::from_secs(3600),
            queue_retry: Duration::from_secs(30),
            danger_accept_invalid_certs: false,
            extra_root_pem: None,
            user_agent: format!("Harbor/{}", harbor_core::VERSION),
        }
    }
}

pub trait Secrets: Send + Sync {
    fn put(&self, account: &str, secret: &str) -> anyhow::Result<()>;
    fn get(&self, account: &str) -> anyhow::Result<Option<String>>;
    fn delete(&self, account: &str) -> anyhow::Result<()>;
}

#[derive(Default)]
pub struct MemorySecrets {
    inner: Mutex<HashMap<String, String>>,
}

impl Secrets for MemorySecrets {
    fn put(&self, account: &str, secret: &str) -> anyhow::Result<()> {
        self.inner.lock().expect("secrets").insert(account.to_string(), secret.to_string());
        Ok(())
    }

    fn get(&self, account: &str) -> anyhow::Result<Option<String>> {
        Ok(self.inner.lock().expect("secrets").get(account).cloned())
    }

    fn delete(&self, account: &str) -> anyhow::Result<()> {
        self.inner.lock().expect("secrets").remove(account);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct ClockOverride {
    pub now_ms: Option<i64>,
    pub now_minutes: Option<i32>,
}

pub struct EngineInner {
    pub store: Store,
    pub secrets: Arc<dyn Secrets>,
    pub config: EngineConfig,
    pub client: RwLock<reqwest::Client>,
    pub proxy: Mutex<String>,
    pub jobs: Mutex<HashMap<String, DownloadJob>>,
    pub supervisors: Mutex<HashMap<String, tokio_util::sync::CancellationToken>>,
    pub stats: Mutex<HashMap<String, RuntimeStats>>,
    pub gauges: Mutex<HashMap<String, Arc<Gauges>>>,
    pub events: broadcast::Sender<EngineEvent>,
    pub limiter: Limiter,
    pub generation: AtomicU64,
    pub clock: Mutex<ClockOverride>,
    pub last_clipboard: Mutex<String>,
}

#[derive(Clone)]
pub struct EngineHandle {
    inner: Arc<EngineInner>,
}

impl std::ops::Deref for EngineHandle {
    type Target = EngineInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl EngineHandle {
    pub fn open(database: PathBuf, config: EngineConfig, secrets: Arc<dyn Secrets>) -> Result<Self, String> {
        let store = Store::open(&database).map_err(|err| err.to_string())?;
        let mut settings = store.settings().map_err(|err| err.to_string())?;
        if settings.default_directory.trim().is_empty() {
            let directory = dirs::download_dir()
                .or_else(|| database.parent().map(|path| path.to_path_buf()))
                .unwrap_or_else(|| PathBuf::from("."))
                .join("Harbor");
            let _ = std::fs::create_dir_all(&directory);
            settings.default_directory = directory.to_string_lossy().to_string();
            store.save_settings(&settings).map_err(|err| err.to_string())?;
        }
        let client = build_client(&config, &settings.proxy_url).map_err(|err| err.to_string())?;
        let (events, _) = broadcast::channel(512);
        let handle = Self {
            inner: Arc::new(EngineInner {
                store,
                secrets,
                limiter: Limiter::new(settings.bandwidth_limit_bps),
                config,
                client: RwLock::new(client),
                proxy: Mutex::new(settings.proxy_url),
                jobs: Mutex::new(HashMap::new()),
                supervisors: Mutex::new(HashMap::new()),
                stats: Mutex::new(HashMap::new()),
                gauges: Mutex::new(HashMap::new()),
                events,
                generation: AtomicU64::new(1),
                clock: Mutex::new(ClockOverride::default()),
                last_clipboard: Mutex::new(String::new()),
            }),
        };
        Ok(handle)
    }

    pub fn boot(&self) -> Result<(), String> {
        if !self.config.resume_on_open {
            return Ok(());
        }
        for id in self.store.ids_with_status(DownloadStatus::Downloading).map_err(|err| err.to_string())? {
            self.start(&id)?;
        }
        for queue in self.store.queues().map_err(|err| err.to_string())? {
            if queue.status == harbor_core::QueueStatus::Running {
                self.spawn_supervisor(queue.id);
            }
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    pub fn now_ms(&self) -> i64 {
        self.clock.lock().expect("clock").now_ms.unwrap_or_else(now_ms)
    }

    pub fn now_minutes(&self) -> i32 {
        let clock = self.clock.lock().expect("clock");
        clock.now_minutes.unwrap_or_else(|| local_minutes(clock.now_ms.unwrap_or_else(now_ms)))
    }

    pub fn set_clock(&self, now_ms: Option<i64>, now_minutes: Option<i32>) {
        let mut clock = self.clock.lock().expect("clock");
        clock.now_ms = now_ms;
        clock.now_minutes = now_minutes;
    }

    pub(crate) fn must_download(&self, id: &str) -> Result<Download, EngineError> {
        self.store
            .get_download(id)
            .map_err(|err| EngineError::Fatal(err.to_string()))?
            .ok_or_else(|| EngineError::Fatal("Download not found.".into()))
    }

    pub(crate) fn emit(&self, event: EngineEvent) {
        let _ = self.events.send(event);
    }

    pub fn apply_runtime_settings(&self) -> Result<(), String> {
        let settings = self.store.settings().map_err(|err| err.to_string())?;
        self.limiter.set_rate(settings.bandwidth_limit_bps);
        let mut proxy = self.proxy.lock().expect("proxy");
        if *proxy != settings.proxy_url {
            let client = build_client(&self.config, &settings.proxy_url).map_err(|err| err.to_string())?;
            *self.client.write().expect("client") = client;
            *proxy = settings.proxy_url;
        }
        Ok(())
    }

    pub fn note_clipboard(&self, text: &str) {
        let Some(url) = first_http_url(text) else { return };
        let mut last = self.last_clipboard.lock().expect("clipboard");
        if *last == url {
            return;
        }
        *last = url.clone();
        drop(last);
        self.emit(EngineEvent::Clipboard { url });
    }

    pub fn database_path(&self) -> PathBuf {
        self.store.path().to_path_buf()
    }
}

fn first_http_url(text: &str) -> Option<String> {
    for token in text.split_whitespace() {
        let token = token.trim_matches(|c: char| matches!(c, '"' | '\'' | '<' | '>' | '(' | ')' | ','));
        if token.starts_with("http://") || token.starts_with("https://") || token.starts_with("ftp://") {
            if url::Url::parse(token).is_ok() {
                return Some(token.to_string());
            }
        }
    }
    None
}
