//! Shared types for Harbor. The download engine and the desktop shell both use these
//! records so a download means the same thing on disk, on the socket, and in the window.

mod filename;
mod schedule;
mod secret;
mod segment;

pub use filename::{filename_from_content_disposition, filename_from_url, sanitize_filename};
pub use schedule::{advance_start, local_minutes, stop_reached, Recurrence};
pub use secret::{split_headers, SecretMaterial, SECRET_HEADER_NAMES};
pub use segment::{ranges_overlap, split_largest, RangePiece};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// Files at least this large can use more than one connection when the server accepts ranges.
pub const ACCELERATION_MIN_BYTES: u64 = 256 * 1024;
/// Default simultaneous connections for a new download. Zero means the user has not set a cap elsewhere.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 16;
pub const APP_NAME: &str = "Harbor";
pub const NATIVE_HOST_NAME: &str = "com.harbor.download";
pub const EXTENSION_ID: &str = "kgfejgpbkjaiaohkoofijjdkonojhikn";
pub const FIREFOX_EXTENSION_ID: &str = "harbor@harbor.local";

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct CoreError(pub String);

impl CoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Completed,
    Failed,
    Stopped,
    Scheduled,
}

impl DownloadStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
            Self::Scheduled => "scheduled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "queued" => Self::Queued,
            "downloading" => Self::Downloading,
            "paused" => Self::Paused,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "stopped" => Self::Stopped,
            "scheduled" => Self::Scheduled,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentStatus {
    Pending,
    Active,
    Complete,
}

impl SegmentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Complete => "complete",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => Self::Pending,
            "active" => Self::Active,
            "complete" => Self::Complete,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueKind {
    Download,
    Sync,
}

impl QueueKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Sync => "sync",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "download" => Self::Download,
            "sync" => Self::Sync,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    Idle,
    Running,
    Paused,
}

impl QueueStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Paused => "paused",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "idle" => Self::Idle,
            "running" => Self::Running,
            "paused" => Self::Paused,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Category {
    pub id: String,
    pub name: String,
    pub extensions: Vec<String>,
    pub directory: Option<String>,
    pub site_filter: Vec<String>,
    pub builtin: bool,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Queue {
    pub id: String,
    pub name: String,
    pub kind: QueueKind,
    pub max_concurrent: u32,
    pub status: QueueStatus,
    pub sort_order: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub queue_id: String,
    pub start_at: Option<i64>,
    pub stop_minutes: Option<i32>,
    pub recurrence: Recurrence,
    pub enabled: bool,
    pub last_run: Option<i64>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: String,
    pub download_id: String,
    pub start_byte: u64,
    pub end_byte: u64,
    pub received_bytes: u64,
    pub status: SegmentStatus,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Download {
    pub id: String,
    pub url: String,
    pub final_url: Option<String>,
    pub filename: String,
    pub filename_explicit: bool,
    pub destination_dir: String,
    pub category_id: Option<String>,
    pub category_name: Option<String>,
    pub queue_id: Option<String>,
    pub queue_name: Option<String>,
    pub queue_kind: Option<QueueKind>,
    pub queue_position: i64,
    pub status: DownloadStatus,
    pub total_bytes: Option<u64>,
    pub downloaded_bytes: u64,
    pub mime: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub accept_ranges: bool,
    pub accelerated: bool,
    pub http_status: Option<u16>,
    pub resume_capable: bool,
    pub error: Option<String>,
    pub warning: Option<String>,
    pub has_credentials: bool,
    pub headers: HashMap<String, String>,
    pub max_connections: u32,
    pub bandwidth_limit_bps: u64,
    pub sha256: Option<String>,
    pub replace_existing: bool,
    pub retry_after: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
    pub speed_bps: u64,
    pub average_bps: u64,
    pub eta_secs: Option<u64>,
    pub active_connections: u32,
}

impl Download {
    pub fn destination_path(&self) -> PathBuf {
        PathBuf::from(&self.destination_dir).join(&self.filename)
    }

    pub fn partial_path(&self) -> PathBuf {
        partial_path(&self.destination_dir, &self.filename)
    }
}

pub fn partial_path(dir: &str, filename: &str) -> PathBuf {
    PathBuf::from(dir).join(format!("{filename}.harborpart"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub id: String,
    pub download_id: Option<String>,
    pub url: String,
    pub filename: String,
    pub destination: String,
    pub total_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub completed_at: i64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteAuth {
    pub id: String,
    pub host: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub theme: String,
    pub max_connections: u32,
    pub bandwidth_limit_bps: u64,
    pub quota_bytes_per_hour: u64,
    pub proxy_url: String,
    pub clipboard_watch: bool,
    pub intercept_downloads: bool,
    pub antivirus_command: String,
    pub default_directory: String,
    pub notifications: bool,
    pub update_manifest_url: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            theme: "system".into(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            bandwidth_limit_bps: 0,
            quota_bytes_per_hour: 0,
            proxy_url: String::new(),
            clipboard_watch: false,
            intercept_downloads: true,
            antivirus_command: String::new(),
            default_directory: String::new(),
            notifications: true,
            update_manifest_url: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewDownload {
    pub url: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub destination_dir: Option<String>,
    #[serde(default)]
    pub category_id: Option<String>,
    #[serde(default)]
    pub queue_id: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub cookies: Option<String>,
    #[serde(default)]
    pub start: bool,
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub bandwidth_limit_bps: Option<u64>,
    #[serde(default)]
    pub replace_existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeRequest {
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub cookies: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub final_url: String,
    pub filename: String,
    pub total_bytes: Option<u64>,
    pub mime: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub accept_ranges: bool,
    pub http_status: u16,
    pub resume_capable: bool,
    pub accelerated_possible: bool,
    pub category_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub all: u64,
    pub downloading: u64,
    pub queued: u64,
    pub completed: u64,
    pub paused: u64,
    pub failed: u64,
    pub speed_bps: u64,
    pub categories: Vec<CategoryCount>,
    pub queues: Vec<Queue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryCount {
    pub id: String,
    pub name: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrabbedLink {
    pub url: String,
    pub filename: String,
    pub extension: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStatus {
    pub configured: bool,
    pub current: String,
    pub latest: Option<String>,
    pub notes: Option<String>,
    pub download_url: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListFilter {
    All,
    Downloading,
    Queued,
    Completed,
    Paused,
    Failed,
}

pub fn builtin_category_seeds() -> Vec<Category> {
    let now = now_ms();
    vec![
        category("cat_videos", "Videos", "mp4 mkv avi mov webm m4v flv wmv mpg mpeg", now),
        category("cat_music", "Music", "mp3 flac wav aac m4a ogg wma", now),
        category(
            "cat_documents",
            "Documents",
            "pdf doc docx xls xlsx ppt pptx txt rtf odt csv",
            now,
        ),
        category(
            "cat_compressed",
            "Compressed",
            "zip rar 7z tar gz bz2 xz iso",
            now,
        ),
        category(
            "cat_applications",
            "Applications",
            "exe msi dmg pkg appimage deb rpm apk",
            now,
        ),
    ]
}

fn category(id: &str, name: &str, extensions: &str, now: i64) -> Category {
    Category {
        id: id.into(),
        name: name.into(),
        extensions: extensions.split_whitespace().map(|s| s.to_string()).collect(),
        directory: None,
        site_filter: Vec::new(),
        builtin: true,
        created_at: now,
    }
}

pub fn assign_category(filename: &str, url: &str, categories: &[Category]) -> Option<String> {
    let ext = file_extension(filename);
    let host = url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()));
    let mut ranked: Vec<&Category> = categories.iter().collect();
    ranked.sort_by_key(|c| (c.builtin, c.name.clone()));
    let mut extension_match = None;
    for category in ranked {
        if category.extensions.iter().any(|e| e.eq_ignore_ascii_case(&ext)) {
            if !category.site_filter.is_empty() {
                if let Some(host) = host.as_deref() {
                    let site_ok = category.site_filter.iter().any(|filter| {
                        let filter = filter.trim().trim_start_matches("*.").to_ascii_lowercase();
                        !filter.is_empty() && (host == filter || host.ends_with(&format!(".{filter}")))
                    });
                    if site_ok {
                        return Some(category.id.clone());
                    }
                }
                continue;
            }
            if extension_match.is_none() || !category.builtin {
                if extension_match.is_none() || !category.builtin {
                    extension_match = Some(category.id.clone());
                    if !category.builtin {
                        return extension_match;
                    }
                }
            }
        }
    }
    extension_match
}

pub fn file_extension(filename: &str) -> String {
    std::path::Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

pub fn redact_url(raw: &str) -> Result<String, CoreError> {
    let mut url = url::Url::parse(raw).map_err(|_| CoreError::new("Invalid URL"))?;
    match url.scheme() {
        "http" | "https" | "ftp" => {}
        _ => return Err(CoreError::new("Only http, https, and ftp URLs can be downloaded")),
    }
    let _ = url.set_password(None);
    let _ = url.set_username("");
    Ok(url.to_string())
}

pub fn version_is_newer(latest: &str, current: &str) -> bool {
    let parse = |v: &str| -> Vec<u64> {
        v.trim()
            .trim_start_matches('v')
            .split('.')
            .map(|p| p.chars().take_while(|c| c.is_ascii_digit()).collect::<String>())
            .map(|p| p.parse().unwrap_or(0))
            .collect()
    };
    let a = parse(latest);
    let b = parse(current);
    let len = a.len().max(b.len());
    for i in 0..len {
        let av = a.get(i).copied().unwrap_or(0);
        let bv = b.get(i).copied().unwrap_or(0);
        if av != bv {
            return av > bv;
        }
    }
    false
}

pub fn hash_file(path: &std::path::Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_encode(hasher.finalize().as_slice()))
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

pub fn endpoint_id(data_dir: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(data_dir.as_bytes());
    hex_encode(&digest[..4])
}
