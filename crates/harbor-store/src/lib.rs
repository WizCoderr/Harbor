//! Transactional SQLite storage. Credentials never go in this database.

use anyhow::{anyhow, Context, Result};
use harbor_core::{
    builtin_category_seeds, now_ms, Category, Download, DownloadStatus, HistoryEntry, ListFilter,
    Queue, QueueKind, QueueStatus, Recurrence, Schedule, Segment, SegmentStatus, Settings,
    SiteAuth,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS categories (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    extensions TEXT NOT NULL DEFAULT '',
    directory TEXT,
    site_filter TEXT NOT NULL DEFAULT '',
    builtin INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS queues (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    max_concurrent INTEGER NOT NULL,
    status TEXT NOT NULL,
    sort_order INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS downloads (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    final_url TEXT,
    filename TEXT NOT NULL,
    filename_explicit INTEGER NOT NULL DEFAULT 0,
    destination_dir TEXT NOT NULL,
    category_id TEXT,
    queue_id TEXT,
    queue_position INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    total_bytes INTEGER,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    mime TEXT,
    etag TEXT,
    last_modified TEXT,
    accept_ranges INTEGER NOT NULL DEFAULT 0,
    accelerated INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    resume_capable INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    warning TEXT,
    credential_ref TEXT,
    headers_json TEXT NOT NULL DEFAULT '{}',
    max_connections INTEGER NOT NULL DEFAULT 8,
    bandwidth_limit_bps INTEGER NOT NULL DEFAULT 0,
    sha256 TEXT,
    replace_existing INTEGER NOT NULL DEFAULT 0,
    retry_after INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER
);
CREATE TABLE IF NOT EXISTS segments (
    id TEXT PRIMARY KEY,
    download_id TEXT NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    start_byte INTEGER NOT NULL,
    end_byte INTEGER NOT NULL,
    received_bytes INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY,
    queue_id TEXT NOT NULL UNIQUE REFERENCES queues(id) ON DELETE CASCADE,
    start_at INTEGER,
    stop_minutes INTEGER,
    recurrence TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_run INTEGER,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS history (
    id TEXT PRIMARY KEY,
    download_id TEXT,
    url TEXT NOT NULL,
    filename TEXT NOT NULL,
    destination TEXT NOT NULL,
    total_bytes INTEGER,
    sha256 TEXT,
    completed_at INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS site_auth (
    id TEXT PRIMARY KEY,
    host TEXT NOT NULL UNIQUE,
    credential_ref TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
CREATE INDEX IF NOT EXISTS idx_downloads_queue ON downloads(queue_id, queue_position);
CREATE INDEX IF NOT EXISTS idx_segments_download ON segments(download_id);
"#;

#[derive(Clone)]
pub struct Store {
    inner: std::sync::Arc<StoreInner>,
}

struct StoreInner {
    conn: Mutex<Connection>,
    path: PathBuf,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA)?;
        let store = Self {
            inner: std::sync::Arc::new(StoreInner {
                conn: Mutex::new(conn),
                path: path.to_path_buf(),
            }),
        };
        store.seed()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.inner.conn.lock().expect("sqlite lock")
    }

    fn seed(&self) -> Result<()> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        for category in builtin_category_seeds() {
            tx.execute(
                "INSERT OR IGNORE INTO categories (id, name, extensions, directory, site_filter, builtin, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
                params![
                    category.id,
                    category.name,
                    category.extensions.join(" "),
                    category.directory,
                    category.site_filter.join(" "),
                    category.created_at
                ],
            )?;
        }
        let now = now_ms();
        tx.execute(
            "INSERT OR IGNORE INTO queues (id, name, kind, max_concurrent, status, sort_order, created_at)
             VALUES ('queue_main', 'Main', 'download', 3, 'idle', 0, ?1)",
            params![now],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO queues (id, name, kind, max_concurrent, status, sort_order, created_at)
             VALUES ('queue_sync', 'Synchronization', 'sync', 2, 'idle', 1, ?1)",
            params![now],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn settings(&self) -> Result<Settings> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT key, value FROM settings")?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
        let mut map = HashMap::new();
        for row in rows {
            let (k, v) = row?;
            map.insert(k, v);
        }
        let mut settings = Settings::default();
        if let Some(v) = map.get("theme") {
            settings.theme = v.clone();
        }
        if let Some(v) = map.get("max_connections") {
            settings.max_connections = v.parse().unwrap_or(harbor_core::DEFAULT_MAX_CONNECTIONS);
        }
        if let Some(v) = map.get("bandwidth_limit_bps") {
            settings.bandwidth_limit_bps = v.parse().unwrap_or(0);
        }
        if let Some(v) = map.get("quota_bytes_per_hour") {
            settings.quota_bytes_per_hour = v.parse().unwrap_or(0);
        }
        if let Some(v) = map.get("proxy_url") {
            settings.proxy_url = v.clone();
        }
        if let Some(v) = map.get("clipboard_watch") {
            settings.clipboard_watch = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = map.get("intercept_downloads") {
            settings.intercept_downloads = v != "0" && !v.eq_ignore_ascii_case("false");
        }
        if let Some(v) = map.get("antivirus_command") {
            settings.antivirus_command = v.clone();
        }
        if let Some(v) = map.get("default_directory") {
            settings.default_directory = v.clone();
        }
        if let Some(v) = map.get("notifications") {
            settings.notifications = v != "0" && !v.eq_ignore_ascii_case("false");
        }
        if let Some(v) = map.get("update_manifest_url") {
            settings.update_manifest_url = v.clone();
        }
        Ok(settings)
    }

    pub fn save_settings(&self, settings: &Settings) -> Result<()> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let pairs = [
            ("theme", settings.theme.clone()),
            ("max_connections", settings.max_connections.to_string()),
            ("bandwidth_limit_bps", settings.bandwidth_limit_bps.to_string()),
            ("quota_bytes_per_hour", settings.quota_bytes_per_hour.to_string()),
            ("proxy_url", settings.proxy_url.clone()),
            ("clipboard_watch", bool_str(settings.clipboard_watch)),
            ("intercept_downloads", bool_str(settings.intercept_downloads)),
            ("antivirus_command", settings.antivirus_command.clone()),
            ("default_directory", settings.default_directory.clone()),
            ("notifications", bool_str(settings.notifications)),
            ("update_manifest_url", settings.update_manifest_url.clone()),
        ];
        for (key, value) in pairs {
            tx.execute(
                "INSERT INTO settings (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn try_consume_quota(&self, bytes: u64, limit: u64, window: Duration, now: i64) -> Result<Option<Duration>> {
        if limit == 0 || bytes == 0 {
            return Ok(None);
        }
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let window_ms = window.as_millis() as i64;
        let mut start = setting_i64(&tx, "quota_window_start")?.unwrap_or(now);
        let mut used = setting_i64(&tx, "quota_bytes_used")?.unwrap_or(0);
        if now.saturating_sub(start) >= window_ms {
            start = now;
            used = 0;
        }
        if bytes as i64 > limit as i64 && used == 0 {
            put_setting(&tx, "quota_window_start", &now.to_string())?;
            put_setting(&tx, "quota_bytes_used", &limit.to_string())?;
            tx.commit()?;
            return Ok(None);
        }
        if used.saturating_add(bytes as i64) > limit as i64 {
            put_setting(&tx, "quota_window_start", &start.to_string())?;
            put_setting(&tx, "quota_bytes_used", &used.to_string())?;
            tx.commit()?;
            let remain = window_ms.saturating_sub(now.saturating_sub(start)).max(200);
            return Ok(Some(Duration::from_millis(remain as u64)));
        }
        used += bytes as i64;
        put_setting(&tx, "quota_window_start", &start.to_string())?;
        put_setting(&tx, "quota_bytes_used", &used.to_string())?;
        tx.commit()?;
        Ok(None)
    }

    pub fn categories(&self) -> Result<Vec<Category>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT id, name, extensions, directory, site_filter, builtin, created_at
             FROM categories ORDER BY builtin DESC, name COLLATE NOCASE",
        )?;
        let rows = stmt.query_map([], map_category)?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn upsert_category(&self, category: &Category) -> Result<()> {
        if category.name.trim().is_empty() {
            return Err(anyhow!("Category name is required"));
        }
        let conn = self.lock();
        conn.execute(
            "INSERT INTO categories (id, name, extensions, directory, site_filter, builtin, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                extensions = excluded.extensions,
                directory = excluded.directory,
                site_filter = excluded.site_filter",
            params![
                category.id,
                category.name.trim(),
                category.extensions.join(" "),
                category.directory,
                category.site_filter.join(","),
                category.builtin as i64,
                category.created_at
            ],
        )?;
        Ok(())
    }

    pub fn delete_category(&self, id: &str) -> Result<()> {
        let conn = self.lock();
        let builtin: i64 = conn
            .query_row("SELECT builtin FROM categories WHERE id = ?1", params![id], |row| row.get(0))
            .optional()?
            .unwrap_or(0);
        if builtin == 1 {
            return Err(anyhow!("Built-in categories stay in the sidebar. Edit their folder or extensions instead."));
        }
        let tx = conn.unchecked_transaction()?;
        tx.execute("UPDATE downloads SET category_id = NULL WHERE category_id = ?1", params![id])?;
        tx.execute("DELETE FROM categories WHERE id = ?1 AND builtin = 0", params![id])?;
        tx.commit()?;
        Ok(())
    }

    pub fn queues(&self) -> Result<Vec<Queue>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT id, name, kind, max_concurrent, status, sort_order, created_at
             FROM queues ORDER BY sort_order, name",
        )?;
        let rows = stmt.query_map([], map_queue)?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn queue(&self, id: &str) -> Result<Option<Queue>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT id, name, kind, max_concurrent, status, sort_order, created_at FROM queues WHERE id = ?1",
            params![id],
            map_queue,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn upsert_queue(&self, queue: &Queue) -> Result<()> {
        if queue.name.trim().is_empty() {
            return Err(anyhow!("Queue name is required"));
        }
        let conn = self.lock();
        conn.execute(
            "INSERT INTO queues (id, name, kind, max_concurrent, status, sort_order, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                kind = excluded.kind,
                max_concurrent = excluded.max_concurrent,
                sort_order = excluded.sort_order",
            params![
                queue.id,
                queue.name.trim(),
                queue.kind.as_str(),
                queue.max_concurrent.max(1),
                queue.status.as_str(),
                queue.sort_order,
                queue.created_at
            ],
        )?;
        Ok(())
    }

    pub fn set_queue_status(&self, id: &str, status: QueueStatus) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE queues SET status = ?1 WHERE id = ?2",
            params![status.as_str(), id],
        )?;
        Ok(())
    }

    pub fn delete_queue(&self, id: &str) -> Result<()> {
        if id == "queue_main" || id == "queue_sync" {
            return Err(anyhow!("The main and synchronization queues cannot be deleted"));
        }
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        tx.execute("UPDATE downloads SET queue_id = NULL WHERE queue_id = ?1", params![id])?;
        tx.execute("DELETE FROM schedules WHERE queue_id = ?1", params![id])?;
        tx.execute("DELETE FROM queues WHERE id = ?1", params![id])?;
        tx.commit()?;
        Ok(())
    }

    pub fn schedules(&self) -> Result<Vec<Schedule>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT id, queue_id, start_at, stop_minutes, recurrence, enabled, last_run, created_at FROM schedules",
        )?;
        let rows = stmt.query_map([], map_schedule)?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn schedule_for_queue(&self, queue_id: &str) -> Result<Option<Schedule>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT id, queue_id, start_at, stop_minutes, recurrence, enabled, last_run, created_at
             FROM schedules WHERE queue_id = ?1",
            params![queue_id],
            map_schedule,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn upsert_schedule(&self, schedule: &Schedule) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO schedules (id, queue_id, start_at, stop_minutes, recurrence, enabled, last_run, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(queue_id) DO UPDATE SET
                start_at = excluded.start_at,
                stop_minutes = excluded.stop_minutes,
                recurrence = excluded.recurrence,
                enabled = excluded.enabled,
                last_run = COALESCE(excluded.last_run, schedules.last_run)",
            params![
                schedule.id,
                schedule.queue_id,
                schedule.start_at,
                schedule.stop_minutes,
                schedule.recurrence.as_str(),
                schedule.enabled as i64,
                schedule.last_run,
                schedule.created_at
            ],
        )?;
        Ok(())
    }

    pub fn delete_schedule(&self, id: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute("DELETE FROM schedules WHERE id = ?1 OR queue_id = ?1", params![id])?;
        Ok(())
    }

    pub fn insert_download(&self, download: &Download) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "INSERT INTO downloads (
                id, url, final_url, filename, filename_explicit, destination_dir, category_id, queue_id,
                queue_position, status, total_bytes, downloaded_bytes, mime, etag, last_modified,
                accept_ranges, accelerated, http_status, resume_capable, error, warning, credential_ref,
                headers_json, max_connections, bandwidth_limit_bps, sha256, replace_existing, retry_after,
                created_at, updated_at, completed_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                ?9, ?10, ?11, ?12, ?13, ?14, ?15,
                ?16, ?17, ?18, ?19, ?20, ?21, ?22,
                ?23, ?24, ?25, ?26, ?27, ?28,
                ?29, ?30, ?31
             )",
            params![
                download.id,
                download.url,
                download.final_url,
                download.filename,
                download.filename_explicit as i64,
                download.destination_dir,
                download.category_id,
                download.queue_id,
                download.queue_position,
                download.status.as_str(),
                opt_u64(download.total_bytes),
                download.downloaded_bytes as i64,
                download.mime,
                download.etag,
                download.last_modified,
                download.accept_ranges as i64,
                download.accelerated as i64,
                download.http_status.map(|v| v as i64),
                download.resume_capable as i64,
                download.error,
                download.warning,
                if download.has_credentials { Some(format!("download:{}", download.id)) } else { None },
                serde_json::to_string(&download.headers).unwrap_or_else(|_| "{}".into()),
                download.max_connections as i64,
                download.bandwidth_limit_bps as i64,
                download.sha256,
                download.replace_existing as i64,
                download.retry_after,
                download.created_at,
                download.updated_at,
                download.completed_at
            ],
        )?;
        Ok(())
    }

    pub fn get_download(&self, id: &str) -> Result<Option<Download>> {
        let conn = self.lock();
        query_download(&conn, id)
    }

    pub fn list_downloads(&self, filter: ListFilter, category_id: Option<&str>, queue_id: Option<&str>, query: &str) -> Result<Vec<Download>> {
        let conn = self.lock();
        let mut sql = String::from(&download_select());
        sql.push_str(" WHERE 1=1");
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        match filter {
            ListFilter::All => {}
            ListFilter::Downloading => sql.push_str(" AND d.status = 'downloading'"),
            ListFilter::Queued => sql.push_str(" AND d.status IN ('queued', 'scheduled')"),
            ListFilter::Completed => sql.push_str(" AND d.status = 'completed'"),
            ListFilter::Paused => sql.push_str(" AND d.status = 'paused'"),
            ListFilter::Failed => sql.push_str(" AND d.status = 'failed'"),
        }
        if let Some(category_id) = category_id {
            sql.push_str(" AND d.category_id = ?");
            values.push(Box::new(category_id.to_string()));
        }
        if let Some(queue_id) = queue_id {
            sql.push_str(" AND d.queue_id = ?");
            values.push(Box::new(queue_id.to_string()));
        }
        if !query.trim().is_empty() {
            sql.push_str(" AND (d.filename LIKE ? ESCAPE '\\' OR d.url LIKE ? ESCAPE '\\')");
            let like = format!("%{}%", escape_like(query.trim()));
            values.push(Box::new(like.clone()));
            values.push(Box::new(like));
        }
        sql.push_str(" ORDER BY d.created_at DESC");
        let mut stmt = conn.prepare(&sql)?;
        let params = rusqlite::params_from_iter(values.iter().map(|v| v.as_ref()));
        let rows = stmt.query_map(params, map_download)?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn ids_with_status(&self, status: DownloadStatus) -> Result<Vec<String>> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT id FROM downloads WHERE status = ?1 ORDER BY queue_position, created_at")?;
        let rows = stmt.query_map(params![status.as_str()], |row| row.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn update_meta(
        &self,
        id: &str,
        final_url: &str,
        filename: &str,
        filename_explicit: bool,
        total_bytes: Option<u64>,
        mime: Option<&str>,
        etag: Option<&str>,
        last_modified: Option<&str>,
        accept_ranges: bool,
        resume_capable: bool,
        http_status: Option<u16>,
        category_id: Option<&str>,
    ) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE downloads SET final_url = ?1, filename = ?2, filename_explicit = ?3, total_bytes = ?4,
             mime = ?5, etag = ?6, last_modified = ?7, accept_ranges = ?8, resume_capable = ?9,
             http_status = ?10, category_id = COALESCE(?11, category_id), updated_at = ?12
             WHERE id = ?13",
            params![
                final_url,
                filename,
                filename_explicit as i64,
                opt_u64(total_bytes),
                mime,
                etag,
                last_modified,
                accept_ranges as i64,
                resume_capable as i64,
                http_status.map(|v| v as i64),
                category_id,
                now_ms(),
                id
            ],
        )?;
        Ok(())
    }

    pub fn set_status(&self, id: &str, status: DownloadStatus, error: Option<&str>, warning: Option<&str>) -> Result<()> {
        let conn = self.lock();
        let completed_at = if status == DownloadStatus::Completed { Some(now_ms()) } else { None };
        conn.execute(
            "UPDATE downloads SET status = ?1, error = ?2, warning = COALESCE(?3, warning), updated_at = ?4,
             completed_at = COALESCE(?5, completed_at) WHERE id = ?6",
            params![status.as_str(), error, warning, now_ms(), completed_at, id],
        )?;
        Ok(())
    }

    pub fn set_http_status(&self, id: &str, status: u16) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE downloads SET http_status = ?1, updated_at = ?2 WHERE id = ?3",
            params![status as i64, now_ms(), id],
        )?;
        Ok(())
    }

    pub fn set_progress(&self, id: &str, downloaded: u64, accelerated: bool, http_status: Option<u16>) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE downloads SET downloaded_bytes = ?1, accelerated = ?2, http_status = COALESCE(?3, http_status), updated_at = ?4 WHERE id = ?5",
            params![downloaded as i64, accelerated as i64, http_status.map(|v| v as i64), now_ms(), id],
        )?;
        Ok(())
    }

    pub fn set_retry_after(&self, id: &str, when: Option<i64>) -> Result<()> {
        let conn = self.lock();
        conn.execute("UPDATE downloads SET retry_after = ?1, updated_at = ?2 WHERE id = ?3", params![when, now_ms(), id])?;
        Ok(())
    }

    pub fn move_to_queue_end(&self, id: &str) -> Result<()> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let queue_id: Option<String> = tx
            .query_row("SELECT queue_id FROM downloads WHERE id = ?1", params![id], |row| row.get(0))
            .optional()?;
        if let Some(queue_id) = queue_id {
            let max_pos: i64 = tx.query_row(
                "SELECT COALESCE(MAX(queue_position), 0) FROM downloads WHERE queue_id = ?1",
                params![queue_id],
                |row| row.get(0),
            )?;
            tx.execute(
                "UPDATE downloads SET queue_position = ?1, updated_at = ?2 WHERE id = ?3",
                params![max_pos + 1, now_ms(), id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn reorder(&self, id: &str, direction: &str) -> Result<()> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let (queue_id, position): (Option<String>, i64) = tx.query_row(
            "SELECT queue_id, queue_position FROM downloads WHERE id = ?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let Some(queue_id) = queue_id else {
            return Ok(());
        };
        let neighbor: Option<(String, i64)> = if direction == "up" {
            tx.query_row(
                "SELECT id, queue_position FROM downloads WHERE queue_id = ?1 AND queue_position < ?2
                 ORDER BY queue_position DESC LIMIT 1",
                params![queue_id, position],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
        } else {
            tx.query_row(
                "SELECT id, queue_position FROM downloads WHERE queue_id = ?1 AND queue_position > ?2
                 ORDER BY queue_position ASC LIMIT 1",
                params![queue_id, position],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
        };
        if let Some((other_id, other_pos)) = neighbor {
            tx.execute("UPDATE downloads SET queue_position = ?1 WHERE id = ?2", params![other_pos, id])?;
            tx.execute("UPDATE downloads SET queue_position = ?1 WHERE id = ?2", params![position, other_id])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn next_queue_position(&self, queue_id: &str) -> Result<i64> {
        let conn = self.lock();
        let max_pos: i64 = conn.query_row(
            "SELECT COALESCE(MAX(queue_position), 0) FROM downloads WHERE queue_id = ?1",
            params![queue_id],
            |row| row.get(0),
        )?;
        Ok(max_pos + 1)
    }

    pub fn next_queue_item(&self, queue_id: &str, now: i64) -> Result<Option<Download>> {
        let conn = self.lock();
        let id: Option<String> = conn
            .query_row(
                "SELECT id FROM downloads
                 WHERE queue_id = ?1
                   AND status IN ('queued', 'paused', 'stopped', 'failed', 'scheduled')
                   AND (retry_after IS NULL OR retry_after <= ?2)
                 ORDER BY queue_position, created_at LIMIT 1",
                params![queue_id, now],
                |row| row.get(0),
            )
            .optional()?;
        drop(conn);
        match id {
            Some(id) => self.get_download(&id),
            None => Ok(None),
        }
    }

    pub fn count_downloading(&self, queue_id: &str) -> Result<u32> {
        let conn = self.lock();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM downloads WHERE queue_id = ?1 AND status = 'downloading'",
            params![queue_id],
            |row| row.get(0),
        )?;
        Ok(count as u32)
    }

    pub fn queue_download_ids(&self, queue_id: &str, status: DownloadStatus) -> Result<Vec<String>> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT id FROM downloads WHERE queue_id = ?1 AND status = ?2")?;
        let rows = stmt.query_map(params![queue_id, status.as_str()], |row| row.get(0))?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn finish_success(&self, id: &str, sha256: &str, warning: Option<&str>) -> Result<()> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let queue_id: Option<String> = tx
            .query_row("SELECT queue_id FROM downloads WHERE id = ?1", params![id], |row| row.get(0))?;
        let kind = if let Some(queue_id) = queue_id.as_deref() {
            tx.query_row("SELECT kind FROM queues WHERE id = ?1", params![queue_id], |row| row.get::<_, String>(0))
                .optional()?
        } else {
            None
        };
        let clear_queue = kind.as_deref() != Some("sync");
        let now = now_ms();
        tx.execute(
            "UPDATE downloads SET status = 'completed', sha256 = ?1, warning = ?2, error = NULL,
             updated_at = ?3, completed_at = ?3, queue_id = CASE WHEN ?4 = 1 THEN NULL ELSE queue_id END
             WHERE id = ?5",
            params![sha256, warning, now, clear_queue as i64, id],
        )?;
        let download = query_download(&tx, id)?.ok_or_else(|| anyhow!("download disappeared"))?;
        tx.execute(
            "INSERT INTO history (id, download_id, url, filename, destination, total_bytes, sha256, completed_at, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'completed')",
            params![
                harbor_core::new_id(),
                download.id,
                download.url,
                download.filename,
                download.destination_path().to_string_lossy().to_string(),
                opt_u64(download.total_bytes),
                sha256,
                now
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn set_filename(&self, id: &str, filename: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE downloads SET filename = ?1, updated_at = ?2 WHERE id = ?3",
            params![filename, now_ms(), id],
        )?;
        Ok(())
    }

    pub fn set_replace_existing(&self, id: &str, replace_existing: bool) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE downloads SET replace_existing = ?1, updated_at = ?2 WHERE id = ?3",
            params![replace_existing as i64, now_ms(), id],
        )?;
        Ok(())
    }

    pub fn reopen_sync_items(&self, queue_id: &str) -> Result<()> {
        let conn = self.lock();
        conn.execute(
            "UPDATE downloads SET status = 'queued', error = NULL, retry_after = NULL, updated_at = ?1
             WHERE queue_id = ?2 AND status = 'completed'",
            params![now_ms(), queue_id],
        )?;
        Ok(())
    }

    pub fn reset_for_restart(&self, id: &str) -> Result<()> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        tx.execute("DELETE FROM segments WHERE download_id = ?1", params![id])?;
        tx.execute(
            "UPDATE downloads SET downloaded_bytes = 0, accelerated = 0, updated_at = ?1 WHERE id = ?2",
            params![now_ms(), id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn segments(&self, download_id: &str) -> Result<Vec<Segment>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT id, download_id, start_byte, end_byte, received_bytes, status, updated_at
             FROM segments WHERE download_id = ?1 ORDER BY start_byte",
        )?;
        let rows = stmt.query_map(params![download_id], map_segment)?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn replace_segments(&self, download_id: &str, segments: &[Segment], downloaded: u64, accelerated: bool) -> Result<()> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        tx.execute("DELETE FROM segments WHERE download_id = ?1", params![download_id])?;
        for segment in segments {
            tx.execute(
                "INSERT INTO segments (id, download_id, start_byte, end_byte, received_bytes, status, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    segment.id,
                    download_id,
                    segment.start_byte as i64,
                    segment.end_byte as i64,
                    segment.received_bytes as i64,
                    segment.status.as_str(),
                    segment.updated_at
                ],
            )?;
        }
        tx.execute(
            "UPDATE downloads SET downloaded_bytes = ?1, accelerated = ?2, updated_at = ?3 WHERE id = ?4",
            params![downloaded as i64, accelerated as i64, now_ms(), download_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete_download(&self, id: &str) -> Result<Option<Download>> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let download = query_download(&tx, id)?;
        tx.execute("DELETE FROM segments WHERE download_id = ?1", params![id])?;
        tx.execute("DELETE FROM downloads WHERE id = ?1", params![id])?;
        tx.commit()?;
        Ok(download)
    }

    pub fn delete_completed(&self) -> Result<Vec<String>> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let mut stmt = tx.prepare("SELECT id FROM downloads WHERE status = 'completed'")?;
        let ids: Vec<String> = stmt.query_map([], |row| row.get(0))?.collect::<std::result::Result<_, _>>()?;
        drop(stmt);
        tx.execute("DELETE FROM segments WHERE download_id IN (SELECT id FROM downloads WHERE status = 'completed')", [])?;
        tx.execute("DELETE FROM downloads WHERE status = 'completed'", [])?;
        tx.commit()?;
        Ok(ids)
    }

    pub fn assign_queue(&self, id: &str, queue_id: Option<&str>) -> Result<()> {
        let conn = self.lock();
        let position = if let Some(queue_id) = queue_id {
            let max_pos: i64 = conn.query_row(
                "SELECT COALESCE(MAX(queue_position), 0) FROM downloads WHERE queue_id = ?1",
                params![queue_id],
                |row| row.get(0),
            )?;
            max_pos + 1
        } else {
            0
        };
        conn.execute(
            "UPDATE downloads SET queue_id = ?1, queue_position = ?2, updated_at = ?3 WHERE id = ?4",
            params![queue_id, position, now_ms(), id],
        )?;
        Ok(())
    }

    pub fn history(&self) -> Result<Vec<HistoryEntry>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT id, download_id, url, filename, destination, total_bytes, sha256, completed_at, status
             FROM history ORDER BY completed_at DESC LIMIT 500",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(HistoryEntry {
                id: row.get(0)?,
                download_id: row.get(1)?,
                url: row.get(2)?,
                filename: row.get(3)?,
                destination: row.get(4)?,
                total_bytes: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                sha256: row.get(6)?,
                completed_at: row.get(7)?,
                status: row.get(8)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn site_auth(&self) -> Result<Vec<SiteAuth>> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT id, host, created_at FROM site_auth ORDER BY host")?;
        let rows = stmt.query_map([], |row| {
            Ok(SiteAuth { id: row.get(0)?, host: row.get(1)?, created_at: row.get(2)? })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn upsert_site_auth(&self, host: &str, credential_ref: &str) -> Result<SiteAuth> {
        let conn = self.lock();
        let now = now_ms();
        let existing: Option<String> = conn
            .query_row("SELECT id FROM site_auth WHERE host = ?1", params![host], |row| row.get(0))
            .optional()?;
        let id = existing.unwrap_or_else(harbor_core::new_id);
        conn.execute(
            "INSERT INTO site_auth (id, host, credential_ref, created_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(host) DO UPDATE SET credential_ref = excluded.credential_ref",
            params![id, host, credential_ref, now],
        )?;
        Ok(SiteAuth { id, host: host.into(), created_at: now })
    }

    pub fn site_credential_ref(&self, host: &str) -> Result<Option<String>> {
        let conn = self.lock();
        conn.query_row(
            "SELECT credential_ref FROM site_auth WHERE host = ?1",
            params![host],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn delete_site_auth(&self, id: &str) -> Result<Option<String>> {
        let conn = self.lock();
        let tx = conn.unchecked_transaction()?;
        let refer: Option<String> = tx
            .query_row(
                "SELECT credential_ref FROM site_auth WHERE id = ?1 OR host = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        tx.execute("DELETE FROM site_auth WHERE id = ?1 OR host = ?1", params![id])?;
        tx.commit()?;
        Ok(refer)
    }

    pub fn counts(&self) -> Result<HashMap<String, u64>> {
        let conn = self.lock();
        let mut map = HashMap::new();
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM downloads", [], |row| row.get(0))?;
        map.insert("all".into(), total as u64);
        let mut stmt = conn.prepare("SELECT status, COUNT(*) FROM downloads GROUP BY status")?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?;
        for row in rows {
            let (status, count) = row?;
            map.insert(status, count as u64);
        }
        Ok(map)
    }

    pub fn running_queue_count(&self) -> Result<i64> {
        let conn = self.lock();
        conn.query_row(
            "SELECT COUNT(*) FROM queues WHERE status = 'running'",
            [],
            |row| row.get(0),
        )
        .map_err(Into::into)
    }
}

fn download_select() -> String {
    "SELECT d.id, d.url, d.final_url, d.filename, d.filename_explicit, d.destination_dir, d.category_id,
            c.name, d.queue_id, q.name, q.kind, d.queue_position, d.status, d.total_bytes, d.downloaded_bytes,
            d.mime, d.etag, d.last_modified, d.accept_ranges, d.accelerated, d.http_status, d.resume_capable,
            d.error, d.warning, d.credential_ref, d.headers_json, d.max_connections, d.bandwidth_limit_bps,
            d.sha256, d.replace_existing, d.retry_after, d.created_at, d.updated_at, d.completed_at
     FROM downloads d
     LEFT JOIN categories c ON c.id = d.category_id
     LEFT JOIN queues q ON q.id = d.queue_id"
        .into()
}

fn query_download(conn: &Connection, id: &str) -> Result<Option<Download>> {
    let sql = format!("{} WHERE d.id = ?1", download_select());
    conn.query_row(&sql, params![id], map_download).optional().map_err(Into::into)
}

fn map_download(row: &rusqlite::Row<'_>) -> rusqlite::Result<Download> {
    let headers_json: String = row.get(25)?;
    let headers = serde_json::from_str(&headers_json).unwrap_or_default();
    let status_raw: String = row.get(12)?;
    Ok(Download {
        id: row.get(0)?,
        url: row.get(1)?,
        final_url: row.get(2)?,
        filename: row.get(3)?,
        filename_explicit: row.get::<_, i64>(4)? != 0,
        destination_dir: row.get(5)?,
        category_id: row.get(6)?,
        category_name: row.get(7)?,
        queue_id: row.get(8)?,
        queue_name: row.get(9)?,
        queue_kind: row.get::<_, Option<String>>(10)?.and_then(|v| QueueKind::parse(&v)),
        queue_position: row.get(11)?,
        status: DownloadStatus::parse(&status_raw).unwrap_or(DownloadStatus::Failed),
        total_bytes: row.get::<_, Option<i64>>(13)?.map(|v| v as u64),
        downloaded_bytes: row.get::<_, i64>(14)? as u64,
        mime: row.get(15)?,
        etag: row.get(16)?,
        last_modified: row.get(17)?,
        accept_ranges: row.get::<_, i64>(18)? != 0,
        accelerated: row.get::<_, i64>(19)? != 0,
        http_status: row.get::<_, Option<i64>>(20)?.map(|v| v as u16),
        resume_capable: row.get::<_, i64>(21)? != 0,
        error: row.get(22)?,
        warning: row.get(23)?,
        has_credentials: row.get::<_, Option<String>>(24)?.is_some(),
        headers,
        max_connections: row.get::<_, i64>(26)? as u32,
        bandwidth_limit_bps: row.get::<_, i64>(27)? as u64,
        sha256: row.get(28)?,
        replace_existing: row.get::<_, i64>(29)? != 0,
        retry_after: row.get(30)?,
        created_at: row.get(31)?,
        updated_at: row.get(32)?,
        completed_at: row.get(33)?,
        speed_bps: 0,
        average_bps: 0,
        eta_secs: None,
        active_connections: 0,
    })
}

fn map_category(row: &rusqlite::Row<'_>) -> rusqlite::Result<Category> {
    let extensions: String = row.get(2)?;
    let sites: String = row.get(4)?;
    Ok(Category {
        id: row.get(0)?,
        name: row.get(1)?,
        extensions: extensions.split_whitespace().filter(|s| !s.is_empty()).map(|s| s.to_string()).collect(),
        directory: row.get(3)?,
        site_filter: sites
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect(),
        builtin: row.get::<_, i64>(5)? != 0,
        created_at: row.get(6)?,
    })
}

fn map_queue(row: &rusqlite::Row<'_>) -> rusqlite::Result<Queue> {
    let kind: String = row.get(2)?;
    let status: String = row.get(4)?;
    Ok(Queue {
        id: row.get(0)?,
        name: row.get(1)?,
        kind: QueueKind::parse(&kind).unwrap_or(QueueKind::Download),
        max_concurrent: row.get::<_, i64>(3)?.max(1) as u32,
        status: QueueStatus::parse(&status).unwrap_or(QueueStatus::Idle),
        sort_order: row.get(5)?,
        created_at: row.get(6)?,
    })
}

fn map_schedule(row: &rusqlite::Row<'_>) -> rusqlite::Result<Schedule> {
    let recurrence: String = row.get(4)?;
    Ok(Schedule {
        id: row.get(0)?,
        queue_id: row.get(1)?,
        start_at: row.get(2)?,
        stop_minutes: row.get(3)?,
        recurrence: Recurrence::parse(&recurrence),
        enabled: row.get::<_, i64>(5)? != 0,
        last_run: row.get(6)?,
        created_at: row.get(7)?,
    })
}

fn map_segment(row: &rusqlite::Row<'_>) -> rusqlite::Result<Segment> {
    let status: String = row.get(5)?;
    Ok(Segment {
        id: row.get(0)?,
        download_id: row.get(1)?,
        start_byte: row.get::<_, i64>(2)? as u64,
        end_byte: row.get::<_, i64>(3)? as u64,
        received_bytes: row.get::<_, i64>(4)? as u64,
        status: SegmentStatus::parse(&status).unwrap_or(SegmentStatus::Pending),
        updated_at: row.get(6)?,
    })
}

fn setting_i64(conn: &Connection, key: &str) -> rusqlite::Result<Option<i64>> {
    let value: Option<String> = conn
        .query_row("SELECT value FROM settings WHERE key = ?1", params![key], |row| row.get(0))
        .optional()?;
    Ok(value.and_then(|v| v.parse().ok()))
}

fn put_setting(conn: &Connection, key: &str, value: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

fn opt_u64(value: Option<u64>) -> Option<i64> {
    value.map(|v| v as i64)
}

fn bool_str(value: bool) -> String {
    if value { "1".into() } else { "0".into() }
}

fn escape_like(value: &str) -> String {
    value.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_categories_and_roundtrips_a_download() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("harbor.db")).unwrap();
        let categories = store.categories().unwrap();
        assert!(categories.iter().any(|c| c.name == "Videos"));
        let queues = store.queues().unwrap();
        assert_eq!(queues.len(), 2);
        let now = now_ms();
        let download = Download {
            id: "d1".into(),
            url: "https://example.test/file.zip".into(),
            final_url: None,
            filename: "file.zip".into(),
            filename_explicit: true,
            destination_dir: dir.path().to_string_lossy().into(),
            category_id: Some("cat_compressed".into()),
            category_name: None,
            queue_id: Some("queue_main".into()),
            queue_name: None,
            queue_kind: None,
            queue_position: 1,
            status: DownloadStatus::Queued,
            total_bytes: Some(10),
            downloaded_bytes: 0,
            mime: Some("application/zip".into()),
            etag: None,
            last_modified: None,
            accept_ranges: true,
            accelerated: false,
            http_status: None,
            resume_capable: true,
            error: None,
            warning: None,
            has_credentials: false,
            headers: HashMap::new(),
            max_connections: 8,
            bandwidth_limit_bps: 0,
            sha256: None,
            replace_existing: false,
            retry_after: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            speed_bps: 0,
            average_bps: 0,
            eta_secs: None,
            active_connections: 0,
        };
        store.insert_download(&download).unwrap();
        let loaded = store.get_download("d1").unwrap().unwrap();
        assert_eq!(loaded.filename, "file.zip");
        assert_eq!(loaded.category_name.as_deref(), Some("Compressed"));
        assert_eq!(loaded.queue_name.as_deref(), Some("Main"));
        store.finish_success("d1", "abc", None).unwrap();
        let done = store.get_download("d1").unwrap().unwrap();
        assert_eq!(done.status, DownloadStatus::Completed);
        assert!(done.queue_id.is_none());
        assert_eq!(store.history().unwrap().len(), 1);
    }
}
