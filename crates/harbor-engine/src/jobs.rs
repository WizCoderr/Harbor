use crate::error::EngineError;
use crate::http::RemoteInfo;
use crate::transfer::{Gauges, TransferNote};
use crate::EngineEvent;
use crate::EngineHandle;
use harbor_core::{assign_category, hash_file, Download, DownloadStatus, QueueKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub struct Job {
    pub generation: u64,
    pub cancel: CancellationToken,
    pub mode: Arc<AtomicU8>,
}

pub struct RuntimeStats {
    pub speed_bps: u64,
    pub average_bps: u64,
    pub eta_secs: Option<u64>,
    pub active_connections: u32,
}

enum Outcome {
    Finished { notify: bool },
    Paused,
    Stopped,
    Abandoned,
    Failed(String),
}

impl EngineHandle {
    pub fn start(&self, id: &str) -> Result<(), String> {
        let download = self.store.get_download(id).map_err(|err| err.to_string())?.ok_or_else(|| "Download not found.".to_string())?;
        if download.status == DownloadStatus::Completed && download.queue_kind != Some(QueueKind::Sync) {
            return Err("That download is already complete.".into());
        }
        {
            let jobs = self.jobs.lock().expect("jobs");
            if jobs.contains_key(id) {
                return Ok(());
            }
        }
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let cancel = CancellationToken::new();
        let mode = Arc::new(AtomicU8::new(0));
        self.jobs.lock().expect("jobs").insert(
            id.to_string(),
            Job { generation, cancel: cancel.clone(), mode: mode.clone() },
        );
        self.store.set_status(id, DownloadStatus::Downloading, None, None).map_err(|err| err.to_string())?;
        let handle = self.clone();
        let download_id = id.to_string();
        tokio::spawn(async move {
            handle.run_job(download_id, generation, cancel, mode).await;
        });
        Ok(())
    }

    pub fn pause(&self, id: &str) -> Result<(), String> {
        if let Some(job) = self.jobs.lock().expect("jobs").get(id) {
            job.mode.store(1, Ordering::SeqCst);
            job.cancel.cancel();
            return Ok(());
        }
        self.mark_if_idle(id, DownloadStatus::Paused)
    }

    pub fn stop(&self, id: &str) -> Result<(), String> {
        if let Some(job) = self.jobs.lock().expect("jobs").get(id) {
            job.mode.store(2, Ordering::SeqCst);
            job.cancel.cancel();
            return Ok(());
        }
        self.mark_if_idle(id, DownloadStatus::Stopped)
    }

    pub fn stop_all(&self) -> Result<(), String> {
        let ids = self.store.list_downloads(harbor_core::ListFilter::All, None, None, "").map_err(|err| err.to_string())?;
        for download in ids {
            if matches!(download.status, DownloadStatus::Downloading | DownloadStatus::Queued | DownloadStatus::Scheduled) {
                self.stop(&download.id)?;
            }
        }
        Ok(())
    }

    pub fn abandon(&self, id: &str) {
        if let Some(job) = self.jobs.lock().expect("jobs").get(id) {
            job.mode.store(3, Ordering::SeqCst);
            job.cancel.cancel();
        }
    }

    pub fn active_jobs(&self) -> usize {
        self.jobs.lock().expect("jobs").len()
    }

    fn mark_if_idle(&self, id: &str, status: DownloadStatus) -> Result<(), String> {
        let Some(download) = self.store.get_download(id).map_err(|err| err.to_string())? else {
            return Err("Download not found.".into());
        };
        if matches!(download.status, DownloadStatus::Completed) {
            return Ok(());
        }
        if matches!(download.status, DownloadStatus::Downloading | DownloadStatus::Queued | DownloadStatus::Scheduled | DownloadStatus::Failed) {
            self.store.set_status(id, status, download.error.as_deref(), None).map_err(|err| err.to_string())?;
            self.emit_status(&download.id, status, download.error.clone(), download.warning.clone());
        }
        Ok(())
    }

    async fn run_job(&self, id: String, generation: u64, cancel: CancellationToken, mode: Arc<AtomicU8>) {
        let gauges = self.install_gauges(&id);
        let ticker_stop = CancellationToken::new();
        let ticker = self.spawn_ticker(id.clone(), gauges.clone(), ticker_stop.clone());
        let outcome = self.execute(&id, &gauges, &cancel, &mode).await;
        ticker_stop.cancel();
        let _ = ticker.await;
        self.publish_progress(&id, &gauges);
        let mut jobs = self.jobs.lock().expect("jobs");
        if jobs.get(&id).map(|job| job.generation) != Some(generation) {
            return;
        }
        let download = self.store.get_download(&id).ok().flatten();
        match outcome {
            Outcome::Finished { notify } => {
                if notify {
                    if let Some(download) = download.as_ref() {
                        self.emit(EngineEvent::Notification {
                            title: "Download complete".into(),
                            body: download.filename.clone(),
                        });
                    }
                }
                if let Some(download) = download.as_ref() {
                    self.emit_status(&id, download.status, download.error.clone(), download.warning.clone());
                }
            }
            Outcome::Paused => {
                let _ = self.store.set_status(&id, DownloadStatus::Paused, None, None);
                self.emit_status(&id, DownloadStatus::Paused, None, None);
            }
            Outcome::Stopped => {
                let _ = self.store.set_status(&id, DownloadStatus::Stopped, None, None);
                self.emit_status(&id, DownloadStatus::Stopped, None, None);
            }
            Outcome::Abandoned => {}
            Outcome::Failed(message) => {
                if let Some(download) = download.as_ref() {
                    if download.queue_id.is_some() {
                        let _ = self.store.move_to_queue_end(&id);
                        let _ = self.store.set_retry_after(&id, Some(self.now_ms() + self.config.queue_retry.as_millis() as i64));
                    }
                }
                let _ = self.store.set_status(&id, DownloadStatus::Failed, Some(&message), None);
                self.emit_status(&id, DownloadStatus::Failed, Some(message.clone()), None);
                let name = download.as_ref().map(|item| item.filename.clone()).unwrap_or_else(|| "Download".into());
                self.emit(EngineEvent::Notification {
                    title: format!("{name} failed"),
                    body: message,
                });
            }
        }
        jobs.remove(&id);
        self.gauges.lock().expect("gauges").remove(&id);
    }

    async fn execute(&self, id: &str, gauges: &Arc<Gauges>, cancel: &CancellationToken, mode: &AtomicU8) -> Outcome {
        if let Some(outcome) = self.sync_precheck(id).await {
            return outcome;
        }
        match self.transfer(id, gauges, cancel, mode).await {
            Ok(TransferNote { warning }) => match self.finalize(id, warning).await {
                Ok(()) => Outcome::Finished { notify: true },
                Err(EngineError::Paused) => Outcome::Paused,
                Err(EngineError::Stopped) => Outcome::Stopped,
                Err(EngineError::Abandoned) => Outcome::Abandoned,
                Err(err) => Outcome::Failed(err.to_string()),
            },
            Err(EngineError::Paused) => Outcome::Paused,
            Err(EngineError::Stopped) => Outcome::Stopped,
            Err(EngineError::Abandoned) => Outcome::Abandoned,
            Err(err) => Outcome::Failed(err.to_string()),
        }
    }

    async fn sync_precheck(&self, id: &str) -> Option<Outcome> {
        let download = self.must_download(id).ok()?;
        if download.queue_kind != Some(QueueKind::Sync) {
            return None;
        }
        let old_etag = download.etag.clone();
        let old_total = download.total_bytes;
        let exists = download.destination_path().exists();
        let info = match self.probe_download(&download).await {
            Ok(info) => info,
            Err(EngineError::Paused) => return Some(Outcome::Paused),
            Err(EngineError::Stopped) => return Some(Outcome::Stopped),
            Err(EngineError::Abandoned) => return Some(Outcome::Abandoned),
            Err(err) => return Some(Outcome::Failed(err.to_string())),
        };
        if self.persist_probe(&download, &info).is_err() {
            return Some(Outcome::Failed("Could not save the file details.".into()));
        }
        let unchanged = exists && old_etag.is_some() && info.etag == old_etag && info.total_bytes == old_total;
        if unchanged {
            let hash = match download.sha256.clone() {
                Some(hash) => hash,
                None => match hash_file(&download.destination_path()) {
                    Ok(hash) => hash,
                    Err(err) => return Some(Outcome::Failed(err.to_string())),
                },
            };
            return match self.store.finish_success(id, &hash, Some("The remote file has not changed.")) {
                Ok(()) => Some(Outcome::Finished { notify: false }),
                Err(err) => Some(Outcome::Failed(err.to_string())),
            };
        }
        if old_etag.is_some() && info.etag != old_etag {
            let _ = self.store.reset_for_restart(id);
            let _ = self.store.set_replace_existing(id, true);
            let _ = std::fs::remove_file(download.partial_path());
        }
        None
    }

    pub(crate) fn persist_probe(&self, download: &Download, info: &RemoteInfo) -> Result<(), EngineError> {
        let filename = if download.filename_explicit { download.filename.clone() } else { info.filename.clone() };
        let categories = self.store.categories().map_err(|err| EngineError::Fatal(err.to_string()))?;
        let category = if download.category_id.is_none() {
            assign_category(&filename, &info.final_url, &categories)
        } else {
            None
        };
        self.store
            .update_meta(
                &download.id,
                &info.final_url,
                &filename,
                download.filename_explicit || filename != "download",
                info.total_bytes.or(download.total_bytes),
                info.mime.as_deref(),
                info.etag.as_deref(),
                info.last_modified.as_deref(),
                info.accept_ranges,
                info.resume_capable,
                Some(info.http_status),
                category.as_deref(),
            )
            .map_err(|err| EngineError::Fatal(err.to_string()))
    }

    async fn finalize(&self, id: &str, warning: Option<String>) -> Result<(), EngineError> {
        let download = self.must_download(id)?;
        let partial = download.partial_path();
        if !partial.exists() {
            let _ = std::fs::File::create(&partial).map_err(crate::error::map_io)?;
        }
        let destination = unique_destination(&download.destination_dir, &download.filename, download.replace_existing);
        if destination.file_name().and_then(|name| name.to_str()) != Some(download.filename.as_str()) {
            if let Some(name) = destination.file_name().and_then(|name| name.to_str()) {
                self.store.set_filename(id, name).map_err(|err| EngineError::Fatal(err.to_string()))?;
            }
        }
        if download.replace_existing && destination.exists() {
            std::fs::remove_file(&destination).map_err(crate::error::map_io)?;
        }
        std::fs::rename(&partial, &destination).map_err(crate::error::map_io)?;
        let hash_path = destination.clone();
        let hash = tokio::task::spawn_blocking(move || hash_file(&hash_path))
            .await
            .map_err(|err| EngineError::Fatal(err.to_string()))?
            .map_err(crate::error::map_io)?;
        let mut warning = warning;
        if let Ok(settings) = self.store.settings() {
            if !settings.antivirus_command.trim().is_empty() {
                match tokio::process::Command::new(settings.antivirus_command.trim()).arg(&destination).status().await {
                    Ok(status) if !status.success() => {
                        warning = Some(merge_warning(warning, format!("Scanner exited with status {}.", status.code().unwrap_or(-1))));
                    }
                    Err(err) => warning = Some(merge_warning(warning, format!("Could not run the scanner: {err}"))),
                    _ => {}
                }
            }
        }
        self.store.finish_success(id, &hash, warning.as_deref()).map_err(|err| EngineError::Fatal(err.to_string()))?;
        let _ = self.store.set_progress(id, download.total_bytes.unwrap_or(0), download.accelerated, download.http_status);
        Ok(())
    }

    fn install_gauges(&self, id: &str) -> Arc<Gauges> {
        let download = self.store.get_download(id).ok().flatten();
        let downloaded = download.as_ref().map(|item| item.downloaded_bytes).unwrap_or(0);
        let limit = download.as_ref().map(|item| item.bandwidth_limit_bps).unwrap_or(0);
        let gauges = Arc::new(Gauges {
            bytes: std::sync::atomic::AtomicU64::new(downloaded),
            connections: std::sync::atomic::AtomicU32::new(0),
            http_status: std::sync::atomic::AtomicU16::new(0),
            accelerated: std::sync::atomic::AtomicBool::new(false),
            segments: std::sync::Mutex::new(Vec::new()),
            local_limit: crate::limiter::Limiter::new(limit),
        });
        self.gauges.lock().expect("gauges").insert(id.to_string(), gauges.clone());
        gauges
    }

    fn spawn_ticker(&self, id: String, gauges: Arc<Gauges>, stop: CancellationToken) -> tokio::task::JoinHandle<()> {
        let handle = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            let started = Instant::now();
            let start_bytes = gauges.bytes.load(Ordering::Relaxed);
            let mut last_bytes = start_bytes;
            let mut last_tick = Instant::now();
            let mut last_flush = Instant::now();
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = interval.tick() => {}
                }
                if stop.is_cancelled() {
                    break;
                }
                handle.publish_sampled(&id, &gauges, started, start_bytes, &mut last_bytes, &mut last_tick);
                if last_flush.elapsed() >= handle.config.flush_interval {
                    if gauges.segments.lock().expect("segments").is_empty() {
                        let _ = handle.store.set_progress(
                            &id,
                            gauges.bytes.load(Ordering::Relaxed),
                            gauges.accelerated.load(Ordering::Relaxed),
                            nonzero_status(gauges.http_status.load(Ordering::Relaxed)),
                        );
                    } else {
                        let _ = handle.flush_segments(&id, &gauges);
                    }
                    last_flush = Instant::now();
                }
            }
        })
    }

    fn publish_sampled(
        &self,
        id: &str,
        gauges: &Gauges,
        started: Instant,
        start_bytes: u64,
        last_bytes: &mut u64,
        last_tick: &mut Instant,
    ) {
        let now_bytes = gauges.bytes.load(Ordering::Relaxed);
        let elapsed = last_tick.elapsed().as_secs_f64().max(0.05);
        let speed = ((now_bytes.saturating_sub(*last_bytes)) as f64 / elapsed) as u64;
        *last_bytes = now_bytes;
        *last_tick = Instant::now();
        let average = ((now_bytes.saturating_sub(start_bytes)) as f64 / started.elapsed().as_secs_f64().max(0.05)) as u64;
        let download = self.store.get_download(id).ok().flatten();
        let total = download.as_ref().and_then(|item| item.total_bytes);
        let eta = total.and_then(|total| {
            if speed > 0 && total > now_bytes {
                Some((total - now_bytes) / speed)
            } else {
                None
            }
        });
        let connections = gauges.connections.load(Ordering::Relaxed);
        self.stats.lock().expect("stats").insert(
            id.to_string(),
            RuntimeStats { speed_bps: speed, average_bps: average, eta_secs: eta, active_connections: connections },
        );
        self.emit(EngineEvent::Progress {
            download_id: id.to_string(),
            downloaded_bytes: now_bytes,
            total_bytes: total,
            speed_bps: speed,
            average_bps: average,
            eta_secs: eta,
            active_connections: connections,
            accelerated: gauges.accelerated.load(Ordering::Relaxed),
            status: download.as_ref().map(|item| item.status).unwrap_or(DownloadStatus::Downloading),
            segments: gauges.segment_snapshot(id),
        });
    }

    fn publish_progress(&self, id: &str, gauges: &Gauges) {
        let accelerated = gauges.accelerated.load(Ordering::Relaxed) || !gauges.segments.lock().expect("segments").is_empty();
        if gauges.segments.lock().expect("segments").is_empty() {
            let _ = self.store.set_progress(id, gauges.bytes.load(Ordering::Relaxed), accelerated, nonzero_status(gauges.http_status.load(Ordering::Relaxed)));
        } else {
            let _ = self.flush_segments(id, gauges);
        }
    }

    pub(crate) fn decorate(&self, mut download: Download) -> Download {
        if let Some(stats) = self.stats.lock().expect("stats").get(&download.id) {
            download.speed_bps = stats.speed_bps;
            download.average_bps = stats.average_bps;
            download.eta_secs = stats.eta_secs;
            download.active_connections = stats.active_connections;
        }
        if let Some(gauges) = self.gauges.lock().expect("gauges").get(&download.id) {
            download.downloaded_bytes = download.downloaded_bytes.max(gauges.bytes.load(Ordering::Relaxed));
            download.active_connections = gauges.connections.load(Ordering::Relaxed);
            if gauges.accelerated.load(Ordering::Relaxed) {
                download.accelerated = true;
            }
        }
        download
    }

    pub(crate) fn emit_status(&self, id: &str, status: DownloadStatus, error: Option<String>, warning: Option<String>) {
        self.emit(EngineEvent::Status { download_id: id.to_string(), status, error, warning });
    }
}

fn unique_destination(dir: &str, filename: &str, replace: bool) -> PathBuf {
    let direct = Path::new(dir).join(filename);
    if replace || !direct.exists() {
        return direct;
    }
    let path = Path::new(filename);
    let stem = path.file_stem().and_then(|value| value.to_str()).unwrap_or(filename);
    let extension = path.extension().and_then(|value| value.to_str());
    for index in 1..10_000 {
        let name = match extension {
            Some(extension) => format!("{stem} ({index}).{extension}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = Path::new(dir).join(name);
        if !candidate.exists() {
            return candidate;
        }
    }
    direct
}

fn merge_warning(current: Option<String>, note: String) -> String {
    match current {
        Some(current) if !current.is_empty() => format!("{current} {note}"),
        _ => note,
    }
}

fn nonzero_status(status: u16) -> Option<u16> {
    if status == 0 { None } else { Some(status) }
}
