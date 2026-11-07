use crate::error::{http_failure, map_io, EngineError};
use crate::http::{apply_validator, authorized_request, content_range_start, load_secret, map_reqwest};
use crate::EngineHandle;
use futures_util::StreamExt;
use harbor_core::{now_ms, split_largest, DownloadStatus, RangePiece, Segment, SegmentStatus};
use reqwest::StatusCode;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

impl Gauges {
    pub(crate) fn segment_snapshot(&self, download_id: &str) -> Vec<harbor_core::Segment> {
        self.segments.lock().expect("segments").iter().map(|segment| segment.to_segment(download_id)).collect()
    }
}

pub struct Gauges {
    pub bytes: AtomicU64,
    pub connections: AtomicU32,
    pub http_status: AtomicU16,
    pub accelerated: AtomicBool,
    pub segments: Mutex<Vec<LiveSeg>>,
    pub local_limit: crate::limiter::Limiter,
}

#[derive(Clone, Debug)]
pub struct LiveSeg {
    pub id: String,
    pub start: u64,
    pub end: u64,
    pub received: u64,
    pub reserved_end: u64,
    pub status: SegmentStatus,
}

impl LiveSeg {
    fn to_piece(&self) -> RangePiece {
        RangePiece {
            start: self.start,
            end: self.end,
            received: self.received,
            reserved_end: self.reserved_end,
        }
    }

    fn to_segment(&self, download_id: &str) -> Segment {
        Segment {
            id: self.id.clone(),
            download_id: download_id.to_string(),
            start_byte: self.start,
            end_byte: self.end,
            received_bytes: self.received.min(self.end.saturating_sub(self.start)),
            status: self.status,
            updated_at: now_ms(),
        }
    }
}

pub struct TransferNote {
    pub warning: Option<String>,
}

impl EngineHandle {
    pub(crate) async fn transfer(
        &self,
        id: &str,
        gauges: &Arc<Gauges>,
        cancel: &CancellationToken,
        mode: &AtomicU8,
    ) -> Result<TransferNote, EngineError> {
        let mut retries = 0u32;
        let mut warning = None;
        loop {
            self.ensure_running(cancel, mode)?;
            let download = self.must_download(id)?;
            if download.url.starts_with("ftp://") {
                crate::ftp::download_ftp(self, &download, gauges, cancel, mode).await?;
                return Ok(TransferNote { warning });
            }
            if download.http_status.is_none() {
                let info = self.probe_download(&download).await?;
                self.persist_probe(&download, &info)?;
            }
            let download = self.must_download(id)?;
            let can_segment = download.accept_ranges
                && download.max_connections > 1
                && download.total_bytes.unwrap_or(0) >= self.config.segment_threshold;
            let result = if can_segment {
                self.transfer_segmented(&download, gauges, cancel, mode).await
            } else {
                gauges.accelerated.store(false, Ordering::Relaxed);
                self.transfer_single(&download, gauges, cancel, mode, false).await
            };
            match result {
                Ok(()) => return Ok(TransferNote { warning }),
                Err(EngineError::Fallback) => {
                    let had_bytes = gauges.bytes.load(Ordering::Relaxed) > 0;
                    let _ = std::fs::remove_file(download.partial_path());
                    self.store.reset_for_restart(id).map_err(|err| EngineError::Fatal(err.to_string()))?;
                    gauges.bytes.store(0, Ordering::Relaxed);
                    gauges.accelerated.store(false, Ordering::Relaxed);
                    {
                        let mut segments = gauges.segments.lock().expect("segments");
                        segments.clear();
                    }
                    let _ = self.store.update_meta(
                        id,
                        download.final_url.as_deref().unwrap_or(&download.url),
                        &download.filename,
                        download.filename_explicit,
                        download.total_bytes,
                        download.mime.as_deref(),
                        download.etag.as_deref(),
                        download.last_modified.as_deref(),
                        false,
                        false,
                        download.http_status,
                        None,
                    );
                    warning = Some(
                        if had_bytes {
                            "The remote file changed or rejected a range request, so Harbor downloaded it again in one connection.".into()
                        } else {
                            "This server does not support segmented downloads. Harbor used one connection.".into()
                        },
                    );
                    let download = self.must_download(id)?;
                    self.transfer_single(&download, gauges, cancel, mode, true).await?;
                    return Ok(TransferNote { warning });
                }
                Err(EngineError::Retryable(message)) => {
                    retries += 1;
                    if retries > self.config.max_retries {
                        return Err(EngineError::Fatal(message));
                    }
                    let delay = std::time::Duration::from_millis(400 * 2u64.saturating_pow(retries.min(4)));
                    self.sleep_interruptible(delay, cancel, mode).await?;
                }
                Err(other) => return Err(other),
            }
        }
    }

    async fn transfer_single(
        &self,
        download: &harbor_core::Download,
        gauges: &Arc<Gauges>,
        cancel: &CancellationToken,
        mode: &AtomicU8,
        after_fallback: bool,
    ) -> Result<(), EngineError> {
        let secret = load_secret(&self.store, self.secrets.as_ref(), download);
        let client = self.client.read().expect("client").clone();
        let mut offset = if after_fallback { 0 } else { gauges.bytes.load(Ordering::Relaxed) };
        let partial = download.partial_path();
        if offset > 0 && !partial.exists() {
            offset = 0;
            gauges.bytes.store(0, Ordering::Relaxed);
        }
        let mut request = authorized_request(&client, &download.url, download, &secret, &self.config.user_agent)?;
        if offset > 0 && download.resume_capable {
            request = request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
            request = apply_validator(request, download.etag.as_deref(), download.last_modified.as_deref());
        }
        gauges.connections.fetch_add(1, Ordering::Relaxed);
        let response = request.send().await.map_err(map_reqwest);
        gauges.connections.fetch_sub(1, Ordering::Relaxed);
        let response = response?;
        let status = response.status();
        gauges.http_status.store(status.as_u16(), Ordering::Relaxed);
        let _ = self.store.set_http_status(&download.id, status.as_u16());
        if status == StatusCode::RANGE_NOT_SATISFIABLE && offset > 0 {
            return Ok(());
        }
        if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
            return Err(http_failure(status.as_u16()));
        }
        let mut restarted = false;
        if offset > 0 && status == StatusCode::OK {
            offset = 0;
            gauges.bytes.store(0, Ordering::Relaxed);
            restarted = true;
            if let Ok(file) = open_partial(&partial) {
                let _ = file.set_len(0);
            }
        } else if offset > 0 && status != StatusCode::PARTIAL_CONTENT {
            return Err(EngineError::Retryable(format!(
                "Expected HTTP 206 while resuming, got HTTP {}.",
                status.as_u16()
            )));
        }
        if let Some(total) = crate::http::total_from_headers(response.headers(), status) {
            if download.total_bytes.is_none() {
                let _ = self.store.update_meta(
                    &download.id,
                    response.url().as_str(),
                    &download.filename,
                    download.filename_explicit,
                    Some(total),
                    download.mime.as_deref(),
                    download.etag.as_deref(),
                    download.last_modified.as_deref(),
                    download.accept_ranges,
                    download.resume_capable,
                    Some(status.as_u16()),
                    None,
                );
            }
        }
        let file = open_partial(&partial).map_err(map_io)?;
        let mut stream = response.bytes_stream();
        let mut position = offset;
        while let Some(chunk) = stream.next().await {
            self.ensure_running(cancel, mode)?;
            let chunk = chunk.map_err(map_reqwest)?;
            self.limiter.acquire(chunk.len()).await;
            gauges.local_limit.acquire(chunk.len()).await;
            self.wait_for_quota(chunk.len() as u64).await?;
            let file = file.try_clone().map_err(map_io)?;
            let bytes = chunk.to_vec();
            let write_at = position;
            tokio::task::spawn_blocking(move || write_all_at(&file, &bytes, write_at))
                .await
                .map_err(|err| EngineError::Fatal(err.to_string()))?
                .map_err(map_io)?;
            position += chunk.len() as u64;
            gauges.bytes.store(position, Ordering::Relaxed);
        }
        file.sync_all().map_err(map_io)?;
        if restarted {
            let _ = self.store.set_status(
                &download.id,
                DownloadStatus::Downloading,
                None,
                Some("The remote file changed, so Harbor started the download again."),
            );
        }
        if let Some(total) = self.must_download(&download.id)?.total_bytes {
            if position < total {
                return Err(EngineError::Retryable(
                    "The connection closed before the file was complete.".into(),
                ));
            }
        }
        Ok(())
    }

    async fn transfer_segmented(
        &self,
        download: &harbor_core::Download,
        gauges: &Arc<Gauges>,
        cancel: &CancellationToken,
        mode: &AtomicU8,
    ) -> Result<(), EngineError> {
        let total = download.total_bytes.ok_or_else(|| EngineError::Fatal("The file size is unknown.".into()))?;
        {
            let mut live = gauges.segments.lock().expect("segments");
            if live.is_empty() {
                let stored = self.store.segments(&download.id).map_err(|err| EngineError::Fatal(err.to_string()))?;
                if stored.is_empty() {
                    live.push(LiveSeg {
                        id: harbor_core::new_id(),
                        start: 0,
                        end: total,
                        received: 0,
                        reserved_end: 0,
                        status: SegmentStatus::Pending,
                    });
                } else {
                    *live = stored
                        .into_iter()
                        .map(|segment| LiveSeg {
                            id: segment.id,
                            start: segment.start_byte,
                            end: segment.end_byte,
                            received: segment.received_bytes.min(segment.end_byte.saturating_sub(segment.start_byte)),
                            reserved_end: 0,
                            status: if segment.received_bytes >= segment.end_byte.saturating_sub(segment.start_byte) {
                                SegmentStatus::Complete
                            } else {
                                SegmentStatus::Pending
                            },
                        })
                        .collect();
                }
            }
            let received: u64 = live.iter().map(|seg| seg.received.min(seg.end.saturating_sub(seg.start))).sum();
            gauges.bytes.store(received, Ordering::Relaxed);
        }
        let partial = download.partial_path();
        let file = open_partial(&partial).map_err(map_io)?;
        if file.metadata().map(|m| m.len()).unwrap_or(0) < total {
            file.set_len(total).map_err(map_io)?;
        }
        let max_connections = download.max_connections.clamp(1, 32) as usize;
        // Open several connections immediately. Extra slots still split the largest remaining range.
        let mut desired = if max_connections == 1 { 1 } else { max_connections.min(8) };
        let mut workers = JoinSet::new();
        let mut adaptive_frozen = false;
        let mut sample_at = std::time::Instant::now();
        let mut sample_bytes = gauges.bytes.load(Ordering::Relaxed);
        let mut baseline = 0.0f64;
        let mut watching = false;
        loop {
            self.ensure_running(cancel, mode)?;
            while workers.len() < desired {
                if !claim_or_split(gauges, self.config.min_split_bytes, self.config.max_segments, workers.len(), desired) {
                    break;
                }
                let Some(segment_id) = claim_pending(gauges) else {
                    break;
                };
                let handle = self.clone();
                let gauges = gauges.clone();
                let file = file.try_clone().map_err(map_io)?;
                let cancel = cancel.clone();
                let download_id = download.id.clone();
                workers.spawn(async move {
                    handle
                        .run_segment(download_id, segment_id, gauges, file, cancel)
                        .await
                });
            }
            if workers.is_empty() {
                if all_complete(gauges, total) {
                    gauges.accelerated.store(segment_count(gauges) > 1, Ordering::Relaxed);
                    self.flush_segments(&download.id, gauges)?;
                    file.sync_all().map_err(map_io)?;
                    return Ok(());
                }
                return Err(EngineError::Retryable("A download segment stopped before the file was complete.".into()));
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    workers.abort_all();
                    self.flush_segments(&download.id, gauges)?;
                    return Err(self.control_error(mode));
                }
                joined = workers.join_next() => {
                    match joined {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(EngineError::Fallback))) => {
                            workers.abort_all();
                            self.flush_segments(&download.id, gauges)?;
                            return Err(EngineError::Fallback);
                        }
                        Some(Ok(Err(EngineError::Paused))) => {
                            workers.abort_all();
                            self.flush_segments(&download.id, gauges)?;
                            return Err(self.control_error(mode));
                        }
                        Some(Ok(Err(err))) => {
                            workers.abort_all();
                            self.flush_segments(&download.id, gauges)?;
                            return Err(err);
                        }
                        Some(Err(err)) => {
                            workers.abort_all();
                            return Err(EngineError::Fatal(format!("A download connection failed: {err}")));
                        }
                        None => {}
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {
                    if self.config.adaptive && desired > 2 && !watching && !adaptive_frozen {
                        baseline = current_speed(gauges, sample_bytes, sample_at);
                        sample_bytes = gauges.bytes.load(Ordering::Relaxed);
                        sample_at = std::time::Instant::now();
                        watching = true;
                    } else if watching && sample_at.elapsed() >= std::time::Duration::from_secs(2) {
                        let speed = current_speed(gauges, sample_bytes, sample_at);
                        if baseline > 1.0 && speed < baseline * 1.1 {
                            adaptive_frozen = true;
                        }
                        watching = false;
                    }
                    if !adaptive_frozen && desired < max_connections {
                        desired += 1;
                    }
                    self.flush_segments(&download.id, gauges)?;
                }
            }
        }
    }

    async fn run_segment(
        &self,
        download_id: String,
        segment_id: String,
        gauges: Arc<Gauges>,
        file: File,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let mut stalls = 0u32;
        loop {
            if cancel.is_cancelled() {
                release_segment(&gauges, &segment_id);
                return Err(EngineError::Paused);
            }
            let window = {
                let mut live = gauges.segments.lock().expect("segments");
                let Some(segment) = live.iter_mut().find(|seg| seg.id == segment_id) else {
                    return Ok(());
                };
                if segment.start + segment.received >= segment.end {
                    segment.status = SegmentStatus::Complete;
                    segment.reserved_end = segment.end;
                    return Ok(());
                }
                let range_start = segment.start + segment.received;
                let range_end = (range_start + self.config.chunk_bytes).min(segment.end);
                if range_end <= range_start {
                    segment.status = SegmentStatus::Complete;
                    return Ok(());
                }
                segment.reserved_end = range_end;
                segment.status = SegmentStatus::Active;
                (range_start, range_end)
            };
            match self.fetch_window(&download_id, &gauges, &file, window.0, window.1, &cancel).await {
                Ok(written) => {
                    let mut live = gauges.segments.lock().expect("segments");
                    if let Some(segment) = live.iter_mut().find(|seg| seg.id == segment_id) {
                        let room = segment.end.saturating_sub(segment.start + segment.received);
                        let applied = written.min(room);
                        segment.received += applied;
                        segment.reserved_end = segment.start + segment.received;
                        if segment.start + segment.received >= segment.end {
                            segment.status = SegmentStatus::Complete;
                            return Ok(());
                        }
                    }
                    let total_received: u64 = live.iter().map(|seg| seg.received).sum();
                    gauges.bytes.store(total_received, Ordering::Relaxed);
                    if written == 0 {
                        stalls += 1;
                        if stalls > 3 {
                            release_segment(&gauges, &segment_id);
                            return Err(EngineError::Retryable("The server ended a segment early.".into()));
                        }
                    } else {
                        stalls = 0;
                    }
                }
                Err(EngineError::Fallback) => {
                    release_segment(&gauges, &segment_id);
                    return Err(EngineError::Fallback);
                }
                Err(EngineError::Paused) | Err(EngineError::Stopped) | Err(EngineError::Abandoned) => {
                    release_segment(&gauges, &segment_id);
                    return Err(EngineError::Paused);
                }
                Err(EngineError::Retryable(message)) => {
                    stalls += 1;
                    release_reservation(&gauges, &segment_id);
                    if stalls > 4 {
                        release_segment(&gauges, &segment_id);
                        return Err(EngineError::Retryable(message));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
                Err(err) => {
                    release_segment(&gauges, &segment_id);
                    return Err(err);
                }
            }
        }
    }

    async fn fetch_window(
        &self,
        download_id: &str,
        gauges: &Gauges,
        file: &File,
        range_start: u64,
        range_end: u64,
        cancel: &CancellationToken,
    ) -> Result<u64, EngineError> {
        let download = self.must_download(download_id)?;
        let secret = load_secret(&self.store, self.secrets.as_ref(), &download);
        let client = self.client.read().expect("client").clone();
        let mut request = authorized_request(&client, &download.url, &download, &secret, &self.config.user_agent)?;
        request = request.header(
            reqwest::header::RANGE,
            format!("bytes={}-{}", range_start, range_end - 1),
        );
        request = apply_validator(request, download.etag.as_deref(), download.last_modified.as_deref());
        gauges.connections.fetch_add(1, Ordering::Relaxed);
        let response = request.send().await.map_err(map_reqwest);
        gauges.connections.fetch_sub(1, Ordering::Relaxed);
        let response = response?;
        let status = response.status();
        gauges.http_status.store(status.as_u16(), Ordering::Relaxed);
        if cancel.is_cancelled() {
            return Err(EngineError::Paused);
        }
        if status == StatusCode::OK {
            return Err(EngineError::Fallback);
        }
        if status == StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(0);
        }
        if status != StatusCode::PARTIAL_CONTENT {
            return Err(http_failure(status.as_u16()));
        }
        if let Some(value) = response.headers().get(reqwest::header::CONTENT_RANGE).and_then(|v| v.to_str().ok()) {
            if let Some(start) = content_range_start(value) {
                if start != range_start {
                    return Err(EngineError::Fallback);
                }
            }
        }
        let mut stream = response.bytes_stream();
        let mut written = 0u64;
        let mut position = range_start;
        let wanted = range_end - range_start;
        while written < wanted {
            if cancel.is_cancelled() {
                return Err(EngineError::Paused);
            }
            let Some(chunk) = stream.next().await else { break };
            let chunk = chunk.map_err(map_reqwest)?;
            let room = (wanted - written) as usize;
            let take = chunk.len().min(room);
            if take == 0 {
                break;
            }
            self.limiter.acquire(take).await;
            gauges.local_limit.acquire(take).await;
            self.wait_for_quota(take as u64).await?;
            let file = file.try_clone().map_err(map_io)?;
            let bytes = chunk.slice(..take).to_vec();
            let offset = position;
            tokio::task::spawn_blocking(move || write_all_at(&file, &bytes, offset))
                .await
                .map_err(|err| EngineError::Fatal(err.to_string()))?
                .map_err(map_io)?;
            written += take as u64;
            position += take as u64;
        }
        Ok(written)
    }

    pub(crate) fn flush_segments(&self, id: &str, gauges: &Gauges) -> Result<(), EngineError> {
        let live = gauges.segments.lock().expect("segments");
        let segments: Vec<Segment> = live.iter().map(|seg| seg.to_segment(id)).collect();
        let downloaded = live.iter().map(|seg| seg.received.min(seg.end.saturating_sub(seg.start))).sum();
        drop(live);
        gauges.bytes.store(downloaded, Ordering::Relaxed);
        let accelerated = gauges.accelerated.load(Ordering::Relaxed);
        self.store
            .replace_segments(id, &segments, downloaded, accelerated)
            .map_err(|err| EngineError::Fatal(err.to_string()))?;
        let status = gauges.http_status.load(Ordering::Relaxed);
        if status > 0 {
            let _ = self.store.set_http_status(id, status);
        }
        Ok(())
    }

    pub(crate) fn ensure_running(&self, cancel: &CancellationToken, mode: &AtomicU8) -> Result<(), EngineError> {
        if cancel.is_cancelled() {
            return Err(self.control_error(mode));
        }
        Ok(())
    }

    pub(crate) fn control_error(&self, mode: &AtomicU8) -> EngineError {
        match mode.load(Ordering::SeqCst) {
            2 => EngineError::Stopped,
            3 => EngineError::Abandoned,
            _ => EngineError::Paused,
        }
    }

    pub(crate) async fn sleep_interruptible(
        &self,
        delay: std::time::Duration,
        cancel: &CancellationToken,
        mode: &AtomicU8,
    ) -> Result<(), EngineError> {
        tokio::select! {
            _ = cancel.cancelled() => Err(self.control_error(mode)),
            _ = tokio::time::sleep(delay) => Ok(()),
        }
    }

    pub(crate) async fn wait_for_quota(&self, bytes: u64) -> Result<(), EngineError> {
        let settings = self.store.settings().map_err(|err| EngineError::Fatal(err.to_string()))?;
        if settings.quota_bytes_per_hour == 0 || bytes == 0 {
            return Ok(());
        }
        loop {
            let now = self.now_ms();
            let wait = self
                .store
                .try_consume_quota(bytes, settings.quota_bytes_per_hour, self.config.quota_window, now)
                .map_err(|err| EngineError::Fatal(err.to_string()))?;
            match wait {
                None => return Ok(()),
                Some(delay) => {
                    tokio::time::sleep(delay.min(std::time::Duration::from_secs(2)).max(std::time::Duration::from_millis(50))).await;
                }
            }
        }
    }
}

fn claim_or_split(gauges: &Gauges, min_split: u64, max_segments: usize, workers: usize, desired: usize) -> bool {
    if workers >= desired {
        return false;
    }
    if has_pending(gauges) {
        return true;
    }
    let mut live = gauges.segments.lock().expect("segments");
    let mut pieces: Vec<RangePiece> = live.iter().map(LiveSeg::to_piece).collect();
    if !split_largest(&mut pieces, min_split, max_segments) {
        return false;
    }
    for piece in &pieces {
        if let Some(segment) = live.iter_mut().find(|seg| seg.start == piece.start) {
            segment.end = piece.end;
            if segment.reserved_end > segment.end {
                segment.reserved_end = segment.end;
            }
            let max_received = segment.end.saturating_sub(segment.start);
            if segment.received > max_received {
                segment.received = max_received;
            }
        } else {
            live.push(LiveSeg {
                id: harbor_core::new_id(),
                start: piece.start,
                end: piece.end,
                received: 0,
                reserved_end: 0,
                status: SegmentStatus::Pending,
            });
        }
    }
    true
}

fn has_pending(gauges: &Gauges) -> bool {
    gauges
        .segments
        .lock()
        .expect("segments")
        .iter()
        .any(|seg| seg.status == SegmentStatus::Pending && seg.start + seg.received < seg.end)
}

fn claim_pending(gauges: &Gauges) -> Option<String> {
    let mut live = gauges.segments.lock().expect("segments");
    let segment = live
        .iter_mut()
        .find(|seg| seg.status == SegmentStatus::Pending && seg.start + seg.received < seg.end)?;
    segment.status = SegmentStatus::Active;
    Some(segment.id.clone())
}

fn release_reservation(gauges: &Gauges, id: &str) {
    if let Some(segment) = gauges.segments.lock().expect("segments").iter_mut().find(|seg| seg.id == id) {
        segment.reserved_end = segment.start + segment.received;
    }
}

fn release_segment(gauges: &Gauges, id: &str) {
    if let Some(segment) = gauges.segments.lock().expect("segments").iter_mut().find(|seg| seg.id == id) {
        segment.reserved_end = segment.start + segment.received;
        if segment.start + segment.received < segment.end && segment.status != SegmentStatus::Complete {
            segment.status = SegmentStatus::Pending;
        }
    }
}

fn all_complete(gauges: &Gauges, total: u64) -> bool {
    let live = gauges.segments.lock().expect("segments");
    if live.is_empty() {
        return false;
    }
    let received: u64 = live.iter().map(|seg| seg.received.min(seg.end.saturating_sub(seg.start))).sum();
    received >= total && live.iter().all(|seg| seg.start + seg.received >= seg.end)
}

fn segment_count(gauges: &Gauges) -> usize {
    gauges.segments.lock().expect("segments").len()
}

fn current_speed(gauges: &Gauges, then_bytes: u64, then: std::time::Instant) -> f64 {
    let elapsed = then.elapsed().as_secs_f64().max(0.001);
    let now = gauges.bytes.load(Ordering::Relaxed);
    now.saturating_sub(then_bytes) as f64 / elapsed
}

pub fn open_partial(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).read(true).write(true).open(path)
}

pub fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let wrote = write_at(file, buf, offset)?;
        if wrote == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write returned 0"));
        }
        buf = &buf[wrote..];
        offset += wrote as u64;
    }
    Ok(())
}

#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}

#[cfg(windows)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}
