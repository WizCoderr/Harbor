use crate::EngineHandle;
use harbor_core::{local_minutes, stop_reached, DownloadStatus, QueueKind, QueueStatus};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

impl EngineHandle {
    pub fn start_queue(&self, id: &str) -> Result<(), String> {
        let queue = self.store.queue(id).map_err(|err| err.to_string())?.ok_or_else(|| "Queue not found.".to_string())?;
        if queue.kind == QueueKind::Sync {
            self.store.reopen_sync_items(id).map_err(|err| err.to_string())?;
        }
        self.store.set_queue_status(id, QueueStatus::Running).map_err(|err| err.to_string())?;
        self.spawn_supervisor(id.to_string());
        Ok(())
    }

    pub fn pause_queue(&self, id: &str) -> Result<(), String> {
        self.cancel_supervisor(id);
        self.store.set_queue_status(id, QueueStatus::Paused).map_err(|err| err.to_string())?;
        self.pause_queue_downloads(id)?;
        Ok(())
    }

    pub fn stop_queue(&self, id: &str) -> Result<(), String> {
        self.cancel_supervisor(id);
        self.store.set_queue_status(id, QueueStatus::Idle).map_err(|err| err.to_string())?;
        for download_id in self
            .store
            .queue_download_ids(id, DownloadStatus::Downloading)
            .map_err(|err| err.to_string())?
        {
            self.stop(&download_id)?;
        }
        Ok(())
    }

    pub fn scheduler_tick(&self) -> Result<Vec<String>, String> {
        let now = self.now_ms();
        let schedules = self.store.schedules().map_err(|err| err.to_string())?;
        let mut started = Vec::new();
        for mut schedule in schedules {
            let Some(start) = schedule.start_at else { continue };
            if !schedule.enabled || start > now {
                continue;
            }
            self.start_queue(&schedule.queue_id)?;
            started.push(schedule.queue_id.clone());
            schedule.last_run = Some(now);
            match harbor_core::advance_start(start, &schedule.recurrence, now) {
                Some(next) => schedule.start_at = Some(next),
                None => schedule.enabled = false,
            }
            self.store.upsert_schedule(&schedule).map_err(|err| err.to_string())?;
        }
        for queue in self.store.queues().map_err(|err| err.to_string())? {
            if queue.status == QueueStatus::Running && self.queue_should_stop(&queue.id)? {
                self.pause_queue(&queue.id)?;
            }
        }
        Ok(started)
    }

    pub fn next_wakeup(&self) -> Result<Option<i64>, String> {
        let now = self.now_ms();
        let mut next = None;
        for schedule in self.store.schedules().map_err(|err| err.to_string())? {
            if schedule.enabled {
                if let Some(start) = schedule.start_at {
                    if start > now {
                        next = Some(next.map(|current: i64| current.min(start)).unwrap_or(start));
                    }
                }
            }
        }
        Ok(next)
    }

    pub(crate) fn spawn_supervisor(&self, id: String) {
        let mut supervisors = self.supervisors.lock().expect("supervisors");
        if supervisors.contains_key(&id) {
            return;
        }
        let cancel = CancellationToken::new();
        supervisors.insert(id.clone(), cancel.clone());
        drop(supervisors);
        let handle = self.clone();
        tokio::spawn(async move {
            handle.supervise(id.clone(), cancel).await;
            handle.supervisors.lock().expect("supervisors").remove(&id);
        });
    }

    async fn supervise(&self, id: String, cancel: CancellationToken) {
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let Ok(Some(queue)) = self.store.queue(&id) else { break };
            if queue.status != QueueStatus::Running {
                break;
            }
            if self.queue_should_stop(&id).unwrap_or(false) {
                let _ = self.pause_queue_downloads(&id);
                let _ = self.store.set_queue_status(&id, QueueStatus::Paused);
                break;
            }
            let active = self.store.count_downloading(&id).unwrap_or(0);
            if active < queue.max_concurrent {
                match self.store.next_queue_item(&id, self.now_ms()) {
                    Ok(Some(item)) => {
                        let _ = self.start(&item.id);
                    }
                    Ok(None) if active == 0 => {
                        let _ = self.store.set_queue_status(&id, QueueStatus::Idle);
                        break;
                    }
                    _ => {}
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
        }
    }

    fn cancel_supervisor(&self, id: &str) {
        if let Some(cancel) = self.supervisors.lock().expect("supervisors").remove(id) {
            cancel.cancel();
        }
    }

    fn pause_queue_downloads(&self, id: &str) -> Result<(), String> {
        for download_id in self
            .store
            .queue_download_ids(id, DownloadStatus::Downloading)
            .map_err(|err| err.to_string())?
        {
            self.pause(&download_id)?;
        }
        Ok(())
    }

    fn queue_should_stop(&self, id: &str) -> Result<bool, String> {
        let Some(schedule) = self.store.schedule_for_queue(id).map_err(|err| err.to_string())? else {
            return Ok(false);
        };
        if !schedule.enabled {
            return Ok(false);
        }
        let start_minutes = schedule.start_at.map(local_minutes);
        Ok(stop_reached(start_minutes, schedule.stop_minutes, self.now_minutes()))
    }
}
