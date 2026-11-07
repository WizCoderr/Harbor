use crate::http::RemoteInfo;
use crate::EngineHandle;
use harbor_core::{
    assign_category, new_id, redact_url, split_headers, version_is_newer, Category, CategoryCount,
    Download, DownloadStatus, ListFilter, NewDownload, ProbeRequest, ProbeResult, Queue, QueueKind,
    QueueStatus, Recurrence, Schedule, SecretMaterial, Settings, SiteAuth, Summary, UpdateStatus, VERSION,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use url::Url;

impl EngineHandle {
    pub async fn dispatch(&self, method: &str, params: Value) -> Result<Value, String> {
        match method {
            "downloads.list" => {
                let query = params.get("query").and_then(|v| v.as_str()).unwrap_or("");
                let filter = parse_filter(params.get("filter").and_then(|v| v.as_str()).unwrap_or("all"));
                let category_id = params.get("category_id").and_then(|v| v.as_str());
                let queue_id = params.get("queue_id").and_then(|v| v.as_str());
                let downloads = self.list(filter, category_id, queue_id, query)?;
                Ok(json!(downloads))
            }
            "downloads.get" => {
                let id = required_str(&params, "id")?;
                Ok(json!(self.detail(id)?))
            }
            "downloads.probe" => {
                let request: ProbeRequest = serde_json::from_value(params).map_err(|err| err.to_string())?;
                Ok(json!(self.probe_request(request).await?))
            }
            "downloads.add" => {
                let request: NewDownload = serde_json::from_value(params).map_err(|err| err.to_string())?;
                Ok(json!(self.add(request).await?))
            }
            "downloads.start" | "downloads.resume" | "downloads.retry" => {
                self.start(required_str(&params, "id")?)?;
                Ok(json!({"ok": true}))
            }
            "downloads.pause" => {
                self.pause(required_str(&params, "id")?)?;
                Ok(json!({"ok": true}))
            }
            "downloads.stop" => {
                self.stop(required_str(&params, "id")?)?;
                Ok(json!({"ok": true}))
            }
            "downloads.stop_all" => {
                self.stop_all()?;
                Ok(json!({"ok": true}))
            }
            "downloads.delete" => {
                let id = required_str(&params, "id")?;
                let delete_files = params.get("delete_files").and_then(|v| v.as_bool()).unwrap_or(false);
                self.delete(id, delete_files)?;
                Ok(json!({"ok": true}))
            }
            "downloads.delete_completed" => {
                let ids = self.store.delete_completed().map_err(|err| err.to_string())?;
                Ok(json!({"deleted": ids.len()}))
            }
            "downloads.reorder" => {
                self.store
                    .reorder(required_str(&params, "id")?, params.get("direction").and_then(|v| v.as_str()).unwrap_or("down"))
                    .map_err(|err| err.to_string())?;
                Ok(json!({"ok": true}))
            }
            "downloads.assign_queue" => {
                let id = required_str(&params, "id")?;
                let queue_id = params.get("queue_id").and_then(|v| v.as_str());
                self.store.assign_queue(id, queue_id).map_err(|err| err.to_string())?;
                Ok(json!({"ok": true}))
            }
            "downloads.summary" => Ok(json!(self.summary()?)),
            "categories.list" => Ok(json!(self.store.categories().map_err(|err| err.to_string())?)),
            "categories.save" => {
                let category = category_from_params(&params)?;
                self.store.upsert_category(&category).map_err(|err| err.to_string())?;
                Ok(json!(category))
            }
            "categories.delete" => {
                self.store.delete_category(required_str(&params, "id")?).map_err(|err| err.to_string())?;
                Ok(json!({"ok": true}))
            }
            "queues.list" => Ok(json!(self.store.queues().map_err(|err| err.to_string())?)),
            "queues.save" => {
                let queue = queue_from_params(&params)?;
                self.store.upsert_queue(&queue).map_err(|err| err.to_string())?;
                Ok(json!(queue))
            }
            "queues.delete" => {
                self.store.delete_queue(required_str(&params, "id")?).map_err(|err| err.to_string())?;
                Ok(json!({"ok": true}))
            }
            "queues.start" => {
                self.start_queue(required_str(&params, "id")?)?;
                Ok(json!({"ok": true}))
            }
            "queues.pause" => {
                self.pause_queue(required_str(&params, "id")?)?;
                Ok(json!({"ok": true}))
            }
            "queues.stop" => {
                self.stop_queue(required_str(&params, "id")?)?;
                Ok(json!({"ok": true}))
            }
            "schedules.list" => Ok(json!(self.store.schedules().map_err(|err| err.to_string())?)),
            "schedules.save" => {
                let schedule = schedule_from_params(&params)?;
                self.store.upsert_schedule(&schedule).map_err(|err| err.to_string())?;
                Ok(json!({"schedule": schedule, "next_wakeup": self.next_wakeup()?}))
            }
            "schedules.delete" => {
                self.store.delete_schedule(required_str(&params, "id")?).map_err(|err| err.to_string())?;
                Ok(json!({"next_wakeup": self.next_wakeup()?}))
            }
            "scheduler.tick" => Ok(json!({"started": self.scheduler_tick()?, "next_wakeup": self.next_wakeup()?})),
            "scheduler.next" => Ok(json!({"next_wakeup": self.next_wakeup()?})),
            "settings.get" => Ok(json!(self.store.settings().map_err(|err| err.to_string())?)),
            "settings.set" => {
                let settings: Settings = serde_json::from_value(params).map_err(|err| err.to_string())?;
                self.store.save_settings(&settings).map_err(|err| err.to_string())?;
                self.apply_runtime_settings()?;
                Ok(json!(settings))
            }
            "grabber.preview" => {
                let url = required_str(&params, "url")?;
                let extensions = string_list(params.get("extensions"));
                let domain = params.get("domain").and_then(|v| v.as_str());
                let depth = params.get("depth").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
                let links = crate::grabber::preview(self, url, &extensions, domain, depth).await.map_err(|err| err.to_string())?;
                Ok(json!(links))
            }
            "batch.import" => {
                let text = required_str(&params, "text")?;
                let destination = params.get("destination_dir").and_then(|v| v.as_str()).map(|v| v.to_string());
                let start = params.get("start").and_then(|v| v.as_bool()).unwrap_or(false);
                let mut added = Vec::new();
                for line in text.lines() {
                    let url = line.trim();
                    if url.is_empty() || url.starts_with('#') {
                        continue;
                    }
                    added.push(self.add(NewDownload {
                        url: url.to_string(),
                        filename: None,
                        destination_dir: destination.clone(),
                        category_id: None,
                        queue_id: None,
                        username: None,
                        password: None,
                        headers: HashMap::new(),
                        cookies: None,
                        start,
                        max_connections: None,
                        bandwidth_limit_bps: None,
                        replace_existing: false,
                    }).await?);
                }
                Ok(json!(added))
            }
            "history.list" => Ok(json!(self.store.history().map_err(|err| err.to_string())?)),
            "site_auth.list" => Ok(json!(self.store.site_auth().map_err(|err| err.to_string())?)),
            "site_auth.save" => Ok(json!(self.save_site_auth(&params)?)),
            "site_auth.delete" => {
                let id = required_str(&params, "id")?;
                if let Some(refer) = self.store.delete_site_auth(id).map_err(|err| err.to_string())? {
                    let _ = self.secrets.delete(&refer);
                }
                Ok(json!({"ok": true}))
            }
            "updates.check" => Ok(json!(self.check_update().await?)),
            "native.handle" => Ok(self.native_message(params).await),
            "agent.status" => Ok(json!({"ok": true, "version": VERSION, "jobs": self.active_jobs()})),
            other => Err(format!("Unknown command: {other}")),
        }
    }

    pub fn list(&self, filter: ListFilter, category_id: Option<&str>, queue_id: Option<&str>, query: &str) -> Result<Vec<Download>, String> {
        let rows = self.store.list_downloads(filter, category_id, queue_id, query).map_err(|err| err.to_string())?;
        Ok(rows.into_iter().map(|download| self.decorate(download)).collect())
    }

    pub fn detail(&self, id: &str) -> Result<Value, String> {
        let download = self.decorate(
            self.store.get_download(id).map_err(|err| err.to_string())?.ok_or_else(|| "Download not found.".to_string())?,
        );
        let segments = if let Some(gauges) = self.gauges.lock().expect("gauges").get(id) {
            gauges.segment_snapshot(id)
        } else {
            self.store.segments(id).map_err(|err| err.to_string())?
        };
        Ok(json!({"download": download, "segments": segments}))
    }

    pub fn summary(&self) -> Result<Summary, String> {
        let counts = self.store.counts().map_err(|err| err.to_string())?;
        let categories = self.store.categories().map_err(|err| err.to_string())?;
        let downloads = self.list(ListFilter::All, None, None, "")?;
        let mut per_category: HashMap<String, u64> = HashMap::new();
        let mut speed = 0u64;
        for download in &downloads {
            if let Some(category_id) = &download.category_id {
                *per_category.entry(category_id.clone()).or_insert(0) += 1;
            }
            if download.status == DownloadStatus::Downloading {
                speed += download.speed_bps;
            }
        }
        Ok(Summary {
            all: *counts.get("all").unwrap_or(&0),
            downloading: *counts.get("downloading").unwrap_or(&0),
            queued: counts.get("queued").copied().unwrap_or(0) + counts.get("scheduled").copied().unwrap_or(0),
            completed: *counts.get("completed").unwrap_or(&0),
            paused: *counts.get("paused").unwrap_or(&0),
            failed: *counts.get("failed").unwrap_or(&0),
            speed_bps: speed,
            categories: categories
                .into_iter()
                .map(|category| CategoryCount {
                    count: *per_category.get(&category.id).unwrap_or(&0),
                    id: category.id,
                    name: category.name,
                })
                .collect(),
            queues: self.store.queues().map_err(|err| err.to_string())?,
        })
    }

    pub async fn probe_request(&self, request: ProbeRequest) -> Result<ProbeResult, String> {
        let url = redact_url(&request.url).map_err(|err| err.to_string())?;
        let (public_headers, secret_headers) = split_headers(request.headers);
        let secret = SecretMaterial {
            username: request.username.filter(|value| !value.is_empty()),
            password: request.password,
            cookies: request.cookies.filter(|value| !value.is_empty()),
            headers: secret_headers,
        };
        let download = transient_download(&url, public_headers);
        let client = self.client.read().expect("client").clone();
        let info = self.probe_url(&client, &url, &download, &secret).await.map_err(|err| err.to_string())?;
        let categories = self.store.categories().map_err(|err| err.to_string())?;
        let category_id = assign_category(&info.filename, &info.final_url, &categories);
        Ok(probe_result(info, category_id))
    }

    pub async fn add(&self, request: NewDownload) -> Result<Download, String> {
        let id = new_id();
        let now = self.now_ms();
        let settings = self.store.settings().map_err(|err| err.to_string())?;
        let classified = classify_url(&request.url);
        let (url, url_user, url_pass, invalid) = match classified {
            Ok(parts) => (parts.0, parts.1, parts.2, None),
            Err(message) => (request.url.clone(), None, None, Some(message)),
        };
        let (public_headers, secret_headers) = split_headers(request.headers.clone());
        let username = request.username.clone().filter(|value| !value.is_empty()).or(url_user);
        let password = request.password.clone().or(url_pass);
        let cookies = request.cookies.clone().filter(|value| !value.is_empty());
        let secret = SecretMaterial { username, password, cookies, headers: secret_headers };
        let has_credentials = !secret.is_empty();
        if has_credentials {
            let payload = serde_json::to_string(&secret).map_err(|err| err.to_string())?;
            self.secrets.put(&format!("download:{id}"), &payload).map_err(|err| err.to_string())?;
        }
        let filename = request
            .filename
            .as_deref()
            .map(harbor_core::sanitize_filename)
            .filter(|name| !name.is_empty() && name != "download")
            .unwrap_or_else(|| "download".into());
        let filename_explicit = request.filename.as_deref().map(str::trim).filter(|name| !name.is_empty()).is_some();
        let categories = self.store.categories().map_err(|err| err.to_string())?;
        let category_id = request.category_id.clone().or_else(|| assign_category(&filename, &url, &categories));
        let destination = request.destination_dir.clone().filter(|dir| !dir.trim().is_empty()).unwrap_or_else(|| {
            categories.iter().find(|category| Some(&category.id) == category_id.as_ref()).and_then(|category| category.directory.clone()).unwrap_or(settings.default_directory)
        });
        let queue_id = if invalid.is_some() {
            None
        } else {
            Some(request.queue_id.clone().unwrap_or_else(|| "queue_main".into()))
        };
        let queue_position = if let Some(queue_id) = queue_id.as_deref() {
            self.store.next_queue_position(queue_id).unwrap_or(1)
        } else {
            0
        };
        let download = Download {
            id: id.clone(),
            url,
            final_url: None,
            filename,
            filename_explicit,
            destination_dir: destination,
            category_id,
            category_name: None,
            queue_id,
            queue_name: None,
            queue_kind: None,
            queue_position,
            status: if invalid.is_some() { DownloadStatus::Failed } else if request.start { DownloadStatus::Queued } else { DownloadStatus::Queued },
            total_bytes: None,
            downloaded_bytes: 0,
            mime: None,
            etag: None,
            last_modified: None,
            accept_ranges: false,
            accelerated: false,
            http_status: None,
            resume_capable: false,
            error: invalid.clone(),
            warning: None,
            has_credentials,
            headers: public_headers,
            max_connections: request.max_connections.unwrap_or(settings.max_connections).clamp(1, 32),
            bandwidth_limit_bps: request.bandwidth_limit_bps.unwrap_or(0),
            sha256: None,
            replace_existing: request.replace_existing,
            retry_after: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            speed_bps: 0,
            average_bps: 0,
            eta_secs: None,
            active_connections: 0,
        };
        self.store.insert_download(&download).map_err(|err| err.to_string())?;
        if invalid.is_none() && request.start {
            self.start(&id)?;
        }
        self.store.get_download(&id).map_err(|err| err.to_string())?.ok_or_else(|| "The download was not saved.".to_string()).map(|download| self.decorate(download))
    }

    pub fn delete(&self, id: &str, delete_files: bool) -> Result<(), String> {
        if self.jobs.lock().expect("jobs").contains_key(id) {
            self.stop(id)?;
        }
        let Some(download) = self.store.delete_download(id).map_err(|err| err.to_string())? else {
            return Ok(());
        };
        let _ = self.secrets.delete(&format!("download:{id}"));
        let _ = std::fs::remove_file(download.partial_path());
        if delete_files {
            let _ = std::fs::remove_file(download.destination_path());
        }
        Ok(())
    }

    pub async fn check_update(&self) -> Result<UpdateStatus, String> {
        let settings = self.store.settings().map_err(|err| err.to_string())?;
        if settings.update_manifest_url.trim().is_empty() {
            return Ok(UpdateStatus {
                configured: false,
                current: VERSION.into(),
                latest: None,
                notes: None,
                download_url: None,
                message: "No update manifest is configured.".into(),
            });
        }
        let client = self.client.read().expect("client").clone();
        let response = client.get(settings.update_manifest_url.trim()).send().await.map_err(|err| err.to_string())?;
        if !response.status().is_success() {
            return Err(format!("The update server returned HTTP {}.", response.status()));
        }
        let body: Value = serde_json::from_str(&response.text().await.map_err(|err| err.to_string())?).map_err(|err| err.to_string())?;
        let latest = body.get("version").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let notes = body.get("notes").and_then(|v| v.as_str()).map(|v| v.to_string());
        let download_url = body.get("download_url").and_then(|v| v.as_str()).map(|v| v.to_string());
        let newer = version_is_newer(&latest, VERSION);
        Ok(UpdateStatus {
            configured: true,
            current: VERSION.into(),
            latest: Some(latest.clone()),
            notes,
            download_url,
            message: if newer {
                format!("Harbor {latest} is available.")
            } else {
                "Harbor is up to date.".into()
            },
        })
    }

    pub async fn native_message(&self, message: Value) -> Value {
        let action = message.get("action").and_then(|v| v.as_str()).unwrap_or("");
        match action {
            "ping" => json!({"ok": true, "name": "Harbor", "version": VERSION}),
            "settings_get" => match self.store.settings() {
                Ok(settings) => json!({"ok": true, "intercept_downloads": settings.intercept_downloads}),
                Err(err) => json!({"ok": false, "error": err.to_string()}),
            },
            "enqueue" => self.native_enqueue(&message).await,
            "enqueue_many" => {
                let items = message.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                let mut ids = Vec::new();
                for item in items {
                    match self.native_enqueue(&item).await {
                        value if value.get("ok").and_then(|v| v.as_bool()) == Some(true) => {
                            if let Some(id) = value.get("id").cloned() {
                                ids.push(id);
                            }
                        }
                        value => return value,
                    }
                }
                json!({"ok": true, "ids": ids})
            }
            _ => json!({"ok": false, "error": "Unknown native message."}),
        }
    }

    async fn native_enqueue(&self, message: &Value) -> Value {
        let Some(url) = message.get("url").and_then(|v| v.as_str()) else {
            return json!({"ok": false, "error": "Missing URL."});
        };
        if classify_url(url).is_err() {
            return json!({"ok": false, "error": "Only http, https, and ftp URLs can be sent to Harbor."});
        }
        let mut headers = HashMap::new();
        if let Some(map) = message.get("headers").and_then(|v| v.as_object()) {
            for (key, value) in map {
                if let Some(value) = value.as_str() {
                    headers.insert(key.clone(), value.to_string());
                }
            }
        }
        if let Some(referrer) = message.get("referrer").and_then(|v| v.as_str()) {
            headers.entry("Referer".into()).or_insert_with(|| referrer.to_string());
        }
        let filename = message.get("filename").and_then(|v| v.as_str()).map(|v| v.to_string());
        let cookies = message.get("cookies").and_then(|v| v.as_str()).map(|v| v.to_string());
        let start = message.get("start").and_then(|v| v.as_bool()).unwrap_or(true);
        match self.add(NewDownload {
            url: url.to_string(),
            filename,
            destination_dir: None,
            category_id: None,
            queue_id: None,
            username: None,
            password: None,
            headers,
            cookies,
            start,
            max_connections: None,
            bandwidth_limit_bps: None,
            replace_existing: false,
        }).await {
            Ok(download) => json!({"ok": true, "id": download.id, "status": download.status}),
            Err(err) => json!({"ok": false, "error": err}),
        }
    }

    fn save_site_auth(&self, params: &Value) -> Result<SiteAuth, String> {
        let host = required_str(params, "host")?.to_ascii_lowercase();
        let secret = SecretMaterial {
            username: params.get("username").and_then(|v| v.as_str()).map(|v| v.to_string()),
            password: params.get("password").and_then(|v| v.as_str()).map(|v| v.to_string()),
            cookies: params.get("cookies").and_then(|v| v.as_str()).map(|v| v.to_string()),
            headers: HashMap::new(),
        };
        let refer = format!("site:{host}");
        let payload = serde_json::to_string(&secret).map_err(|err| err.to_string())?;
        self.secrets.put(&refer, &payload).map_err(|err| err.to_string())?;
        self.store.upsert_site_auth(&host, &refer).map_err(|err| err.to_string())
    }
}

fn transient_download(url: &str, headers: HashMap<String, String>) -> Download {
    Download {
        id: "probe".into(),
        url: url.into(),
        final_url: None,
        filename: "download".into(),
        filename_explicit: false,
        destination_dir: ".".into(),
        category_id: None,
        category_name: None,
        queue_id: None,
        queue_name: None,
        queue_kind: None,
        queue_position: 0,
        status: DownloadStatus::Queued,
        total_bytes: None,
        downloaded_bytes: 0,
        mime: None,
        etag: None,
        last_modified: None,
        accept_ranges: false,
        accelerated: false,
        http_status: None,
        resume_capable: false,
        error: None,
        warning: None,
        has_credentials: false,
        headers,
        max_connections: 1,
        bandwidth_limit_bps: 0,
        sha256: None,
        replace_existing: false,
        retry_after: None,
        created_at: 0,
        updated_at: 0,
        completed_at: None,
        speed_bps: 0,
        average_bps: 0,
        eta_secs: None,
        active_connections: 0,
    }
}

fn probe_result(info: RemoteInfo, category_id: Option<String>) -> ProbeResult {
    let accelerated_possible = info.resume_capable && info.total_bytes.unwrap_or(0) >= harbor_core::ACCELERATION_MIN_BYTES;
    ProbeResult {
        final_url: info.final_url,
        filename: info.filename,
        total_bytes: info.total_bytes,
        mime: info.mime,
        etag: info.etag,
        last_modified: info.last_modified,
        accept_ranges: info.accept_ranges,
        http_status: info.http_status,
        resume_capable: info.resume_capable,
        accelerated_possible,
        category_id,
    }
}

fn classify_url(raw: &str) -> Result<(String, Option<String>, Option<String>), String> {
    let url = Url::parse(raw).map_err(|_| "Invalid URL".to_string())?;
    match url.scheme() {
        "http" | "https" | "ftp" => {}
        _ => return Err("Only http, https, and ftp URLs can be downloaded".into()),
    }
    let username = {
        let value = url.username();
        if value.is_empty() { None } else { Some(value.to_string()) }
    };
    let password = url.password().map(|value| value.to_string());
    redact_url(url.as_str()).map(|clean| (clean, username, password)).map_err(|err| err.to_string())
}

fn required_str<'a>(params: &'a Value, key: &str) -> Result<&'a str, String> {
    params.get(key).and_then(|v| v.as_str()).filter(|v| !v.is_empty()).ok_or_else(|| format!("{key} is required"))
}

fn parse_filter(value: &str) -> ListFilter {
    match value {
        "downloading" => ListFilter::Downloading,
        "queued" => ListFilter::Queued,
        "completed" => ListFilter::Completed,
        "paused" => ListFilter::Paused,
        "failed" => ListFilter::Failed,
        _ => ListFilter::All,
    }
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items.iter().filter_map(|item| item.as_str().map(|s| s.to_string())).collect(),
        Some(Value::String(text)) => text.split([',', ' ']).filter(|s| !s.is_empty()).map(|s| s.to_string()).collect(),
        _ => Vec::new(),
    }
}

fn category_from_params(params: &Value) -> Result<Category, String> {
    let name = required_str(params, "name")?.to_string();
    let id = params.get("id").and_then(|v| v.as_str()).filter(|v| !v.is_empty()).map(|v| v.to_string()).unwrap_or_else(new_id);
    Ok(Category {
        id,
        name,
        extensions: string_list(params.get("extensions")),
        directory: params.get("directory").and_then(|v| v.as_str()).map(|v| v.to_string()),
        site_filter: string_list(params.get("site_filter")),
        builtin: params.get("builtin").and_then(|v| v.as_bool()).unwrap_or(false),
        created_at: harbor_core::now_ms(),
    })
}

fn queue_from_params(params: &Value) -> Result<Queue, String> {
    let name = required_str(params, "name")?.to_string();
    let id = params.get("id").and_then(|v| v.as_str()).filter(|v| !v.is_empty()).map(|v| v.to_string()).unwrap_or_else(new_id);
    let kind = match params.get("kind").and_then(|v| v.as_str()).unwrap_or("download") {
        "sync" => QueueKind::Sync,
        _ => QueueKind::Download,
    };
    Ok(Queue {
        id,
        name,
        kind,
        max_concurrent: params.get("max_concurrent").and_then(|v| v.as_u64()).unwrap_or(3).clamp(1, 10) as u32,
        status: QueueStatus::Idle,
        sort_order: params.get("sort_order").and_then(|v| v.as_i64()).unwrap_or(10),
        created_at: harbor_core::now_ms(),
    })
}

fn schedule_from_params(params: &Value) -> Result<Schedule, String> {
    let queue_id = required_str(params, "queue_id")?.to_string();
    let id = params.get("id").and_then(|v| v.as_str()).filter(|v| !v.is_empty()).map(|v| v.to_string()).unwrap_or_else(new_id);
    Ok(Schedule {
        id,
        queue_id,
        start_at: params.get("start_at").and_then(|v| v.as_i64()),
        stop_minutes: params.get("stop_minutes").and_then(|v| v.as_i64()).map(|v| v as i32),
        recurrence: match params.get("recurrence").and_then(|v| v.as_str()).unwrap_or("once") {
            "daily" => Recurrence::Daily,
            "weekly" => Recurrence::Weekly,
            _ => Recurrence::Once,
        },
        enabled: params.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
        last_run: None,
        created_at: harbor_core::now_ms(),
    })
}
