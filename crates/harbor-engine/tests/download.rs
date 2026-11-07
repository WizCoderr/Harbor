use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use harbor_core::{hash_file, DownloadStatus, NewDownload};
use harbor_engine::{EngineConfig, EngineHandle, MemorySecrets};
use tokio::net::TcpListener;

struct Mode {
    data: Vec<u8>,
    etag: String,
    honor_range: bool,
    ranged_probe_only: bool,
    force_status: Option<StatusCode>,
    delay: Duration,
    delay_first_range: bool,
    drop_after: Option<u64>,
    auth: Option<(String, String)>,
    hits: u64,
    bytes_sent: u64,
}

struct World {
    mode: Mutex<Mode>,
}

async fn serve(world: Arc<World>) -> SocketAddr {
    let app = Router::new()
        .route("/file", get(file))
        .route("/go", get(redirect))
        .route("/page", get(page))
        .route("/page2", get(page_two))
        .route("/files/{name}", get(named))
        .with_state(world);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

async fn redirect() -> impl IntoResponse {
    axum::response::Redirect::temporary("/file")
}

async fn page() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html")],
        r#"<a href="/files/a.zip">a</a><a href="https://evil.example/b.zip">b</a><a href="/page2">next</a>"#,
    )
}

async fn page_two() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/html")], r#"<a href="/files/c.zip">c</a>"#)
}

async fn named(axum::extract::Path(name): axum::extract::Path<String>) -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (header::CONTENT_DISPOSITION, format!("attachment; filename=\"{name}\"")),
        ],
        Bytes::from(format!("payload-{name}")),
    )
}

struct Snap {
    data: Vec<u8>,
    etag: String,
    honor_range: bool,
    probe_only: bool,
    delay: Duration,
    delay_first: bool,
    drop_after: Option<u64>,
}

fn snap(world: &World, headers: &HeaderMap) -> Result<Snap, Response> {
    let mut mode = world.mode.lock().unwrap();
    mode.hits += 1;
    if let Some((user, pass)) = &mode.auth {
        let expected = format!("Basic {}", b64(&format!("{user}:{pass}")));
        let presented = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or("");
        if presented != expected {
            return Err((StatusCode::UNAUTHORIZED, "auth").into_response());
        }
    }
    if let Some(status) = mode.force_status {
        return Err((status, "nope").into_response());
    }
    Ok(Snap {
        data: mode.data.clone(),
        etag: mode.etag.clone(),
        honor_range: mode.honor_range,
        probe_only: mode.ranged_probe_only,
        delay: mode.delay,
        delay_first: mode.delay_first_range,
        drop_after: mode.drop_after,
    })
}

async fn file(State(world): State<Arc<World>>, headers: HeaderMap) -> Response {
    let Snap { data, etag, honor_range, probe_only, delay, delay_first, drop_after } = match snap(&world, &headers) {
        Ok(snap) => snap,
        Err(response) => return response,
    };
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok()).map(|v| v.to_string());
    let probe = range.as_deref() == Some("bytes=0-0");
    if let Some(range) = range.as_deref() {
        if honor_range && (!probe_only || probe) {
            if let Some((start, end)) = parse_range(range, data.len() as u64) {
                if delay_first && start == 0 && !probe {
                    tokio::time::sleep(Duration::from_millis(900)).await;
                } else if delay > Duration::ZERO && !probe {
                    tokio::time::sleep(delay).await;
                }
                let mut slice = data[start as usize..=end as usize].to_vec();
                if let Some(limit) = drop_after {
                    if !probe && slice.len() as u64 > limit {
                        slice.truncate(limit as usize);
                        let mut mode = world.mode.lock().unwrap();
                        mode.drop_after = None;
                        mode.bytes_sent += slice.len() as u64;
                        drop(mode);
                        let length = slice.len() as u64;
                        return ranged(start, data.len() as u64, &etag, Body::from(slice), length);
                    }
                }
                world.mode.lock().unwrap().bytes_sent += slice.len() as u64;
                let length = slice.len() as u64;
                let paced = if delay.is_zero() { Body::from(slice) } else { paced_body(slice, delay) };
                return ranged(start, data.len() as u64, &etag, paced, length);
            }
        }
    }
    if delay > Duration::ZERO {
        tokio::time::sleep(delay).await;
    }
    world.mode.lock().unwrap().bytes_sent += data.len() as u64;
    let length = data.len();
    let body = if delay.is_zero() { Body::from(data) } else { paced_body(data, delay) };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, length)
        .header(header::ETAG, etag)
        .header(header::CONTENT_DISPOSITION, "attachment; filename=\"report.bin\"")
        .body(body)
        .unwrap()
}

fn paced_body(bytes: Vec<u8>, delay: Duration) -> Body {
    let stream = futures_util::stream::unfold((0usize, bytes), move |(pos, bytes)| async move {
        if pos >= bytes.len() {
            return None;
        }
        tokio::time::sleep(delay).await;
        let end = (pos + 16 * 1024).min(bytes.len());
        let chunk = Bytes::copy_from_slice(&bytes[pos..end]);
        Some((Ok::<Bytes, std::io::Error>(chunk), (end, bytes)))
    });
    Body::from_stream(stream)
}

fn ranged(start: u64, total: u64, etag: &str, body: Body, content_length: u64) -> Response {
    let actual_end = start + content_length.saturating_sub(1);
    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_RANGE, format!("bytes {start}-{actual_end}/{total}"))
        .header(header::CONTENT_LENGTH, content_length)
        .header(header::ETAG, etag)
        .header(header::CONTENT_DISPOSITION, "attachment; filename=\"report.bin\"")
        .body(body)
        .unwrap()
}

fn parse_range(header: &str, len: u64) -> Option<(u64, u64)> {
    let rest = header.strip_prefix("bytes=")?;
    let (start, end) = rest.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    if start >= len {
        return None;
    }
    let end = if end.is_empty() { len - 1 } else { end.parse().ok()? };
    Some((start, end.min(len - 1)))
}

fn b64(value: &str) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = value.as_bytes();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 63) as usize] as char);
        out.push(TABLE[((triple >> 12) & 63) as usize] as char);
        if chunk.len() > 1 { out.push(TABLE[((triple >> 6) & 63) as usize] as char); } else { out.push('='); }
        if chunk.len() > 2 { out.push(TABLE[(triple & 63) as usize] as char); } else { out.push('='); }
    }
    out
}

fn world(data: Vec<u8>) -> Arc<World> {
    Arc::new(World {
        mode: Mutex::new(Mode {
            data,
            etag: "\"v1\"".into(),
            honor_range: true,
            ranged_probe_only: false,
            force_status: None,
            delay: Duration::ZERO,
            delay_first_range: false,
            drop_after: None,
            auth: None,
            hits: 0,
            bytes_sent: 0,
        }),
    })
}

fn engine(dir: &std::path::Path, config: EngineConfig) -> EngineHandle {
    EngineHandle::open(dir.join("harbor.db"), config, Arc::new(MemorySecrets::default())).unwrap()
}

fn config() -> EngineConfig {
    EngineConfig {
        segment_threshold: 2 * 1024 * 1024,
        min_split_bytes: 64 * 1024,
        chunk_bytes: 64 * 1024,
        adaptive: false,
        resume_on_open: false,
        quota_window: Duration::from_secs(1),
        queue_retry: Duration::from_millis(300),
        max_retries: 5,
        max_segments: 16,
        ..EngineConfig::default()
    }
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add((i % 251) as u8).wrapping_mul(3)).collect()
}

async fn wait_status(engine: &EngineHandle, id: &str, pred: impl Fn(DownloadStatus) -> bool) -> harbor_core::Download {
    let start = Instant::now();
    loop {
        let download = engine.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == id).unwrap();
        if pred(download.status) {
            return download;
        }
        if start.elapsed() > Duration::from_secs(25) {
            panic!("timed out waiting for {id}: status {:?} error {:?}", download.status, download.error);
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

fn new_download(url: &str, dir: &std::path::Path, start: bool) -> NewDownload {
    NewDownload {
        url: url.into(),
        filename: None,
        destination_dir: Some(dir.to_string_lossy().into()),
        category_id: None,
        queue_id: None,
        username: None,
        password: None,
        headers: HashMap::new(),
        cookies: None,
        start,
        max_connections: Some(4),
        bandwidth_limit_bps: None,
        replace_existing: false,
    }
}

fn assert_file(path: &std::path::Path, data: &[u8]) {
    let saved = std::fs::read(path).unwrap();
    assert_eq!(saved, data);
    let expected = {
        let scratch = path.with_extension("expected");
        std::fs::write(&scratch, data).unwrap();
        let hash = hash_file(&scratch).unwrap();
        std::fs::remove_file(scratch).unwrap();
        hash
    };
    assert_eq!(hash_file(path).unwrap(), expected);
}

#[tokio::test]
async fn http_download_redirect_and_checksum() {
    let data = pattern(180_000, 7);
    let state = world(data.clone());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.add(new_download(&format!("http://{addr}/go"), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(done.filename, "report.bin");
    assert!(done.final_url.unwrap().ends_with("/file"));
    assert!(!done.accelerated);
    assert_file(&PathBuf::from(&done.destination_dir).join(&done.filename), &data);
}

#[tokio::test]
async fn https_download_with_pinned_certificate() {
    let data = b"https-body".to_vec();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let mut tls = config();
    tls.extra_root_pem = Some(cert_pem.clone());
    let app = Router::new().route("/file", get(move || async move {
        ([(header::CONTENT_TYPE, "text/plain")], data.clone())
    }));
    let rustls = axum_server::tls_rustls::RustlsConfig::from_pem(cert_pem.into_bytes(), key_pem.into_bytes()).await.unwrap();
    let handle = axum_server::Handle::new();
    let server = axum_server::bind_rustls("127.0.0.1:0".parse().unwrap(), rustls).handle(handle.clone()).serve(app.into_make_service());
    tokio::spawn(async move { server.await.unwrap() });
    let bound = handle.listening().await.expect("https listener");
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), tls);
    let added = engine.add(new_download(&format!("https://127.0.0.1:{}/file", bound.port()), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(std::fs::read(done.destination_path()).unwrap(), b"https-body");
}

#[tokio::test]
async fn segmented_download_has_no_overlapping_ranges() {
    let data = pattern(900_000, 3);
    let state = world(data.clone());
    state.mode.lock().unwrap().delay_first_range = true;
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.segment_threshold = 64 * 1024;
    cfg.min_split_bytes = 64 * 1024;
    let engine = engine(dir.path(), cfg);
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert!(done.accelerated, "server accepted ranges, so this download is accelerated");
    let detail = engine.detail(&added.id).unwrap();
    let segments = detail["segments"].as_array().unwrap();
    assert!(segments.len() >= 2, "dynamic segmentation should open more than one range");
    let mut spans: Vec<(u64, u64)> = segments.iter().map(|seg| (seg["start_byte"].as_u64().unwrap(), seg["end_byte"].as_u64().unwrap())).collect();
    spans.sort();
    for pair in spans.windows(2) {
        assert!(pair[1].0 >= pair[0].1, "ranges overlap: {spans:?}");
    }
    assert_eq!(spans.first().unwrap().0, 0);
    assert_eq!(spans.last().unwrap().1, data.len() as u64);
    let mut cursor = 0u64;
    for (start, end) in spans {
        assert_eq!(start, cursor);
        cursor = end;
    }
    assert_file(&done.destination_path(), &data);
}

#[tokio::test]
async fn server_without_ranges_stays_single_stream() {
    let data = pattern(120_000, 9);
    let state = world(data.clone());
    state.mode.lock().unwrap().honor_range = false;
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.segment_threshold = 1024;
    let engine = engine(dir.path(), cfg);
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert!(!done.accelerated);
    assert!(!done.resume_capable);
    assert_file(&done.destination_path(), &data);
}

#[tokio::test]
async fn ranged_probe_that_later_rejects_ranges_falls_back() {
    let data = pattern(300_000, 4);
    let state = world(data.clone());
    state.mode.lock().unwrap().ranged_probe_only = true;
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.segment_threshold = 1024;
    let engine = engine(dir.path(), cfg);
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert!(!done.accelerated);
    assert!(done.warning.clone().unwrap_or_default().contains("one connection"));
    assert_file(&done.destination_path(), &data);
}

#[tokio::test]
async fn pause_and_resume_keeps_the_bytes() {
    let data = pattern(500_000, 11);
    let state = world(data.clone());
    state.mode.lock().unwrap().delay = Duration::from_millis(80);
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let start = Instant::now();
    loop {
        let current = engine.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == added.id).unwrap();
        if current.downloaded_bytes > 20_000 { break; }
        if start.elapsed() > Duration::from_secs(15) { panic!("download did not progress: {current:?}"); }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    engine.pause(&added.id).unwrap();
    let paused = wait_status(&engine, &added.id, |s| s == DownloadStatus::Paused).await;
    assert!(paused.downloaded_bytes > 0 && paused.downloaded_bytes < data.len() as u64);
    assert!(paused.partial_path().exists());
    engine.start(&added.id).unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_file(&done.destination_path(), &data);
}

#[tokio::test]
async fn dropped_connection_is_retried() {
    let data = pattern(250_000, 5);
    let state = world(data.clone());
    state.mode.lock().unwrap().drop_after = Some(40_000);
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_file(&done.destination_path(), &data);
}

#[tokio::test]
async fn invalid_url_is_recorded_as_failed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.add(new_download("not a url", dir.path(), true)).await.unwrap();
    assert_eq!(added.status, DownloadStatus::Failed);
    assert!(added.error.unwrap().contains("Invalid URL"));
}

#[tokio::test]
async fn expired_url_keeps_the_partial_file() {
    let data = pattern(200_000, 8);
    let state = world(data);
    state.mode.lock().unwrap().delay = Duration::from_millis(60);
    let addr = serve(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let start = Instant::now();
    loop {
        let current = engine.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == added.id).unwrap();
        if current.downloaded_bytes > 10_000 { break; }
        if start.elapsed() > Duration::from_secs(15) { panic!("no progress"); }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    engine.pause(&added.id).unwrap();
    let paused = wait_status(&engine, &added.id, |s| s == DownloadStatus::Paused).await;
    state.mode.lock().unwrap().force_status = Some(StatusCode::GONE);
    engine.start(&added.id).unwrap();
    let failed = wait_status(&engine, &added.id, |s| s == DownloadStatus::Failed).await;
    assert!(failed.error.unwrap().contains("no longer available"));
    assert!(paused.partial_path().exists());
}

#[tokio::test]
async fn changed_remote_file_restarts_cleanly() {
    let first = pattern(180_000, 1);
    let second = pattern(180_000, 2);
    let state = world(first);
    state.mode.lock().unwrap().delay = Duration::from_millis(40);
    let addr = serve(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut request = new_download(&format!("http://{addr}/file"), dir.path(), true);
    request.max_connections = Some(1);
    let added = engine.add(request).await.unwrap();
    let start = Instant::now();
    loop {
        let current = engine.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == added.id).unwrap();
        if current.downloaded_bytes > 30_000 { break; }
        if start.elapsed() > Duration::from_secs(10) { panic!("no progress {:?}", current.error); }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    engine.pause(&added.id).unwrap();
    wait_status(&engine, &added.id, |s| s == DownloadStatus::Paused).await;
    {
        let mut mode = state.mode.lock().unwrap();
        mode.data = second.clone();
        mode.etag = "\"v2\"".into();
        mode.honor_range = false;
    }
    engine.start(&added.id).unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_file(&done.destination_path(), &second);
}

#[tokio::test]
async fn basic_auth_and_secrets_stay_out_of_the_database() {
    let data = b"private-file".to_vec();
    let state = world(data.clone());
    state.mode.lock().unwrap().auth = Some(("user".into(), "s3cret-token-value".into()));
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut request = new_download(&format!("http://{addr}/file"), dir.path(), true);
    request.username = Some("user".into());
    request.password = Some("s3cret-token-value".into());
    request.cookies = Some("session=supersecretcookie".into());
    let added = engine.add(request).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(std::fs::read(done.destination_path()).unwrap(), data);
    let db = std::fs::read(engine.database_path()).unwrap();
    let wal = std::fs::read(engine.database_path().with_extension("db-wal")).unwrap_or_default();
    let blob = [db, wal].concat();
    assert!(!blob.windows(b"s3cret-token-value".len()).any(|w| w == b"s3cret-token-value"));
    assert!(!blob.windows(b"supersecretcookie".len()).any(|w| w == b"supersecretcookie"));
}

#[tokio::test]
async fn auth_failure_is_reported() {
    let state = world(b"nope".to_vec());
    state.mode.lock().unwrap().auth = Some(("user".into(), "secret".into()));
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let failed = wait_status(&engine, &added.id, |s| s == DownloadStatus::Failed).await;
    assert!(failed.error.unwrap().to_ascii_lowercase().contains("authentication"));
}

#[tokio::test]
async fn concurrent_downloads_both_finish() {
    let left = pattern(80_000, 12);
    let right = pattern(90_000, 13);
    let state = world(left.clone());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut a = new_download(&format!("http://{addr}/files/left.bin"), dir.path(), true);
    a.filename = Some("left.bin".into());
    let mut b = new_download(&format!("http://{addr}/files/right.bin"), dir.path(), true);
    b.filename = Some("right.bin".into());
    let _ = right;
    let first = engine.add(a).await.unwrap();
    let second = engine.add(b).await.unwrap();
    let done_a = wait_status(&engine, &first.id, |s| s == DownloadStatus::Completed).await;
    let done_b = wait_status(&engine, &second.id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(std::fs::read(done_a.destination_path()).unwrap(), b"payload-left.bin");
    assert_eq!(std::fs::read(done_b.destination_path()).unwrap(), b"payload-right.bin");
}

#[tokio::test]
async fn large_stream_is_not_kept_as_one_buffer_and_matches() {
    let data = pattern(3 * 1024 * 1024, 15);
    let state = world(data.clone());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.segment_threshold = 512 * 1024;
    let engine = engine(dir.path(), cfg);
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(std::fs::metadata(done.destination_path()).unwrap().len(), data.len() as u64);
    assert_file(&done.destination_path(), &data);
}

#[tokio::test]
async fn bandwidth_limit_slows_the_transfer() {
    let data = pattern(120_000, 6);
    let state = world(data);
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut settings = engine.dispatch("settings.get", serde_json::json!({})).await.unwrap();
    settings["bandwidth_limit_bps"] = serde_json::json!(40_000);
    engine.dispatch("settings.set", settings).await.unwrap();
    let started = Instant::now();
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert!(started.elapsed() >= Duration::from_millis(1800), "elapsed {:?}", started.elapsed());
}

#[tokio::test]
async fn quota_holds_the_transfer_until_the_window_moves() {
    let data = pattern(90_000, 6);
    let state = world(data);
    state.mode.lock().unwrap().delay = Duration::from_millis(15);
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut settings = engine.dispatch("settings.get", serde_json::json!({})).await.unwrap();
    settings["quota_bytes_per_hour"] = serde_json::json!(30_000);
    engine.dispatch("settings.set", settings).await.unwrap();
    let started = Instant::now();
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert!(started.elapsed() >= Duration::from_millis(900), "elapsed {:?}", started.elapsed());
}

#[tokio::test]
async fn queue_schedule_starts_a_due_queue() {
    let data = b"queued".to_vec();
    let state = world(data.clone());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    engine.set_clock(Some(1_700_000_000_000), Some(12 * 60));
    let mut request = new_download(&format!("http://{addr}/file"), dir.path(), false);
    request.queue_id = Some("queue_main".into());
    let added = engine.add(request).await.unwrap();
    engine.dispatch("schedules.save", serde_json::json!({
        "queue_id": "queue_main",
        "start_at": 1_700_000_000_000_i64 - 1_000,
        "recurrence": "once",
        "enabled": true
    })).await.unwrap();
    engine.scheduler_tick().unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(std::fs::read(done.destination_path()).unwrap(), data);
    assert!(done.queue_id.is_none());
}

#[tokio::test]
async fn stop_time_pauses_a_running_queue() {
    let state = world(pattern(200_000, 1));
    state.mode.lock().unwrap().delay = Duration::from_millis(200);
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    engine.set_clock(Some(harbor_core::now_ms()), Some(15 * 60));
    engine.dispatch("schedules.save", serde_json::json!({
        "queue_id": "queue_main",
        "stop_minutes": 0,
        "recurrence": "once",
        "enabled": true
    })).await.unwrap();
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), false)).await.unwrap();
    engine.start_queue("queue_main").unwrap();
    let start = Instant::now();
    loop {
        let queues = engine.dispatch("queues.list", serde_json::json!({})).await.unwrap();
        let status = queues.as_array().unwrap().iter().find(|q| q["id"] == "queue_main").unwrap()["status"].as_str().unwrap();
        if status == "paused" { break; }
        if start.elapsed() > Duration::from_secs(5) { panic!("queue status {status}"); }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let download = engine.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == added.id).unwrap();
    assert_ne!(download.status, DownloadStatus::Completed);
}

#[tokio::test]
async fn restart_recovers_a_partial_download() {
    let data = pattern(400_000, 14);
    let state = world(data.clone());
    state.mode.lock().unwrap().delay = Duration::from_millis(40);
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config();
    cfg.resume_on_open = true;
    let first = engine(dir.path(), cfg.clone());
    let added = first.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let id = added.id.clone();
    let start = Instant::now();
    loop {
        let current = first.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == id).unwrap();
        if current.downloaded_bytes > 50_000 { break; }
        if start.elapsed() > Duration::from_secs(15) { panic!("no progress"); }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    first.abandon(&id);
    let start = Instant::now();
    while first.active_jobs() > 0 {
        if start.elapsed() > Duration::from_secs(5) { panic!("job did not stop"); }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let stored = first.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == id).unwrap();
    assert_eq!(stored.status, DownloadStatus::Downloading);
    drop(first);
    let engine = engine(dir.path(), cfg);
    engine.boot().unwrap();
    let done = wait_status(&engine, &id, |s| s == DownloadStatus::Completed).await;
    assert_file(&done.destination_path(), &data);
}

#[tokio::test]
async fn read_only_destination_reports_permission_denied() {
    let data = b"cannot-write".to_vec();
    let state = world(data);
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).unwrap();
    let mut perms = std::fs::metadata(&locked).unwrap().permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(&locked, perms).unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.add(new_download(&format!("http://{addr}/file"), &locked, true)).await.unwrap();
    let failed = wait_status(&engine, &added.id, |s| s == DownloadStatus::Failed).await;
    assert!(failed.error.unwrap().to_ascii_lowercase().contains("permission"));
    let mut perms = std::fs::metadata(&locked).unwrap().permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
    }
    let _ = std::fs::set_permissions(&locked, perms);
}

#[tokio::test]
async fn grabber_filters_extension_and_domain() {
    let state = world(Vec::new());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let links = engine.dispatch("grabber.preview", serde_json::json!({
        "url": format!("http://{addr}/page"),
        "extensions": ["zip"],
        "domain": "127.0.0.1",
        "depth": 2
    })).await.unwrap();
    let urls: Vec<String> = links.as_array().unwrap().iter().map(|item| item["url"].as_str().unwrap().to_string()).collect();
    assert!(urls.iter().any(|url| url.ends_with("/files/a.zip")));
    assert!(urls.iter().any(|url| url.ends_with("/files/c.zip")));
    assert!(urls.iter().all(|url| !url.contains("evil.example")));
}

#[tokio::test]
async fn batch_import_queues_every_url() {
    let state = world(b"batch".to_vec());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let added = engine.dispatch("batch.import", serde_json::json!({
        "text": format!("http://{addr}/file\nnot a url\n"),
        "destination_dir": dir.path(),
        "start": false
    })).await.unwrap();
    assert_eq!(added.as_array().unwrap().len(), 2);
    assert!(added.as_array().unwrap().iter().any(|item| item["status"] == "failed"));
    assert!(added.as_array().unwrap().iter().any(|item| item["status"] == "queued"));
}

#[tokio::test]
async fn sync_queue_skips_unchanged_files_and_replaces_changed_ones() {
    let first = b"version-one".to_vec();
    let state = world(first.clone());
    let addr = serve(state.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut request = new_download(&format!("http://{addr}/file"), dir.path(), true);
    request.queue_id = Some("queue_sync".into());
    request.filename = Some("sync.bin".into());
    let added = engine.add(request).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(done.queue_id.as_deref(), Some("queue_sync"));
    let sent = state.mode.lock().unwrap().bytes_sent;
    engine.start_queue("queue_sync").unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let again = engine.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == added.id).unwrap();
    assert_eq!(again.status, DownloadStatus::Completed);
    assert!(state.mode.lock().unwrap().bytes_sent - sent < first.len() as u64);
    {
        let mut mode = state.mode.lock().unwrap();
        mode.data = b"version-two-changed".to_vec();
        mode.etag = "\"v2\"".into();
    }
    engine.start_queue("queue_sync").unwrap();
    let start = Instant::now();
    loop {
        let current = engine.list(harbor_core::ListFilter::All, None, None, "").unwrap().into_iter().find(|d| d.id == added.id).unwrap();
        if current.status == DownloadStatus::Completed && std::fs::read(current.destination_path()).ok().as_deref() == Some(b"version-two-changed".as_slice()) {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!("sync did not replace the file: {:?}", current.error);
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

#[tokio::test]
async fn ftp_download_and_rest_resume() {
    let data = b"ftp-payload-0123456789".to_vec();
    let addr = serve_ftp(data.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut request = new_download(&format!("ftp://anonymous:harbor@127.0.0.1:{}/report.bin", addr.port()), dir.path(), true);
    request.filename = Some("report.bin".into());
    let added = engine.add(request).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| matches!(s, DownloadStatus::Completed | DownloadStatus::Failed)).await;
    assert_eq!(done.status, DownloadStatus::Completed, "{:?}", done.error);
    assert_eq!(std::fs::read(done.destination_path()).unwrap(), data);
}

#[tokio::test]
async fn native_message_enqueues_a_download() {
    let data = b"from-browser".to_vec();
    let state = world(data.clone());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let response = engine.native_message(serde_json::json!({
        "action": "enqueue",
        "url": format!("http://{addr}/file"),
        "filename": "browser.bin",
        "cookies": "sid=abc",
        "referrer": format!("http://{addr}/page"),
        "start": true
    })).await;
    assert_eq!(response["ok"], true, "{response}");
    let id = response["id"].as_str().unwrap();
    let done = wait_status(&engine, id, |s| s == DownloadStatus::Completed).await;
    assert_eq!(std::fs::read(done.destination_path()).unwrap(), data);
}

#[tokio::test]
async fn antivirus_command_records_a_warning() {
    let state = world(b"scan-me".to_vec());
    let addr = serve(state).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), config());
    let mut settings = engine.dispatch("settings.get", serde_json::json!({})).await.unwrap();
    settings["antivirus_command"] = serde_json::json!("/usr/bin/false");
    engine.dispatch("settings.set", settings).await.unwrap();
    let added = engine.add(new_download(&format!("http://{addr}/file"), dir.path(), true)).await.unwrap();
    let done = wait_status(&engine, &added.id, |s| s == DownloadStatus::Completed).await;
    assert!(done.warning.unwrap_or_default().contains("Scanner"));
}

async fn serve_ftp(data: Vec<u8>) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut control, _) = listener.accept().unwrap();
        use std::io::{Read, Write};
        let _ = control.write_all(b"220 Harbor test FTP\r\n");
        let mut rest = 0usize;
        let mut passive: Option<std::net::TcpListener> = None;
        let mut buf = [0u8; 1024];
        let mut pending = String::new();
        loop {
            let n = control.read(&mut buf).unwrap_or(0);
            if n == 0 { break; }
            pending.push_str(&String::from_utf8_lossy(&buf[..n]));
            while let Some(pos) = pending.find("\r\n") {
                let line = pending[..pos].to_string();
                pending = pending[pos + 2..].to_string();
                let (cmd, arg) = line.split_once(' ').unwrap_or((line.as_str(), ""));
                let cmd = cmd.to_ascii_uppercase();
                let reply = match cmd.as_str() {
                    "USER" => "331 Password required\r\n".to_string(),
                    "PASS" => "230 Logged in\r\n".to_string(),
                    "TYPE" => "200 Type set\r\n".to_string(),
                    "SIZE" => format!("213 {}\r\n", data.len()),
                    "REST" => {
                        rest = arg.parse().unwrap_or(0);
                        "350 Restarting\r\n".to_string()
                    }
                    "PASV" => {
                        let data_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                        let port = data_listener.local_addr().unwrap().port();
                        passive = Some(data_listener);
                        format!("227 Entering Passive Mode (127,0,0,1,{},{})\r\n", port / 256, port % 256)
                    }
                    "RETR" => {
                        let listener = passive.take().unwrap();
                        let (mut data_socket, _) = listener.accept().unwrap();
                        let _ = control.write_all(b"150 Opening data connection\r\n");
                        let _ = data_socket.write_all(&data[rest.min(data.len())..]);
                        drop(data_socket);
                        rest = 0;
                        "226 Transfer complete\r\n".to_string()
                    }
                    "QUIT" => {
                        let _ = control.write_all(b"221 Goodbye\r\n");
                        return;
                    }
                    _ => "502 Command not implemented\r\n".to_string(),
                };
                if cmd != "RETR" || true {
                    let _ = control.write_all(reply.as_bytes());
                }
            }
        }
    });
    addr
}

#[allow(dead_code)]
fn _headers_unused(_: &HeaderMap) {}

#[allow(dead_code)]
static _HITS: AtomicU64 = AtomicU64::new(0);
