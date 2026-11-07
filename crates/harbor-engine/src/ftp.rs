use crate::error::{map_io, EngineError};
use crate::http::load_secret;
use crate::limiter::pace_blocking;
use crate::transfer::{open_partial, write_all_at, Gauges};
use crate::EngineHandle;
use harbor_core::Download;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

pub async fn download_ftp(
    engine: &EngineHandle,
    download: &Download,
    gauges: &Arc<Gauges>,
    cancel: &CancellationToken,
    mode: &AtomicU8,
) -> Result<(), EngineError> {
    let url = Url::parse(&download.url).map_err(|_| EngineError::Fatal("Invalid URL".into()))?;
    let host = url
        .host_str()
        .ok_or_else(|| EngineError::Fatal("The FTP URL is missing a host.".into()))?
        .to_string();
    let port = url.port().unwrap_or(21);
    let path = url.path().to_string();
    if path.is_empty() || path == "/" {
        return Err(EngineError::Fatal("The FTP URL is missing a file path.".into()));
    }
    let secret = load_secret(&engine.store, engine.secrets.as_ref(), download);
    let username = secret.username.clone().filter(|value| !value.is_empty()).unwrap_or_else(|| "anonymous".into());
    let password = secret.password.clone().unwrap_or_else(|| "harbor@local".into());
    let partial = download.partial_path();
    let _ = open_partial(&partial).map_err(map_io)?;
    let rate = if download.bandwidth_limit_bps > 0 {
        download.bandwidth_limit_bps
    } else {
        engine.limiter.rate()
    };
    let quota = engine.store.settings().map(|settings| settings.quota_bytes_per_hour).unwrap_or(0);
    let window = engine.config.quota_window;
    let store = engine.store.clone();
    let gauges = gauges.clone();
    let cancel = cancel.clone();
    let cancel_for_thread = cancel.clone();
    let offset = if partial.exists() { gauges.bytes.load(Ordering::Relaxed) } else { 0 };
    let size = match tokio::task::spawn_blocking(move || {
        ftp_blocking(host, port, path, username, password, partial, offset, rate, quota, window, store, gauges, cancel_for_thread)
    })
    .await
    {
        Ok(Ok(size)) => size,
        Ok(Err(EngineError::Paused)) => return Err(engine.control_error(mode)),
        Ok(Err(err)) => return Err(err),
        Err(err) => return Err(EngineError::Fatal(err.to_string())),
    };
    if let Some(size) = size {
        if download.total_bytes.is_none() {
            let _ = engine.store.update_meta(
                &download.id,
                &download.url,
                &download.filename,
                download.filename_explicit,
                Some(size),
                download.mime.as_deref(),
                download.etag.as_deref(),
                download.last_modified.as_deref(),
                true,
                true,
                Some(226),
                None,
            );
        }
    }
    if mode.load(Ordering::SeqCst) == 2 && cancel.is_cancelled() {
        return Err(EngineError::Stopped);
    }
    if mode.load(Ordering::SeqCst) == 3 && cancel.is_cancelled() {
        return Err(EngineError::Abandoned);
    }
    Ok(())
}

fn ftp_blocking(
    host: String,
    port: u16,
    path: String,
    username: String,
    password: String,
    partial: PathBuf,
    mut offset: u64,
    rate: u64,
    quota: u64,
    window: Duration,
    store: harbor_store::Store,
    gauges: Arc<Gauges>,
    cancel: CancellationToken,
) -> Result<Option<u64>, EngineError> {
    let mut ftp = suppaftp::FtpStream::connect(format!("{host}:{port}"))
        .map_err(|err| EngineError::Retryable(format!("Could not connect to the FTP server: {err}")))?;
    ftp.login(&username, &password)
        .map_err(|err| EngineError::Fatal(format!("FTP login failed: {err}")))?;
    ftp.transfer_type(suppaftp::types::FileType::Binary)
        .map_err(|err| EngineError::Retryable(err.to_string()))?;
    let size = ftp.size(&path).ok().map(|value| value as u64);
    if offset > 0 && ftp.resume_transfer(offset as usize).is_err() {
        offset = 0;
    }
    gauges.bytes.store(offset, Ordering::Relaxed);
    let mut reader = ftp
        .retr_as_stream(&path)
        .map_err(|err| EngineError::Retryable(format!("FTP download failed: {err}")))?;
    let file = open_partial(&partial).map_err(map_io)?;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        if cancel.is_cancelled() {
            let _ = ftp.finalize_retr_stream(reader);
            return Err(EngineError::Paused);
        }
        let read = reader.read(&mut buf).map_err(|err| EngineError::Retryable(err.to_string()))?;
        if read == 0 {
            break;
        }
        pace_blocking(rate, read);
        if quota > 0 {
            let now = harbor_core::now_ms();
            if let Some(wait) = store
                .try_consume_quota(read as u64, quota, window, now)
                .map_err(|err| EngineError::Fatal(err.to_string()))?
            {
                std::thread::sleep(wait.min(Duration::from_secs(2)));
            }
        }
        write_all_at(&file, &buf[..read], offset).map_err(map_io)?;
        offset += read as u64;
        gauges.bytes.store(offset, Ordering::Relaxed);
        gauges.connections.store(1, Ordering::Relaxed);
    }
    file.sync_all().map_err(map_io)?;
    let _ = ftp.finalize_retr_stream(reader);
    let _ = ftp.quit();
    Ok(size)
}
