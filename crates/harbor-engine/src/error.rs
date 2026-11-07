use std::io;

#[derive(Debug)]
pub enum EngineError {
    Paused,
    Stopped,
    Abandoned,
    Fallback,
    Retryable(String),
    Fatal(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paused => write!(f, "paused"),
            Self::Stopped => write!(f, "stopped"),
            Self::Abandoned => write!(f, "abandoned"),
            Self::Fallback => write!(f, "server rejected a ranged request"),
            Self::Retryable(message) | Self::Fatal(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for EngineError {}

pub fn map_io(err: io::Error) -> EngineError {
    let raw = err.raw_os_error();
    if err.kind() == io::ErrorKind::StorageFull || matches!(raw, Some(28 | 112)) || err.kind() == io::ErrorKind::WriteZero
    {
        EngineError::Fatal("Disk is full. Free space and resume the download.".into())
    } else if err.kind() == io::ErrorKind::PermissionDenied {
        EngineError::Fatal("Permission denied while writing the file.".into())
    } else {
        EngineError::Retryable(format!("Could not write the file: {err}"))
    }
}

pub fn http_failure(status: u16) -> EngineError {
    match status {
        401 | 403 => EngineError::Fatal(
            "Authentication failed. Check the username and password, or the download URL may have expired.".into(),
        ),
        404 | 410 => EngineError::Fatal(format!(
            "The download URL is no longer available (HTTP {status})."
        )),
        408 | 425 | 429 | 500 | 502 | 503 | 504 => {
            EngineError::Retryable(format!("The server returned HTTP {status}. Harbor will retry."))
        }
        _ if (500..600).contains(&status) => {
            EngineError::Retryable(format!("The server returned HTTP {status}. Harbor will retry."))
        }
        _ => EngineError::Fatal(format!("The server returned HTTP {status}.")),
    }
}
