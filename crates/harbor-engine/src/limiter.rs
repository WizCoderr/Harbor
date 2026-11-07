use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug)]
struct Bucket {
    available: f64,
    updated: Instant,
}

/// Shared token bucket. A rate of zero means the bucket does not limit anything.
#[derive(Debug)]
pub struct Limiter {
    rate: AtomicU64,
    state: Mutex<Bucket>,
}

impl Limiter {
    pub fn new(rate: u64) -> Self {
        Self {
            rate: AtomicU64::new(rate),
            state: Mutex::new(Bucket { available: rate as f64, updated: Instant::now() }),
        }
    }

    pub fn set_rate(&self, rate: u64) {
        self.rate.store(rate, Ordering::Relaxed);
    }

    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    pub async fn acquire(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        loop {
            let wait = {
                let rate = self.rate();
                if rate == 0 {
                    return;
                }
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(state.updated).as_secs_f64();
                state.available = (state.available + elapsed * rate as f64).min(rate as f64 * 2.0);
                state.updated = now;
                if state.available >= bytes as f64 {
                    state.available -= bytes as f64;
                    return;
                }
                let missing = bytes as f64 - state.available;
                Duration::from_secs_f64((missing / rate as f64).max(0.001))
            };
            tokio::time::sleep(wait).await;
        }
    }
}

impl Default for Limiter {
    fn default() -> Self {
        Self::new(0)
    }
}

pub fn pace_blocking(rate: u64, bytes: usize) {
    if rate == 0 || bytes == 0 {
        return;
    }
    let seconds = bytes as f64 / rate as f64;
    std::thread::sleep(Duration::from_secs_f64(seconds.max(0.001)));
}
