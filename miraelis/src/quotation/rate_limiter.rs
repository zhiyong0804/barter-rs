use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// 令牌桶限速器。
/// Binance Futures REST 上限 2400 weight/min，klines weight=1，
/// 保守取 35 req/s（留约 12% 余量）。
pub struct RateLimiter {
    interval: Duration,
    last: tokio::sync::Mutex<tokio::time::Instant>,
}

impl RateLimiter {
    pub fn new(rate_per_sec: u32) -> Arc<Self> {
        Arc::new(Self {
            interval: Duration::from_nanos(1_000_000_000 / rate_per_sec.max(1) as u64),
            last: tokio::sync::Mutex::new(tokio::time::Instant::now()),
        })
    }

    pub async fn acquire(&self) {
        let mut last = self.last.lock().await;
        let now = tokio::time::Instant::now();
        let elapsed = now.duration_since(*last);
        if elapsed < self.interval {
            sleep(self.interval - elapsed).await;
        }
        *last = tokio::time::Instant::now();
    }
}

