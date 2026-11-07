use chrono::Timelike;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Recurrence {
    Once,
    Daily,
    Weekly,
}

impl Recurrence {
    pub fn parse(value: &str) -> Self {
        match value {
            "daily" => Self::Daily,
            "weekly" => Self::Weekly,
            _ => Self::Once,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Once => "once",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
        }
    }
}

pub fn advance_start(current: i64, recurrence: &Recurrence, now: i64) -> Option<i64> {
    let step = match recurrence {
        Recurrence::Once => return None,
        Recurrence::Daily => 86_400_000,
        Recurrence::Weekly => 7 * 86_400_000,
    };
    let mut next = current;
    while next <= now {
        next = next.saturating_add(step);
    }
    Some(next)
}

pub fn stop_reached(start_minutes: Option<i32>, stop_minutes: Option<i32>, now_minutes: i32) -> bool {
    let Some(stop) = stop_minutes else {
        return false;
    };
    let now = ((now_minutes % (24 * 60)) + (24 * 60)) % (24 * 60);
    let stop = ((stop % (24 * 60)) + (24 * 60)) % (24 * 60);
    match start_minutes {
        Some(start) if stop <= start => now >= stop && now < start,
        _ => now >= stop,
    }
}

pub fn local_minutes(timestamp_ms: i64) -> i32 {
    let dt = chrono::DateTime::from_timestamp_millis(timestamp_ms)
        .unwrap_or_else(chrono::Utc::now)
        .with_timezone(&chrono::Local);
    dt.hour() as i32 * 60 + dt.minute() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_start_moves_forward() {
        let next = advance_start(1_000, &Recurrence::Daily, 1_000).unwrap();
        assert!(next > 1_000);
        assert!(advance_start(1_000, &Recurrence::Once, 1_000).is_none());
    }

    #[test]
    fn overnight_stop_window() {
        assert!(!stop_reached(Some(23 * 60), Some(60), 23 * 60 + 30));
        assert!(stop_reached(Some(23 * 60), Some(60), 90));
        assert!(stop_reached(Some(8 * 60), Some(17 * 60), 17 * 60));
        assert!(!stop_reached(Some(8 * 60), Some(17 * 60), 12 * 60));
    }
}
