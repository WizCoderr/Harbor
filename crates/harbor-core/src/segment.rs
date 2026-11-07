#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangePiece {
    pub start: u64,
    pub end: u64,
    pub received: u64,
    pub reserved_end: u64,
}

impl RangePiece {
    pub fn committed_end(&self) -> u64 {
        self.start.saturating_add(self.received).min(self.end)
    }

    pub fn locked_end(&self) -> u64 {
        let committed = self.committed_end();
        if self.reserved_end > committed {
            self.reserved_end.min(self.end)
        } else {
            committed
        }
    }

    pub fn remaining_unlocked(&self) -> u64 {
        self.end.saturating_sub(self.locked_end())
    }
}

/// Split the largest unlocked tail in half.
///
/// Bytes at or after `reserved_end` can be given to a new connection. Bytes already
/// reserved by the connection that is fetching this piece stay with it, so two
/// connections never own the same range.
pub fn split_largest(segments: &mut Vec<RangePiece>, min_split: u64, max_segments: usize) -> bool {
    if min_split == 0 || segments.len() >= max_segments {
        return false;
    }
    let Some(index) = segments
        .iter()
        .enumerate()
        .filter(|(_, seg)| seg.end > seg.start && seg.remaining_unlocked() >= min_split)
        .max_by_key(|(_, seg)| seg.remaining_unlocked())
        .map(|(index, _)| index)
    else {
        return false;
    };
    let locked = segments[index].locked_end();
    let end = segments[index].end;
    let unlocked = end.saturating_sub(locked);
    if unlocked < min_split || unlocked < 2 {
        return false;
    }
    let mid = locked + unlocked / 2;
    if mid <= locked || mid >= end {
        return false;
    }
    let new_piece = RangePiece {
        start: mid,
        end,
        received: 0,
        reserved_end: 0,
    };
    segments[index].end = mid;
    let max_received = segments[index].end.saturating_sub(segments[index].start);
    if segments[index].received > max_received {
        segments[index].received = max_received;
    }
    if segments[index].reserved_end > segments[index].end {
        segments[index].reserved_end = segments[index].end;
    }
    segments.push(new_piece);
    if ranges_overlap(segments) {
        segments.pop();
        return false;
    }
    true
}

pub fn ranges_overlap(segments: &[RangePiece]) -> bool {
    let mut spans: Vec<(u64, u64)> = segments.iter().map(|s| (s.start, s.end)).collect();
    spans.sort_unstable();
    if spans.iter().any(|(start, end)| end < start) {
        return true;
    }
    spans.windows(2).any(|pair| pair[1].0 < pair[0].1)
}

#[cfg(test)]
pub fn covers_without_gaps(segments: &[RangePiece], total: u64) -> bool {
    if ranges_overlap(segments) {
        return false;
    }
    let mut spans: Vec<(u64, u64)> = segments.iter().map(|s| (s.start, s.end)).collect();
    spans.sort_unstable();
    let mut cursor = 0u64;
    for (start, end) in spans {
        if start != cursor {
            return false;
        }
        cursor = end;
    }
    cursor == total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_the_largest_unlocked_tail() {
        let mut segments = vec![
            RangePiece { start: 0, end: 100, received: 80, reserved_end: 0 },
            RangePiece { start: 100, end: 400, received: 0, reserved_end: 0 },
        ];
        assert!(split_largest(&mut segments, 50, 8));
        assert!(!ranges_overlap(&segments));
        assert!(covers_without_gaps(&segments, 400));
        assert!(segments.iter().any(|s| s.start == 100));
        assert!(segments.len() == 3);
    }

    #[test]
    fn does_not_divide_a_reserved_window() {
        let mut segments = vec![RangePiece {
            start: 0,
            end: 1_000_000,
            received: 0,
            reserved_end: 250_000,
        }];
        assert!(split_largest(&mut segments, 100_000, 8));
        assert!(!ranges_overlap(&segments));
        assert!(covers_without_gaps(&segments, 1_000_000));
        let new_piece = segments.iter().find(|s| s.start != 0).unwrap();
        assert!(new_piece.start >= 250_000);
        let original = segments.iter().find(|s| s.start == 0).unwrap();
        assert!(original.end <= new_piece.start);
        assert!(original.reserved_end <= original.end);
    }

    #[test]
    fn refuses_tiny_or_full_maps() {
        let mut tiny = vec![RangePiece { start: 0, end: 10, received: 0, reserved_end: 0 }];
        assert!(!split_largest(&mut tiny, 50, 8));
        let mut full = vec![RangePiece { start: 0, end: 500, received: 0, reserved_end: 0 }];
        assert!(!split_largest(&mut full, 10, 1));
    }
}
