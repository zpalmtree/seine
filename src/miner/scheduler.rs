#[derive(Debug, Clone, Copy)]
pub struct NonceReservation {
    pub start_nonce: u64,
    pub max_iters_per_lane: u64,
}

#[derive(Debug, Clone)]
pub struct NonceScheduler {
    next_start_nonce: u64,
    max_iters_per_lane: u64,
}

impl NonceScheduler {
    pub fn new(start_nonce: u64, max_iters_per_lane: u64) -> Self {
        Self {
            next_start_nonce: start_nonce,
            max_iters_per_lane: max_iters_per_lane.max(1),
        }
    }

    pub fn reserve(&mut self, total_lanes: u64) -> NonceReservation {
        let lanes = total_lanes.max(1);
        let reservation = NonceReservation {
            start_nonce: self.next_start_nonce,
            max_iters_per_lane: self.max_iters_per_lane,
        };
        let span = lanes.saturating_mul(self.max_iters_per_lane);
        self.next_start_nonce = self.next_start_nonce.wrapping_add(span.max(lanes));
        reservation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_is_deterministic_and_non_overlapping() {
        let mut scheduler = NonceScheduler::new(100, 10);
        let first = scheduler.reserve(4);
        let second = scheduler.reserve(4);
        assert_eq!(first.start_nonce, 100);
        assert_eq!(first.max_iters_per_lane, 10);
        assert_eq!(second.start_nonce, 140);
    }

    #[test]
    fn reservation_stays_non_overlapping_when_lane_count_changes() {
        let mut scheduler = NonceScheduler::new(50, 10);
        let first = scheduler.reserve(4);
        let second = scheduler.reserve(1);
        let third = scheduler.reserve(8);

        assert_eq!(first.start_nonce, 50);
        assert_eq!(second.start_nonce, 90);
        assert_eq!(third.start_nonce, 100);
    }
}
