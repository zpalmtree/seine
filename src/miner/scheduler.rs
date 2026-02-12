#[derive(Debug, Clone, Copy)]
pub struct NonceReservation {
    pub start_nonce: u64,
    pub max_iters_per_lane: u64,
    pub reserved_span: u64,
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
        let reserved_span = lanes.saturating_mul(self.max_iters_per_lane).max(lanes);
        let reservation = NonceReservation {
            start_nonce: self.next_start_nonce,
            max_iters_per_lane: self.max_iters_per_lane,
            reserved_span,
        };
        self.next_start_nonce = self.next_start_nonce.wrapping_add(reserved_span);
        reservation
    }

    pub fn consume_additional_span(&mut self, span: u64) {
        if span == 0 {
            return;
        }
        self.next_start_nonce = self.next_start_nonce.wrapping_add(span);
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
        assert_eq!(first.reserved_span, 40);
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

    #[test]
    fn additional_span_consumption_skips_retry_windows() {
        let mut scheduler = NonceScheduler::new(1_000, 10);
        let first = scheduler.reserve(4);
        scheduler.consume_additional_span(40);
        let second = scheduler.reserve(4);

        assert_eq!(first.start_nonce, 1_000);
        assert_eq!(second.start_nonce, 1_080);
    }
}
