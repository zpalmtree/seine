//! Resubmission of shares the pool rejected only because it was overloaded.
//!
//! The pool answers with errors such as `server busy, retry` or
//! `rate limited, retry` when it cannot process a share right now. Those
//! rejects are not persisted pool-side, so the exact same submit is accepted
//! later. This module keeps such shares, resubmits them with jittered
//! exponential backoff, and gives up when the job they belong to goes stale,
//! when the retry budget runs out, or when the connection drops.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use super::{PoolJob, PoolSubmit};
use crate::types::{hash_meets_target, parse_target};

/// Retry budget and backoff curve for one class of share.
#[derive(Debug, Clone, Copy)]
pub(super) struct RetryPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_retries: u32,
}

/// Ordinary shares: 250 ms doubling to 4 s, at most 5 resubmits (~8 s total).
pub(super) const SHARE_RETRY_POLICY: RetryPolicy = RetryPolicy {
    initial_backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(4),
    max_retries: 5,
};

/// Block candidates are worth a whole block reward, so retry sooner and longer.
/// Staleness still bounds them: the pool will not accept a candidate for a
/// template that has been replaced by a new block.
pub(super) const CANDIDATE_RETRY_POLICY: RetryPolicy = RetryPolicy {
    initial_backoff: Duration::from_millis(100),
    max_backoff: Duration::from_secs(2),
    max_retries: 12,
};

/// Upper bound on shares waiting for a resubmit. Kept well below the miner's
/// in-flight submit window so fresh shares always have room.
pub(super) const RETRY_QUEUE_CAPACITY: usize = 8;

/// The pool keeps accepting shares for a superseded assignment/template for a
/// short grace period (bntpool default `stale_submit_grace` = 8 s). Retrying
/// after that only earns a stale reject.
pub(super) const RETRY_STALE_GRACE: Duration = Duration::from_secs(8);

const TRACKED_JOB_CAPACITY: usize = 64;

const KNOWN_RETRYABLE_REJECTS: &[&str] = &[
    "server busy, retry",
    "rate limited, retry",
    "candidate claim busy, retry",
    "candidate claim rate limited, retry",
];

/// Rejects that mean the share itself is bad or already counted. These are
/// never retried even if the text also mentions "retry".
const NON_RETRYABLE_MARKERS: &[&str] = &[
    "duplicate",
    "low difficulty",
    "invalid",
    "stale",
    "out of assigned range",
    "not assigned",
    "quarantined",
];

/// Returns true when a submit reject only reflects transient pool load.
pub(super) fn is_retryable_submit_reject(reason: &str) -> bool {
    let lower = reason.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return false;
    }
    if NON_RETRYABLE_MARKERS
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return false;
    }
    if KNOWN_RETRYABLE_REJECTS
        .iter()
        .any(|known| lower.starts_with(known))
    {
        return true;
    }
    lower
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|word| word == "retry")
}

#[derive(Debug, Clone)]
struct TrackedJob {
    height: u64,
    network_target: Option<[u8; 32]>,
    /// Set when a newer job for the same height replaced this one (vardiff
    /// rebind, fresh range, or a same-height template refresh).
    superseded_at: Option<Instant>,
    /// Set when a job for a higher height arrived: shares for this job can no
    /// longer be accepted.
    dead: bool,
}

#[derive(Debug, Default)]
struct JobTracker {
    jobs: HashMap<String, TrackedJob>,
    order: VecDeque<String>,
}

impl JobTracker {
    fn observe(&mut self, job: &PoolJob, now: Instant) {
        for (job_id, tracked) in self.jobs.iter_mut() {
            if job_id == &job.job_id {
                continue;
            }
            if job.height > tracked.height {
                tracked.dead = true;
            } else if tracked.superseded_at.is_none() {
                tracked.superseded_at = Some(now);
            }
        }
        let network_target = job
            .network_target
            .as_deref()
            .and_then(|value| parse_target(value).ok());
        if let Some(existing) = self.jobs.get_mut(&job.job_id) {
            existing.height = job.height;
            existing.network_target = network_target;
            existing.superseded_at = None;
            existing.dead = false;
            return;
        }
        self.jobs.insert(
            job.job_id.clone(),
            TrackedJob {
                height: job.height,
                network_target,
                superseded_at: None,
                dead: false,
            },
        );
        self.order.push_back(job.job_id.clone());
        while self.order.len() > TRACKED_JOB_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.jobs.remove(&oldest);
            }
        }
    }

    fn staleness(&self, job_id: &str, now: Instant) -> Option<&'static str> {
        let tracked = self.jobs.get(job_id)?;
        if tracked.dead {
            return Some("new block");
        }
        let superseded_at = tracked.superseded_at?;
        if now.saturating_duration_since(superseded_at) >= RETRY_STALE_GRACE {
            return Some("job superseded");
        }
        None
    }

    fn is_block_candidate(&self, submit: &PoolSubmit) -> bool {
        let Some(hash) = submit.claimed_hash.as_ref() else {
            return false;
        };
        self.jobs
            .get(&submit.job_id)
            .and_then(|tracked| tracked.network_target.as_ref())
            .is_some_and(|network_target| hash_meets_target(hash, network_target))
    }
}

#[derive(Debug)]
struct RetryEntry {
    submit: PoolSubmit,
    due_at: Instant,
    block_candidate: bool,
    last_reason: String,
}

/// A share the retry queue gave up on, with the reject text to report.
#[derive(Debug)]
pub(super) struct AbandonedSubmit {
    pub submit: PoolSubmit,
    pub reason: String,
}

#[derive(Debug)]
pub(super) enum ScheduleOutcome {
    Scheduled {
        retry_number: u32,
        max_retries: u32,
        delay: Duration,
        block_candidate: bool,
        evicted: Option<AbandonedSubmit>,
    },
    GaveUp(AbandonedSubmit),
}

#[derive(Debug)]
pub(super) struct SubmitRetryQueue {
    entries: VecDeque<RetryEntry>,
    jobs: JobTracker,
    capacity: usize,
    rng_state: u64,
}

impl SubmitRetryQueue {
    pub fn new() -> Self {
        Self::with_capacity(RETRY_QUEUE_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut seed = [0u8; 8];
        let rng_state = if getrandom::getrandom(&mut seed).is_ok() {
            u64::from_le_bytes(seed)
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15)
        };
        Self {
            entries: VecDeque::new(),
            jobs: JobTracker::default(),
            capacity: capacity.max(1),
            rng_state,
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Records a job notification. Queued shares whose job a new block made
    /// stale are returned so the caller can report them as rejected.
    pub fn observe_job(&mut self, job: &PoolJob, now: Instant) -> Vec<AbandonedSubmit> {
        self.jobs.observe(job, now);
        self.take_stale(now)
    }

    #[cfg(test)]
    pub fn is_block_candidate(&self, submit: &PoolSubmit) -> bool {
        self.jobs.is_block_candidate(submit)
    }

    /// Keeps a share the pool rejected with a retryable reason, or returns it
    /// when it must not be retried again.
    pub fn schedule(&mut self, submit: PoolSubmit, reason: &str, now: Instant) -> ScheduleOutcome {
        let block_candidate = self.jobs.is_block_candidate(&submit);
        let policy = if block_candidate {
            CANDIDATE_RETRY_POLICY
        } else {
            SHARE_RETRY_POLICY
        };
        if let Some(stale) = self.jobs.staleness(&submit.job_id, now) {
            return ScheduleOutcome::GaveUp(AbandonedSubmit {
                submit,
                reason: format!("{reason}; retry abandoned ({stale})"),
            });
        }
        if submit.retries >= policy.max_retries {
            return ScheduleOutcome::GaveUp(AbandonedSubmit {
                submit,
                reason: reason.to_string(),
            });
        }

        let mut evicted = None;
        if self.entries.len() >= self.capacity {
            let victim = self
                .entries
                .iter()
                .position(|entry| !entry.block_candidate)
                .or_else(|| block_candidate.then_some(0));
            match victim {
                Some(index) => {
                    let entry = self
                        .entries
                        .remove(index)
                        .expect("victim index is in range");
                    evicted = Some(AbandonedSubmit {
                        submit: entry.submit,
                        reason: format!(
                            "{}; retry abandoned (retry queue full)",
                            entry.last_reason
                        ),
                    });
                }
                None => {
                    // Queue is full of block candidates; never displace one
                    // for an ordinary share.
                    return ScheduleOutcome::GaveUp(AbandonedSubmit {
                        submit,
                        reason: format!("{reason}; retry abandoned (retry queue full)"),
                    });
                }
            }
        }

        let retry_number = submit.retries.saturating_add(1);
        let delay = self.backoff(policy, retry_number);
        let mut submit = submit;
        // Count the retry as soon as it is committed to, so a share dropped
        // while waiting (disconnect, staleness) still reports that it was
        // retried.
        submit.retries = retry_number;
        self.entries.push_back(RetryEntry {
            submit,
            due_at: now + delay,
            block_candidate,
            last_reason: reason.to_string(),
        });
        ScheduleOutcome::Scheduled {
            retry_number,
            max_retries: policy.max_retries,
            delay,
            block_candidate,
            evicted,
        }
    }

    /// Removes up to `max` shares whose backoff has elapsed, block candidates
    /// first. Shares that went stale while waiting are returned separately.
    pub fn take_due(
        &mut self,
        now: Instant,
        max: usize,
    ) -> (Vec<PoolSubmit>, Vec<AbandonedSubmit>) {
        let abandoned = self.take_stale(now);
        let mut due = Vec::new();
        for candidates_pass in [true, false] {
            let mut index = 0;
            while index < self.entries.len() && due.len() < max {
                let entry = &self.entries[index];
                if entry.block_candidate == candidates_pass && entry.due_at <= now {
                    let entry = self.entries.remove(index).expect("due index is in range");
                    due.push(entry.submit);
                } else {
                    index += 1;
                }
            }
        }
        (due, abandoned)
    }

    /// Empties the queue, e.g. on disconnect; job ids do not survive a new session.
    pub fn drain(&mut self) -> Vec<PoolSubmit> {
        self.jobs = JobTracker::default();
        self.entries.drain(..).map(|entry| entry.submit).collect()
    }

    fn take_stale(&mut self, now: Instant) -> Vec<AbandonedSubmit> {
        let mut abandoned = Vec::new();
        let mut index = 0;
        while index < self.entries.len() {
            let stale = self.jobs.staleness(&self.entries[index].submit.job_id, now);
            if let Some(stale) = stale {
                let entry = self.entries.remove(index).expect("stale index is in range");
                abandoned.push(AbandonedSubmit {
                    submit: entry.submit,
                    reason: format!("{}; retry abandoned ({stale})", entry.last_reason),
                });
            } else {
                index += 1;
            }
        }
        abandoned
    }

    fn backoff(&mut self, policy: RetryPolicy, retry_number: u32) -> Duration {
        let exponent = retry_number.saturating_sub(1).min(16);
        let base = policy
            .initial_backoff
            .saturating_mul(1u32 << exponent)
            .min(policy.max_backoff);
        // +/-25% jitter so many rigs rejected by the same overload burst do not
        // come back in lockstep.
        let base_micros = base.as_micros().min(u64::MAX as u128) as u64;
        let spread = base_micros / 2;
        let offset = if spread == 0 {
            0
        } else {
            self.next_random() % (spread + 1)
        };
        Duration::from_micros(base_micros - base_micros / 4 + offset)
    }

    fn next_random(&mut self) -> u64 {
        // splitmix64
        self.rng_state = self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(job_id: &str, height: u64, network_target: Option<[u8; 32]>) -> PoolJob {
        PoolJob {
            job_id: job_id.to_string(),
            template_id: Some("tpl".to_string()),
            header_base: "00".repeat(92),
            target: "ff".repeat(32),
            difficulty: Some(1),
            network_target: network_target.map(hex::encode),
            height,
            nonce_start: 0,
            nonce_end: 1_000,
            received_at: None,
        }
    }

    fn submit(job_id: &str, nonce: u64, hash: Option<[u8; 32]>) -> PoolSubmit {
        PoolSubmit {
            job_id: job_id.to_string(),
            nonce,
            claimed_hash: hash,
            backend: Some("cpu#1".to_string()),
            retries: 0,
            timing: None,
            worker: None,
        }
    }

    fn scheduled_delay(outcome: ScheduleOutcome) -> Duration {
        match outcome {
            ScheduleOutcome::Scheduled { delay, .. } => delay,
            ScheduleOutcome::GaveUp(abandoned) => {
                panic!("expected scheduled retry, gave up: {}", abandoned.reason)
            }
        }
    }

    #[test]
    fn classifies_retryable_rejects_conservatively() {
        for reason in [
            "server busy, retry",
            "rate limited, retry",
            "candidate claim busy, retry",
            "candidate claim rate limited, retry",
            "Server Busy, Retry",
            "validation queue full; retry",
        ] {
            assert!(is_retryable_submit_reject(reason), "{reason}");
        }
        for reason in [
            "duplicate share",
            "low difficulty share",
            "invalid share proof",
            "stale job: assignment not found",
            "stale job: retry with new job",
            "duplicate share, retry",
            "validation timeout",
            "pool disconnected",
            "submit interrupted during reconnect",
            "retrying",
            "",
        ] {
            assert!(!is_retryable_submit_reject(reason), "{reason}");
        }
    }

    #[test]
    fn backoff_doubles_with_jitter_and_caps() {
        let mut queue = SubmitRetryQueue::with_capacity(64);
        let now = Instant::now();
        let mut entry = submit("job-1", 1, None);
        let expected_base_ms = [250u64, 500, 1_000, 2_000, 4_000];
        for (attempt, base_ms) in expected_base_ms.iter().enumerate() {
            entry.retries = attempt as u32;
            let delay = scheduled_delay(queue.schedule(entry.clone(), "server busy, retry", now));
            let millis = delay.as_millis() as u64;
            assert!(
                millis >= base_ms * 3 / 4 && millis <= base_ms * 5 / 4,
                "retry {} delay {millis}ms outside jitter window for {base_ms}ms",
                attempt + 1
            );
        }
        entry.retries = SHARE_RETRY_POLICY.max_retries;
        assert!(matches!(
            queue.schedule(entry, "server busy, retry", now),
            ScheduleOutcome::GaveUp(_)
        ));
    }

    #[test]
    fn due_shares_are_released_after_backoff_with_counter_advanced() {
        let mut queue = SubmitRetryQueue::new();
        let now = Instant::now();
        queue.observe_job(&job("job-1", 10, None), now);
        let delay =
            scheduled_delay(queue.schedule(submit("job-1", 7, None), "server busy, retry", now));

        let (due, abandoned) = queue.take_due(now, 8);
        assert!(due.is_empty());
        assert!(abandoned.is_empty());

        let (due, abandoned) = queue.take_due(now + delay, 8);
        assert!(abandoned.is_empty());
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].nonce, 7);
        assert_eq!(due[0].retries, 1);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn block_candidates_retry_longer_and_go_first() {
        let mut queue = SubmitRetryQueue::new();
        let now = Instant::now();
        let mut network_target = [0u8; 32];
        network_target[0] = 0x10;
        queue.observe_job(&job("job-1", 10, Some(network_target)), now);

        let share_hash = [0xFFu8; 32];
        let candidate_hash = [0x01u8; 32];
        let share = submit("job-1", 1, Some(share_hash));
        let candidate = submit("job-1", 2, Some(candidate_hash));
        assert!(!queue.is_block_candidate(&share));
        assert!(queue.is_block_candidate(&candidate));

        let mut exhausted_share = share.clone();
        exhausted_share.retries = SHARE_RETRY_POLICY.max_retries;
        assert!(matches!(
            queue.schedule(exhausted_share, "server busy, retry", now),
            ScheduleOutcome::GaveUp(_)
        ));
        let mut long_candidate = candidate.clone();
        long_candidate.retries = SHARE_RETRY_POLICY.max_retries;
        match queue.schedule(long_candidate, "candidate claim busy, retry", now) {
            ScheduleOutcome::Scheduled {
                block_candidate,
                max_retries,
                ..
            } => {
                assert!(block_candidate);
                assert_eq!(max_retries, CANDIDATE_RETRY_POLICY.max_retries);
            }
            ScheduleOutcome::GaveUp(abandoned) => panic!("candidate gave up: {}", abandoned.reason),
        }
        scheduled_delay(queue.schedule(share, "server busy, retry", now));

        let later = now + Duration::from_secs(10);
        let (due, _) = queue.take_due(later, 1);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].nonce, 2, "block candidate must be resubmitted first");
    }

    #[test]
    fn full_queue_drops_oldest_share_but_keeps_candidates() {
        let mut queue = SubmitRetryQueue::with_capacity(2);
        let now = Instant::now();
        let mut network_target = [0u8; 32];
        network_target[0] = 0x10;
        queue.observe_job(&job("job-1", 10, Some(network_target)), now);
        let share_hash = Some([0xFFu8; 32]);
        let candidate_hash = Some([0x01u8; 32]);

        scheduled_delay(queue.schedule(submit("job-1", 1, share_hash), "server busy, retry", now));
        scheduled_delay(queue.schedule(submit("job-1", 2, share_hash), "server busy, retry", now));
        match queue.schedule(submit("job-1", 3, share_hash), "server busy, retry", now) {
            ScheduleOutcome::Scheduled { evicted, .. } => {
                let evicted = evicted.expect("oldest share should be evicted");
                assert_eq!(evicted.submit.nonce, 1);
                assert!(evicted.reason.contains("retry queue full"));
            }
            ScheduleOutcome::GaveUp(abandoned) => panic!("gave up: {}", abandoned.reason),
        }

        // Candidates displace ordinary shares, and ordinary shares never
        // displace candidates.
        scheduled_delay(queue.schedule(
            submit("job-1", 4, candidate_hash),
            "candidate claim busy, retry",
            now,
        ));
        scheduled_delay(queue.schedule(
            submit("job-1", 5, candidate_hash),
            "candidate claim busy, retry",
            now,
        ));
        assert!(matches!(
            queue.schedule(submit("job-1", 6, share_hash), "server busy, retry", now),
            ScheduleOutcome::GaveUp(_)
        ));
        let (due, _) = queue.take_due(now + Duration::from_secs(10), 8);
        let mut nonces = due.iter().map(|submit| submit.nonce).collect::<Vec<_>>();
        nonces.sort_unstable();
        assert_eq!(nonces, vec![4, 5]);
    }

    #[test]
    fn new_block_abandons_queued_retries_immediately() {
        let mut queue = SubmitRetryQueue::new();
        let now = Instant::now();
        queue.observe_job(&job("job-1", 10, None), now);
        scheduled_delay(queue.schedule(submit("job-1", 1, None), "server busy, retry", now));

        let abandoned = queue.observe_job(&job("job-2", 11, None), now);
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].submit.nonce, 1);
        assert!(abandoned[0].reason.contains("new block"));
        assert_eq!(queue.len(), 0);

        // A late busy ack for the dead job is not retried either.
        assert!(matches!(
            queue.schedule(submit("job-1", 2, None), "server busy, retry", now),
            ScheduleOutcome::GaveUp(_)
        ));
    }

    #[test]
    fn superseded_job_retries_only_within_stale_grace() {
        let mut queue = SubmitRetryQueue::new();
        let now = Instant::now();
        queue.observe_job(&job("job-1", 10, None), now);
        scheduled_delay(queue.schedule(submit("job-1", 1, None), "server busy, retry", now));

        // Same-height rebind (vardiff) supersedes job-1 but the pool still
        // accepts it during the stale grace.
        assert!(queue.observe_job(&job("job-2", 10, None), now).is_empty());
        let (due, abandoned) = queue.take_due(now + Duration::from_secs(1), 8);
        assert!(abandoned.is_empty());
        assert_eq!(due.len(), 1);

        let again = due[0].clone();
        assert_eq!(again.retries, 1);
        scheduled_delay(queue.schedule(again, "server busy, retry", now + Duration::from_secs(1)));
        let (due, abandoned) = queue.take_due(now + RETRY_STALE_GRACE, 8);
        assert!(due.is_empty());
        assert_eq!(abandoned.len(), 1);
        assert!(abandoned[0].reason.contains("job superseded"));
    }

    #[test]
    fn drain_returns_everything_for_disconnect_rejects() {
        let mut queue = SubmitRetryQueue::new();
        let now = Instant::now();
        scheduled_delay(queue.schedule(submit("job-1", 1, None), "server busy, retry", now));
        scheduled_delay(queue.schedule(submit("job-1", 2, None), "rate limited, retry", now));
        let drained = queue.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(queue.len(), 0);
    }
}
