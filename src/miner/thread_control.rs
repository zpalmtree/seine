use crossbeam_channel::Sender;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct ThreadChangeRequest {
    pub run_id: u64,
    pub threads: usize,
    /// Current runtime RAM/worker bound, which may differ from the supervisor's
    /// original configuration after automatic recovery or startup fallback.
    pub max_threads: usize,
    pub quit_requested: Arc<AtomicBool>,
}

#[derive(Clone)]
pub(crate) struct ThreadControl {
    pub run_id: u64,
    pub sender: Sender<ThreadChangeRequest>,
    pub max_threads: usize,
    pub current_threads: usize,
    pub quit_requested: Arc<AtomicBool>,
}
