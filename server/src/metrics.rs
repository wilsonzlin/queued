use seekable_async_file::SeekableAsyncFileMetrics;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(Default)]
pub struct Metrics {
  pub io: Arc<SeekableAsyncFileMetrics>,
  pub available_gauge: AtomicU64,
  pub empty_poll_counter: AtomicU64,
  pub missing_delete_counter: AtomicU64,
  pub successful_delete_counter: AtomicU64,
  pub successful_poll_counter: AtomicU64,
  pub successful_push_counter: AtomicU64,
  pub suspended_delete_counter: AtomicU64,
  pub suspended_poll_counter: AtomicU64,
  pub suspended_push_counter: AtomicU64,
  pub vacant_gauge: AtomicU64,
}
