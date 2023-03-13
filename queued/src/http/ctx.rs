use libqueued::Queued;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::sync::Arc;

pub struct HttpCtx {
  pub io_metrics: Arc<SeekableAsyncFileMetrics>,
  pub queued: Queued,
}
