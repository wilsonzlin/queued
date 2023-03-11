use crate::available::AvailableMessages;
use crate::layout::StorageLayout;
use crate::metrics::Metrics;
use crate::vacant::VacantSlots;
use seekable_async_file::SeekableAsyncFile;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Ctx {
  pub available: Mutex<AvailableMessages>,
  pub device: SeekableAsyncFile,
  pub layout: Arc<dyn StorageLayout + Send + Sync>,
  pub metrics: Arc<Metrics>,
  pub suspend_delete: AtomicBool,
  pub suspend_update: AtomicBool,
  pub suspend_poll: AtomicBool,
  pub suspend_push: AtomicBool,
  pub vacant: Mutex<VacantSlots>,
}
