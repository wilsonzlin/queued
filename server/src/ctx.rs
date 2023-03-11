use crate::id_gen::IdGenerator;
use crate::invisible::InvisibleMessages;
use crate::layout::StorageLayout;
use crate::metrics::Metrics;
use crate::throttler::Throttler;
use crate::visible::VisibleMessages;
use seekable_async_file::SeekableAsyncFile;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Ctx {
  pub device: SeekableAsyncFile,
  pub id_gen: IdGenerator,
  pub invisible: Arc<Mutex<InvisibleMessages>>,
  pub layout: Arc<dyn StorageLayout + Send + Sync>,
  pub metrics: Arc<Metrics>,
  pub suspend_delete: AtomicBool,
  pub suspend_poll: AtomicBool,
  pub suspend_push: AtomicBool,
  pub suspend_update: AtomicBool,
  pub throttler: Mutex<Option<Throttler>>,
  pub visible: Arc<VisibleMessages>,
}
