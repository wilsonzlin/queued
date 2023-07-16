use crate::batch_sync::BatchSync;
use crate::messages::Messages;
use crate::metrics::Metrics;
use crate::suspend::SuspendState;
use crate::throttler::Throttler;
use parking_lot::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub(crate) struct Ctx {
  pub batch_sync: BatchSync,
  pub db: Arc<rocksdb::DB>,
  pub messages: Mutex<Messages>,
  pub metrics: Arc<Metrics>,
  pub next_id: AtomicU64,
  pub suspension: Arc<SuspendState>,
  pub throttler: Mutex<Option<Throttler>>,
}
