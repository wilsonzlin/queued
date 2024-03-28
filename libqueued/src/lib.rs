pub mod batch_sync;
pub mod ctx;
pub mod db;
pub mod messages;
pub mod metrics;
pub mod op;
pub mod suspend;
pub mod throttler;

use crate::batch_sync::BatchSync;
use ctx::Ctx;
use db::rocksdb_load;
use db::rocksdb_open;
use metrics::Metrics;
use op::delete::op_delete;
use op::delete::OpDeleteInput;
use op::delete::OpDeleteOutput;
use op::poll::op_poll;
use op::poll::OpPollInput;
use op::poll::OpPollOutput;
use op::push::op_push;
use op::push::OpPushInput;
use op::push::OpPushOutput;
use op::result::OpResult;
use op::update::op_update;
use op::update::OpUpdateInput;
use op::update::OpUpdateOutput;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use suspend::SuspendState;
use throttler::Throttler;

#[derive(Clone)]
pub struct QueuedCfg {
  pub batch_sync_delay: Duration,
}

// This is intentionally not cheaply cloneable to make it clear and explicit that dropping this will safely close the database and free all resources.
pub struct Queued {
  ctx: Ctx,
}

#[derive(Serialize, Deserialize)]
pub struct ThrottleState {
  max_polls_per_time_window: u64,
  time_window_sec: i64,
}

impl Queued {
  pub async fn load_and_start(data_dir: &Path, cfg: QueuedCfg) -> Self {
    let metrics = Arc::new(Metrics::default());

    let db = rocksdb_open(data_dir);
    let data = rocksdb_load(&db, metrics.clone());

    let ctx = Ctx {
      // We can safely create a strong reference clone to the database, as BatchSync's background thread will stop once the channel sender is dropped, which will then drop the DB.
      batch_sync: BatchSync::start(cfg.batch_sync_delay, db.clone(), data.next_id),
      db,
      messages: Mutex::new(data.messages),
      metrics,
      next_id: AtomicU64::new(data.next_id),
      suspension: Arc::new(SuspendState::default()),
      throttler: Mutex::new(None),
    };

    Self { ctx }
  }

  pub async fn delete(&self, input: OpDeleteInput) -> OpResult<OpDeleteOutput> {
    op_delete(&self.ctx, input).await
  }

  pub async fn poll(&self, input: OpPollInput) -> OpResult<OpPollOutput> {
    op_poll(&self.ctx, input).await
  }

  pub async fn push(&self, input: OpPushInput) -> OpResult<OpPushOutput> {
    op_push(&self.ctx, input).await
  }

  pub async fn update(&self, input: OpUpdateInput) -> OpResult<OpUpdateOutput> {
    op_update(&self.ctx, input).await
  }

  pub fn youngest_message_time(&self) -> Option<i64> {
    self.ctx.messages.lock().youngest_time()
  }

  pub fn oldest_message_time(&self) -> Option<i64> {
    self.ctx.messages.lock().oldest_time()
  }

  pub fn metrics(&self) -> &Arc<Metrics> {
    &self.ctx.metrics
  }

  pub fn suspension(&self) -> Arc<SuspendState> {
    self.ctx.suspension.clone()
  }

  pub fn get_throttle_state(&self) -> Option<ThrottleState> {
    let throttler = self.ctx.throttler.lock();
    throttler.as_ref().map(|t| ThrottleState {
      max_polls_per_time_window: t.get_max_reqs_per_time_window(),
      time_window_sec: t.get_time_window_sec(),
    })
  }

  pub fn set_throttle(&self, t: Option<ThrottleState>) {
    *self.ctx.throttler.lock() =
      t.map(|t| Throttler::new(t.max_polls_per_time_window, t.time_window_sec));
  }
}
