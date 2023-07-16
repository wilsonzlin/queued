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
use suspend::SuspendState;
use throttler::Throttler;
use tracing::info;

#[derive(Clone)]
pub struct Queued {
  ctx: Arc<Ctx>,
}

#[derive(Serialize, Deserialize)]
pub struct ThrottleState {
  max_polls_per_time_window: u64,
  time_window_sec: i64,
}

impl Queued {
  pub async fn load_and_start(data_dir: &Path) -> Self {
    let metrics = Arc::new(Metrics::default());

    let db = rocksdb_open(data_dir);
    let data = rocksdb_load(&db, metrics.clone());

    info!(
      message_count = data.messages.len(),
      next_id = data.next_id,
      "queued loaded",
    );

    let ctx = Ctx {
      batch_sync: BatchSync::start(db.clone()),
      db,
      messages: Mutex::new(data.messages),
      metrics,
      next_id: AtomicU64::new(data.next_id),
      suspension: Arc::new(SuspendState::default()),
      throttler: Mutex::new(None),
    };

    Self { ctx: Arc::new(ctx) }
  }

  pub async fn delete(&self, input: OpDeleteInput) -> OpResult<OpDeleteOutput> {
    op_delete(self.ctx.clone(), input).await
  }

  pub async fn poll(&self, input: OpPollInput) -> OpResult<OpPollOutput> {
    op_poll(self.ctx.clone(), input).await
  }

  pub async fn push(&self, input: OpPushInput) -> OpResult<OpPushOutput> {
    op_push(self.ctx.clone(), input).await
  }

  pub async fn update(&self, input: OpUpdateInput) -> OpResult<OpUpdateOutput> {
    op_update(self.ctx.clone(), input).await
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
