pub mod ctx;
pub mod id_gen;
pub mod invisible;
pub mod journal;
pub mod layout;
pub mod metrics;
pub mod op;
pub mod suspend;
pub mod throttler;
pub mod util;
pub mod vacant;
pub mod visible;

use crate::layout::LoadedData;
use ctx::Ctx;
use id_gen::IdGenerator;
use journal::Journal;
use layout::fixed_slots::FixedSlotsLayout;
use layout::log_structured::LogStructuredLayout;
use layout::StorageLayout;
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
use seekable_async_file::SeekableAsyncFile;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use suspend::SuspendState;
use throttler::Throttler;
use tokio::join;
use tokio::sync::Mutex;

const OFFSETOF_JOURNAL: u64 = 0;
const JOURNAL_CAPACITY: u64 = 1024 * 1024;
const OFFSETOF_ID_GEN: u64 = OFFSETOF_JOURNAL + JOURNAL_CAPACITY;
const OFFSETOF_DATA: u64 = OFFSETOF_ID_GEN + 8;

pub struct QueuedLoader {
  device: SeekableAsyncFile,
  id_gen: Arc<IdGenerator>,
  journal: Arc<Journal>,
  layout: Arc<dyn StorageLayout + Send + Sync>,
  metrics: Arc<Metrics>,
}

#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug)]
pub enum QueuedLayoutType {
  FixedSlots,
  LogStructured,
}

impl QueuedLoader {
  pub fn new(device: SeekableAsyncFile, device_size: u64, layout_type: QueuedLayoutType) -> Self {
    let metrics = Arc::new(Metrics::default());

    let journal = Arc::new(Journal::new(
      device.clone(),
      OFFSETOF_JOURNAL,
      JOURNAL_CAPACITY,
    ));
    let id_gen = Arc::new(IdGenerator::new(
      device.clone(),
      journal.clone(),
      OFFSETOF_ID_GEN,
    ));

    let layout: Arc<dyn StorageLayout + Send + Sync> = match layout_type {
      QueuedLayoutType::FixedSlots => Arc::new(FixedSlotsLayout::new(
        device.clone(),
        OFFSETOF_DATA,
        device_size,
        metrics.clone(),
      )),
      QueuedLayoutType::LogStructured => Arc::new(LogStructuredLayout::new(
        device.clone(),
        OFFSETOF_DATA,
        device_size,
        journal.clone(),
      )),
    };

    Self {
      device,
      id_gen,
      journal,
      layout,
      metrics,
    }
  }

  pub async fn format(&self) {
    self.journal.format_device().await;
    self.id_gen.format_device().await;
    self.layout.format_device().await;
    self.device.sync_data().await;
  }

  pub async fn load(&self) -> Queued {
    self.journal.recover().await;

    // Ensure journal has been recovered first before loading any other data, including ID generator state.
    self.id_gen.load_from_device().await;

    let LoadedData { invisible, visible } = self
      .layout
      .load_data_from_device(self.metrics.clone())
      .await;

    let invisible = Arc::new(Mutex::new(invisible));
    let visible = Arc::new(visible);

    let ctx = Arc::new(Ctx {
      id_gen: self.id_gen.clone(),
      invisible: invisible.clone(),
      layout: self.layout.clone(),
      metrics: self.metrics.clone(),
      suspension: Arc::new(SuspendState::default()),
      throttler: Mutex::new(None),
      visible: visible.clone(),
    });

    Queued {
      ctx,
      journal: self.journal.clone(),
    }
  }
}

#[derive(Clone)]
pub struct Queued {
  journal: Arc<Journal>,
  ctx: Arc<Ctx>,
}

#[derive(Serialize, Deserialize)]
pub struct ThrottleState {
  max_polls_per_time_window: u64,
  time_window_sec: i64,
}

impl Queued {
  // WARNING: `device.start_delayed_data_sync_background_loop()` must also be running. Since `device` was provided, it's left up to the provider to run it.
  pub async fn start(&self) {
    join! {
      self.ctx.id_gen.start_background_commit_loop(),
      self.ctx.layout.start_background_loops(),
      self.ctx.visible.start_invisible_consumption_background_loop(self.ctx.invisible.clone()),
      self.journal.start_commit_background_loop(),
    };
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

  pub fn metrics(&self) -> Arc<Metrics> {
    self.ctx.metrics.clone()
  }

  pub fn suspension(&self) -> Arc<SuspendState> {
    self.ctx.suspension.clone()
  }

  pub async fn get_throttle_state(&self) -> Option<ThrottleState> {
    let throttler = self.ctx.throttler.lock().await;
    throttler.as_ref().map(|t| ThrottleState {
      max_polls_per_time_window: t.get_max_reqs_per_time_window(),
      time_window_sec: t.get_time_window_sec(),
    })
  }

  pub async fn set_throttle(&self, t: Option<ThrottleState>) {
    *self.ctx.throttler.lock().await =
      t.map(|t| Throttler::new(t.max_polls_per_time_window, t.time_window_sec));
  }
}
