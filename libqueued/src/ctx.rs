use crate::id_gen::IdGenerator;
use crate::invisible::InvisibleMessages;
use crate::layout::StorageLayout;
use crate::metrics::Metrics;
use crate::suspend::SuspendState;
use crate::throttler::Throttler;
use crate::visible::VisibleMessages;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) struct Ctx {
  pub(crate) id_gen: Arc<IdGenerator>,
  pub(crate) invisible: Arc<Mutex<InvisibleMessages>>,
  pub(crate) layout: Arc<dyn StorageLayout + Send + Sync>,
  pub(crate) metrics: Arc<Metrics>,
  pub(crate) suspension: Arc<SuspendState>,
  pub(crate) throttler: Mutex<Option<Throttler>>,
  pub(crate) visible: Arc<VisibleMessages>,
}
