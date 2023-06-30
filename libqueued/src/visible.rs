use crate::invisible::InvisibleMessages;
use crate::metrics::Metrics;
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::sleep;

pub(crate) struct VisibleMessages {
  metrics: Arc<Metrics>,
  messages: Mutex<VecDeque<u64>>,
}

impl VisibleMessages {
  pub fn new_with_list(messages: VecDeque<u64>, metrics: Arc<Metrics>) -> Self {
    Self {
      metrics,
      messages: Mutex::new(messages),
    }
  }

  pub fn len(&self) -> usize {
    self.messages.lock().len()
  }

  pub async fn start_invisible_consumption_background_loop(
    &self,
    invisible: Arc<Mutex<InvisibleMessages>>,
  ) {
    loop {
      sleep(std::time::Duration::from_millis(1000)).await;

      let now = Utc::now();
      let mut invisible = invisible.lock();
      let mut messages = self.messages.lock();
      while let Some(id) = invisible.remove_earliest_up_to(&now) {
        messages.push_back(id);
      }
      self
        .metrics
        .visible_gauge
        .store(messages.len().try_into().unwrap(), Ordering::Relaxed);
    }
  }

  pub fn push_all(&self, ids: impl IntoIterator<Item = u64>) {
    self.messages.lock().extend(ids);
  }

  pub fn pop_next(&self) -> Option<u64> {
    // Let background loop update metrics to increase performance.
    self.messages.lock().pop_front()
  }
}
