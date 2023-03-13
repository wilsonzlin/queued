use crate::invisible::InvisibleMessages;
use crate::metrics::Metrics;
use chrono::Utc;
use std::collections::LinkedList;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;

pub(crate) struct VisibleMessages {
  metrics: Arc<Metrics>,
  messages: Mutex<LinkedList<u64>>,
}

impl VisibleMessages {
  pub fn new_with_list(messages: LinkedList<u64>, metrics: Arc<Metrics>) -> Self {
    Self {
      metrics,
      messages: Mutex::new(messages),
    }
  }

  pub async fn len(&self) -> usize {
    self.messages.lock().await.len()
  }

  pub async fn start_invisible_consumption_background_loop(
    &self,
    invisible: Arc<Mutex<InvisibleMessages>>,
  ) {
    loop {
      sleep(std::time::Duration::from_millis(1000)).await;

      let now = Utc::now();
      let mut invisible = invisible.lock().await;
      let mut messages = self.messages.lock().await;
      while let Some(id) = invisible.remove_earliest_up_to(&now) {
        messages.push_back(id);
      }
      self
        .metrics
        .visible_gauge
        .store(messages.len().try_into().unwrap(), Ordering::Relaxed);
    }
  }

  pub async fn push_all(&self, ids: Vec<u64>) {
    let mut messages = self.messages.lock().await;
    for id in ids {
      messages.push_back(id);
    }
  }

  pub async fn pop_next(&self) -> Option<u64> {
    // Let background loop update metrics to increase performance.
    self.messages.lock().await.pop_front()
  }
}
