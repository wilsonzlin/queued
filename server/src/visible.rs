use crate::invisible::InvisibleMessages;
use crate::metrics::Metrics;
use chrono::Utc;
use std::collections::LinkedList;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;

pub struct VisibleMessages {
  metrics: Arc<Metrics>,
  messages: Mutex<LinkedList<u32>>,
}

impl VisibleMessages {
  pub fn new(metrics: Arc<Metrics>) -> Self {
    Self {
      metrics,
      messages: Mutex::new(LinkedList::new()),
    }
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
      while let Some(index) = invisible.remove_earliest_up_to(&now) {
        messages.push_back(index);
      }
      self
        .metrics
        .visible_gauge
        .store(messages.len().try_into().unwrap(), Ordering::Relaxed);
    }
  }

  pub async fn pop_next(&self) -> Option<u32> {
    // Let background loop update metrics to increase performance.
    self.messages.lock().await.pop_front()
  }
}
