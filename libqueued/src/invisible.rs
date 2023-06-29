use crate::metrics::Metrics;
use chrono::DateTime;
use chrono::Utc;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub(crate) struct InvisibleMessages {
  metrics: Arc<Metrics>,
  // We use a map instead of a heap as we want to be able to remove/mutate individual specific entries.
  ordered_by_visible_time: BTreeMap<DateTime<Utc>, HashSet<u64>>,
  by_id: HashMap<u64, DateTime<Utc>>,
}

impl InvisibleMessages {
  pub fn new(metrics: Arc<Metrics>) -> Self {
    InvisibleMessages {
      metrics,
      by_id: HashMap::new(),
      ordered_by_visible_time: BTreeMap::new(),
    }
  }

  pub fn insert(&mut self, id: u64, ts: DateTime<Utc>) {
    if !self
      .ordered_by_visible_time
      .entry(ts)
      .or_default()
      .insert(id)
    {
      panic!("ID already exists");
    }
    let None = self.by_id.insert(id, ts) else {
      panic!("ID already exists");
    };
    self.metrics.invisible_gauge.fetch_add(1, Ordering::Relaxed);
  }

  pub fn has(&self, id: u64) -> bool {
    self.by_id.contains_key(&id)
  }

  pub fn remove(&mut self, id: u64) -> Option<()> {
    let ts = self.by_id.remove(&id)?;
    let set = self.ordered_by_visible_time.get_mut(&ts).unwrap();
    if !set.remove(&id) {
      panic!("ID does not exist");
    };
    if set.is_empty() {
      self.ordered_by_visible_time.remove(&ts).unwrap();
    }
    self.metrics.invisible_gauge.fetch_sub(1, Ordering::Relaxed);
    Some(())
  }

  pub fn update_timestamp(&mut self, id: u64, new_ts: DateTime<Utc>) {
    let old_ts = self.by_id.insert(id, new_ts).unwrap();
    let old_ts_set = self.ordered_by_visible_time.get_mut(&old_ts).unwrap();
    if !old_ts_set.remove(&id) {
      panic!("ID does not exist");
    };
    if old_ts_set.is_empty() {
      self.ordered_by_visible_time.remove(&old_ts).unwrap();
    };
    if !self
      .ordered_by_visible_time
      .entry(new_ts)
      .or_default()
      .insert(id)
    {
      panic!("ID already exists");
    };
  }

  pub fn get_earliest(&self) -> Option<(&DateTime<Utc>, &HashSet<u64>)> {
    self.ordered_by_visible_time.first_key_value()
  }

  pub fn remove_earliest_up_to(&mut self, up_to_ts: &DateTime<Utc>) -> Option<u64> {
    let indices = self
      .ordered_by_visible_time
      .first_entry()
      .filter(|e| e.key() <= up_to_ts)?;
    let id = *indices.get().iter().next().unwrap();
    self.remove(id).unwrap();
    Some(id)
  }
}
