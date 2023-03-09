use crate::metrics::Metrics;
use chrono::DateTime;
use chrono::Utc;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct AvailableMessages {
  metrics: Arc<Metrics>,
  // Since we don't expect there to be many entries in each DateTime<Utc> entry, a HashSet is more optimised than a RoaringBitmap.
  ordered_by_visible_time: BTreeMap<DateTime<Utc>, HashSet<u32>>,
  by_index: HashMap<u32, DateTime<Utc>>,
}

impl AvailableMessages {
  pub fn new(metrics: Arc<Metrics>) -> Self {
    AvailableMessages {
      metrics,
      by_index: HashMap::new(),
      ordered_by_visible_time: BTreeMap::new(),
    }
  }

  pub fn len(&self) -> u64 {
    self.by_index.len().try_into().unwrap()
  }

  pub fn insert(&mut self, index: u32, ts: DateTime<Utc>) {
    if !self
      .ordered_by_visible_time
      .entry(ts)
      .or_default()
      .insert(index)
    {
      panic!("slot already exists");
    }
    let None = self.by_index.insert(index, ts) else {
      panic!("slot already exists");
    };
    self.metrics.available_gauge.fetch_add(1, Ordering::Relaxed);
  }

  pub fn has(&self, index: u32) -> bool {
    self.by_index.contains_key(&index)
  }

  pub fn remove(&mut self, index: u32) -> Option<()> {
    let ts = self.by_index.remove(&index)?;
    let set = self.ordered_by_visible_time.get_mut(&ts).unwrap();
    if !set.remove(&index) {
      panic!("slot does not exist");
    };
    if set.is_empty() {
      self.ordered_by_visible_time.remove(&ts).unwrap();
    }
    self.metrics.available_gauge.fetch_sub(1, Ordering::Relaxed);
    Some(())
  }

  pub fn update_timestamp(&mut self, index: u32, new_ts: DateTime<Utc>) {
    let old_ts = self.by_index.insert(index, new_ts).unwrap();
    let old_ts_set = self.ordered_by_visible_time.get_mut(&old_ts).unwrap();
    if !old_ts_set.remove(&index) {
      panic!("slot does not exist");
    };
    if old_ts_set.is_empty() {
      self.ordered_by_visible_time.remove(&old_ts).unwrap();
    };
    if !self
      .ordered_by_visible_time
      .entry(new_ts)
      .or_default()
      .insert(index)
    {
      panic!("slot already exists");
    };
  }

  pub fn get_earliest(&self) -> Option<(&DateTime<Utc>, &HashSet<u32>)> {
    self.ordered_by_visible_time.first_key_value()
  }

  pub fn remove_earliest_up_to(&mut self, up_to_ts: &DateTime<Utc>) -> Option<u32> {
    let indices = self
      .ordered_by_visible_time
      .first_entry()
      .filter(|e| e.key() <= up_to_ts)?;
    let index = *indices.get().iter().next().unwrap();
    self.remove(index).unwrap();
    Some(index)
  }
}
