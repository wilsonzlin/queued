use crate::metrics::Metrics;
use chrono::Utc;
use itertools::Itertools;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;

type TimestampSec = i64;

pub(crate) struct Messages {
  metrics: Arc<Metrics>,
  // We use a map instead of a heap as we want to be able to remove/mutate individual specific entries.
  ordered_by_visible_time: BTreeMap<TimestampSec, HashSet<u64>>,
  by_id: HashMap<u64, (TimestampSec, u32)>,
}

impl Messages {
  pub fn new(metrics: Arc<Metrics>) -> Self {
    Messages {
      metrics,
      by_id: HashMap::new(),
      ordered_by_visible_time: BTreeMap::new(),
    }
  }

  #[allow(unused)]
  pub fn len(&self) -> usize {
    self.by_id.len()
  }

  pub fn youngest_time(&self) -> Option<TimestampSec> {
    self
      .ordered_by_visible_time
      .first_key_value()
      .map(|(k, _v)| *k)
  }

  pub fn oldest_time(&self) -> Option<TimestampSec> {
    self
      .ordered_by_visible_time
      .last_key_value()
      .map(|(k, _v)| *k)
  }

  pub fn insert(&mut self, id: u64, ts: TimestampSec, poll_tag: u32) {
    if !self
      .ordered_by_visible_time
      .entry(ts)
      .or_default()
      .insert(id)
    {
      panic!("ID already exists");
    }
    let None = self.by_id.insert(id, (ts, poll_tag)) else {
      panic!("ID already exists");
    };
    self.metrics.message_counter.fetch_add(1, Ordering::Relaxed);
  }

  fn remove_if<F: Fn((TimestampSec, u32)) -> bool>(
    &mut self,
    id: u64,
    pred: F,
  ) -> Option<(TimestampSec, u32)> {
    let (ts, poll_tag) = match self.by_id.entry(id) {
      Entry::Occupied(e) if pred(*e.get()) => e.remove(),
      _ => return None,
    };
    let set = self.ordered_by_visible_time.get_mut(&ts).unwrap();
    if !set.remove(&id) {
      panic!("ID does not exist");
    };
    if set.is_empty() {
      self.ordered_by_visible_time.remove(&ts).unwrap();
    }
    self.metrics.message_counter.fetch_sub(1, Ordering::Relaxed);
    Some((ts, poll_tag))
  }

  pub fn remove_if_poll_tag_matches(&mut self, id: u64, expected_poll_tag: u32) -> bool {
    self
      .remove_if(id, |(_ts, poll_tag)| poll_tag == expected_poll_tag)
      .is_some()
  }

  pub fn remove_earliest_n(
    &mut self,
    n: usize,
    ignore_existing_visibility_timeouts: bool,
  ) -> Vec<(u64, u32)> {
    let now = Utc::now().timestamp();
    let mut removed_ids = Vec::new();
    while removed_ids.len() < n {
      let Some(mut ids) = self
        .ordered_by_visible_time
        .first_entry()
        .filter(|e| ignore_existing_visibility_timeouts || *e.key() <= now)
      else {
        break;
      };
      for id in ids
        .get()
        .iter()
        .cloned()
        .take(n - removed_ids.len())
        .collect_vec()
      {
        removed_ids.push(id);
        assert!(ids.get_mut().remove(&id));
      }
      if ids.get().is_empty() {
        ids.remove();
      }
    }
    assert!(removed_ids.len() <= n);
    self
      .metrics
      .message_counter
      .fetch_sub(removed_ids.len() as u64, Ordering::Relaxed);
    removed_ids
      .into_iter()
      .map(|id| (id, self.by_id.remove(&id).unwrap().1))
      .collect_vec()
  }
}
