use crate::metrics::Metrics;
use croaring::Bitmap;
use itertools::Itertools;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct VacantSlots {
  bitmap: Bitmap,
  metrics: Arc<Metrics>,
}

impl VacantSlots {
  pub fn new(metrics: Arc<Metrics>) -> VacantSlots {
    VacantSlots {
      bitmap: Bitmap::create(),
      metrics,
    }
  }

  pub fn add(&mut self, index: u32) {
    if !self.bitmap.add_checked(index) {
      panic!("slot already exists");
    };
    self.metrics.vacant_gauge.fetch_add(1, Ordering::Relaxed);
  }

  pub fn count(&self) -> u64 {
    self.bitmap.cardinality()
  }

  pub fn take(&mut self) -> Option<u32> {
    let index = self.bitmap.minimum();
    if let Some(index) = index {
      self.bitmap.remove(index);
      self.metrics.vacant_gauge.fetch_sub(1, Ordering::Relaxed);
    };
    index
  }

  pub fn take_up_to_n(&mut self, n: usize) -> Vec<u32> {
    let indices = self.bitmap.iter().take(n).collect_vec();
    if !indices.is_empty() {
      self
        .bitmap
        .remove_range(indices[0]..=*indices.last().unwrap());
      self
        .metrics
        .vacant_gauge
        .fetch_sub(indices.len().try_into().unwrap(), Ordering::Relaxed);
    };
    indices
  }
}
