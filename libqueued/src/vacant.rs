use crate::metrics::Metrics;
use croaring::Bitmap;
use itertools::Itertools;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub(crate) struct VacantSlots {
  bitmap: Bitmap,
  capacity: u64,
  metrics: Arc<Metrics>,
}

impl VacantSlots {
  pub fn new(capacity: u64, metrics: Arc<Metrics>) -> VacantSlots {
    VacantSlots {
      bitmap: Bitmap::create(),
      capacity,
      metrics,
    }
  }

  fn update_metrics(&self) {
    self
      .metrics
      .free_space_gauge
      .store(self.capacity - self.bitmap.cardinality(), Ordering::Relaxed);
  }

  pub fn fill(&mut self, from: u32, to_exc: u32) {
    self.bitmap.add_range(from..to_exc);
  }

  pub fn add(&mut self, index: u32) {
    if !self.bitmap.add_checked(index) {
      panic!("slot already exists");
    };
    self.update_metrics();
  }

  pub fn remove_specific(&mut self, index: u32) {
    if !self.bitmap.remove_checked(index) {
      panic!("slot does not exist");
    };
  }

  pub fn count(&self) -> u64 {
    self.bitmap.cardinality()
  }

  pub fn take(&mut self) -> Option<u32> {
    let index = self.bitmap.minimum();
    if let Some(index) = index {
      self.bitmap.remove(index);
      self.update_metrics();
    };
    index
  }

  pub fn take_up_to_n(&mut self, n: usize) -> Vec<u32> {
    let indices = self.bitmap.iter().take(n).collect_vec();
    if !indices.is_empty() {
      self
        .bitmap
        .remove_range(indices[0]..=*indices.last().unwrap());
      self.update_metrics();
    };
    indices
  }
}
