use chrono::DateTime;
use chrono::Utc;
use croaring::Bitmap;
use itertools::Itertools;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct AvailableSlot {
  pub index: u64,
  pub visible_time: DateTime<Utc>,
}

pub struct AvailableSlots {
  metrics: Arc<Metrics>,
  // Since we don't expect there to be many entries in each DateTime<Utc> entry, a HashSet is more optimised than a RoaringBitmap.
  ordered_by_visible_time: BTreeMap<DateTime<Utc>, HashSet<u32>>,
  by_index: HashMap<u32, DateTime<Utc>>,
}

impl AvailableSlots {
  pub fn new(metrics: Arc<Metrics>) -> AvailableSlots {
    AvailableSlots {
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

#[derive(Default)]
pub struct Metrics {
  pub io: Arc<SeekableAsyncFileMetrics>,
  pub available_gauge: AtomicU64,
  pub empty_poll_counter: AtomicU64,
  pub missing_delete_counter: AtomicU64,
  pub successful_delete_counter: AtomicU64,
  pub successful_poll_counter: AtomicU64,
  pub successful_push_counter: AtomicU64,
  pub suspended_delete_counter: AtomicU64,
  pub suspended_poll_counter: AtomicU64,
  pub suspended_push_counter: AtomicU64,
  pub vacant_gauge: AtomicU64,
}

pub struct Ctx {
  pub available: Mutex<AvailableSlots>,
  pub device: SeekableAsyncFile,
  pub metrics: Arc<Metrics>,
  pub suspend_delete: AtomicBool,
  pub suspend_poll: AtomicBool,
  pub suspend_push: AtomicBool,
  pub vacant: Mutex<VacantSlots>,
}
