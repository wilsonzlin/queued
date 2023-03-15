use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Default)]
pub struct Metrics {
  pub(crate) empty_poll_counter: AtomicU64,
  pub(crate) free_space_gauge: AtomicU64, // We report free space instead of used as otherwise we'd need to also consider space used outside of storage layout (e.g. journal).
  pub(crate) invisible_gauge: AtomicU64,
  pub(crate) missing_delete_counter: AtomicU64,
  pub(crate) missing_update_counter: AtomicU64,
  pub(crate) successful_delete_counter: AtomicU64,
  pub(crate) successful_poll_counter: AtomicU64,
  pub(crate) successful_push_counter: AtomicU64,
  pub(crate) successful_update_counter: AtomicU64,
  pub(crate) suspended_delete_counter: AtomicU64,
  pub(crate) suspended_poll_counter: AtomicU64,
  pub(crate) suspended_push_counter: AtomicU64,
  pub(crate) suspended_update_counter: AtomicU64,
  pub(crate) throttled_poll_counter: AtomicU64,
  pub(crate) visible_gauge: AtomicU64,
}

impl Metrics {
  pub fn empty_poll_counter(&self) -> u64 {
    self.empty_poll_counter.load(Ordering::Relaxed)
  }

  pub fn free_space_gauge(&self) -> u64 {
    self.free_space_gauge.load(Ordering::Relaxed)
  }

  pub fn invisible_gauge(&self) -> u64 {
    self.invisible_gauge.load(Ordering::Relaxed)
  }

  pub fn missing_delete_counter(&self) -> u64 {
    self.missing_delete_counter.load(Ordering::Relaxed)
  }

  pub fn missing_update_counter(&self) -> u64 {
    self.missing_update_counter.load(Ordering::Relaxed)
  }

  pub fn successful_delete_counter(&self) -> u64 {
    self.successful_delete_counter.load(Ordering::Relaxed)
  }

  pub fn successful_poll_counter(&self) -> u64 {
    self.successful_poll_counter.load(Ordering::Relaxed)
  }

  pub fn successful_push_counter(&self) -> u64 {
    self.successful_push_counter.load(Ordering::Relaxed)
  }

  pub fn successful_update_counter(&self) -> u64 {
    self.successful_update_counter.load(Ordering::Relaxed)
  }

  pub fn suspended_delete_counter(&self) -> u64 {
    self.suspended_delete_counter.load(Ordering::Relaxed)
  }

  pub fn suspended_poll_counter(&self) -> u64 {
    self.suspended_poll_counter.load(Ordering::Relaxed)
  }

  pub fn suspended_push_counter(&self) -> u64 {
    self.suspended_push_counter.load(Ordering::Relaxed)
  }

  pub fn suspended_update_counter(&self) -> u64 {
    self.suspended_update_counter.load(Ordering::Relaxed)
  }

  pub fn throttled_poll_counter(&self) -> u64 {
    self.throttled_poll_counter.load(Ordering::Relaxed)
  }

  pub fn visible_gauge(&self) -> u64 {
    self.visible_gauge.load(Ordering::Relaxed)
  }
}
