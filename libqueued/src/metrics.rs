use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Default)]
pub struct Metrics {
  /// Total number of poll requests that failed due to no message being available.
  pub(crate) empty_poll_counter: AtomicU64,
  /// Amount of messages currently in the queue. They may have been created, polled, or updated.
  pub(crate) message_counter: AtomicU64,
  /// Total number of delete requests that failed due to the requested message not being found.
  pub(crate) missing_delete_counter: AtomicU64,
  /// Total number of update requests that failed due to the requested message not being found.
  pub(crate) missing_update_counter: AtomicU64,
  /// Total number of delete requests that did delete a message successfully.
  pub(crate) successful_delete_counter: AtomicU64,
  /// Total number of poll requests that did poll a message successfully.
  pub(crate) successful_poll_counter: AtomicU64,
  /// Total number of push requests that did push a message successfully.
  pub(crate) successful_push_counter: AtomicU64,
  /// Total number of update requests that did update a message successfully.
  pub(crate) successful_update_counter: AtomicU64,
  /// Total number of delete requests while the endpoint was suspended.
  pub(crate) suspended_delete_counter: AtomicU64,
  /// Total number of poll requests while the endpoint was suspended.
  pub(crate) suspended_poll_counter: AtomicU64,
  /// Total number of push requests while the endpoint was suspended.
  pub(crate) suspended_push_counter: AtomicU64,
  /// Total number of update requests while the endpoint was suspended.
  pub(crate) suspended_update_counter: AtomicU64,
  /// Total number of poll requests that were throttled.
  pub(crate) throttled_poll_counter: AtomicU64,
}

impl Metrics {
  pub fn empty_poll_counter(&self) -> u64 {
    self.empty_poll_counter.load(Ordering::Relaxed)
  }

  pub fn message_counter(&self) -> u64 {
    self.message_counter.load(Ordering::Relaxed)
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
}
