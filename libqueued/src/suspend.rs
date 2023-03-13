use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

#[derive(Default)]
pub struct SuspendState {
  delete: AtomicBool,
  poll: AtomicBool,
  push: AtomicBool,
  update: AtomicBool,
}

impl SuspendState {
  pub fn is_delete_suspended(&self) -> bool {
    self.delete.load(Ordering::Relaxed)
  }

  pub fn is_poll_suspended(&self) -> bool {
    self.poll.load(Ordering::Relaxed)
  }

  pub fn is_push_suspended(&self) -> bool {
    self.push.load(Ordering::Relaxed)
  }

  pub fn is_update_suspended(&self) -> bool {
    self.update.load(Ordering::Relaxed)
  }

  pub fn set_delete_suspension(&self, s: bool) {
    self.delete.store(s, Ordering::Relaxed);
  }

  pub fn set_poll_suspension(&self, s: bool) {
    self.poll.store(s, Ordering::Relaxed);
  }

  pub fn set_push_suspension(&self, s: bool) {
    self.push.store(s, Ordering::Relaxed);
  }

  pub fn set_update_suspension(&self, s: bool) {
    self.update.store(s, Ordering::Relaxed);
  }
}
