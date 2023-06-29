use chrono::Utc;

pub(crate) struct Throttler {
  // State.
  count: u64,
  time_base: i64,
  // Configuration.
  max: u64,
  window: i64,
}

impl Throttler {
  pub fn new(max_reqs_per_time_window: u64, time_window_sec: i64) -> Self {
    assert!(time_window_sec > 1);
    Self {
      count: 0,
      time_base: i64::MIN,
      max: max_reqs_per_time_window,
      window: time_window_sec,
    }
  }

  pub fn increment_count(&mut self) -> bool {
    let cur_window = Utc::now().timestamp() / self.time_base;
    if self.window != cur_window {
      self.window = cur_window;
      self.count = 0;
    };
    self.count += 1;
    self.count <= self.max
  }

  pub fn get_max_reqs_per_time_window(&self) -> u64 {
    self.max
  }

  pub fn get_time_window_sec(&self) -> i64 {
    self.time_base
  }
}
