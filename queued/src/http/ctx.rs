use chrono::Utc;
use libqueued::Queued;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::max;

#[derive(Serialize, Deserialize)]
pub struct ServerMetrics {
  pub empty_poll_counter: u64,
  pub message_counter: u64,
  pub missing_delete_counter: u64,
  pub missing_update_counter: u64,
  pub successful_delete_counter: u64,
  pub successful_poll_counter: u64,
  pub successful_push_counter: u64,
  pub successful_update_counter: u64,
  pub suspended_delete_counter: u64,
  pub suspended_poll_counter: u64,
  pub suspended_push_counter: u64,
  pub suspended_update_counter: u64,
  pub throttled_poll_counter: u64,

  pub first_message_visibility_timeout_sec_gauge: u64,
  pub last_message_visibility_timeout_sec_gauge: u64,
  pub longest_unpolled_message_sec_gauge: u64,
}

pub struct HttpCtx {
  pub queued: Queued,
}

impl HttpCtx {
  pub fn build_server_metrics(&self) -> ServerMetrics {
    let now = Utc::now().timestamp();
    let m = self.queued.metrics();
    ServerMetrics {
      empty_poll_counter: m.empty_poll_counter(),
      message_counter: m.message_counter(),
      missing_delete_counter: m.missing_delete_counter(),
      missing_update_counter: m.missing_update_counter(),
      successful_delete_counter: m.successful_delete_counter(),
      successful_poll_counter: m.successful_poll_counter(),
      successful_push_counter: m.successful_push_counter(),
      successful_update_counter: m.successful_update_counter(),
      suspended_delete_counter: m.suspended_delete_counter(),
      suspended_poll_counter: m.suspended_poll_counter(),
      suspended_push_counter: m.suspended_push_counter(),
      suspended_update_counter: m.suspended_update_counter(),
      throttled_poll_counter: m.throttled_poll_counter(),

      first_message_visibility_timeout_sec_gauge: self
        .queued
        .youngest_message_time()
        .map(|t| max(0, t - now) as u64)
        .unwrap_or(0),
      last_message_visibility_timeout_sec_gauge: self
        .queued
        .oldest_message_time()
        .map(|t| max(0, t - now) as u64)
        .unwrap_or(0),
      longest_unpolled_message_sec_gauge: self
        .queued
        .youngest_message_time()
        .map(|t| max(0, now - t) as u64)
        .unwrap_or(0),
    }
  }
}
