//! Queue metrics using the `metrics` crate facade.
//!
//! This module defines the metric names and provides helper functions for
//! recording queue metrics. Metrics are recorded using the standard `metrics`
//! crate macros, which allows any compatible exporter (Prometheus, StatsD, etc.)
//! to be used.

use metrics::{counter, describe_counter, describe_gauge, gauge};

/// Metric name constants for consistent usage across the codebase.
pub mod names {
    /// Total number of poll requests that failed due to no message being available.
    pub const EMPTY_POLL: &str = "queued_empty_polls_total";
    /// Current number of messages in the queue.
    pub const MESSAGE_COUNT: &str = "queued_messages";
    /// Total number of delete requests that failed due to message not found.
    pub const MISSING_DELETE: &str = "queued_missing_deletes_total";
    /// Total number of update requests that failed due to message not found.
    pub const MISSING_UPDATE: &str = "queued_missing_updates_total";
    /// Total number of successful delete requests.
    pub const SUCCESSFUL_DELETE: &str = "queued_successful_deletes_total";
    /// Total number of successful poll requests.
    pub const SUCCESSFUL_POLL: &str = "queued_successful_polls_total";
    /// Total number of successful push requests.
    pub const SUCCESSFUL_PUSH: &str = "queued_successful_pushes_total";
    /// Total number of successful update requests.
    pub const SUCCESSFUL_UPDATE: &str = "queued_successful_updates_total";
    /// Total number of delete requests while suspended.
    pub const SUSPENDED_DELETE: &str = "queued_suspended_deletes_total";
    /// Total number of poll requests while suspended.
    pub const SUSPENDED_POLL: &str = "queued_suspended_polls_total";
    /// Total number of push requests while suspended.
    pub const SUSPENDED_PUSH: &str = "queued_suspended_pushes_total";
    /// Total number of update requests while suspended.
    pub const SUSPENDED_UPDATE: &str = "queued_suspended_updates_total";
    /// Total number of poll requests that were throttled.
    pub const THROTTLED_POLL: &str = "queued_throttled_polls_total";
    /// Seconds until the first (youngest) message becomes visible.
    pub const FIRST_MESSAGE_VISIBILITY_TIMEOUT_SEC: &str =
        "queued_first_message_visibility_timeout_seconds";
    /// Seconds until the last (oldest) message becomes visible.
    pub const LAST_MESSAGE_VISIBILITY_TIMEOUT_SEC: &str =
        "queued_last_message_visibility_timeout_seconds";
    /// Seconds the longest unpolled message has been waiting.
    pub const LONGEST_UNPOLLED_MESSAGE_SEC: &str = "queued_longest_unpolled_message_seconds";
}

/// Register metric descriptions with the metrics registry.
/// This should be called once at startup to provide descriptions for all metrics.
pub fn describe_metrics() {
    describe_counter!(
        names::EMPTY_POLL,
        "Total number of poll requests that failed due to no message being available"
    );
    describe_gauge!(
        names::MESSAGE_COUNT,
        "Current number of messages in the queue"
    );
    describe_counter!(
        names::MISSING_DELETE,
        "Total number of delete requests that failed due to message not found"
    );
    describe_counter!(
        names::MISSING_UPDATE,
        "Total number of update requests that failed due to message not found"
    );
    describe_counter!(
        names::SUCCESSFUL_DELETE,
        "Total number of successful delete requests"
    );
    describe_counter!(
        names::SUCCESSFUL_POLL,
        "Total number of successful poll requests"
    );
    describe_counter!(
        names::SUCCESSFUL_PUSH,
        "Total number of successful push requests"
    );
    describe_counter!(
        names::SUCCESSFUL_UPDATE,
        "Total number of successful update requests"
    );
    describe_counter!(
        names::SUSPENDED_DELETE,
        "Total number of delete requests while suspended"
    );
    describe_counter!(
        names::SUSPENDED_POLL,
        "Total number of poll requests while suspended"
    );
    describe_counter!(
        names::SUSPENDED_PUSH,
        "Total number of push requests while suspended"
    );
    describe_counter!(
        names::SUSPENDED_UPDATE,
        "Total number of update requests while suspended"
    );
    describe_counter!(
        names::THROTTLED_POLL,
        "Total number of poll requests that were throttled"
    );
    describe_gauge!(
        names::FIRST_MESSAGE_VISIBILITY_TIMEOUT_SEC,
        "Seconds until the first (youngest) message becomes visible"
    );
    describe_gauge!(
        names::LAST_MESSAGE_VISIBILITY_TIMEOUT_SEC,
        "Seconds until the last (oldest) message becomes visible"
    );
    describe_gauge!(
        names::LONGEST_UNPOLLED_MESSAGE_SEC,
        "Seconds the longest unpolled message has been waiting"
    );
}

/// Helper struct to record metrics for a specific queue.
/// Uses labels to distinguish between different queues.
#[derive(Clone)]
pub struct QueueMetrics {
    queue_name: String,
}

impl QueueMetrics {
    pub fn new(queue_name: impl Into<String>) -> Self {
        Self {
            queue_name: queue_name.into(),
        }
    }

    fn labels(&self) -> [(&'static str, String); 1] {
        [("queue", self.queue_name.clone())]
    }

    pub fn inc_empty_poll(&self) {
        counter!(names::EMPTY_POLL, &self.labels()).increment(1);
    }

    pub fn set_message_count(&self, count: u64) {
        gauge!(names::MESSAGE_COUNT, &self.labels()).set(count as f64);
    }

    pub fn inc_message_count(&self) {
        gauge!(names::MESSAGE_COUNT, &self.labels()).increment(1.0);
    }

    pub fn dec_message_count(&self) {
        gauge!(names::MESSAGE_COUNT, &self.labels()).decrement(1.0);
    }

    pub fn dec_message_count_by(&self, n: u64) {
        gauge!(names::MESSAGE_COUNT, &self.labels()).decrement(n as f64);
    }

    pub fn inc_missing_delete(&self) {
        counter!(names::MISSING_DELETE, &self.labels()).increment(1);
    }

    pub fn inc_missing_update(&self) {
        counter!(names::MISSING_UPDATE, &self.labels()).increment(1);
    }

    pub fn inc_successful_delete(&self) {
        counter!(names::SUCCESSFUL_DELETE, &self.labels()).increment(1);
    }

    pub fn inc_successful_poll(&self, n: u64) {
        counter!(names::SUCCESSFUL_POLL, &self.labels()).increment(n);
    }

    pub fn inc_successful_push(&self, n: u64) {
        counter!(names::SUCCESSFUL_PUSH, &self.labels()).increment(n);
    }

    pub fn inc_successful_update(&self) {
        counter!(names::SUCCESSFUL_UPDATE, &self.labels()).increment(1);
    }

    pub fn inc_suspended_delete(&self) {
        counter!(names::SUSPENDED_DELETE, &self.labels()).increment(1);
    }

    pub fn inc_suspended_poll(&self) {
        counter!(names::SUSPENDED_POLL, &self.labels()).increment(1);
    }

    pub fn inc_suspended_push(&self) {
        counter!(names::SUSPENDED_PUSH, &self.labels()).increment(1);
    }

    pub fn inc_suspended_update(&self) {
        counter!(names::SUSPENDED_UPDATE, &self.labels()).increment(1);
    }

    pub fn inc_throttled_poll(&self) {
        counter!(names::THROTTLED_POLL, &self.labels()).increment(1);
    }

    pub fn set_first_message_visibility_timeout_sec(&self, secs: f64) {
        gauge!(names::FIRST_MESSAGE_VISIBILITY_TIMEOUT_SEC, &self.labels()).set(secs);
    }

    pub fn set_last_message_visibility_timeout_sec(&self, secs: f64) {
        gauge!(names::LAST_MESSAGE_VISIBILITY_TIMEOUT_SEC, &self.labels()).set(secs);
    }

    pub fn set_longest_unpolled_message_sec(&self, secs: f64) {
        gauge!(names::LONGEST_UNPOLLED_MESSAGE_SEC, &self.labels()).set(secs);
    }
}
