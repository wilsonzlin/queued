//! Queue metrics using the `metrics` crate facade.
//!
//! Metrics are recorded directly using the `metrics` crate macros.
//! The queue name is passed as a label to distinguish between queues.

use metrics::{counter, describe_counter, describe_gauge, gauge};

/// Register metric descriptions. Call once at startup.
pub fn describe_metrics() {
    describe_counter!(
        "queued_empty_polls_total",
        "Total poll requests with no message available"
    );
    describe_gauge!("queued_messages", "Current message count in the queue");
    describe_counter!(
        "queued_missing_deletes_total",
        "Delete requests where message was not found"
    );
    describe_counter!(
        "queued_missing_updates_total",
        "Update requests where message was not found"
    );
    describe_counter!(
        "queued_successful_deletes_total",
        "Successful delete requests"
    );
    describe_counter!(
        "queued_successful_polls_total",
        "Successful poll requests"
    );
    describe_counter!(
        "queued_successful_pushes_total",
        "Successful push requests"
    );
    describe_counter!(
        "queued_successful_updates_total",
        "Successful update requests"
    );
    describe_counter!(
        "queued_suspended_deletes_total",
        "Delete requests while suspended"
    );
    describe_counter!(
        "queued_suspended_polls_total",
        "Poll requests while suspended"
    );
    describe_counter!(
        "queued_suspended_pushes_total",
        "Push requests while suspended"
    );
    describe_counter!(
        "queued_suspended_updates_total",
        "Update requests while suspended"
    );
    describe_counter!(
        "queued_throttled_polls_total",
        "Poll requests that were throttled"
    );
    describe_gauge!(
        "queued_visibility_timeout_first_seconds",
        "Seconds until first message becomes visible"
    );
    describe_gauge!(
        "queued_visibility_timeout_last_seconds",
        "Seconds until last message becomes visible"
    );
    describe_gauge!(
        "queued_oldest_unpolled_seconds",
        "Age in seconds of oldest unpolled message"
    );
}

// Direct metric recording functions - call these from operations

#[inline]
pub fn inc_empty_poll(queue: &str) {
    counter!("queued_empty_polls_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn set_message_count(queue: &str, count: f64) {
    gauge!("queued_messages", "queue" => queue.to_string()).set(count);
}

#[inline]
pub fn inc_message_count(queue: &str) {
    gauge!("queued_messages", "queue" => queue.to_string()).increment(1.0);
}

#[inline]
pub fn dec_message_count(queue: &str) {
    gauge!("queued_messages", "queue" => queue.to_string()).decrement(1.0);
}

#[inline]
pub fn dec_message_count_by(queue: &str, n: u64) {
    gauge!("queued_messages", "queue" => queue.to_string()).decrement(n as f64);
}

#[inline]
pub fn inc_missing_delete(queue: &str) {
    counter!("queued_missing_deletes_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_missing_update(queue: &str) {
    counter!("queued_missing_updates_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_successful_delete(queue: &str) {
    counter!("queued_successful_deletes_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_successful_poll(queue: &str, n: u64) {
    counter!("queued_successful_polls_total", "queue" => queue.to_string()).increment(n);
}

#[inline]
pub fn inc_successful_push(queue: &str, n: u64) {
    counter!("queued_successful_pushes_total", "queue" => queue.to_string()).increment(n);
}

#[inline]
pub fn inc_successful_update(queue: &str) {
    counter!("queued_successful_updates_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_suspended_delete(queue: &str) {
    counter!("queued_suspended_deletes_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_suspended_poll(queue: &str) {
    counter!("queued_suspended_polls_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_suspended_push(queue: &str) {
    counter!("queued_suspended_pushes_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_suspended_update(queue: &str) {
    counter!("queued_suspended_updates_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn inc_throttled_poll(queue: &str) {
    counter!("queued_throttled_polls_total", "queue" => queue.to_string()).increment(1);
}

#[inline]
pub fn set_visibility_timeout_first(queue: &str, secs: f64) {
    gauge!("queued_visibility_timeout_first_seconds", "queue" => queue.to_string()).set(secs);
}

#[inline]
pub fn set_visibility_timeout_last(queue: &str, secs: f64) {
    gauge!("queued_visibility_timeout_last_seconds", "queue" => queue.to_string()).set(secs);
}

#[inline]
pub fn set_oldest_unpolled(queue: &str, secs: f64) {
    gauge!("queued_oldest_unpolled_seconds", "queue" => queue.to_string()).set(secs);
}
