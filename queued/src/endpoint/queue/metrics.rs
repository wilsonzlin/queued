use crate::endpoint::HttpCtx;
use crate::endpoint::QueuedHttpError;
use crate::metrics::METRICS_HANDLE;
use axum::extract::Path;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderMap;
use chrono::Utc;
use libqueued::metrics::names;
use serde::Serialize;
use std::cmp::max;
use std::sync::Arc;

/// Metrics response for JSON/MessagePack formats.
/// Provides backwards compatibility with the old metrics format.
#[derive(Serialize)]
pub(crate) struct MetricsResponse {
    empty_poll_counter: u64,
    message_counter: u64,
    missing_delete_counter: u64,
    missing_update_counter: u64,
    successful_delete_counter: u64,
    successful_poll_counter: u64,
    successful_push_counter: u64,
    successful_update_counter: u64,
    suspended_delete_counter: u64,
    suspended_poll_counter: u64,
    suspended_push_counter: u64,
    suspended_update_counter: u64,
    throttled_poll_counter: u64,

    first_message_visibility_timeout_sec_gauge: u64,
    last_message_visibility_timeout_sec_gauge: u64,
    longest_unpolled_message_sec_gauge: u64,
}

/// Extract metric value from the Prometheus output for a given metric name and queue.
fn extract_metric_value(prometheus_output: &str, metric_name: &str, queue: &str) -> u64 {
    for line in prometheus_output.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        // Match lines like: metric_name{queue="queue_name"} value
        if line.starts_with(metric_name) {
            let label_pattern = format!("queue=\"{}\"", queue);
            if line.contains(&label_pattern) {
                // Extract the value after the closing brace
                if let Some(value_str) = line.rsplit_once('}').and_then(|(_, v)| Some(v.trim())) {
                    // Handle floating point values
                    if let Ok(value) = value_str.parse::<f64>() {
                        return value as u64;
                    }
                }
            }
        }
    }
    0
}

fn build_metrics_response(q: &libqueued::Queued, queue_name: &str) -> MetricsResponse {
    let now = Utc::now().timestamp();

    // Get gauge values from the Queued instance
    let first_message_visibility_timeout_sec = q
        .youngest_message_time()
        .map(|t| max(0, t - now) as u64)
        .unwrap_or(0);
    let last_message_visibility_timeout_sec = q
        .oldest_message_time()
        .map(|t| max(0, t - now) as u64)
        .unwrap_or(0);
    let longest_unpolled_message_sec = q
        .youngest_message_time()
        .map(|t| max(0, now - t) as u64)
        .unwrap_or(0);

    // Update the visibility timeout gauges
    let metrics = q.metrics();
    metrics.set_first_message_visibility_timeout_sec(first_message_visibility_timeout_sec as f64);
    metrics.set_last_message_visibility_timeout_sec(last_message_visibility_timeout_sec as f64);
    metrics.set_longest_unpolled_message_sec(longest_unpolled_message_sec as f64);

    // Get counter values from the Prometheus exporter
    let prometheus_output = METRICS_HANDLE.render();

    MetricsResponse {
        empty_poll_counter: extract_metric_value(&prometheus_output, names::EMPTY_POLL, queue_name),
        message_counter: extract_metric_value(&prometheus_output, names::MESSAGE_COUNT, queue_name),
        missing_delete_counter: extract_metric_value(
            &prometheus_output,
            names::MISSING_DELETE,
            queue_name,
        ),
        missing_update_counter: extract_metric_value(
            &prometheus_output,
            names::MISSING_UPDATE,
            queue_name,
        ),
        successful_delete_counter: extract_metric_value(
            &prometheus_output,
            names::SUCCESSFUL_DELETE,
            queue_name,
        ),
        successful_poll_counter: extract_metric_value(
            &prometheus_output,
            names::SUCCESSFUL_POLL,
            queue_name,
        ),
        successful_push_counter: extract_metric_value(
            &prometheus_output,
            names::SUCCESSFUL_PUSH,
            queue_name,
        ),
        successful_update_counter: extract_metric_value(
            &prometheus_output,
            names::SUCCESSFUL_UPDATE,
            queue_name,
        ),
        suspended_delete_counter: extract_metric_value(
            &prometheus_output,
            names::SUSPENDED_DELETE,
            queue_name,
        ),
        suspended_poll_counter: extract_metric_value(
            &prometheus_output,
            names::SUSPENDED_POLL,
            queue_name,
        ),
        suspended_push_counter: extract_metric_value(
            &prometheus_output,
            names::SUSPENDED_PUSH,
            queue_name,
        ),
        suspended_update_counter: extract_metric_value(
            &prometheus_output,
            names::SUSPENDED_UPDATE,
            queue_name,
        ),
        throttled_poll_counter: extract_metric_value(
            &prometheus_output,
            names::THROTTLED_POLL,
            queue_name,
        ),
        first_message_visibility_timeout_sec_gauge: first_message_visibility_timeout_sec,
        last_message_visibility_timeout_sec_gauge: last_message_visibility_timeout_sec,
        longest_unpolled_message_sec_gauge: longest_unpolled_message_sec,
    }
}

pub(crate) async fn endpoint_metrics(
    State(ctx): State<Arc<HttpCtx>>,
    Path(queue_name): Path<String>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Vec<u8>), QueuedHttpError> {
    let q = ctx.q(&queue_name, &headers)?;

    // Update visibility timeout gauges
    let now = Utc::now().timestamp();
    let metrics = q.metrics();
    metrics.set_first_message_visibility_timeout_sec(
        q.youngest_message_time()
            .map(|t| max(0, t - now) as f64)
            .unwrap_or(0.0),
    );
    metrics.set_last_message_visibility_timeout_sec(
        q.oldest_message_time()
            .map(|t| max(0, t - now) as f64)
            .unwrap_or(0.0),
    );
    metrics.set_longest_unpolled_message_sec(
        q.youngest_message_time()
            .map(|t| max(0, now - t) as f64)
            .unwrap_or(0.0),
    );

    let accept = headers.get("accept").and_then(|h| h.to_str().ok());
    let (ct, raw) = match accept {
        Some("application/json") => {
            let out = build_metrics_response(&q, &queue_name);
            ("application/json", serde_json::to_vec(&out).unwrap())
        }
        Some("application/msgpack") => {
            let out = build_metrics_response(&q, &queue_name);
            ("application/msgpack", rmp_serde::to_vec_named(&out).unwrap())
        }
        _ => {
            // Default: Prometheus text format
            let output = METRICS_HANDLE.render();
            ("text/plain; version=0.0.4; charset=utf-8", output.into_bytes())
        }
    };
    let mut h = HeaderMap::new();
    h.insert(CONTENT_TYPE, ct.parse().unwrap());
    Ok((h, raw))
}

/// Global metrics endpoint that returns all queue metrics in Prometheus format.
pub(crate) async fn endpoint_metrics_global() -> (HeaderMap, Vec<u8>) {
    let output = METRICS_HANDLE.render();
    let mut h = HeaderMap::new();
    h.insert(
        CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
    );
    (h, output.into_bytes())
}
