use crate::endpoint::HttpCtx;
use crate::endpoint::QueuedHttpError;
use crate::metrics::METRICS_HANDLE;
use axum::extract::Path;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderMap;
use chrono::Utc;
use libqueued::metrics as queue_metrics;
use std::cmp::max;
use std::sync::Arc;

/// Update visibility timeout gauges for a queue.
fn update_visibility_gauges(q: &libqueued::Queued) {
    let now = Utc::now().timestamp();
    let queue_name = q.queue_name();
    
    queue_metrics::set_visibility_timeout_first(
        queue_name,
        q.youngest_message_time()
            .map(|t| max(0, t - now) as f64)
            .unwrap_or(0.0),
    );
    queue_metrics::set_visibility_timeout_last(
        queue_name,
        q.oldest_message_time()
            .map(|t| max(0, t - now) as f64)
            .unwrap_or(0.0),
    );
    queue_metrics::set_oldest_unpolled(
        queue_name,
        q.youngest_message_time()
            .map(|t| max(0, now - t) as f64)
            .unwrap_or(0.0),
    );
}

/// Per-queue metrics endpoint. Updates gauges and returns Prometheus format.
pub(crate) async fn endpoint_metrics(
    State(ctx): State<Arc<HttpCtx>>,
    Path(queue_name): Path<String>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Vec<u8>), QueuedHttpError> {
    let q = ctx.q(&queue_name, &headers)?;
    update_visibility_gauges(&q);
    
    let output = METRICS_HANDLE.render();
    let mut h = HeaderMap::new();
    h.insert(
        CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
    );
    Ok((h, output.into_bytes()))
}

/// Global metrics endpoint - returns all metrics in Prometheus format.
pub(crate) async fn endpoint_metrics_global(
    State(ctx): State<Arc<HttpCtx>>,
) -> (HeaderMap, Vec<u8>) {
    // Update visibility gauges for all queues
    for entry in ctx.queues.iter() {
        update_visibility_gauges(&entry);
    }
    
    let output = METRICS_HANDLE.render();
    let mut h = HeaderMap::new();
    h.insert(
        CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
    );
    (h, output.into_bytes())
}
