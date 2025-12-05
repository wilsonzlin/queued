//! Metrics initialization using the `metrics` crate with Prometheus exporter.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::Lazy;

/// Global Prometheus metrics handle.
pub static METRICS_HANDLE: Lazy<PrometheusHandle> = Lazy::new(|| {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    
    libqueued::metrics::describe_metrics();
    handle
});

/// Initialize metrics. Call early in startup.
pub fn init() {
    Lazy::force(&METRICS_HANDLE);
}
