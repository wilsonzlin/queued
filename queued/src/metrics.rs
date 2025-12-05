//! Metrics initialization and management using the `metrics` crate ecosystem.
//!
//! This module sets up the Prometheus exporter as the global metrics recorder.
//! All metrics from `libqueued` are automatically exported via the Prometheus
//! endpoint.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::Lazy;

/// Global handle to the Prometheus metrics recorder.
/// Use `METRICS_HANDLE.render()` to get the current metrics in Prometheus format.
pub static METRICS_HANDLE: Lazy<PrometheusHandle> = Lazy::new(|| {
    let builder = PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .expect("failed to install Prometheus metrics recorder");
    
    // Register metric descriptions
    libqueued::metrics::describe_metrics();
    
    handle
});

/// Initialize the metrics system.
/// This must be called early in the application startup.
pub fn init_metrics() {
    // Force initialization of the lazy static
    Lazy::force(&METRICS_HANDLE);
}
