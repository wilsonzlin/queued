use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize, Deserialize)]
pub struct EndpointHealthzOutput {
  version: String,
}

pub async fn endpoint_healthz() -> Result<Json<EndpointHealthzOutput>, (StatusCode, &'static str)> {
  Ok(Json(EndpointHealthzOutput {
    version: VERSION.to_string(),
  }))
}
