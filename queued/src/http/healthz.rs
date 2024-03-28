use axum_msgpack::MsgPack;
use serde::Deserialize;
use serde::Serialize;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize, Deserialize)]
pub struct EndpointHealthzOutput {
  version: String,
}

pub async fn endpoint_healthz() -> MsgPack<EndpointHealthzOutput> {
  MsgPack(EndpointHealthzOutput {
    version: VERSION.to_string(),
  })
}
