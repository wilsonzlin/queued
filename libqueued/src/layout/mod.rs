use crate::invisible::InvisibleMessages;
use crate::metrics::Metrics;
use crate::visible::VisibleMessages;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use std::sync::Arc;

pub mod fixed_slots;
pub mod log_structured;

pub(crate) struct LoadedData {
  pub invisible: InvisibleMessages,
  pub visible: VisibleMessages,
}

pub(crate) struct MessageOnDisk {
  pub created: DateTime<Utc>,
  pub poll_count: u32,
  pub contents: Vec<u8>,
}

pub(crate) struct MessagePoll {
  pub poll_tag: Vec<u8>,
  pub visible_time: DateTime<Utc>,
  pub poll_count: u32,
}

pub(crate) struct MessageCreation {
  pub id: u64,
  pub visible_time: DateTime<Utc>,
  pub contents: Vec<u8>,
}

#[async_trait]
pub(crate) trait StorageLayout {
  fn max_contents_len(&self) -> u64;

  async fn start_background_loops(&self) -> ();

  async fn format_device(&self) -> ();

  // It's safe to assume that this method will only ever be called at most once for the entire lifetime of this StorageLayout, so it's safe to mutate internal state and "initialise" it.
  async fn load_data_from_device(&self, metrics: Arc<Metrics>) -> LoadedData;

  async fn read_poll_tag(&self, id: u64) -> Vec<u8>;

  async fn update_visibility_time(&self, id: u64, visible_time: DateTime<Utc>) -> ();

  async fn delete_message(&self, id: u64) -> ();

  async fn read_message(&self, id: u64) -> MessageOnDisk;

  async fn mark_as_polled(&self, id: u64, update: MessagePoll) -> ();

  async fn create_messages(&self, creations: Vec<MessageCreation>) -> ();
}
