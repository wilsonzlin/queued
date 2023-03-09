use self::fixed_slots::SlotState;
use crate::available::AvailableMessages;
use crate::metrics::Metrics;
use crate::vacant::VacantSlots;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use seekable_async_file::WriteRequest;
use std::sync::Arc;

pub mod fixed_slots;

pub struct LoadedData {
  pub available: AvailableMessages,
  pub vacant: VacantSlots,
}

pub struct MessageOnDisk {
  pub created: DateTime<Utc>,
  pub poll_count: u32,
  pub contents: String,
}

pub struct MessageMetadataUpdate {
  pub state: SlotState,
  pub poll_tag: Vec<u8>,
  pub created_time: DateTime<Utc>, // This should simply be repeated from the existing value; any change will NOT get persisted.
  pub visible_time: DateTime<Utc>,
  pub poll_count: u32,
}

pub struct MessageCreation {
  pub state: SlotState,
  pub visible_time: DateTime<Utc>,
  pub contents: String,
}

#[async_trait]
pub trait StorageLayout {
  fn max_content_len(&self) -> u64;

  async fn format_device(&self) -> ();

  async fn load_data_from_device(&self, metrics: Arc<Metrics>) -> LoadedData;

  async fn read_poll_tag(&self, index: u32) -> Vec<u8>;

  async fn delete_message(&self, index: u32) -> ();

  async fn read_message(&self, index: u32) -> MessageOnDisk;

  async fn update_message_metadata(&self, index: u32, update: MessageMetadataUpdate) -> ();

  fn prepare_message_creation_write(&self, index: u32, creation: MessageCreation) -> WriteRequest;
}
