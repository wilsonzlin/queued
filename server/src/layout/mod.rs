use self::fixed_slots::SlotState;
use crate::available::AvailableMessages;
use crate::metrics::Metrics;
use crate::vacant::VacantSlots;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use std::sync::Arc;

pub mod fixed_slots;
pub mod log_structured;

pub struct LoadedData {
  pub available: AvailableMessages,
  pub vacant: VacantSlots,
}

pub struct MessageOnDisk {
  pub created: DateTime<Utc>,
  pub poll_count: u32,
  pub contents: String,
}

pub struct MessagePoll {
  pub state: SlotState,
  pub poll_tag: Vec<u8>,
  pub created_time: DateTime<Utc>, // This should simply be repeated from the existing value; any change will NOT get persisted.
  pub visible_time: DateTime<Utc>,
  pub poll_count: u32,
}

pub struct MessageCreation {
  pub index: u32,
  pub state: SlotState,
  pub visible_time: DateTime<Utc>,
  pub contents: String,
}

#[async_trait]
pub trait StorageLayout {
  fn max_contents_len(&self) -> u64;

  async fn start_background_loops(&self) -> ();

  async fn format_device(&self) -> ();

  // It's safe to assume that this method will only ever be called at most once for the entire lifetime of this StorageLayout, so it's safe to mutate internal state and "initialise" it.
  async fn load_data_from_device(&self, metrics: Arc<Metrics>) -> LoadedData;

  async fn read_poll_tag(&self, index: u32) -> Vec<u8>;

  async fn update_visibility_time(&self, index: u32, visible_time: DateTime<Utc>) -> ();

  async fn delete_message(&self, index: u32) -> ();

  async fn read_message(&self, index: u32) -> MessageOnDisk;

  async fn mark_as_polled(&self, index: u32, update: MessagePoll) -> ();

  async fn create_messages(&self, creations: Vec<MessageCreation>) -> ();
}
