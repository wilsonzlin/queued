use super::LoadedData;
use super::MessageCreation;
use super::MessageMetadataUpdate;
use super::MessageOnDisk;
use super::StorageLayout;
use crate::available::AvailableMessages;
use crate::metrics::Metrics;
use crate::util::as_usize;
use crate::util::read_ts;
use crate::util::read_u16;
use crate::util::read_u32;
use crate::util::repeated_copy;
use crate::util::u64_slice;
use crate::util::u64_slice_write;
use crate::vacant::VacantSlots;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use lazy_static::lazy_static;
use num_enum::TryFromPrimitive;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use std::cmp::min;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
pub enum SlotState {
  Vacant = 0,
  Available = 1,
}

const SLOT_LEN: u64 = 1024;
const SLOT_OFFSETOF_HASH: u64 = 0;
const SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS: u64 = SLOT_OFFSETOF_HASH + 32;
const SLOT_OFFSETOF_STATE: u64 = SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS + 1;
const SLOT_OFFSETOF_POLL_TAG: u64 = SLOT_OFFSETOF_STATE + 1;
const SLOT_OFFSETOF_CREATED_TS: u64 = SLOT_OFFSETOF_POLL_TAG + 30;
const SLOT_OFFSETOF_VISIBLE_TS: u64 = SLOT_OFFSETOF_CREATED_TS + 8;
const SLOT_OFFSETOF_POLL_COUNT: u64 = SLOT_OFFSETOF_VISIBLE_TS + 8;
const SLOT_OFFSETOF_LEN: u64 = SLOT_OFFSETOF_POLL_COUNT + 4;
const SLOT_OFFSETOF_CONTENTS: u64 = SLOT_OFFSETOF_LEN + 2;
const SLOT_FIXED_FIELDS_LEN: u64 = SLOT_OFFSETOF_CONTENTS;
const MESSAGE_SLOT_CONTENTS_LEN_MAX: u64 = SLOT_LEN - SLOT_FIXED_FIELDS_LEN;

lazy_static! {
  pub static ref SLOT_VACANT_TEMPLATE: Vec<u8> = {
    let mut slot_template = vec![0u8; as_usize!(SLOT_FIXED_FIELDS_LEN)];
    let hash = blake3::hash(&slot_template[32..]);
    slot_template[..32].copy_from_slice(hash.as_bytes());
    slot_template
  };
}

fn slot_offset(index: u32) -> u64 {
  u64::from(index) * SLOT_LEN
}

pub struct FixedSlotsLayout {
  device: SeekableAsyncFile,
  device_size: u64,
}

impl FixedSlotsLayout {
  pub fn new(device: SeekableAsyncFile, device_size: u64) -> Self {
    Self {
      device,
      device_size,
    }
  }
}

#[async_trait]
impl StorageLayout for FixedSlotsLayout {
  fn max_contents_len(&self) -> u64 {
    MESSAGE_SLOT_CONTENTS_LEN_MAX
  }

  async fn format_device(&self) {
    let mut template_base_padded = vec![0u8; as_usize!(SLOT_LEN)];
    template_base_padded[..as_usize!(SLOT_FIXED_FIELDS_LEN)].copy_from_slice(&SLOT_VACANT_TEMPLATE);

    let mut template = vec![0u8; min(as_usize!(self.device_size), 1024 * 1024 * 1024)];
    repeated_copy(&mut template, &template_base_padded);
    let template_len: u64 = template.len().try_into().unwrap();

    let mut next = 0;
    while next < self.device_size {
      self
        .device
        .write_at(
          next,
          template[..min(template.len(), as_usize!(self.device_size - next))].to_vec(),
        )
        .await;
      next += template_len;
    }

    self.device.sync_data().await;
  }

  async fn load_data_from_device(&mut self, metrics: Arc<Metrics>) -> LoadedData {
    // Downgrade to non-mut reference to allow copying without lifetime errors.
    let layout: &Self = self;
    let available = Arc::new(Mutex::new(AvailableMessages::new(metrics.clone())));
    let vacant = Arc::new(Mutex::new(VacantSlots::new(metrics.clone())));
    let progress = Arc::new(AtomicU64::new(0));

    futures::stream::iter((0..layout.device_size).step_by(as_usize!(SLOT_LEN)))
      .for_each_concurrent(None, |offset| {
        let available = available.clone();
        let vacant = vacant.clone();
        let progress = progress.clone();

        async move {
          let mut slot_data = layout.device.read_at(offset, SLOT_LEN).await;

          let hash_includes_contents = slot_data[as_usize!(SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS)];
          match hash_includes_contents {
            0 => slot_data.truncate(as_usize!(SLOT_FIXED_FIELDS_LEN)),
            1 => {
              let content_len: u64 = read_u16(&slot_data, SLOT_OFFSETOF_LEN).into();
              if content_len > SLOT_LEN - SLOT_FIXED_FIELDS_LEN {
                panic!(
                  "data corruption: slot at {} contains invalid content length",
                  offset
                );
              }
              slot_data.truncate(as_usize!(SLOT_FIXED_FIELDS_LEN + content_len));
            }
            _ => panic!(
              "data corruption: slot at {} contains invalid content hashing indicator",
              offset
            ),
          };

          let actual_hash = &slot_data[..32];
          let expected_hash = blake3::hash(&slot_data[32..]);
          if actual_hash != expected_hash.as_bytes() {
            panic!(
              "data corruption: slot at {} contains hash {:x?} but data hashes to {:x?}",
              offset,
              actual_hash,
              expected_hash.as_bytes()
            );
          }

          let state = SlotState::try_from(slot_data[as_usize!(SLOT_OFFSETOF_STATE)]).unwrap();
          let index: u32 = (offset / SLOT_LEN).try_into().unwrap();
          match state {
            SlotState::Available => {
              let visible_time = read_ts(&slot_data, SLOT_OFFSETOF_VISIBLE_TS);
              available.lock().await.insert(index, visible_time);
            }
            SlotState::Vacant => {
              vacant.lock().await.add(index);
            }
          };

          // Use a counter instead of `offset / device_size` as slots are processed out of order.
          let completed = progress.fetch_add(1, Ordering::Relaxed);
          if completed % 4194304 == 0 {
            println!(
              "Loaded {:.2}%",
              completed as f64 / (layout.device_size / SLOT_LEN) as f64 * 100.0
            );
          };
        }
      })
      .await;

    let available = Arc::try_unwrap(available)
      .unwrap_or_else(|_| unreachable!())
      .into_inner();
    let vacant = Arc::try_unwrap(vacant)
      .unwrap_or_else(|_| unreachable!())
      .into_inner();
    LoadedData { available, vacant }
  }

  async fn read_poll_tag(&self, index: u32) -> Vec<u8> {
    self
      .device
      .read_at(slot_offset(index) + SLOT_OFFSETOF_POLL_TAG, 30)
      .await
  }

  async fn delete_message(&self, index: u32) -> () {
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest {
        data: SLOT_VACANT_TEMPLATE.clone(),
        offset: slot_offset(index),
      }])
      .await;
  }

  async fn read_message(&self, index: u32) -> MessageOnDisk {
    let slot_data = self.device.read_at(slot_offset(index), SLOT_LEN).await;

    let created = read_ts(&slot_data, SLOT_OFFSETOF_CREATED_TS);
    let poll_count = read_u32(&slot_data, SLOT_OFFSETOF_POLL_COUNT);
    let len: u64 = read_u16(&slot_data, SLOT_OFFSETOF_LEN).into();
    let contents =
      String::from_utf8(u64_slice(&slot_data, SLOT_OFFSETOF_CONTENTS, len).to_vec()).unwrap();

    MessageOnDisk {
      created,
      poll_count,
      contents,
    }
  }

  async fn update_message_metadata(
    &self,
    index: u32,
    MessageMetadataUpdate {
      state,
      poll_tag,
      created_time,
      visible_time,
      poll_count,
    }: MessageMetadataUpdate,
  ) {
    // For efficiency, hash does not cover contents, as contents have already been durabilty persisted. This also saves wasting writes on rewriting contents.
    let mut slot_data = vec![0u8; as_usize!(SLOT_FIXED_FIELDS_LEN)];

    u64_slice_write(&mut slot_data, SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS, &[0]);
    u64_slice_write(&mut slot_data, SLOT_OFFSETOF_STATE, &[state as u8]);
    u64_slice_write(&mut slot_data, SLOT_OFFSETOF_POLL_TAG, &poll_tag);
    u64_slice_write(
      &mut slot_data,
      SLOT_OFFSETOF_CREATED_TS,
      &created_time.timestamp().to_be_bytes(),
    );
    u64_slice_write(
      &mut slot_data,
      SLOT_OFFSETOF_VISIBLE_TS,
      &visible_time.timestamp().to_be_bytes(),
    );
    u64_slice_write(
      &mut slot_data,
      SLOT_OFFSETOF_POLL_COUNT,
      &poll_count.to_be_bytes(),
    );
    let hash = blake3::hash(&slot_data[32..]);
    u64_slice_write(&mut slot_data, SLOT_OFFSETOF_HASH, hash.as_bytes());

    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest {
        data: slot_data,
        offset: slot_offset(index),
      }])
      .await;
  }

  async fn create_messages(&self, creations: Vec<MessageCreation>) {
    let mut writes = vec![];
    for MessageCreation {
      index,
      state,
      contents,
      visible_time,
    } in creations
    {
      let mut slot_data = vec![0u8; as_usize!(SLOT_FIXED_FIELDS_LEN) + contents.len()];

      u64_slice_write(&mut slot_data, SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS, &[1]);
      u64_slice_write(&mut slot_data, SLOT_OFFSETOF_STATE, &[state as u8]);
      u64_slice_write(
        &mut slot_data,
        SLOT_OFFSETOF_CREATED_TS,
        &Utc::now().timestamp().to_be_bytes(),
      );
      u64_slice_write(
        &mut slot_data,
        SLOT_OFFSETOF_VISIBLE_TS,
        &visible_time.timestamp().to_be_bytes(),
      );
      u64_slice_write(
        &mut slot_data,
        SLOT_OFFSETOF_LEN,
        &u16::try_from(contents.len()).unwrap().to_be_bytes(),
      );
      u64_slice_write(&mut slot_data, SLOT_OFFSETOF_CONTENTS, contents.as_bytes());

      let hash = blake3::hash(&slot_data[32..]);
      u64_slice_write(&mut slot_data, SLOT_OFFSETOF_HASH, hash.as_bytes());

      writes.push(WriteRequest {
        data: slot_data,
        offset: slot_offset(index),
      });
    }

    self.device.write_at_with_delayed_sync(writes).await;
  }
}
