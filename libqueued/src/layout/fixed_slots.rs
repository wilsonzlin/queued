use super::LoadedData;
use super::MessageCreation;
use super::MessageOnDisk;
use super::MessagePoll;
use super::StorageLayout;
use crate::invisible::InvisibleMessages;
use crate::metrics::Metrics;
use crate::util::repeated_copy;
use crate::vacant::VacantSlots;
use crate::visible::VisibleMessages;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use dashmap::DashMap;
use futures::StreamExt;
use lazy_static::lazy_static;
use num_enum::TryFromPrimitive;
use off64::chrono::Off64ReadChrono;
use off64::chrono::Off64WriteMutChrono;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use off64::Off64Read;
use off64::Off64WriteMut;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use std::cmp::min;
use std::collections::LinkedList;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tokio::sync::Mutex;

#[derive(Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u8)]
enum SlotState {
  Vacant = 0,
  Available = 1,
}

const SLOT_LEN: u64 = 1024;
const SLOT_OFFSETOF_HASH: u64 = 0;
const SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS: u64 = SLOT_OFFSETOF_HASH + 32;
const SLOT_OFFSETOF_STATE: u64 = SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS + 1;
const SLOT_OFFSETOF_ID: u64 = SLOT_OFFSETOF_STATE + 1;
const SLOT_OFFSETOF_POLL_TAG: u64 = SLOT_OFFSETOF_ID + 8;
const SLOT_OFFSETOF_CREATED_TS: u64 = SLOT_OFFSETOF_POLL_TAG + 30;
const SLOT_OFFSETOF_VISIBLE_TS: u64 = SLOT_OFFSETOF_CREATED_TS + 8;
const SLOT_OFFSETOF_POLL_COUNT: u64 = SLOT_OFFSETOF_VISIBLE_TS + 8;
const SLOT_OFFSETOF_LEN: u64 = SLOT_OFFSETOF_POLL_COUNT + 4;
const SLOT_OFFSETOF_CONTENTS: u64 = SLOT_OFFSETOF_LEN + 2;
const SLOT_FIXED_FIELDS_LEN: u64 = SLOT_OFFSETOF_CONTENTS;
const MESSAGE_SLOT_CONTENTS_LEN_MAX: u64 = SLOT_LEN - SLOT_FIXED_FIELDS_LEN;

lazy_static! {
  static ref SLOT_VACANT_TEMPLATE: Vec<u8> = {
    let mut slot_template = vec![0u8; usz!(SLOT_FIXED_FIELDS_LEN)];
    let hash = blake3::hash(&slot_template[32..]);
    slot_template[..32].copy_from_slice(hash.as_bytes());
    slot_template
  };
}

#[derive(Default)]
struct MessageState {
  slot_index: u32,
}

pub(crate) struct FixedSlotsLayout {
  device: SeekableAsyncFile,
  device_offset: u64,
  device_size: u64,
  message_state: DashMap<u64, MessageState>,
  vacant: Arc<Mutex<VacantSlots>>,
}

impl FixedSlotsLayout {
  pub fn new(
    device: SeekableAsyncFile,
    device_offset: u64,
    device_size: u64,
    metrics: Arc<Metrics>,
  ) -> Self {
    Self {
      device,
      device_offset,
      device_size,
      message_state: DashMap::new(),
      vacant: Arc::new(Mutex::new(VacantSlots::new(
        (device_size - device_offset) / SLOT_LEN,
        metrics,
      ))),
    }
  }
}

impl FixedSlotsLayout {
  fn slot_offset(&self, index: u32) -> u64 {
    self.device_offset + (u64::from(index) * SLOT_LEN)
  }

  fn slot_index(&self, offset: u64) -> u32 {
    assert_eq!((offset - self.device_offset) % SLOT_LEN, 0);
    ((offset - self.device_offset) / SLOT_LEN)
      .try_into()
      .unwrap()
  }
}

#[async_trait]
impl StorageLayout for FixedSlotsLayout {
  fn max_contents_len(&self) -> u64 {
    MESSAGE_SLOT_CONTENTS_LEN_MAX
  }

  async fn start_background_loops(&self) {}

  async fn format_device(&self) {
    let mut template_base_padded = vec![0u8; usz!(SLOT_LEN)];
    template_base_padded[..usz!(SLOT_FIXED_FIELDS_LEN)].copy_from_slice(&SLOT_VACANT_TEMPLATE);

    let mut template = vec![
      0u8;
      min(
        usz!(self.device_size - self.device_offset),
        1024 * 1024 * 1024
      )
    ];
    repeated_copy(&mut template, &template_base_padded);
    let template_len: u64 = template.len().try_into().unwrap();

    let mut next = self.device_offset;
    while next < self.device_size {
      self
        .device
        .write_at(
          next,
          template[..min(template.len(), usz!(self.device_size - next))].to_vec(),
        )
        .await;
      next += template_len;
    }

    self.device.sync_data().await;
  }

  async fn load_data_from_device(&self, metrics: Arc<Metrics>) -> LoadedData {
    let now = Utc::now();
    let invisible = Arc::new(Mutex::new(InvisibleMessages::new(metrics.clone())));
    // Since we guarantee that messages are polled in order of visibility time, we can't directly insert into VisibleMessages, we'll need to sort them first.
    let visible_unsorted = Arc::new(Mutex::new(Vec::new()));

    futures::stream::iter((self.device_offset..self.device_size).step_by(usz!(SLOT_LEN)))
      .for_each_concurrent(None, |offset| {
        let invisible = invisible.clone();
        let visible_unsorted = visible_unsorted.clone();
        let vacant = self.vacant.clone();

        async move {
          let mut slot_data = self.device.read_at(offset, SLOT_LEN).await.to_vec();

          let hash_includes_contents = slot_data[usz!(SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS)];
          match hash_includes_contents {
            0 => slot_data.truncate(usz!(SLOT_FIXED_FIELDS_LEN)),
            1 => {
              let content_len: u64 = slot_data.read_u16_be_at(SLOT_OFFSETOF_LEN).into();
              if content_len > SLOT_LEN - SLOT_FIXED_FIELDS_LEN {
                panic!(
                  "data corruption: slot at {} contains invalid content length",
                  offset
                );
              }
              slot_data.truncate(usz!(SLOT_FIXED_FIELDS_LEN + content_len));
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

          let state = SlotState::try_from(slot_data[usz!(SLOT_OFFSETOF_STATE)]).unwrap();
          let slot_index = self.slot_index(offset);
          let id = slot_data.read_u64_be_at(SLOT_OFFSETOF_ID);
          match state {
            SlotState::Available => {
              let visible_time = slot_data.read_timestamp_be_at(SLOT_OFFSETOF_VISIBLE_TS);
              if visible_time <= now {
                visible_unsorted.lock().await.push((id, visible_time));
              } else {
                invisible.lock().await.insert(id, visible_time);
              };
              let None = self.message_state.insert(id, MessageState {
                slot_index,
              }) else {
                unreachable!();
              };
            }
            SlotState::Vacant => {
              vacant.lock().await.add(slot_index);
            }
          };
        }
      })
      .await;

    let mut visible_unsorted = Arc::try_unwrap(visible_unsorted)
      .unwrap_or_else(|_| unreachable!())
      .into_inner();
    visible_unsorted.sort_unstable_by_key(|(_, visible_ts)| *visible_ts);
    let mut visible_list = LinkedList::new();
    for (id, _) in visible_unsorted {
      visible_list.push_back(id);
    }

    let invisible = Arc::try_unwrap(invisible)
      .unwrap_or_else(|_| unreachable!())
      .into_inner();
    let visible = VisibleMessages::new_with_list(visible_list, metrics.clone());
    LoadedData { invisible, visible }
  }

  async fn read_poll_tag(&self, id: u64) -> TinyBuf {
    let index = self.message_state.get(&id).unwrap().slot_index;
    self
      .device
      .read_at(self.slot_offset(index) + SLOT_OFFSETOF_POLL_TAG, 30)
      .await
  }

  async fn update_visibility_time(&self, id: u64, visible_time: DateTime<Utc>) {
    let index = self.message_state.get(&id).unwrap().slot_index;
    let slot_offset = self.slot_offset(index);
    let mut slot_data = self.device.read_at(slot_offset, SLOT_LEN).await.to_vec();
    let visible_time_bytes = visible_time.timestamp().to_be_bytes().to_vec();
    slot_data.write_at(SLOT_OFFSETOF_VISIBLE_TS, &visible_time_bytes);
    let hash = blake3::hash(&slot_data[32..]);

    self
      .device
      .write_at_with_delayed_sync(vec![
        WriteRequest::new(slot_offset + SLOT_OFFSETOF_HASH, hash.as_bytes().to_vec()),
        WriteRequest::new(slot_offset + SLOT_OFFSETOF_VISIBLE_TS, visible_time_bytes),
      ])
      .await;
  }

  async fn delete_message(&self, id: u64) -> () {
    let index = self.message_state.remove(&id).unwrap().1.slot_index;
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest::new(
        self.slot_offset(index),
        SLOT_VACANT_TEMPLATE.clone(),
      )])
      .await;
    self.vacant.lock().await.add(index);
  }

  async fn read_message(&self, id: u64) -> MessageOnDisk {
    let index = self.message_state.get(&id).unwrap().slot_index;
    let slot_data = self.device.read_at(self.slot_offset(index), SLOT_LEN).await;

    let created = slot_data.read_timestamp_be_at(SLOT_OFFSETOF_CREATED_TS);
    let poll_count = slot_data.read_u32_be_at(SLOT_OFFSETOF_POLL_COUNT);
    let len: u64 = slot_data.read_u16_be_at(SLOT_OFFSETOF_LEN).into();
    let contents = TinyBuf::from_slice(slot_data.read_at(SLOT_OFFSETOF_CONTENTS, len));

    MessageOnDisk {
      created,
      poll_count,
      contents,
    }
  }

  async fn mark_as_polled(
    &self,
    id: u64,
    MessagePoll {
      poll_tag,
      visible_time,
      poll_count,
    }: MessagePoll,
  ) {
    let index = self.message_state.get(&id).unwrap().slot_index;
    // For efficiency, hash does not cover contents, as contents have already been durabilty persisted. This also saves wasting writes on rewriting contents.
    let mut slot_data = self
      .device
      .read_at(self.slot_offset(index), SLOT_FIXED_FIELDS_LEN)
      .await
      .to_vec();

    slot_data.write_at(SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS, &[0]);
    slot_data.write_at(SLOT_OFFSETOF_STATE, &[SlotState::Available as u8]);
    slot_data.write_at(SLOT_OFFSETOF_POLL_TAG, &poll_tag);
    slot_data.write_timestamp_be_at(SLOT_OFFSETOF_VISIBLE_TS, visible_time);
    slot_data.write_u32_be_at(SLOT_OFFSETOF_POLL_COUNT, poll_count);
    let hash = blake3::hash(&slot_data[32..]);
    slot_data.write_at(SLOT_OFFSETOF_HASH, hash.as_bytes());

    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest::new(self.slot_offset(index), slot_data)])
      .await;
  }

  async fn create_messages(&self, creations: Vec<MessageCreation>) {
    let indices = self.vacant.lock().await.take_up_to_n(creations.len());
    if indices.len() != creations.len() {
      panic!("out of disk space");
    };

    let mut writes = vec![];
    for (
      i,
      MessageCreation {
        id,
        contents,
        visible_time,
      },
    ) in creations.into_iter().enumerate()
    {
      let index = indices[i];
      let mut slot_data = vec![0u8; usz!(SLOT_FIXED_FIELDS_LEN) + contents.len()];

      slot_data.write_at(SLOT_OFFSETOF_HASH_INCLUDES_CONTENTS, &[1]);
      slot_data.write_at(SLOT_OFFSETOF_STATE, &[SlotState::Available as u8]);
      slot_data.write_u64_be_at(SLOT_OFFSETOF_ID, id);
      slot_data.write_timestamp_be_at(SLOT_OFFSETOF_CREATED_TS, Utc::now());
      slot_data.write_timestamp_be_at(SLOT_OFFSETOF_VISIBLE_TS, visible_time);
      slot_data.write_u16_be_at(SLOT_OFFSETOF_LEN, u16::try_from(contents.len()).unwrap());
      slot_data.write_at(SLOT_OFFSETOF_CONTENTS, &contents);

      let hash = blake3::hash(&slot_data[32..]);
      slot_data.write_at(SLOT_OFFSETOF_HASH, hash.as_bytes());

      writes.push(WriteRequest::new(self.slot_offset(index), slot_data));
    }

    self.device.write_at_with_delayed_sync(writes).await;
  }
}
