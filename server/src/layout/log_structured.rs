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
use crate::util::u64_slice_write;
use crate::vacant::VacantSlots;
use async_trait::async_trait;
use chrono::Utc;
use num_enum::TryFromPrimitive;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(TryFromPrimitive)]
#[repr(u8)]
enum LogEntryType {
  Create,
  Poll,
  Delete,
}

const STATE_OFFSETOF_HEAD: u64 = 0;
const STATE_OFFSETOF_TAIL: u64 = STATE_OFFSETOF_HEAD + 8;
const STATE_SIZE: u64 = STATE_OFFSETOF_TAIL + 8;

const LOGENT_START: u64 = STATE_SIZE;

const LOGENT_CREATE_OFFSETOF_INDEX: u64 = 0;
const LOGENT_CREATE_OFFSETOF_CREATED_TS: u64 = LOGENT_CREATE_OFFSETOF_INDEX + 4;
const LOGENT_CREATE_OFFSETOF_VISIBLE_TS: u64 = LOGENT_CREATE_OFFSETOF_CREATED_TS + 8;
const LOGENT_CREATE_OFFSETOF_LEN: u64 = LOGENT_CREATE_OFFSETOF_VISIBLE_TS + 8;
const LOGENT_CREATE_OFFSETOF_CONTENTS: u64 = LOGENT_CREATE_OFFSETOF_LEN + 2;

const LOGENT_POLL_OFFSETOF_INDEX: u64 = 0;
const LOGENT_POLL_OFFSETOF_POLL_TAG: u64 = LOGENT_POLL_OFFSETOF_INDEX + 4;
const LOGENT_POLL_OFFSETOF_VISIBLE_TS: u64 = LOGENT_POLL_OFFSETOF_POLL_TAG + 30;
const LOGENT_POLL_SIZE: u64 = LOGENT_POLL_OFFSETOF_VISIBLE_TS + 8;

const LOGENT_DELETE_OFFSETOF_INDEX: u64 = 0;
const LOGENT_DELETE_SIZE: u64 = LOGENT_DELETE_OFFSETOF_INDEX + 4;

#[derive(Default)]
struct MessageState {
  // These offsets are actual offsets i.e. after modulo by device size.
  create_offset: u64,
  poll_offset: u64,
  poll_count: u32,
}

#[derive(Default)]
struct LogState {
  head: u64,
  tail: u64,
  // This is to prevent the scenario where a write at a later offset (i.e. subsequent request B) finishes before a write at an earlier offset (i.e. earlier request A); we can't immediately update the tail on disk after writing B because it would include A, which hasn't been synced yet.
  pending_tail_bumps: BTreeMap<u64, bool>,
}

pub struct LogStructuredLayout {
  device: SeekableAsyncFile,
  device_size: u64,
  // We don't store this in AvailableMessages as that will more than double the size of the index but not be useful for FixedSlotsLayout.
  index_state: HashMap<u32, MessageState>,
  log_state: Mutex<LogState>,
}

#[derive(Clone, Copy)]
struct TailBump {
  acquired_actual_offset: u64,
  uncommitted_virtual_offset: u64,
}

impl LogStructuredLayout {
  pub fn new(device: SeekableAsyncFile, device_size: u64) -> Self {
    Self {
      device,
      device_size,
      index_state: HashMap::new(),
      log_state: Mutex::new(LogState::default()),
    }
  }

  fn actual_offset(&self, virtual_offset: u64) -> u64 {
    LOGENT_START + (virtual_offset % (self.device_size - LOGENT_START))
  }

  // How to use: bump tail first, perform the write to the acquired tail offset, then persist the bumped tail. If tail is committed first, then it'll point to invalid data if the main data write doesn't complete.
  async fn bump_tail(&self, usage: usize) -> TailBump {
    let usage: u64 = usage.try_into().unwrap();
    let mut state = self.log_state.lock().await;
    let tail = state.tail;
    // TODO Check if overlaps with head.
    state.tail += usage;
    let new_tail = state.tail;
    state.pending_tail_bumps.insert(new_tail, false);
    TailBump {
      acquired_actual_offset: self.actual_offset(tail),
      uncommitted_virtual_offset: new_tail,
    }
  }

  async fn commit_tail_bump(&self, bump: TailBump) {
    // It's fine performance-wise to hold the lock, even while asynchronously writing, since only one write is allowed to be happening to this shared location at one time anyway.
    // TODO This still prevents others from getting a bump (not just commit_tail_bump).
    let mut state = self.log_state.lock().await;
    *state
      .pending_tail_bumps
      .get_mut(&bump.uncommitted_virtual_offset)
      .unwrap() = true;

    let mut new_tail_to_write = None;
    loop {
      let Some(e) = state.pending_tail_bumps.first_entry() else {
        break;
      };
      if !e.get() {
        break;
      };
      let (k, _) = e.remove_entry();
      new_tail_to_write = Some(k);
    }

    if let Some(new_tail_to_write) = new_tail_to_write {
      // TODO Journal this.
      self
        .device
        .write_at_with_delayed_sync(vec![WriteRequest {
          data: new_tail_to_write.to_be_bytes().to_vec(),
          offset: STATE_OFFSETOF_TAIL,
        }])
        .await;
    };
  }
}

#[async_trait]
impl StorageLayout for LogStructuredLayout {
  fn max_contents_len(&self) -> u64 {
    u64::MAX
  }

  async fn format_device(&self) {
    self
      .device
      .write_at(STATE_OFFSETOF_HEAD, 0u64.to_be_bytes().to_vec())
      .await;
    self
      .device
      .write_at(STATE_OFFSETOF_TAIL, 0u64.to_be_bytes().to_vec())
      .await;
  }

  async fn load_data_from_device(&mut self, metrics: Arc<Metrics>) -> LoadedData {
    let mut available = AvailableMessages::new(metrics.clone());
    let mut vacant = VacantSlots::new(metrics.clone());
    vacant.fill(0, u32::MAX);

    let head = self.device.read_u64_at(STATE_OFFSETOF_HEAD).await;
    let tail = self.device.read_u64_at(STATE_OFFSETOF_TAIL).await;
    {
      let mut log_state = self.log_state.lock().await;
      log_state.head = head;
      log_state.tail = tail;
    };

    let mut virtual_offset = head;
    while virtual_offset < tail {
      let typ = LogEntryType::try_from(
        self
          .device
          .read_at(self.actual_offset(virtual_offset), 1)
          .await[0],
      )
      .unwrap();
      virtual_offset += 1;
      match typ {
        LogEntryType::Create => {
          let raw = self
            .device
            .read_at(
              self.actual_offset(virtual_offset),
              LOGENT_CREATE_OFFSETOF_CONTENTS,
            )
            .await;
          let index = read_u32(&raw, LOGENT_POLL_OFFSETOF_INDEX);
          let visible_time = read_ts(&raw, LOGENT_CREATE_OFFSETOF_VISIBLE_TS);
          let len: u64 = read_u16(&raw, LOGENT_CREATE_OFFSETOF_LEN).into();
          self.index_state.entry(index).or_default().create_offset =
            self.actual_offset(virtual_offset);
          available.insert(index, visible_time);
          vacant.remove_specific(index);
          virtual_offset += LOGENT_CREATE_OFFSETOF_CONTENTS + len;
        }
        LogEntryType::Poll => {
          let raw = self
            .device
            .read_at(self.actual_offset(virtual_offset), LOGENT_POLL_SIZE)
            .await;
          let index = read_u32(&raw, LOGENT_POLL_OFFSETOF_INDEX);
          let visible_time = read_ts(&raw, LOGENT_POLL_OFFSETOF_VISIBLE_TS);
          let actual_offset = self.actual_offset(virtual_offset);
          let mut state = self.index_state.get_mut(&index).unwrap();
          state.poll_offset = actual_offset;
          state.poll_count += 1;
          available.update_timestamp(index, visible_time);
          virtual_offset += LOGENT_POLL_SIZE;
        }
        LogEntryType::Delete => {
          let raw = self
            .device
            .read_at(self.actual_offset(virtual_offset), LOGENT_DELETE_SIZE)
            .await;
          let index = read_u32(&raw, LOGENT_DELETE_OFFSETOF_INDEX);
          self.index_state.remove(&index).unwrap();
          available.remove(index);
          vacant.add(index);
          virtual_offset += LOGENT_DELETE_SIZE;
        }
      };
    }

    LoadedData { available, vacant }
  }

  async fn read_poll_tag(&self, index: u32) -> Vec<u8> {
    let offset = self.index_state.get(&index).unwrap().poll_offset;
    self
      .device
      .read_at(offset + LOGENT_POLL_OFFSETOF_POLL_TAG, 30)
      .await
  }

  async fn delete_message(&self, index: u32) {
    let mut data = vec![0u8; as_usize!(LOGENT_DELETE_SIZE) + 1];
    data[0] = LogEntryType::Delete as u8;
    u64_slice_write(
      &mut data,
      LOGENT_DELETE_OFFSETOF_INDEX,
      &index.to_be_bytes(),
    );
    let bump = self.bump_tail(data.len()).await;
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest {
        data,
        offset: bump.acquired_actual_offset,
      }])
      .await;
    self.commit_tail_bump(bump).await;
  }

  async fn read_message(&self, index: u32) -> MessageOnDisk {
    let state = self.index_state.get(&index).unwrap();
    let offset = state.create_offset;
    let poll_count = state.poll_count;
    let slot_data = self
      .device
      .read_at(offset, LOGENT_CREATE_OFFSETOF_CONTENTS)
      .await;

    let created = read_ts(&slot_data, LOGENT_CREATE_OFFSETOF_CREATED_TS);
    let len: u64 = read_u16(&slot_data, LOGENT_CREATE_OFFSETOF_LEN).into();
    let contents = String::from_utf8(
      self
        .device
        .read_at(offset + LOGENT_CREATE_OFFSETOF_CONTENTS, len)
        .await,
    )
    .unwrap();

    MessageOnDisk {
      created,
      poll_count,
      contents,
    }
  }

  async fn update_message_metadata(&self, index: u32, update: MessageMetadataUpdate) {
    let mut data = vec![0u8; as_usize!(LOGENT_POLL_SIZE) + 1];
    data[0] = LogEntryType::Poll as u8;
    u64_slice_write(&mut data, LOGENT_POLL_OFFSETOF_INDEX, &index.to_be_bytes());
    u64_slice_write(&mut data, LOGENT_POLL_OFFSETOF_POLL_TAG, &update.poll_tag);
    u64_slice_write(
      &mut data,
      LOGENT_POLL_OFFSETOF_VISIBLE_TS,
      &update.visible_time.timestamp().to_be_bytes(),
    );
    let bump = self.bump_tail(data.len()).await;
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest {
        data,
        offset: bump.acquired_actual_offset,
      }])
      .await;
    self.commit_tail_bump(bump).await;
  }

  async fn create_messages(&self, creations: Vec<MessageCreation>) {
    let mut writes = vec![];
    let mut last_bump = None;
    for MessageCreation {
      index,
      visible_time,
      contents,
      ..
    } in creations
    {
      let mut data = vec![0u8; as_usize!(LOGENT_CREATE_OFFSETOF_CONTENTS) + 1 + contents.len()];
      data[0] = LogEntryType::Create as u8;
      u64_slice_write(
        &mut data,
        LOGENT_CREATE_OFFSETOF_INDEX,
        &index.to_be_bytes(),
      );
      u64_slice_write(
        &mut data,
        LOGENT_CREATE_OFFSETOF_CREATED_TS,
        &Utc::now().timestamp().to_be_bytes(),
      );
      u64_slice_write(
        &mut data,
        LOGENT_CREATE_OFFSETOF_VISIBLE_TS,
        &visible_time.timestamp().to_be_bytes(),
      );
      u64_slice_write(
        &mut data,
        LOGENT_CREATE_OFFSETOF_LEN,
        &u16::try_from(contents.len()).unwrap().to_be_bytes(),
      );
      u64_slice_write(
        &mut data,
        LOGENT_CREATE_OFFSETOF_CONTENTS,
        contents.as_bytes(),
      );
      let bump = self.bump_tail(data.len().try_into().unwrap()).await;
      writes.push(WriteRequest {
        data,
        offset: bump.acquired_actual_offset,
      });
      last_bump = Some(bump);
    }

    self.device.write_at_with_delayed_sync(writes).await;
    self.commit_tail_bump(last_bump.unwrap()).await;
  }
}
