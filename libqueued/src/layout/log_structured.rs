use super::LoadedData;
use super::MessageCreation;
use super::MessageOnDisk;
use super::MessagePoll;
use super::StorageLayout;
use crate::invisible::InvisibleMessages;
use crate::metrics::Metrics;
use crate::visible::VisibleMessages;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use dashmap::DashMap;
use futures::stream::iter;
use futures::StreamExt;
use log_structured::GarbageCheck;
use log_structured::GarbageChecker;
use log_structured::LogStructured;
use num_enum::TryFromPrimitive;
use off64::usz;
use off64::Off64;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::sync::Arc;
use write_journal::WriteJournal;

#[derive(TryFromPrimitive, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum LogEntryType {
  Create,
  Delete,
  DummyFiller,
  Poll,
  Update,
}

const LOGENT_OFFSETOF_TYPE: u64 = 0;

const LOGENT_CREATE_OFFSETOF_ID: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_CREATE_OFFSETOF_CREATED_TS: u64 = LOGENT_CREATE_OFFSETOF_ID + 8;
const LOGENT_CREATE_OFFSETOF_VISIBLE_TS: u64 = LOGENT_CREATE_OFFSETOF_CREATED_TS + 8;
const LOGENT_CREATE_OFFSETOF_LEN: u64 = LOGENT_CREATE_OFFSETOF_VISIBLE_TS + 8;
const LOGENT_CREATE_OFFSETOF_CONTENTS: u64 = LOGENT_CREATE_OFFSETOF_LEN + 2;

const LOGENT_POLL_OFFSETOF_ID: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_POLL_OFFSETOF_POLL_TAG: u64 = LOGENT_POLL_OFFSETOF_ID + 8;
const LOGENT_POLL_OFFSETOF_VISIBLE_TS: u64 = LOGENT_POLL_OFFSETOF_POLL_TAG + 30;
const LOGENT_POLL_SIZE: u64 = LOGENT_POLL_OFFSETOF_VISIBLE_TS + 8;

const LOGENT_UPDATE_OFFSETOF_ID: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_UPDATE_OFFSETOF_VISIBLE_TS: u64 = LOGENT_UPDATE_OFFSETOF_ID + 8;
const LOGENT_UPDATE_SIZE: u64 = LOGENT_UPDATE_OFFSETOF_VISIBLE_TS + 8;

const LOGENT_DELETE_OFFSETOF_ID: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_DELETE_SIZE: u64 = LOGENT_DELETE_OFFSETOF_ID + 8;

#[derive(Default)]
struct MessageState {
  // These offsets are physical offsets.
  create_offset: u64,
  poll_offset: u64,
  poll_count: u32,
}

struct LogEntryGarbageChecker {
  device: SeekableAsyncFile,
  message_state: Arc<DashMap<u64, MessageState>>,
}

#[async_trait]
impl GarbageChecker for LogEntryGarbageChecker {
  async fn check_offset(&self, physical_offset: u64) -> GarbageCheck {
    let typ = LogEntryType::try_from(self.device.read_at(physical_offset, 1).await[0]).unwrap();
    let (id, ent_size) = match typ {
      LogEntryType::DummyFiller => {
        return GarbageCheck::IsPadding;
      }
      LogEntryType::Create => {
        let raw = self
          .device
          .read_at(physical_offset, LOGENT_CREATE_OFFSETOF_CONTENTS)
          .await;
        let id = raw.read_u64_be_at(LOGENT_POLL_OFFSETOF_ID);
        let len: u64 = raw.read_u16_be_at(LOGENT_CREATE_OFFSETOF_LEN).into();
        (id, LOGENT_CREATE_OFFSETOF_CONTENTS + len)
      }
      LogEntryType::Poll => {
        let id = self
          .device
          .read_u64_at(physical_offset + LOGENT_POLL_OFFSETOF_ID)
          .await;
        (id, LOGENT_POLL_SIZE)
      }
      LogEntryType::Update => {
        let id = self
          .device
          .read_u64_at(physical_offset + LOGENT_UPDATE_OFFSETOF_ID)
          .await;
        (id, LOGENT_UPDATE_SIZE)
      }
      LogEntryType::Delete => {
        let id = self
          .device
          .read_u64_at(physical_offset + LOGENT_DELETE_OFFSETOF_ID)
          .await;
        (id, LOGENT_DELETE_SIZE)
      }
    };
    if self.message_state.contains_key(&id) {
      GarbageCheck::IsNotGarbage
    } else {
      GarbageCheck::IsGarbage(ent_size)
    }
  }
}

pub(crate) struct LogStructuredLayout {
  device_size: u64,
  device: SeekableAsyncFile,
  log_structured: LogStructured<LogEntryGarbageChecker>,
  message_state: Arc<DashMap<u64, MessageState>>,
}

impl LogStructuredLayout {
  pub fn new(
    device: SeekableAsyncFile,
    device_offset: u64,
    device_size: u64,
    journal: Arc<WriteJournal>,
    metrics: Arc<Metrics>,
  ) -> Self {
    let message_state = Arc::new(DashMap::new());
    let log_structured = LogStructured::new(
      device.clone(),
      device_offset,
      device_size,
      journal.clone(),
      LogEntryGarbageChecker {
        device: device.clone(),
        message_state: message_state.clone(),
      },
      vec![LogEntryType::DummyFiller as u8],
      metrics.free_space_gauge.clone(),
    );
    Self {
      device: device.clone(),
      device_size,
      log_structured,
      message_state: message_state.clone(),
    }
  }

  fn update_message_state_on_create(&self, id: u64, physical_offset: u64) {
    let existing = self.message_state.insert(id, MessageState {
      create_offset: physical_offset,
      ..Default::default()
    });
    assert!(existing.is_none());
  }

  fn update_message_state_on_delete(&self, id: u64) {
    self.message_state.remove(&id).unwrap();
  }

  fn update_message_state_on_poll(&self, id: u64, physical_offset: u64) {
    let mut state = self.message_state.get_mut(&id).unwrap();
    state.poll_offset = physical_offset;
    state.poll_count += 1;
  }
}

#[async_trait]
impl StorageLayout for LogStructuredLayout {
  fn max_contents_len(&self) -> u64 {
    u64::MAX
  }

  async fn start_background_loops(&self) {
    self.log_structured.start_background_loops().await;
  }

  async fn format_device(&self) {
    self.log_structured.format_device().await;
  }

  async fn load_data_from_device(&self, metrics: Arc<Metrics>) -> LoadedData {
    self.log_structured.load_state_from_device().await;

    // Since we guarantee that messages are polled in order of visibility time, we can't directly insert into VisibleMessages, we'll need to sort them first. Note that an earlier poll can make an earlier message have a visibility timeout later than a later message, so we can't use our log structured layout to our advantage here, nor directly insert into InvisibleMessages either.
    let mut messages = HashMap::<u64, DateTime<Utc>>::new();
    let (head, tail) = self.log_structured.get_head_and_tail().await;

    let mut virtual_offset = head;
    while virtual_offset < tail {
      let physical_offset = self.log_structured.physical_offset(virtual_offset);
      let typ = LogEntryType::try_from(self.device.read_at(physical_offset, 1).await[0]).unwrap();
      match typ {
        LogEntryType::DummyFiller => {
          virtual_offset += self.device_size - physical_offset;
        }
        LogEntryType::Create => {
          let raw = self
            .device
            .read_at(physical_offset, LOGENT_CREATE_OFFSETOF_CONTENTS)
            .await;
          let id = raw.read_u64_be_at(LOGENT_POLL_OFFSETOF_ID);
          let visible_time = raw.read_timestamp_be_at(LOGENT_CREATE_OFFSETOF_VISIBLE_TS);
          let len: u64 = raw.read_u16_be_at(LOGENT_CREATE_OFFSETOF_LEN).into();
          self.update_message_state_on_create(id, physical_offset);
          messages.insert(id, visible_time);
          virtual_offset += LOGENT_CREATE_OFFSETOF_CONTENTS + len;
        }
        LogEntryType::Poll => {
          let raw = self.device.read_at(physical_offset, LOGENT_POLL_SIZE).await;
          let id = raw.read_u64_be_at(LOGENT_POLL_OFFSETOF_ID);
          let visible_time = raw.read_timestamp_be_at(LOGENT_POLL_OFFSETOF_VISIBLE_TS);
          self.update_message_state_on_poll(id, physical_offset);
          messages.insert(id, visible_time);
          virtual_offset += LOGENT_POLL_SIZE;
        }
        LogEntryType::Update => {
          let raw = self
            .device
            .read_at(physical_offset, LOGENT_UPDATE_SIZE)
            .await;
          let id = raw.read_u64_be_at(LOGENT_UPDATE_OFFSETOF_ID);
          let visible_time = raw.read_timestamp_be_at(LOGENT_UPDATE_OFFSETOF_VISIBLE_TS);
          messages.insert(id, visible_time);
          virtual_offset += LOGENT_UPDATE_SIZE;
        }
        LogEntryType::Delete => {
          let raw = self
            .device
            .read_at(physical_offset, LOGENT_DELETE_SIZE)
            .await;
          let id = raw.read_u64_be_at(LOGENT_DELETE_OFFSETOF_ID);
          self.update_message_state_on_delete(id);
          messages.remove(&id).unwrap();
          virtual_offset += LOGENT_DELETE_SIZE;
        }
      };
    }

    let now = Utc::now();
    let mut invisible = InvisibleMessages::new(metrics.clone());
    let mut visible_unsorted = Vec::new();
    for (id, visible_ts) in messages.into_iter() {
      if visible_ts <= now {
        visible_unsorted.push((id, visible_ts));
      } else {
        invisible.insert(id, visible_ts);
      };
    }
    visible_unsorted.sort_unstable_by_key(|(_, visible_ts)| *visible_ts);
    let mut visible_messages = LinkedList::new();
    for (id, _) in visible_unsorted {
      visible_messages.push_back(id);
    }
    let visible = VisibleMessages::new_with_list(visible_messages, metrics);

    LoadedData { invisible, visible }
  }

  async fn read_poll_tag(&self, id: u64) -> Vec<u8> {
    let offset = self.message_state.get(&id).unwrap().poll_offset;
    self
      .device
      .read_at(offset + LOGENT_POLL_OFFSETOF_POLL_TAG, 30)
      .await
  }

  async fn update_visibility_time(&self, id: u64, visible_time: DateTime<Utc>) {
    let mut data = vec![0u8; usz!(LOGENT_UPDATE_SIZE)];
    data[usz!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Update as u8;
    data.write_u64_be_at(LOGENT_UPDATE_OFFSETOF_ID, id);
    data.write_timestamp_be_at(LOGENT_UPDATE_OFFSETOF_VISIBLE_TS, visible_time);
    let bump = self.log_structured.bump_tail(data.len()).await;
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest::new(bump.acquired_physical_offset, data)])
      .await;
    self.log_structured.commit_tail_bump(bump).await;
  }

  async fn delete_message(&self, id: u64) {
    let mut data = vec![0u8; usz!(LOGENT_DELETE_SIZE)];
    data[usz!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Delete as u8;
    data.write_u64_be_at(LOGENT_DELETE_OFFSETOF_ID, id);
    let bump = self.log_structured.bump_tail(data.len()).await;
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest::new(bump.acquired_physical_offset, data)])
      .await;
    self.log_structured.commit_tail_bump(bump).await;
    // Ensure to do this after physically writing to disk, so GC can clean that written data if necessary.
    self.update_message_state_on_delete(id);
  }

  async fn read_message(&self, id: u64) -> MessageOnDisk {
    let state = self.message_state.get(&id).unwrap();
    let offset = state.create_offset;
    let poll_count = state.poll_count;
    let slot_data = self
      .device
      .read_at(offset, LOGENT_CREATE_OFFSETOF_CONTENTS)
      .await;

    let created = slot_data.read_timestamp_be_at(LOGENT_CREATE_OFFSETOF_CREATED_TS);
    let len: u64 = slot_data.read_u16_be_at(LOGENT_CREATE_OFFSETOF_LEN).into();
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

  async fn mark_as_polled(&self, id: u64, update: MessagePoll) {
    let mut data = vec![0u8; usz!(LOGENT_POLL_SIZE)];
    data[usz!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Poll as u8;
    data.write_u64_be_at(LOGENT_POLL_OFFSETOF_ID, id);
    data.write_slice_at(LOGENT_POLL_OFFSETOF_POLL_TAG, &update.poll_tag);
    data.write_timestamp_be_at(LOGENT_POLL_OFFSETOF_VISIBLE_TS, update.visible_time);
    let bump = self.log_structured.bump_tail(data.len()).await;
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest::new(bump.acquired_physical_offset, data)])
      .await;
    self.log_structured.commit_tail_bump(bump).await;
    self.update_message_state_on_poll(id, bump.acquired_physical_offset);
  }

  async fn create_messages(&self, creations: Vec<MessageCreation>) {
    let mut writes = vec![];
    // We must commit all bumps, not just the last/largest/highest one, due to the way our commit logic currently works.
    let mut bumps = vec![];
    for MessageCreation {
      id,
      visible_time,
      contents,
      ..
    } in creations
    {
      let mut data = vec![0u8; usz!(LOGENT_CREATE_OFFSETOF_CONTENTS) + contents.len()];
      data[usz!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Create as u8;
      data.write_u64_be_at(LOGENT_CREATE_OFFSETOF_ID, id);
      data.write_timestamp_be_at(LOGENT_CREATE_OFFSETOF_CREATED_TS, Utc::now());
      data.write_timestamp_be_at(LOGENT_CREATE_OFFSETOF_VISIBLE_TS, visible_time);
      data.write_u16_be_at(
        LOGENT_CREATE_OFFSETOF_LEN,
        u16::try_from(contents.len()).unwrap(),
      );
      data.write_slice_at(LOGENT_CREATE_OFFSETOF_CONTENTS, contents.as_bytes());
      let bump = self.log_structured.bump_tail(data.len()).await;
      writes.push(WriteRequest::new(bump.acquired_physical_offset, data));
      bumps.push(bump);
      // Ensure to do this before physically writing to disk, so GC doesn't erase that written data.
      self.update_message_state_on_create(id, bump.acquired_physical_offset);
    }

    self.device.write_at_with_delayed_sync(writes).await;
    iter(bumps)
      .for_each_concurrent(None, |bump| async move {
        self.log_structured.commit_tail_bump(bump).await;
      })
      .await;
  }
}
