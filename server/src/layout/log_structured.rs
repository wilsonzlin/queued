use super::LoadedData;
use super::MessageCreation;
use super::MessageOnDisk;
use super::MessagePoll;
use super::StorageLayout;
use crate::invisible::InvisibleMessages;
use crate::metrics::Metrics;
use crate::util::as_usize;
use crate::util::read_ts;
use crate::util::read_u16;
use crate::util::read_u32;
use crate::util::u64_slice_write;
use crate::vacant::VacantSlots;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use dashmap::DashMap;
use futures::stream::iter;
use futures::Future;
use futures::StreamExt;
use num_enum::TryFromPrimitive;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(TryFromPrimitive, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum LogEntryType {
  Create,
  Delete,
  DummyFiller,
  Poll,
  Update,
}

const STATE_OFFSETOF_HEAD: u64 = 0;
const STATE_OFFSETOF_TAIL: u64 = STATE_OFFSETOF_HEAD + 8;
const STATE_SIZE: u64 = STATE_OFFSETOF_TAIL + 8;

const LOGENT_START: u64 = STATE_SIZE;

const LOGENT_OFFSETOF_TYPE: u64 = 0;

const LOGENT_CREATE_OFFSETOF_INDEX: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_CREATE_OFFSETOF_CREATED_TS: u64 = LOGENT_CREATE_OFFSETOF_INDEX + 4;
const LOGENT_CREATE_OFFSETOF_VISIBLE_TS: u64 = LOGENT_CREATE_OFFSETOF_CREATED_TS + 8;
const LOGENT_CREATE_OFFSETOF_LEN: u64 = LOGENT_CREATE_OFFSETOF_VISIBLE_TS + 8;
const LOGENT_CREATE_OFFSETOF_CONTENTS: u64 = LOGENT_CREATE_OFFSETOF_LEN + 2;

const LOGENT_POLL_OFFSETOF_INDEX: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_POLL_OFFSETOF_POLL_TAG: u64 = LOGENT_POLL_OFFSETOF_INDEX + 4;
const LOGENT_POLL_OFFSETOF_VISIBLE_TS: u64 = LOGENT_POLL_OFFSETOF_POLL_TAG + 30;
const LOGENT_POLL_SIZE: u64 = LOGENT_POLL_OFFSETOF_VISIBLE_TS + 8;

const LOGENT_UPDATE_OFFSETOF_INDEX: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_UPDATE_OFFSETOF_VISIBLE_TS: u64 = LOGENT_UPDATE_OFFSETOF_INDEX + 4;
const LOGENT_UPDATE_SIZE: u64 = LOGENT_UPDATE_OFFSETOF_VISIBLE_TS + 8;

const LOGENT_DELETE_OFFSETOF_INDEX: u64 = LOGENT_OFFSETOF_TYPE + 1;
const LOGENT_DELETE_SIZE: u64 = LOGENT_DELETE_OFFSETOF_INDEX + 4;

#[derive(Default)]
struct MessageState {
  // These offsets are physical offsets.
  create_offset: u64,
  poll_offset: u64,
  poll_count: u32,
}

#[derive(Debug)]
struct TailBumpCommitFutureState {
  completed: bool,
  waker: Option<Waker>,
}

struct TailBumpCommitFuture {
  shared_state: Arc<std::sync::Mutex<TailBumpCommitFutureState>>,
}

impl Future for TailBumpCommitFuture {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let mut shared_state = self.shared_state.lock().unwrap();
    if shared_state.completed {
      Poll::Ready(())
    } else {
      shared_state.waker = Some(cx.waker().clone());
      Poll::Pending
    }
  }
}

#[derive(Default)]
struct LogState {
  head: u64,
  tail: u64,
  // This is to prevent the scenario where a write at a later offset (i.e. subsequent request B) finishes before a write at an earlier offset (i.e. earlier request A); we can't immediately update the tail on disk after writing B because it would include A, which hasn't been synced yet.
  pending_tail_bumps: BTreeMap<u64, Option<Arc<std::sync::Mutex<TailBumpCommitFutureState>>>>,
}

pub struct LogStructuredLayout {
  device: SeekableAsyncFile,
  device_size: u64,
  // We don't store this in AvailableMessages as that will more than double the size of the index but not be useful for FixedSlotsLayout.
  index_state: DashMap<u32, MessageState>,
  log_state: Mutex<LogState>,
}

#[derive(Clone, Copy)]
struct TailBump {
  acquired_physical_offset: u64,
  uncommitted_virtual_offset: u64,
}

impl LogStructuredLayout {
  pub fn new(device: SeekableAsyncFile, device_size: u64) -> Self {
    Self {
      device,
      device_size,
      index_state: DashMap::new(),
      log_state: Mutex::new(LogState::default()),
    }
  }

  fn physical_offset(&self, virtual_offset: u64) -> u64 {
    LOGENT_START + (virtual_offset % (self.device_size - LOGENT_START))
  }

  fn update_index_state_on_create(&self, index: u32, physical_offset: u64) {
    let existing = self.index_state.insert(index, MessageState {
      create_offset: physical_offset,
      ..Default::default()
    });
    assert!(existing.is_none());
  }

  fn update_index_state_on_delete(&self, index: u32) {
    self.index_state.remove(&index).unwrap();
  }

  fn update_index_state_on_poll(&self, index: u32, physical_offset: u64) {
    let mut state = self.index_state.get_mut(&index).unwrap();
    state.poll_offset = physical_offset;
    state.poll_count += 1;
  }

  // How to use: bump tail, perform the write to the acquired tail offset, then persist the bumped tail. If tail is committed before write is persisted, it'll point to invalid data if the write didn't complete.
  async fn bump_tail(&self, usage: usize) -> TailBump {
    let usage: u64 = usage.try_into().unwrap();
    assert!(usage > 0);
    if usage > self.device_size {
      panic!("out of storage space");
    };

    let (physical_offset, new_tail, write_filler_at) = {
      let mut state = self.log_state.lock().await;
      let mut physical_offset = self.physical_offset(state.tail);
      let mut write_filler_at = None;
      if physical_offset + usage >= self.device_size {
        // Write after releasing lock (performance) and checking tail >= head (safety).
        write_filler_at = Some(physical_offset);
        let filler = self.device_size - physical_offset;
        physical_offset += filler;
        state.tail += filler;
      };

      state.tail += usage;
      let new_tail = state.tail;
      if new_tail < state.head {
        // TODO Should this be recoverable? It would require complex redesigning to allow unwinding.
        panic!("out of storage space");
      };

      let None = state.pending_tail_bumps.insert(new_tail, None) else {
        unreachable!();
      };
      (physical_offset, new_tail, write_filler_at)
    };

    if let Some(write_filler_at) = write_filler_at {
      self
        .device
        .write_at(write_filler_at, vec![LogEntryType::DummyFiller as u8])
        .await;
    };

    TailBump {
      acquired_physical_offset: physical_offset,
      uncommitted_virtual_offset: new_tail,
    }
  }

  async fn start_background_garbage_collection_loop(&self) {
    // TODO
  }

  async fn start_background_tail_bump_commit_loop(&self) {
    loop {
      sleep(std::time::Duration::from_micros(200)).await;

      let mut to_resolve = vec![];
      let mut new_tail_to_write = None;
      {
        let mut state = self.log_state.lock().await;
        loop {
          let Some(e) = state.pending_tail_bumps.first_entry() else {
            break;
          };
          if e.get().is_none() {
            break;
          };
          let (k, fut_state) = e.remove_entry();
          to_resolve.push(fut_state.unwrap());
          new_tail_to_write = Some(k);
        }
      };

      if let Some(new_tail_to_write) = new_tail_to_write {
        // TODO Journal this.
        self
          .device
          .write_at_with_delayed_sync(vec![WriteRequest {
            data: new_tail_to_write.to_be_bytes().to_vec(),
            offset: STATE_OFFSETOF_TAIL,
          }])
          .await;

        for ft in to_resolve {
          let mut ft = ft.lock().unwrap();
          ft.completed = true;
          if let Some(waker) = ft.waker.take() {
            waker.wake();
          };
        }
      };
    }
  }

  async fn commit_tail_bump(&self, bump: TailBump) {
    let fut_state = Arc::new(std::sync::Mutex::new(TailBumpCommitFutureState {
      completed: false,
      waker: None,
    }));

    {
      let mut state = self.log_state.lock().await;

      *state
        .pending_tail_bumps
        .get_mut(&bump.uncommitted_virtual_offset)
        .unwrap() = Some(fut_state.clone());
    };

    TailBumpCommitFuture {
      shared_state: fut_state,
    }
    .await;
  }
}

#[async_trait]
impl StorageLayout for LogStructuredLayout {
  fn max_contents_len(&self) -> u64 {
    u64::MAX
  }

  async fn start_background_loops(&self) {
    self.start_background_tail_bump_commit_loop().await;
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

  async fn load_data_from_device(&self, metrics: Arc<Metrics>) -> LoadedData {
    let mut available = InvisibleMessages::new(metrics.clone());
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
      let physical_offset = self.physical_offset(virtual_offset);
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
          let index = read_u32(&raw, LOGENT_POLL_OFFSETOF_INDEX);
          let visible_time = read_ts(&raw, LOGENT_CREATE_OFFSETOF_VISIBLE_TS);
          let len: u64 = read_u16(&raw, LOGENT_CREATE_OFFSETOF_LEN).into();
          self.update_index_state_on_create(index, physical_offset);
          available.insert(index, visible_time);
          vacant.remove_specific(index);
          virtual_offset += LOGENT_CREATE_OFFSETOF_CONTENTS + len;
        }
        LogEntryType::Poll => {
          let raw = self.device.read_at(physical_offset, LOGENT_POLL_SIZE).await;
          let index = read_u32(&raw, LOGENT_POLL_OFFSETOF_INDEX);
          let visible_time = read_ts(&raw, LOGENT_POLL_OFFSETOF_VISIBLE_TS);
          self.update_index_state_on_poll(index, physical_offset);
          available.update_timestamp(index, visible_time);
          virtual_offset += LOGENT_POLL_SIZE;
        }
        LogEntryType::Update => {
          let raw = self
            .device
            .read_at(physical_offset, LOGENT_UPDATE_SIZE)
            .await;
          let index = read_u32(&raw, LOGENT_UPDATE_OFFSETOF_INDEX);
          let visible_time = read_ts(&raw, LOGENT_UPDATE_OFFSETOF_VISIBLE_TS);
          available.update_timestamp(index, visible_time);
          virtual_offset += LOGENT_UPDATE_SIZE;
        }
        LogEntryType::Delete => {
          let raw = self
            .device
            .read_at(physical_offset, LOGENT_DELETE_SIZE)
            .await;
          let index = read_u32(&raw, LOGENT_DELETE_OFFSETOF_INDEX);
          self.update_index_state_on_delete(index);
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

  async fn update_visibility_time(&self, index: u32, visible_time: DateTime<Utc>) {
    let mut data = vec![0u8; as_usize!(LOGENT_UPDATE_SIZE)];
    data[as_usize!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Update as u8;
    u64_slice_write(
      &mut data,
      LOGENT_UPDATE_OFFSETOF_INDEX,
      &index.to_be_bytes(),
    );
    u64_slice_write(
      &mut data,
      LOGENT_UPDATE_OFFSETOF_VISIBLE_TS,
      &visible_time.timestamp().to_be_bytes(),
    );
    let bump = self.bump_tail(data.len()).await;
    self
      .device
      .write_at_with_delayed_sync(vec![WriteRequest {
        data,
        offset: bump.acquired_physical_offset,
      }])
      .await;
    self.commit_tail_bump(bump).await;
  }

  async fn delete_message(&self, index: u32) {
    let mut data = vec![0u8; as_usize!(LOGENT_DELETE_SIZE)];
    data[as_usize!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Delete as u8;
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
        offset: bump.acquired_physical_offset,
      }])
      .await;
    self.commit_tail_bump(bump).await;
    // Ensure to do this after physically writing to disk, so GC can clean that written data if necessary.
    self.update_index_state_on_delete(index);
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

  async fn mark_as_polled(&self, index: u32, update: MessagePoll) {
    let mut data = vec![0u8; as_usize!(LOGENT_POLL_SIZE)];
    data[as_usize!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Poll as u8;
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
        offset: bump.acquired_physical_offset,
      }])
      .await;
    self.commit_tail_bump(bump).await;
    self.update_index_state_on_poll(index, bump.acquired_physical_offset);
  }

  async fn create_messages(&self, creations: Vec<MessageCreation>) {
    let mut writes = vec![];
    // We must commit all bumps, not just the last/largest/highest one, due to the way our commit logic currently works.
    let mut bumps = vec![];
    for MessageCreation {
      index,
      visible_time,
      contents,
      ..
    } in creations
    {
      let mut data = vec![0u8; as_usize!(LOGENT_CREATE_OFFSETOF_CONTENTS) + contents.len()];
      data[as_usize!(LOGENT_OFFSETOF_TYPE)] = LogEntryType::Create as u8;
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
      let bump = self.bump_tail(data.len()).await;
      writes.push(WriteRequest {
        data,
        offset: bump.acquired_physical_offset,
      });
      bumps.push(bump);
      // Ensure to do this before physically writing to disk, so GC doesn't erase that written data.
      self.update_index_state_on_create(index, bump.acquired_physical_offset);
    }

    self.device.write_at_with_delayed_sync(writes).await;
    iter(bumps)
      .for_each_concurrent(None, |bump| async move {
        self.commit_tail_bump(bump).await;
      })
      .await;
  }
}
