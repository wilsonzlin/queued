use crate::util::as_usize;
use crate::util::read_u32;
use crate::util::read_u64;
use crate::util::u64_slice_write;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::WriteRequest;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::LinkedList;
use tokio::sync::Mutex;
use tokio::time::sleep;

const OFFSETOF_HASH: u64 = 0;
const OFFSETOF_LEN: u64 = OFFSETOF_HASH + 32;
const OFFSETOF_ENTRIES: u64 = OFFSETOF_LEN + 4;

pub(crate) struct Journal {
  device: SeekableAsyncFile,
  offset: u64,
  capacity: u64,
  pending: Mutex<LinkedList<(u64, Vec<u8>, SignalFutureController)>>,
}

impl Journal {
  pub fn new(device: SeekableAsyncFile, offset: u64, capacity: u64) -> Self {
    assert!(capacity > OFFSETOF_ENTRIES && capacity <= u32::MAX.into());
    Self {
      device,
      offset,
      capacity,
      pending: Mutex::new(LinkedList::new()),
    }
  }

  pub fn generate_blank_state(&self) -> Vec<u8> {
    let mut raw = vec![0u8; as_usize!(OFFSETOF_ENTRIES)];
    u64_slice_write(&mut raw, OFFSETOF_LEN, &0u32.to_be_bytes());
    let hash = blake3::hash(&raw[as_usize!(OFFSETOF_LEN)..]);
    u64_slice_write(&mut raw, OFFSETOF_HASH, hash.as_bytes());
    raw
  }

  pub async fn format_device(&self) {
    self
      .device
      .write_at(self.offset, self.generate_blank_state())
      .await;
  }

  pub async fn recover(&self) {
    let mut raw = self.device.read_at(self.offset, OFFSETOF_ENTRIES).await;
    let len: u64 = read_u32(&raw, OFFSETOF_LEN).into();
    if len > self.capacity - OFFSETOF_ENTRIES {
      println!("Journal is corrupt, ignoring");
      return;
    };
    raw.append(
      &mut self
        .device
        .read_at(self.offset + OFFSETOF_ENTRIES, len.into())
        .await,
    );
    let expected_hash = blake3::hash(&raw[as_usize!(OFFSETOF_LEN)..]);
    let recorded_hash = &raw[..as_usize!(OFFSETOF_LEN)];
    if expected_hash.as_bytes() != recorded_hash {
      println!("Journal is corrupt, ignoring");
      return;
    };
    if len == 0 {
      return;
    };
    println!("Recovering {} journal entries", len);
    let mut journal_offset = OFFSETOF_ENTRIES;
    while journal_offset < len {
      let offset = read_u64(&raw, journal_offset);
      journal_offset += 8;
      let data_len = read_u32(&raw, journal_offset);
      journal_offset += 4;
      let data =
        raw[as_usize!(journal_offset)..as_usize!(journal_offset) + as_usize!(data_len)].to_vec();
      journal_offset += u64::from(data_len);
      self.device.write_at(offset, data).await;
    }
    self
      .device
      .write_at(self.offset, self.generate_blank_state())
      .await;
    self.device.sync_data().await;
    println!("Journal recovered");
  }

  pub async fn write(&self, offset: u64, data: Vec<u8>) {
    assert!(data.len() <= as_usize!(self.capacity - 8 - OFFSETOF_ENTRIES));
    let (fut, fut_ctl) = SignalFuture::new();
    self.pending.lock().await.push_back((offset, data, fut_ctl));
    fut.await;
  }

  pub async fn start_commit_background_loop(&self) {
    loop {
      sleep(std::time::Duration::from_micros(200)).await;

      let mut len = 0;
      let mut raw = vec![0u8; as_usize!(OFFSETOF_ENTRIES)];
      let mut writes = Vec::new();
      let mut fut_ctls = Vec::new();
      {
        let mut pending = self.pending.lock().await;
        while let Some(e) = pending.pop_front() {
          let entry_len = 8 + 4 + u64::try_from(e.1.len()).unwrap();
          if len + entry_len > self.capacity - OFFSETOF_ENTRIES {
            pending.push_front(e);
            break;
          };
          let (offset, data, fut_ctl) = e;
          let data_len: u32 = data.len().try_into().unwrap();
          raw.extend_from_slice(&offset.to_be_bytes());
          raw.extend_from_slice(&data_len.to_be_bytes());
          raw.extend_from_slice(&data);
          len += entry_len;
          writes.push(WriteRequest { data, offset });
          fut_ctls.push(fut_ctl)
        }
      };
      if writes.is_empty() {
        continue;
      };
      u64_slice_write(
        &mut raw,
        OFFSETOF_LEN,
        &u32::try_from(len).unwrap().to_be_bytes(),
      );
      let hash = blake3::hash(&raw[as_usize!(OFFSETOF_LEN)..]);
      u64_slice_write(&mut raw, OFFSETOF_HASH, hash.as_bytes());
      self
        .device
        .write_at_with_delayed_sync(vec![WriteRequest {
          data: raw,
          offset: self.offset,
        }])
        .await;

      self.device.write_at_with_delayed_sync(writes).await;

      for fut_ctl in fut_ctls {
        fut_ctl.signal();
      }

      // We cannot write_at_with_delayed_sync, as we may write to the journal again by then and have a conflict due to reordering.
      self
        .device
        .write_at(self.offset, self.generate_blank_state())
        .await;
      self.device.sync_data().await;
    }
  }
}
