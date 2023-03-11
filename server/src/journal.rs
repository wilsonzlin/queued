use crate::util::as_usize;
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

pub struct Journal {
  device: SeekableAsyncFile,
  offset: u64,
  capacity: u64,
  pending: Mutex<LinkedList<(u64, Vec<u8>, SignalFutureController)>>,
}

impl Journal {
  pub fn new(device: SeekableAsyncFile, offset: u64, capacity: u64) -> Self {
    Self {
      device,
      offset,
      capacity,
      pending: Mutex::new(LinkedList::new()),
    }
  }

  pub async fn write(&self, offset: u64, data: Vec<u8>) {
    assert!(data.len() <= as_usize!(self.capacity - 8 - OFFSETOF_ENTRIES));
    let (fut, fut_ctl) = SignalFuture::new();
    self.pending.lock().await.push_back((offset, data, fut_ctl));
    fut.await;
  }

  pub async fn start_commit_background_loop(&self) {
    loop {
      sleep(std::time::Duration::from_millis(1000)).await;

      let mut len = 0u32;
      let mut raw = vec![0u8; as_usize!(OFFSETOF_ENTRIES)];
      let mut writes = Vec::new();
      let mut fut_ctls = Vec::new();
      {
        let mut pending = self.pending.lock().await;
        while let Some(e) = pending.pop_front() {
          if raw.len() + 8 + e.1.len() > as_usize!(self.capacity) {
            pending.push_front(e);
            break;
          };
          let (offset, data, fut_ctl) = e;
          raw.extend_from_slice(&offset.to_be_bytes());
          raw.extend_from_slice(&data);
          len += 1;
          writes.push(WriteRequest { data, offset });
          fut_ctls.push(fut_ctl)
        }
      };
      if len == 0 {
        break;
      };
      u64_slice_write(&mut raw, OFFSETOF_LEN, &len.to_be_bytes());
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
    }
  }
}
