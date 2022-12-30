use crate::file::SeekableAsyncFile;
use crate::slot::SlotLists;
use crate::util::drain_linked_list;
use crate::util::u64_len;
use crate::util::u64_slice;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;

pub async fn clear_journal(journal_fd: &SeekableAsyncFile) {
  let zero_hash = blake3::hash(&0u64.to_be_bytes());
  journal_fd.write_at(0, zero_hash.as_bytes().to_vec()).await;
  journal_fd.write_at(32, 0u64.to_be_bytes().to_vec()).await;
}

pub async fn restore_journal(data_fd: &SeekableAsyncFile, journal_fd: &SeekableAsyncFile) {
  let expected_hash = journal_fd.read_at(0, 32).await;
  let len = journal_fd.read_u64_at(32).await;
  let raw = journal_fd.read_at(8, len).await;
  let mut actual_hasher = blake3::Hasher::new();
  actual_hasher.update(&len.to_be_bytes());
  actual_hasher.update(&raw);
  let actual_hash = actual_hasher.finalize();
  if actual_hash.as_bytes() != expected_hash.as_slice() {
    panic!(
      "journal hash is invalid; calculated {:x?} with length {} but file has {:x?}",
      actual_hash, len, expected_hash
    );
  }

  if len > 0 {
    println!("Restoring journal");

    let mut cur = 0u64;
    while cur < len {
      let offset = u64::from_be_bytes(u64_slice(&raw, cur, 8).try_into().unwrap());
      cur += 8;

      let data_len = u64::from_be_bytes(u64_slice(&raw, cur, 8).try_into().unwrap());
      cur += 8;

      let data = u64_slice(&raw, cur, data_len);
      cur += data_len;

      println!("Recovering {} bytes at {}", data_len, offset);
      data_fd.write_at(offset, data.to_vec()).await;
    }
    data_fd.sync_all().await;

    println!("Restored journal");
    clear_journal(journal_fd).await;
    println!("Journal cleared");
  }
}

struct PendingWriteFutureSharedState {
  completed: bool,
  waker: Option<Waker>,
}

struct PendingWriteFuture {
  shared_state: Arc<std::sync::Mutex<PendingWriteFutureSharedState>>,
}

impl Future for PendingWriteFuture {
  type Output = ();

  // https://rust-lang.github.io/async-book/02_execution/03_wakeups.html
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

struct JournalPendingEntry {
  // We may want to atomically write to many discontiguous file offsets as a whole.
  writes: Vec<(u64, Vec<u8>)>,
  future_shared_state: Arc<std::sync::Mutex<PendingWriteFutureSharedState>>,
}

pub struct JournalPending {
  list: tokio::sync::Mutex<Vec<JournalPendingEntry>>,
}

impl JournalPending {
  pub fn new() -> Self {
    JournalPending {
      list: tokio::sync::Mutex::new(Vec::new()),
    }
  }

  pub async fn write(&self, writes: Vec<(u64, Vec<u8>)>) -> () {
    let shared_state = Arc::new(std::sync::Mutex::new(PendingWriteFutureSharedState {
      completed: false,
      waker: None,
    }));
    let entry = JournalPendingEntry {
      writes,
      future_shared_state: shared_state.clone(),
    };
    self.list.lock().await.push(entry);
    let fut = PendingWriteFuture {
      shared_state: shared_state.clone(),
    };
    fut.await;
  }
}

pub struct JournalFlushing {
  data_fd: SeekableAsyncFile,
  journal_fd: SeekableAsyncFile,
  lists: Arc<RwLock<SlotLists>>,
  pending: Arc<JournalPending>,
}

impl JournalFlushing {
  pub fn new(
    data_fd: SeekableAsyncFile,
    journal_fd: SeekableAsyncFile,
    lists: Arc<RwLock<SlotLists>>,
    pending: Arc<JournalPending>,
  ) -> Self {
    JournalFlushing {
      data_fd,
      journal_fd,
      lists,
      pending,
    }
  }

  pub async fn start_flush_loop(&self, tick_rate: Duration) -> () {
    loop {
      sleep(tick_rate).await;
      // We need to lock both journal pending and lists simulatenously to have a consistent view. Locking lists first should be enough, given that we always lock it first in the endpoints.
      let (available_pending, invisible_pending, vacant_pending, pending_writes, pending_futures) = {
        let mut lists = self.lists.write().await;
        // Unzip so we can take ownership and consume/move pending writes without pending futures, as they are processed in different stages.
        let (pending_writes, pending_futures): (Vec<_>, Vec<_>) = self
          .pending
          .list
          .lock()
          .await
          .drain(..)
          .map(|e| (e.writes, e.future_shared_state))
          .unzip();
        (
          drain_linked_list(&mut lists.available.pending),
          drain_linked_list(&mut lists.invisible.pending),
          drain_linked_list(&mut lists.vacant.pending),
          pending_writes,
          pending_futures,
        )
      };
      if pending_writes.is_empty() {
        continue;
      };

      // First stage: write journal.
      let mut hasher = blake3::Hasher::new();
      // 32 bytes for hash and 8 bytes for length. We must write hash first as otherwise we have to read the length to know where hash is stored but the length metadata itself may be corrupted.
      let mut cur = self.journal_fd.cursor(40);
      // We need to include the length in the hash, so we need a separate initial pass to calculate the length first.
      let len: u64 = pending_writes
        .iter()
        .map(|p| p.iter().map(|w| 8 + 8 + u64_len(&w.1)).sum::<u64>())
        .sum();
      hasher.update(&len.to_be_bytes());
      for e in pending_writes.iter() {
        for (offset, data) in e.iter() {
          let offset_encoded = offset.to_be_bytes();
          hasher.update(&offset_encoded);
          cur.write_all(offset_encoded.to_vec()).await;

          let data_len_encoded = u64_len(data).to_be_bytes();
          hasher.update(&data_len_encoded);
          cur.write_all(data_len_encoded.to_vec()).await;

          hasher.update(&data);
          cur.write_all(data.to_vec()).await;
        }
      }
      let hash = hasher.finalize();
      cur.seek(0);
      cur.write_all(hash.as_bytes().to_vec()).await;
      cur.write_all(len.to_be_bytes().to_vec()).await;
      cur.sync_all().await;

      // Second stage: apply journal.
      // To avoid repeatedly flushing, we write everything then flush once afterwards. However, this means we cannot mark futures as completed until the flush succeeds, so we'll need another loop after this one.
      for e in pending_writes {
        for (offset, data) in e {
          self.data_fd.write_at(offset, data).await;
        }
      }
      self.data_fd.sync_all().await;

      // Third stage: move pending items in lists.
      {
        let mut lists = self.lists.write().await;
        lists.available.ready.extend(available_pending);
        lists.invisible.ready.extend(invisible_pending);
        lists.vacant.ready.extend(vacant_pending);
      }

      // Fourth stage: complete futures.
      for e in pending_futures {
        // https://rust-lang.github.io/async-book/02_execution/03_wakeups.html
        let mut shared_state = e.lock().unwrap();
        shared_state.completed = true;
        if let Some(waker) = shared_state.waker.take() {
          waker.wake();
        }
      }
    }
  }
}
