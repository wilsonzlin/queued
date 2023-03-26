use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;
use write_journal::AtomicWriteGroup;
use write_journal::WriteJournal;

pub(crate) struct IdGenerator {
  journal: Arc<WriteJournal>,
  device: SeekableAsyncFile,
  device_offset: u64,
  next: AtomicU64,
  committed: AtomicU64,
  uncommitted_ids: Mutex<BTreeMap<u64, SignalFutureController>>,
}

impl IdGenerator {
  pub fn new(device: SeekableAsyncFile, journal: Arc<WriteJournal>, device_offset: u64) -> Self {
    Self {
      journal,
      device,
      device_offset,
      next: AtomicU64::new(0),
      committed: AtomicU64::new(0),
      uncommitted_ids: Mutex::new(BTreeMap::new()),
    }
  }

  pub async fn load_from_device(&self) {
    let next = self
      .device
      .read_at(self.device_offset, 8)
      .await
      .read_u64_be_at(0);
    self.next.store(next, Ordering::Relaxed);
    self.committed.store(next, Ordering::Relaxed);
  }

  pub async fn format_device(&self) {
    self
      .device
      .write_at(self.device_offset, 0u64.to_be_bytes().to_vec())
      .await;
  }

  pub async fn generate(&self, n: u64) -> u64 {
    let base = self.next.fetch_add(n, Ordering::Relaxed);
    let (fut, fut_ctl) = SignalFuture::new();
    // We always immediately await commit, as it's safe to skip some serials but not to use before they're committed, so we should just always do so immediately instead of providing another function to commit and hoping it always gets called appropriately (e.g. before data is committed).
    self.uncommitted_ids.lock().await.insert(base + n, fut_ctl);
    fut.await;
    base
  }

  pub async fn start_background_commit_loop(&self) {
    loop {
      sleep(std::time::Duration::from_micros(200)).await;
      // We must always check this map, as there may be futures waiting on an ID that has already been committed, so we won't write but we must still signal the futures.
      let mut uncommited_ids = self.uncommitted_ids.lock().await;
      if uncommited_ids.is_empty() {
        continue;
      };
      let id = self.next.load(Ordering::Relaxed);
      if id != self.committed.load(Ordering::Relaxed) {
        self
          .journal
          .write(AtomicWriteGroup(vec![(
            self.device_offset,
            id.to_be_bytes().to_vec(),
          )]))
          .await;
      };
      // TODO Use split_off or drain_range.
      loop {
        let Some(e) = uncommited_ids.first_entry() else {
          break;
        };
        // The stored disk state represents the next ID available, so if we use ID 8 we cannot resolve its commit if we're committing 8. However, the ID we store in `uncommitted_ids` is actually the one subsequent to the ID we use, so it's correct to break on `>` and not `>=`.
        if *e.key() > id {
          break;
        };
        e.remove_entry().1.signal();
      }
      self.committed.store(id, Ordering::Relaxed);
    }
  }
}
