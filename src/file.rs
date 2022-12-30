use std::ops::Deref;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::Arc;
use tokio::task::spawn_blocking;

// Tokio has still not implemented read_at and write_at: https://github.com/tokio-rs/tokio/issues/1529. We need these to be able to share a file descriptor across threads (e.g. use from within async function).
// Apparently spawn_blocking is how Tokio does all file operations (as not all platforms have native async I/O), so our use is not worse but not optimised for async I/O either.
// We considered mmap but it's a bit more complex and makes it more difficult to extend files.
// We take a usize for `offset` instead of u64 as most of our constants (e.g. SLOT_OFFSETOF_*) are usize values (which in turn is to make it easier when reading/slicing raw bytes).
struct SeekableAsyncFileInner(std::fs::File);

impl Deref for SeekableAsyncFileInner {
  type Target = std::fs::File;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

pub struct SeekableAsyncFile(Arc<SeekableAsyncFileInner>);

impl SeekableAsyncFile {
  pub async fn open(path: &Path) -> Self {
    let async_fd = tokio::fs::File::open(path).await.unwrap();
    let fd = async_fd.into_std().await;
    SeekableAsyncFile(Arc::new(SeekableAsyncFileInner(fd)))
  }

  pub async fn create(path: &Path) -> Self {
    let async_fd = tokio::fs::File::create(path).await.unwrap();
    let fd = async_fd.into_std().await;
    SeekableAsyncFile(Arc::new(SeekableAsyncFileInner(fd)))
  }

  pub fn cursor(&self, pos: usize) -> SeekableAsyncFileCursor {
    SeekableAsyncFileCursor {
      fd: SeekableAsyncFile(self.0.clone()),
      pos,
    }
  }

  // Since spawn_blocking requires 'static lifetime, we don't have a read_into_at function taht takes a &mut [u8] buffer, as it would be more like a Arc<Mutex<Vec<u8>>>, at which point the overhead is not really worth it for small reads.
  pub async fn read_at(&self, offset: usize, len: usize) -> Vec<u8> {
    let fd = self.0.clone();
    let mut buf = vec![0u8; len];
    spawn_blocking(move || {
      fd.read_exact_at(&mut buf, offset as u64).unwrap();
      buf
    })
    .await
    .unwrap()
  }

  pub async fn read_u64_at(&self, offset: usize) -> u64 {
    let bytes = self.read_at(offset, 8).await;
    u64::from_be_bytes(bytes.try_into().unwrap())
  }

  pub async fn write_at(&self, offset: usize, data: Vec<u8>) {
    let fd = self.0.clone();
    spawn_blocking(move || fd.write_all_at(&data, offset as u64).unwrap())
      .await
      .unwrap();
  }

  pub async fn sync_all(&self) {
    let fd = self.0.clone();
    // WARNING: sync_all -> fsync, sync_data -> fdatasync, flush -> (no-op). https://stackoverflow.com/a/69820437/6249022
    spawn_blocking(move || fd.sync_all().unwrap())
      .await
      .unwrap();
  }
}

pub struct SeekableAsyncFileCursor {
  fd: SeekableAsyncFile,
  pos: usize,
}

impl SeekableAsyncFileCursor {
  pub fn seek(&mut self, pos: usize) {
    self.pos = pos;
  }

  pub async fn read_exact(&mut self, len: usize) -> Vec<u8> {
    let data = self.fd.read_at(self.pos, len).await;
    self.pos += len;
    data
  }

  pub async fn write_all(&mut self, data: Vec<u8>) {
    let len = data.len();
    self.fd.write_at(self.pos, data).await;
    self.pos += len;
  }

  pub async fn sync_all(&self) {
    self.fd.sync_all().await;
  }
}
