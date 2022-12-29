use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Json;
use axum::Router;
use axum::Server;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use clap::arg;
use clap::command;
use clap::Parser;
use serde::Deserialize;
use serde::Serialize;
use std::collections::LinkedList;
use std::future::Future;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::ops::Deref;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::join;
use tokio::sync::RwLock;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

const STATE_LEN_RESERVED: usize = 1024 * 1024 * 4;
const STATE_OFFSETOF_AVAILABLE_HEAD: u64 = 0;
const STATE_OFFSETOF_INVISIBLE_HEAD: u64 = STATE_OFFSETOF_AVAILABLE_HEAD + 8;
const STATE_OFFSETOF_VACANT_HEAD: u64 = STATE_OFFSETOF_INVISIBLE_HEAD + 8;

const SLOTS_OFFSET_START: u64 = STATE_LEN_RESERVED as u64;
const SLOT_LEN_MAX: usize = 1024;
const SLOT_OFFSETOF_CREATED_TS: usize = 0;
const SLOT_OFFSETOF_VISIBLE_TS: usize = SLOT_OFFSETOF_CREATED_TS + 8;
const SLOT_OFFSETOF_POLL_COUNT: usize = SLOT_OFFSETOF_VISIBLE_TS + 8;
const SLOT_OFFSETOF_NEXT: usize = SLOT_OFFSETOF_POLL_COUNT + 4;
const SLOT_OFFSETOF_LEN: usize = SLOT_OFFSETOF_NEXT + 8;
const SLOT_OFFSETOF_CONTENTS: usize = SLOT_OFFSETOF_LEN + 2;
const SLOT_FIXED_FIELDS_LEN: usize = SLOT_OFFSETOF_CONTENTS;
const MESSAGE_SLOT_CONTENT_LEN_MAX: usize = SLOT_LEN_MAX - SLOT_FIXED_FIELDS_LEN;

pub fn now() -> i64 {
  Utc::now().timestamp()
}

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

struct SeekableAsyncFile(Arc<SeekableAsyncFileInner>);

impl SeekableAsyncFile {
  async fn open(path: &Path) -> Self {
    let async_fd = tokio::fs::File::open(path).await.unwrap();
    let fd = async_fd.into_std().await;
    SeekableAsyncFile(Arc::new(SeekableAsyncFileInner(fd)))
  }

  async fn create(path: &Path) -> Self {
    let async_fd = tokio::fs::File::create(path).await.unwrap();
    let fd = async_fd.into_std().await;
    SeekableAsyncFile(Arc::new(SeekableAsyncFileInner(fd)))
  }

  fn cursor(&self, pos: usize) -> SeekableAsyncFileCursor {
    SeekableAsyncFileCursor {
      fd: SeekableAsyncFile(self.0.clone()),
      pos,
    }
  }

  // Since spawn_blocking requires 'static lifetime, we don't have a read_into_at function taht takes a &mut [u8] buffer, as it would be more like a Arc<Mutex<Vec<u8>>>, at which point the overhead is not really worth it for small reads.
  async fn read_at(&self, offset: usize, len: usize) -> Vec<u8> {
    let fd = self.0.clone();
    let mut buf = vec![0u8; len];
    spawn_blocking(move || {
      fd.read_exact_at(&mut buf, offset as u64).unwrap();
      buf
    })
    .await
    .unwrap()
  }

  async fn read_u64_at(&self, offset: usize) -> u64 {
    let bytes = self.read_at(offset, 8).await;
    u64::from_be_bytes(bytes.try_into().unwrap())
  }

  async fn write_at(&self, offset: usize, data: Vec<u8>) {
    let fd = self.0.clone();
    spawn_blocking(move || fd.write_all_at(&data, offset as u64).unwrap())
      .await
      .unwrap();
  }

  async fn sync_all(&self) {
    let fd = self.0.clone();
    // WARNING: sync_all -> fsync, sync_data -> fdatasync, flush -> (no-op). https://stackoverflow.com/a/69820437/6249022
    spawn_blocking(move || fd.sync_all().unwrap())
      .await
      .unwrap();
  }
}

struct SeekableAsyncFileCursor {
  fd: SeekableAsyncFile,
  pos: usize,
}

impl SeekableAsyncFileCursor {
  fn seek(&mut self, pos: usize) {
    self.pos = pos;
  }

  async fn read_exact(&mut self, len: usize) -> Vec<u8> {
    let data = self.fd.read_at(self.pos, len).await;
    self.pos += len;
    data
  }

  async fn write_all(&mut self, data: Vec<u8>) {
    let len = data.len();
    self.fd.write_at(self.pos, data).await;
    self.pos += len;
  }

  async fn sync_all(&self) {
    self.fd.sync_all().await;
  }
}

struct Slot {
  offset: u64,
}

// For all lists (available, invisible, vacant), we require two in-memory lists. Many mutating operations (pushing, polling, deleting) involve moving across lists. We use one list that can be consumed/introspected from, but we don't immediately move to the new list, as the underlying data to be changed hasn't been written to the file yet.
// For example, when pushing, we immediately pop from `vacant.ready`, to prevent someone else from also taking the same slot. However, we cannot immediately move it into `available.ready`, as the slot data (metadata and contents) is still awaiting write and cannot be read yet (e.g. by a poller). We also don't want writes to go out of order (e.g. updating metadata of a slot before it's populated), causing corruption.
// An alternative is to store everything in memory and rely on atomic memory operations, but that's not memory efficient. Treating operations as if they're successful despite the data not having been safely written yet also seems like it will cause lots of complexity and/or subtle safety issues.
struct SlotList {
  ready: LinkedList<Slot>,
  pending: LinkedList<Slot>,
}

// The problem with using an individual lock per list is that it works fine for in-memory representations, but starts to get tricky and complex when also trying to keep journal-writes ordered and atomic, due to most mutating operations (push, poll, delete) interacting with more than one list at once. For example, when calling the push API, we pop from the vacant list and push to the available list. If two people are calling the push API at the same, it's possible for one to acquire the vacant list lock first and the other to acquire the available list lock first, and now don't have a consistent view of the new linked list offset values to write to disk. Holding the first lock (vacant list) for the entire operation only solves it for the push API, but does not solve interactions with other APIs. We can't just create multiple journal-write entries for whoever-is-first-wins, since all writes must be atomic across the entire push API operation. Serialising all API calls (e.g. placing in a MPSC queue) is another possibility, but this is mostly a less-efficient global lock.
// Having one lock across all lists is the most simple and less-prone-to-subtle-bugs option, and it's unlikely to cause much of a performance slowdown given we must perform I/O for each operation anyway (and I/O is much slower than acquiring even contentious locks).
struct SlotLists {
  available: SlotList,
  invisible: SlotList,
  vacant: SlotList,
}

async fn load_list_from_data(data_file: &SeekableAsyncFile, head_offset: u64) -> LinkedList<Slot> {
  let mut list = LinkedList::new();
  let mut offset = head_offset;
  while offset != 0 {
    list.push_back(Slot { offset });
    offset = data_file
      .read_u64_at(offset as usize + SLOT_OFFSETOF_NEXT)
      .await;
  }
  list
}

struct LoadedData {
  available_list: LinkedList<Slot>,
  invisible_list: LinkedList<Slot>,
  vacant_list: LinkedList<Slot>,
}

async fn load_data(data_file: &SeekableAsyncFile) -> LoadedData {
  let available_head_offset = data_file
    .read_u64_at(STATE_OFFSETOF_AVAILABLE_HEAD as usize)
    .await;
  let available_list = load_list_from_data(data_file, available_head_offset).await;

  let invisible_head_offset = data_file
    .read_u64_at(STATE_OFFSETOF_INVISIBLE_HEAD as usize)
    .await;
  let invisible_list = load_list_from_data(data_file, invisible_head_offset).await;

  let vacant_head_offset = data_file
    .read_u64_at(STATE_OFFSETOF_VACANT_HEAD as usize)
    .await;
  let vacant_list = load_list_from_data(data_file, vacant_head_offset).await;

  LoadedData {
    available_list,
    invisible_list,
    vacant_list,
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

struct JournalPending {
  list: tokio::sync::Mutex<Vec<JournalPendingEntry>>,
}

impl JournalPending {
  async fn write(&self, writes: Vec<(u64, Vec<u8>)>) -> () {
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

struct JournalFlushing {
  journal_fd: SeekableAsyncFile,
  data_fd: SeekableAsyncFile,
  pending: Arc<JournalPending>,
}

impl JournalFlushing {
  async fn start_flush_loop(&self, tick_rate: Duration) -> () {
    loop {
      sleep(tick_rate).await;
      // Unzip so we can take ownership and consume/move pending writes without pending futures, as they are processed in different stages.
      let (pending_writes, pending_futures): (Vec<_>, Vec<_>) = self
        .pending
        .list
        .lock()
        .await
        .drain(..)
        .map(|e| (e.writes, e.future_shared_state))
        .unzip();
      if pending_writes.is_empty() {
        continue;
      };

      // First stage: write journal.
      let mut hasher = blake3::Hasher::new();
      // 32 bytes for hash and 8 bytes for length. We must write hash first as otherwise we have to read the length to know where hash is stored but the length metadata itself may be corrupted.
      let mut cur = self.journal_fd.cursor(40);
      // We need to include the length in the hash, so we need a separate initial pass to calculate the length first.
      let len = pending_writes
        .iter()
        .map(|p| p.iter().map(|w| 8 + 8 + w.1.len()).sum::<usize>())
        .sum::<usize>() as u64;
      hasher.update(&len.to_be_bytes());
      for e in pending_writes.iter() {
        for (offset, data) in e.iter() {
          let offset_encoded = offset.to_be_bytes();
          hasher.update(&offset_encoded);
          cur.write_all(offset_encoded.to_vec()).await;

          let data_len_encoded = (data.len() as u64).to_be_bytes();
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
          self.data_fd.write_at(offset as usize, data).await;
        }
      }
      self.data_fd.sync_all().await;

      // Third stage: complete futures.
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

async fn clear_journal(journal_fd: &SeekableAsyncFile) {
  let zero_hash = blake3::hash(&0u64.to_be_bytes());
  journal_fd.write_at(0, zero_hash.as_bytes().to_vec()).await;
  journal_fd.write_at(8, 0u64.to_be_bytes().to_vec()).await;
}

async fn restore_journal(data_fd: &SeekableAsyncFile, journal_fd: &SeekableAsyncFile) {
  let expected_hash = journal_fd.read_at(0, 32).await;
  let len = journal_fd.read_u64_at(32).await;
  let raw = journal_fd.read_at(8, len as usize).await;
  let mut actual_hasher = blake3::Hasher::new();
  actual_hasher.update(&len.to_be_bytes());
  actual_hasher.update(&raw);
  let actual_hash = actual_hasher.finalize();
  if actual_hash.as_bytes() != expected_hash.as_slice() {
    panic!("journal hash is invalid");
  }

  if len > 0 {
    println!("Recovering {} segments", len);

    let mut cur = 0;
    while cur < raw.len() {
      let offset = u64::from_be_bytes(raw[cur..cur + 8].try_into().unwrap());
      cur += 8;

      let data_len = u64::from_be_bytes(raw[cur..cur + 8].try_into().unwrap()) as usize;
      cur += 8;

      let data = &raw[cur..cur + data_len];
      cur += data_len;

      println!("Recovering {} bytes at {}", data_len, offset);
      data_fd.write_at(offset as usize, data.to_vec()).await;
    }
    data_fd.sync_all().await;

    println!("Recovered {} segments", len);
    clear_journal(journal_fd).await;
    println!("Journal cleared");
  }
}

struct Ctx {
  data_fd: SeekableAsyncFile,
  journal_pending: Arc<JournalPending>,
  lists: RwLock<SlotLists>,
}

#[derive(Deserialize)]
struct EndpointPushInput {
  content: String,
}

#[derive(Serialize)]
struct EndpointPushOutput {
  offset: u64,
}

async fn endpoint_push(
  State(ctx): State<Arc<Ctx>>,
  Json(req): Json<EndpointPushInput>,
) -> Result<Json<EndpointPushOutput>, (StatusCode, &'static str)> {
  if req.content.len() > MESSAGE_SLOT_CONTENT_LEN_MAX as usize {
    return Err((StatusCode::PAYLOAD_TOO_LARGE, "content is too large"));
  };

  // We must hold the lock until we journal-write. We can get a consistent view of the updated heads across lists, but we still need to ensure this update is written before any other API does any update.
  let (offset, pending_write_future) = {
    let mut slots = ctx.lists.write().await;

    let slot = slots.vacant.ready.pop_front().unwrap();
    let offset = slot.offset;
    let prev_avail_offset = slots
      .available
      .pending
      .back()
      .map(|p| p.offset)
      .or_else(|| slots.available.ready.back().map(|p| p.offset))
      .unwrap_or(0);
    slots.available.pending.push_back(slot);

    let mut journal_writes = vec![];

    // Populate slot.
    let mut slot_data = vec![];
    slot_data.extend_from_slice(&0i64.to_be_bytes());
    slot_data.extend_from_slice(&now().to_be_bytes());
    slot_data.extend_from_slice(&0u32.to_be_bytes());
    slot_data.extend_from_slice(&0u64.to_be_bytes());
    slot_data.extend_from_slice(&(req.content.len() as u16).to_be_bytes());
    slot_data.extend_from_slice(&req.content.into_bytes());
    journal_writes.push((offset, slot_data));

    // Update vacant list head.
    journal_writes.push((
      STATE_OFFSETOF_VACANT_HEAD,
      slots
        .vacant
        .ready
        .front()
        .unwrap()
        .offset
        .to_be_bytes()
        .to_vec(),
    ));

    // Update available list tail.
    journal_writes.push((
      prev_avail_offset + SLOT_OFFSETOF_NEXT as u64,
      offset.to_be_bytes().to_vec(),
    ));

    // Drop the lock AFTER creating the journal-write but BEFORE the future completes.
    (offset, ctx.journal_pending.write(journal_writes))
  };

  pending_write_future.await;

  Ok(Json(EndpointPushOutput { offset }))
}

#[derive(Deserialize)]
struct EndpointPollInput {}

#[derive(Serialize)]
struct EndpointPollOutputMessage {
  offset: u64,
  created: DateTime<Utc>,
  poll_count: u32,
  contents: String,
}

#[derive(Serialize)]
struct EndpointPollOutput {
  message: Option<EndpointPollOutputMessage>,
}

async fn endpoint_poll(
  State(ctx): State<Arc<Ctx>>,
  Json(_req): Json<EndpointPollInput>,
) -> Result<Json<EndpointPollOutput>, (StatusCode, &'static str)> {
  let polled = {
    let mut slots = ctx.lists.write().await;

    if let Some(slot) = slots.available.ready.pop_front() {
      let offset = slot.offset;
      let prev_invis_offset = slots
        .invisible
        .pending
        .back()
        .map(|s| s.offset)
        .or_else(|| slots.invisible.ready.back().map(|p| p.offset))
        .unwrap_or(0);
      slots.invisible.pending.push_back(slot);

      let mut journal_writes = vec![];

      let raw_data = ctx.data_fd.read_at(offset as usize, SLOT_LEN_MAX).await;

      let next_avail_offset = u64::from_be_bytes(
        raw_data[SLOT_OFFSETOF_NEXT..SLOT_OFFSETOF_NEXT + 8]
          .try_into()
          .unwrap(),
      );
      let new_poll_count = u32::from_be_bytes(
        raw_data[SLOT_OFFSETOF_POLL_COUNT..SLOT_OFFSETOF_POLL_COUNT + 4]
          .try_into()
          .unwrap(),
      ) + 1;

      // Update visible timestamp and poll count.
      journal_writes.push((
        offset + SLOT_OFFSETOF_VISIBLE_TS as u64,
        now().to_be_bytes().to_vec(),
      ));
      journal_writes.push((
        offset + SLOT_OFFSETOF_POLL_COUNT as u64,
        new_poll_count.to_be_bytes().to_vec(),
      ));

      // Update available list head.
      journal_writes.push((
        STATE_OFFSETOF_AVAILABLE_HEAD,
        next_avail_offset.to_be_bytes().to_vec(),
      ));

      // Update invisible list tail.
      journal_writes.push((
        prev_invis_offset + SLOT_OFFSETOF_NEXT as u64,
        offset.to_be_bytes().to_vec(),
      ));

      // Don't read raw data yet, to save some unnecessary time spent holding lock.
      Some((
        offset,
        new_poll_count,
        raw_data,
        ctx.journal_pending.write(journal_writes),
      ))
    } else {
      None
    }
  };

  let message = if let Some((offset, poll_count, raw_data, pending_write_future)) = polled {
    pending_write_future.await;
    let len = u16::from_be_bytes(
      raw_data[SLOT_OFFSETOF_LEN..SLOT_OFFSETOF_LEN + 2]
        .try_into()
        .unwrap(),
    ) as usize;
    Some(EndpointPollOutputMessage {
      contents: String::from_utf8(
        raw_data[SLOT_OFFSETOF_CONTENTS..SLOT_OFFSETOF_CONTENTS + len].to_vec(),
      )
      .unwrap(),
      created: Utc
        .timestamp_millis_opt(
          i64::from_be_bytes(
            raw_data[SLOT_OFFSETOF_CREATED_TS..SLOT_OFFSETOF_CREATED_TS + 8]
              .try_into()
              .unwrap(),
          ) * 1000,
        )
        .unwrap(),
      offset,
      poll_count,
    })
  } else {
    None
  };

  Ok(Json(EndpointPollOutput { message }))
}

async fn start_server_loop(
  available_list: LinkedList<Slot>,
  data_fd: SeekableAsyncFile,
  invisible_list: LinkedList<Slot>,
  journal_pending: Arc<JournalPending>,
  vacant_list: LinkedList<Slot>,
) {
  let ctx = Arc::new(Ctx {
    data_fd,
    journal_pending,
    lists: RwLock::new(SlotLists {
      available: SlotList {
        ready: available_list,
        pending: LinkedList::new(),
      },
      invisible: SlotList {
        ready: invisible_list,
        pending: LinkedList::new(),
      },
      vacant: SlotList {
        ready: vacant_list,
        pending: LinkedList::new(),
      },
    }),
  });

  let app = Router::new()
    .route("/push", post(endpoint_push))
    .with_state(ctx.clone());

  let addr = SocketAddr::from(([0, 0, 0, 0], 3333));

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}

async fn format_data_file(data_file_path: &Path) {
  let mut file = tokio::fs::File::create(data_file_path).await.unwrap();
  // TODO ftruncate if possible. WARNING: Must fill with zeros.
  file.write_all(&[0u8; 1024 * 1024 * 16]).await.unwrap();

  file
    .seek(SeekFrom::Start(STATE_OFFSETOF_VACANT_HEAD as u64))
    .await
    .unwrap();
  file.write(&SLOTS_OFFSET_START.to_be_bytes()).await.unwrap();

  println!("Data file formatted");
}

async fn format_journal_file(journal_file_path: &Path) {
  let file = SeekableAsyncFile::create(journal_file_path).await;
  clear_journal(&file).await;
  println!("Journal file formatted");
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
  #[arg(long)]
  datadir: PathBuf,

  #[arg(long)]
  format: bool,
}

#[tokio::main]
async fn main() {
  let cli = Cli::parse();

  let data_file_path = cli.datadir.join("data");
  let journal_file_path = cli.datadir.join("journal");

  if cli.format {
    format_journal_file(data_file_path.as_path()).await;
    format_data_file(journal_file_path.as_path()).await;
  }

  restore_journal(
    &SeekableAsyncFile::open(&data_file_path).await,
    &SeekableAsyncFile::open(&journal_file_path).await,
  )
  .await;

  let LoadedData {
    available_list,
    invisible_list,
    vacant_list,
  } = load_data(&SeekableAsyncFile::open(&data_file_path).await).await;

  let journal_pending = Arc::new(JournalPending {
    list: tokio::sync::Mutex::new(Vec::new()),
  });

  let journal_flushing = JournalFlushing {
    data_fd: SeekableAsyncFile::open(&data_file_path).await,
    journal_fd: SeekableAsyncFile::open(&journal_file_path).await,
    pending: journal_pending.clone(),
  };

  join!(
    journal_flushing.start_flush_loop(Duration::from_millis(100)),
    start_server_loop(
      available_list,
      SeekableAsyncFile::open(&data_file_path).await,
      invisible_list,
      journal_pending.clone(),
      vacant_list,
    ),
  );
}
