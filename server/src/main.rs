pub mod const_;
pub mod ctx;
pub mod endpoint;
pub mod file;
pub mod journal;
pub mod slot;
pub mod util;

use crate::const_::SLOTS_OFFSET_START;
use crate::const_::STATE_LEN_RESERVED;
use crate::const_::STATE_OFFSETOF_FRONTIER;
use crate::file::SeekableAsyncFile;
use crate::journal::clear_journal;
use crate::journal::restore_journal;
use crate::journal::JournalFlushing;
use axum::routing::post;
use axum::Router;
use axum::Server;
use clap::arg;
use clap::command;
use clap::Parser;
use const_::SLOT_OFFSETOF_NEXT;
use const_::STATE_OFFSETOF_AVAILABLE_HEAD;
use const_::STATE_OFFSETOF_INVISIBLE_HEAD;
use const_::STATE_OFFSETOF_VACANT_HEAD;
use ctx::Ctx;
use endpoint::poll::endpoint_poll;
use endpoint::push::endpoint_push;
use journal::JournalPending;
use slot::Slot;
use slot::SlotList;
use slot::SlotLists;
use std::collections::LinkedList;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::join;
use tokio::sync::RwLock;

async fn load_list_from_data(data_file: &SeekableAsyncFile, head_offset: u64) -> LinkedList<Slot> {
  let mut list = LinkedList::new();
  let mut offset = head_offset;
  while offset != 0 {
    list.push_back(Slot { offset });
    offset = data_file.read_u64_at(offset + SLOT_OFFSETOF_NEXT).await;
  }
  list
}

struct LoadedData {
  frontier: u64,
  lists: SlotLists,
}

async fn load_data(data_file: &SeekableAsyncFile) -> LoadedData {
  let available_head_offset = data_file.read_u64_at(STATE_OFFSETOF_AVAILABLE_HEAD).await;
  let available_list = load_list_from_data(data_file, available_head_offset).await;

  let invisible_head_offset = data_file.read_u64_at(STATE_OFFSETOF_INVISIBLE_HEAD).await;
  let invisible_list = load_list_from_data(data_file, invisible_head_offset).await;

  let vacant_head_offset = data_file.read_u64_at(STATE_OFFSETOF_VACANT_HEAD).await;
  let vacant_list = load_list_from_data(data_file, vacant_head_offset).await;

  let lists = SlotLists {
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
  };

  let frontier = data_file.read_u64_at(STATE_OFFSETOF_FRONTIER).await;

  LoadedData { frontier, lists }
}

async fn start_server_loop(
  data_fd: SeekableAsyncFile,
  frontier: u64,
  journal_pending: Arc<JournalPending>,
  lists: Arc<RwLock<SlotLists>>,
) {
  let ctx = Arc::new(Ctx {
    data_fd,
    frontier: AtomicU64::new(frontier),
    journal_pending,
    lists,
  });

  let app = Router::new()
    .route("/poll", post(endpoint_poll))
    .route("/push", post(endpoint_push))
    .with_state(ctx.clone());

  let addr = SocketAddr::from(([0, 0, 0, 0], 3333));

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}

async fn format_data_file(data_file_path: &Path) {
  let file = SeekableAsyncFile::create(data_file_path).await;
  // WARNING: Must fill with zeros. File::set_len is guaranteed to fill with zeroes.
  file.truncate(STATE_LEN_RESERVED).await;

  file
    .write_at(
      STATE_OFFSETOF_FRONTIER,
      SLOTS_OFFSET_START.to_be_bytes().to_vec(),
    )
    .await;

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
    format_data_file(&data_file_path).await;
    format_journal_file(&journal_file_path).await;
  }

  restore_journal(
    &SeekableAsyncFile::open(&data_file_path).await,
    &SeekableAsyncFile::open(&journal_file_path).await,
  )
  .await;

  let LoadedData { frontier, lists } =
    load_data(&SeekableAsyncFile::open(&data_file_path).await).await;
  let lists = Arc::new(RwLock::new(lists));

  let journal_pending = Arc::new(JournalPending::new());

  let journal_flushing = JournalFlushing::new(
    SeekableAsyncFile::open(&data_file_path).await,
    SeekableAsyncFile::open(&journal_file_path).await,
    lists.clone(),
    journal_pending.clone(),
  );

  join!(
    journal_flushing.start_flush_loop(Duration::from_millis(1)),
    start_server_loop(
      SeekableAsyncFile::open(&data_file_path).await,
      frontier,
      journal_pending.clone(),
      lists.clone(),
    ),
  );
}
