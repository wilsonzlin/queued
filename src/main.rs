pub mod const_;
pub mod ctx;
pub mod endpoint;
pub mod file;
pub mod journal;
pub mod slice;
pub mod slot;
pub mod time;

use crate::const_::SLOTS_OFFSET_START;
use crate::const_::STATE_LEN_RESERVED;
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
use std::sync::Arc;
use std::time::Duration;
use tokio::join;
use tokio::sync::RwLock;

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
  // TODO ftruncate if possible. WARNING: Must fill with zeros.
  file
    .write_at(0, vec![0u8; 1024 * 1024 * STATE_LEN_RESERVED])
    .await;

  file
    .write_at(
      STATE_OFFSETOF_VACANT_HEAD,
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

  let journal_pending = Arc::new(JournalPending::new());

  let journal_flushing = JournalFlushing::new(
    SeekableAsyncFile::open(&data_file_path).await,
    SeekableAsyncFile::open(&journal_file_path).await,
    journal_pending.clone(),
  );

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
