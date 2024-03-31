use clap::Parser;
use serde::Deserialize;
use std::env::var;
use std::env::var_os;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

// We cannot simply rely on default value if omitted, as we need to differentiate between a set (but empty/default) value and an omitted value to know if they override/are overriden by defaults, env vars, CLI, etc.
#[derive(Parser, Debug, Deserialize)]
#[command(author, version, about)]
struct Cli {
  /// Path to the config file.
  #[arg(long)]
  config: Option<PathBuf>,

  /// Path to the data directory.
  #[arg(long)]
  data_dir: Option<PathBuf>,

  /// Optional API key that clients must use to authenticate.
  #[arg(long)]
  api_key: Option<String>,

  /// Interface for server to listen on. Defaults to 127.0.0.1.
  #[arg(long)]
  interface: Option<Ipv4Addr>,

  /// Port for server to listen on. Defaults to 3333.
  #[arg(long)]
  port: Option<u16>,

  /// If provided, the server will create and listen on this Unix socket; `interface` and `port` will be ignored.
  #[arg(long)]
  unix_socket: Option<PathBuf>,

  /// Optional StatsD server to send metrics to.
  #[arg(long)]
  statsd: Option<SocketAddr>,

  /// StatsD prefix. Defaults to "queued".
  #[arg(long)]
  statsd_prefix: Option<String>,

  /// Tags to add to all StatsD metric values sent. Use the format: `name1:value1,name2:value2,name3:value3`.
  #[arg(long)]
  statsd_tags: Option<String>,

  /// Batch sync delay time, in microseconds. For advanced usage only.
  #[arg(long)]
  batch_sync_delay_us: Option<u64>,
}

// We cannot simply rely on default value if omitted, as we need to differentiate between a set (but empty/default) value and an omitted value to know if they override/are overriden by defaults, env vars, CLI, etc.
#[derive(Default, Deserialize)]
struct CfgFile {
  data_dir: Option<PathBuf>,
  api_key: Option<String>,
  interface: Option<Ipv4Addr>,
  port: Option<u16>,
  unix_socket: Option<PathBuf>,
  statsd: Option<SocketAddr>,
  statsd_prefix: Option<String>,
  statsd_tags: Option<String>,
  batch_sync_delay_us: Option<u64>,
}

pub(crate) struct Cfg {
  pub data_dir: PathBuf,
  pub api_key: Option<String>,
  pub interface: Ipv4Addr,
  pub port: u16,
  pub unix_socket: Option<PathBuf>,
  pub statsd: Option<SocketAddr>,
  pub statsd_prefix: String,
  pub statsd_tags: Vec<(String, String)>,
  pub batch_sync_delay: Duration,
}

fn env_parsed<T: FromStr>(name: &str) -> Option<T> {
  let raw = var(name).ok()?;
  let Ok(parsed) = raw.parse::<T>() else {
    panic!("invalid {name}");
  };
  Some(parsed)
}

fn env_path(name: &str) -> Option<PathBuf> {
  let raw = var_os(name)?;
  Some(PathBuf::from(raw))
}

fn env_str(name: &str) -> Option<String> {
  var(name).ok()
}

// Precedence:
// - Lowest: config file.
// - Then: env vars.
// - Highest: CLI args.
pub(crate) fn load_cfg() -> Cfg {
  let cli = Cli::parse();

  let f = cli
    .config
    .or_else(|| env_path("QUEUED_CONFIG"))
    .map(|cfg_path| {
      let cfg = std::fs::read_to_string(&cfg_path).expect("failed to read config file");
      let cfg: CfgFile = toml::from_str(&cfg).expect("failed to parse config file");
      cfg
    })
    .unwrap_or_default();

  Cfg {
    data_dir: cli
      .data_dir
      .or(env_path("QUEUED_DATA_DIR"))
      .or(f.data_dir)
      .expect("no data dir provided"),
    api_key: cli.api_key.or(env_str("QUEUED_API_KEY")).or(f.api_key),
    interface: cli
      .interface
      .or(env_parsed("QUEUED_INTERFACE"))
      .or(f.interface)
      .unwrap_or_else(|| "127.0.0.1".parse().unwrap()),
    port: cli
      .port
      .or(env_parsed("QUEUED_PORT"))
      .or(f.port)
      .unwrap_or(3333),
    unix_socket: cli
      .unix_socket
      .or(env_path("QUEUED_UNIX_SOCKET"))
      .or(f.unix_socket),
    statsd: cli.statsd.or(env_parsed("QUEUED_STATSD")).or(f.statsd),
    statsd_prefix: cli
      .statsd_prefix
      .or(env_str("QUEUED_STATSD_PREFIX"))
      .or(f.statsd_prefix)
      .unwrap_or("queued".to_string()),
    statsd_tags: cli
      .statsd_tags
      .or(env_str("QUEUED_STATSD_TAGS"))
      .or(f.statsd_tags)
      .unwrap_or_default()
      .split(',')
      .filter_map(|p| p.split_once(':'))
      .map(|(k, v)| (k.to_string(), v.to_string()))
      .collect::<Vec<_>>(),
    batch_sync_delay: Duration::from_micros(
      cli
        .batch_sync_delay_us
        .or(env_parsed("QUEUED_BATCH_SYNC_DELAY_US"))
        .or(f.batch_sync_delay_us)
        .unwrap_or(10000),
    ),
  }
}
