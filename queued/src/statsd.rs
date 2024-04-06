use cadence::Counted;
use cadence::Gauged;
use cadence::QueuingMetricSink;
use cadence::StatsdClient;
use cadence::UdpMetricSink;
use chrono::Utc;
use libqueued::Queued;
use serde::Serialize;
use std::cmp::max;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::sync::Weak;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;

#[derive(Serialize)]
pub(crate) struct Metrics {
  empty_poll_counter: u64,
  message_counter: u64,
  missing_delete_counter: u64,
  missing_update_counter: u64,
  successful_delete_counter: u64,
  successful_poll_counter: u64,
  successful_push_counter: u64,
  successful_update_counter: u64,
  suspended_delete_counter: u64,
  suspended_poll_counter: u64,
  suspended_push_counter: u64,
  suspended_update_counter: u64,
  throttled_poll_counter: u64,

  first_message_visibility_timeout_sec_gauge: u64,
  last_message_visibility_timeout_sec_gauge: u64,
  longest_unpolled_message_sec_gauge: u64,
}

pub(crate) fn build_metrics(q: &Queued) -> Metrics {
  let now = Utc::now().timestamp();
  let m = q.metrics();
  Metrics {
    empty_poll_counter: m.empty_poll_counter(),
    message_counter: m.message_counter(),
    missing_delete_counter: m.missing_delete_counter(),
    missing_update_counter: m.missing_update_counter(),
    successful_delete_counter: m.successful_delete_counter(),
    successful_poll_counter: m.successful_poll_counter(),
    successful_push_counter: m.successful_push_counter(),
    successful_update_counter: m.successful_update_counter(),
    suspended_delete_counter: m.suspended_delete_counter(),
    suspended_poll_counter: m.suspended_poll_counter(),
    suspended_push_counter: m.suspended_push_counter(),
    suspended_update_counter: m.suspended_update_counter(),
    throttled_poll_counter: m.throttled_poll_counter(),

    first_message_visibility_timeout_sec_gauge: q
      .youngest_message_time()
      .map(|t| max(0, t - now) as u64)
      .unwrap_or(0),
    last_message_visibility_timeout_sec_gauge: q
      .oldest_message_time()
      .map(|t| max(0, t - now) as u64)
      .unwrap_or(0),
    longest_unpolled_message_sec_gauge: q
      .youngest_message_time()
      .map(|t| max(0, now - t) as u64)
      .unwrap_or(0),
  }
}

pub(crate) fn spawn_statsd_emitter(
  addr: SocketAddr,
  statsd_prefix: &str,
  statsd_tags: &[(String, String)],
  queue_name: &str,
  qref: Weak<Queued>,
) {
  let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
  socket.set_nonblocking(true).unwrap();
  let sink = UdpMetricSink::from(addr, socket).unwrap();
  let sink = QueuingMetricSink::from(sink);
  let mut sb = StatsdClient::builder(statsd_prefix, sink);
  for (k, v) in statsd_tags {
    sb = sb.with_tag(k, v);
  }
  sb = sb.with_tag("queue", queue_name);
  let s = sb.build();

  #[rustfmt::skip]
  spawn({
    async move {
      // Avoid holding on to `qref` as much as possible, it blocks endpoint_delete_queue.
      let mut p = {
        let Some(q) = qref.upgrade() else {
          return;
        };
        build_metrics(&q)
      };
      loop {
        sleep(Duration::from_millis(1000)).await;
        // Avoid holding on to `qref` as much as possible, it blocks endpoint_delete_queue.
        let m = {
          let Some(q) = qref.upgrade() else {
            break;
          };
          build_metrics(&q)
        };
        macro_rules! d {
          ($f:ident) => {
            i64::try_from(m.$f).unwrap() - i64::try_from(p.$f).unwrap()
          };
        }
        s.count("empty_poll", d!(empty_poll_counter)).unwrap();
        s.gauge("message_count", m.message_counter).unwrap();
        s.count("missing_delete", d!(missing_delete_counter)).unwrap();
        s.count("missing_update", d!(missing_update_counter)).unwrap();
        s.count("successful_delete", d!(successful_delete_counter)).unwrap();
        s.count("successful_poll", d!(successful_poll_counter)).unwrap();
        s.count("successful_push", d!(successful_push_counter)).unwrap();
        s.count("successful_update", d!(successful_update_counter)).unwrap();
        s.count("suspended_delete", d!(suspended_delete_counter)).unwrap();
        s.count("suspended_poll", d!(suspended_poll_counter)).unwrap();
        s.count("suspended_push", d!(suspended_push_counter)).unwrap();
        s.count("suspended_update", d!(suspended_update_counter)).unwrap();
        s.count("throttled_poll", d!(throttled_poll_counter)).unwrap();
        s.gauge("first_message_visibility_timeout_sec", m.first_message_visibility_timeout_sec_gauge).unwrap();
        s.gauge("last_message_visibility_timeout_sec", m.last_message_visibility_timeout_sec_gauge).unwrap();
        s.gauge("longest_unpolled_message_sec", m.longest_unpolled_message_sec_gauge).unwrap();
        p = m;
      };
    }
  });
}
