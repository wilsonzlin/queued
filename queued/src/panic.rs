use std::backtrace::Backtrace;
use std::io::stderr;
use std::io::Write;
use std::panic;
use std::process;

pub(crate) fn set_up_panic_hook() {
  // Currently, even with `panic = abort`, thread panics will still not kill the process, so we'll install our own handler to ensure that any panic exits the process, as most likely some internal state/invariant has become corrupted and it's not safe to continue. Copied from https://stackoverflow.com/a/36031130.
  let _orig_hook = panic::take_hook();
  panic::set_hook(Box::new(move |panic_info| {
    let bt = Backtrace::force_capture();
    // Don't use `tracing::*` as it may be dangerous to do so from within a panic handler (e.g. it could itself panic).
    // Do not lock stderr as we could deadlock.
    // Build string first so we (hopefully) do one write syscall to stderr and don't get it mangled.
    // Prepend with a newline to avoid mangling with any existing half-written stderr line.
    let json = format!(
      "\r\n{}\r\n",
      serde_json::json!({
        "level": "CRITICAL",
        "panic": true,
        "message": panic_info.to_string(),
        "stack_trace": bt.to_string(),
      })
    );
    // Try our best to write all and then flush, but don't panic if we don't.
    let mut out = stderr();
    let _ = out.write_all(json.as_bytes());
    let _ = out.flush();
    process::exit(1);
  }));
}
