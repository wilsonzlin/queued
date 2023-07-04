use off64::u64;
use off64::usz;
use off64::Off64Read;
use seekable_async_file::SeekableAsyncFile;
use std::cmp::min;
use tinybuf::TinyBuf;

pub(crate) struct SeekableAsyncFileBuffer {
  file: SeekableAsyncFile,
  file_len: u64,
  cache_dev_offset: u64,
  cache: Vec<u8>,
  cache_size: u64,
}

impl SeekableAsyncFileBuffer {
  pub fn new(file: SeekableAsyncFile, file_len: u64, cache_size: u64) -> SeekableAsyncFileBuffer {
    Self {
      cache: Vec::new(),
      cache_dev_offset: 0,
      cache_size,
      file,
      file_len,
    }
  }

  pub async fn read_at(&mut self, offset: u64, len: u64) -> TinyBuf {
    let mut out = Vec::new();
    let (start, end) = (offset, offset + len);
    let (cache_start, cache_end) = (
      self.cache_dev_offset,
      self.cache_dev_offset + u64!(self.cache.len()),
    );
    if cache_start <= start && start < cache_end {
      out.extend_from_slice(
        &self
          .cache
          .read_at(start - cache_start, min(cache_end, end) - start),
      );
    };
    if u64!(out.len()) < len {
      let new_cache_start = start + u64!(out.len());
      let res = self
        .file
        .read_at(
          new_cache_start,
          min(self.cache_size, self.file_len - new_cache_start),
        )
        .await;
      // This will panic if not enough bytes read i.e. requested `offset + len` is beyond file.
      out.extend_from_slice(&res[..usz!(len) - out.len()]);
      self.cache_dev_offset = new_cache_start;
      self.cache_size = u64!(res.len());
      self.cache = res.to_vec();
    }
    assert_eq!(u64!(out.len()), len);
    out.into()
  }
}
