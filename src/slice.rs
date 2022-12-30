pub fn slice_len<T>(slice: &[T], offset: usize, len: usize) -> &[T] {
  &slice[offset..offset + len]
}
