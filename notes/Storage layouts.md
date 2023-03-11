# Storage layouts

## Fixed slots

### Cons

- Messages are limited to 1 KiB, including metadata. This will be adjustable at format time in the future.
- The server is limited to up to 2<sup>32</sup> (around 4 billion) messages at any time. Note that this would require 16 TiBs of storage. This is currently a simplification optimisation, and may be adjusted in the future.
- Due to hashing, we need to read and rehash lots of data even if we only want to update a small part (e.g. one field).
  - TODO We could use journaling instead to avoid this.

## Log structured

### Pros

- Individual messages cannot get corrupted from crashing (as the tail pointer won't point to them yet), so they don't require hashes.
- Stores messages compactly, doesn't leave unused space, and doesn't have upper limit on message size other than free device space remaining.
- Better I/O performance due to writing in large sequential chunks (instead of lots of small chunks at many random offsets).
- Formatting is extremely fast and doesn't require full disk writing.
- Starting up is very fast due to not needing a full disk scan or hashing.

### Cons

- Cannot resize easily.
- Space could be quickly used up under heavy load, especially by operations that would otherwise mutate in place or free space.
- Fragmentation from interspersed log entries for the same messages makes it more difficult to free space from the head.

### Implementation

- We still keep VacantSlots to generate u32 indices. We still prefer reusable u32 over unique u64, even though we're not actually using slots anymore, because u32 is much more compact in memory and this is important when every message's metadata is loaded in memory. In this layout, there is no upper limit except u32::MAX.
