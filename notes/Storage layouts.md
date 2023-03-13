# Storage layouts

## Fixed slots

The device is divided into fixed-length slots. Each slot is either vacant or contains a message. Creating a message acquires any vacant slot, and deleting a message makes its slot vacant. Free space is determined by how many slots are vacant.

### Pros

- No garbage collector pauses or unreclaimed (and therefore unusable) free space.
- No journaling or careful serialisation of repeated writes to same locations.

### Cons

- Messages are limited to 1 KiB, including metadata. This will be adjustable at format time in the future.
- The server is limited to up to 2<sup>32</sup> (around 4 billion) messages at any time. Note that this would require 16 TiBs of storage. This is currently a simplification optimisation, and may be adjusted in the future.
- Due to hashing, we need to read and rehash lots of data even if we only want to update a small part (e.g. one field).

## Log structured

The device is a circuar buffer of bytes, used as a log. Each operation (create, poll, update, delete) records a log entry at the tail, consuming the amount of bytes necessary for the entry. Free space is determined by the distance from the tail to the head, and is increased by moving the head past entries representing messages that have since been deleted. If the tail is near the physical end and there is not enough physical space for a message, a dummy entry is inserted into the remaining space.

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
