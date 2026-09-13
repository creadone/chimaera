# Chimaera design

[Русский](DESIGN.ru.md) · [README](../README.md)

Chimaera is an embedded time-series storage engine written in Rust. It runs inside an application and manages one database in a local directory. Applications submit batches of points, receive write acknowledgements and query data through `Storage`.

The engine combines a write-ahead log, in-memory tables and immutable disk segments. Background workers flush tables and merge segments. Precomputed aggregate states let suitable time-grouping queries avoid reading every raw point.

## Data model

A series has the full key `(space_id: u32, bucket_id: u32, key: u64)`. Each point contains that key, an event timestamp `timestamp_us: i64` in Unix microseconds and a finite `f64` value. NaN and infinities are rejected.

The catalog stores spaces, buckets and policies. A catalog bucket belongs to a space and selects a storage policy for its series. `SeriesRef` addresses a series by space and bucket names; `resolve_series` maps those names to a numeric key. Applications map metric names and label sets to keys themselves.

The application supplies event time. It determines a point's disk placement and position in a query. Read ranges are always half-open: `[from_us, to_us)`. A point at the upper bound is excluded.

## Writes

A batch passes through a bounded queue, a single writer, the WAL and an in-memory table. The writer establishes the commit order within a database. Multiple clients can submit work and await acknowledgements concurrently.

Each batch receives one `commit_id`; operations within it have an `ordinal`. For a given point, the greatest `(commit_id, ordinal)` pair wins. A delete is a version that hides the earlier value. The entire batch becomes visible atomically.

The write-ahead log records changes not yet covered by a disk checkpoint. The durability mode determines when a write is acknowledged:

| Mode | Acknowledgement |
| --- | --- |
| `Sync` | after WAL synchronization and publication of the batch |
| `GroupCommit` | after a shared WAL synchronization and publication of the participating batches |
| `Async` | after publication, without confirmed WAL synchronization |

Group commit combines up to 64 waiting batches within a window of up to 200 microseconds. Applications keep a bounded number of writes in flight so that batches can share a synchronization. A serial sender that waits after every batch rarely provides that opportunity.

`enqueue_batch` and `enqueue_operations` return a `WriteTicket` once the batch enters the queue. A ticket distinguishes a pending reply, confirmed durability, visibility without confirmed synchronization and failure. A wait timeout does not cancel the batch. WAL append or synchronization failure permanently fails that writer; determining an accepted batch's outcome may require recovery.

### Point ordering

New policies use `StrictlyIncreasing`: each successive timestamp within a series must exceed its predecessor. Repeated or decreasing timestamps and deletes are rejected. The last timestamp of each series is restored from segments and the WAL at open.

Applications explicitly select `Mutable` for replacements, deletes and out-of-order writes within the policy's time bounds. Strict ordering gives the engine additional information: fresh points can extend an aggregate state without checking whether they replace old values.

## From memory to segments

After publication, a batch is available in a memtable. When a flush is needed, the table becomes immutable. A flush coordinator submits work to a bounded packing pool. Workers sort and encode data, build indexes and prepare files.

A segment is a logical data region. A pack file is a physical container that may hold multiple segments. Segments contain blocks, which are the units of reading, checksum verification, decoding and caching.

Timestamp codecs include plain `i64`, delta and delta-of-delta encoding. Values use plain `f64` or XOR encoding. Optional outer block compression uses gzip or zstd. A policy selects compression profiles for hot and cold data.

Prepared files go to the publisher. It writes a new manifest describing the current segment set, aggregate archives and checkpoint. Atomic publication makes that set current. Immutable files let readers retain the previous set while a replacement is built.

## L0, L1 and L2

Fresh flushes form L0. Background compaction merges segments, resolves point versions and reduces repeated work during reads. Active data resides in L1; sealed history resides in L2.

Work is divided by virtual shard, policy and time partition. A virtual shard is a local placement unit within one database. L0 compaction thresholds apply to each such domain. Several segments below the threshold may therefore remain after the queue has drained.

For each policy, the engine tracks an event watermark: the greatest accepted event timestamp. A configured time margin determines the boundary for sealing history. Allowed lateness and future timestamp bounds affect which region can be treated as sealed.

Active and historical compaction have separate queues. An admissible late version in L2 first becomes visible through the normal versioning mechanism. Historical compaction later incorporates it into replacement files. The coordinator selects work, pack workers prepare independent outputs and the publisher serializes publication.

Flush and compaction share a bounded packing pool. More workers help when independent regions and spare CPU and disk capacity are available. Queues absorb bursts; growing them does not replace sufficient background throughput.

## Reads and snapshots

Each query captures the current visible `commit_id`. This read snapshot holds one data version for the lifetime of the query. Concurrent writes or compaction do not change an existing stream. Independent queries may capture different snapshots.

The planner first prunes segments outside the requested range. It then uses series indexes, metadata pages and a published directory of block locations. The directory locates sealed data without repeatedly traversing every segment's internal structures.

The executor reads selected memory and disk sources, merges versions and applies tombstones. Blocks and metadata are loaded as needed. Bounded prefetch prepares upcoming blocks; coalesced loading lets concurrent queries wait for the same block load.

| Operation | Result |
| --- | --- |
| `point` | an exact timestamp match or `None` |
| `scan` | one series' points in ascending timestamp order |
| `group_stream` | a stream of time-grouped aggregate rows |
| `group` | the complete grouped result in memory |
| `each_group` | aggregate rows delivered to an application callback |

Multi-series grouping uses a bounded executor pool. `PerKey` produces results for individual series; `Combined` merges selected series into shared time buckets. One request contains series from a single catalog bucket in a single space.

Each stream item is a `Result`: a read can fail after earlier rows were returned successfully. Queries have bounds on series count, work performed, memory and result size, together with a deadline and cancellation signal.

## Aggregate summaries

Aggregate summaries store state for a minute, hour or day. The state includes sum, count, minimum, maximum, mean, a variance component and the first and last point. It also supports derived standard deviation, delta and rate.

`GroupSource::Auto` selects compatible aggregate summaries and raw data. Complete intervals can use a summary; query boundaries or fresh changes may require additional work. `GroupSource::Raw` requires raw points.

For an unfinished interval, the engine can use an aggregate summary as a base and add fresh points from L0 and memory. Under `Mutable`, a replacement or delete needs the previous value to adjust the aggregate correctly. Under `StrictlyIncreasing`, fresh points cannot replace old ones, simplifying this path.

Grouping supports fixed intervals and calendar boundaries in IANA time zones. A calendar day in a zone with daylight-saving changes can differ from a fixed 24-hour interval. Aggregate summaries preserve exact aggregate states; `f64` arithmetic still involves rounding.

## Retention

Policies specify separate retention periods for raw points and selected aggregates. Before physically deleting raw data, the engine publishes the required aggregate archive. Daily aggregates, for example, can outlive the detailed points.

Retention boundaries follow the engine clock. This is separate from sealing history using an event watermark. With retained aggregates, boundaries are aligned to UTC days to preserve complete aggregate intervals. A persisted boundary does not retreat after a clock rollback.

New queries stop seeing expired points, while an older snapshot may still read them. Physical deletion waits for the required background work and release of file references. Once raw points have been deleted, their aggregate cannot reconstruct an arbitrary subinterval or accept a correction that requires the original values.

## Memory and file resources

The engine accounts separately for persistent working structures and temporary work. Managed memory includes queued writes, memtables, strict-series timestamps and caches. Temporary memory covers query plans, decoding, worker results, packing and compaction. Its limit is derived from `memory_max_bytes`, as is the managed-memory limit.

Metadata is read in pages. Directory page caches participate in shared accounting and can release memory under pressure from other work. A one-off wide scan should not replace the whole hot block set: multi-series reads admit a block to the shared cache after repeated access.

RSS measures physical memory occupied by the entire process, including the allocator and stacks. It therefore differs from engine accounting. The application chooses the global allocator.

A shared permit pool bounds open files. The file cache uses that pool and reserves capacity for background work. Long-lived snapshots can retain old tables and replaced files; statistics expose their age and retained volume.

## Recovery, backups and shutdown

At open, the engine checks identity and the manifest, replays the required WAL records and starts its workers. Only the final WAL may have a recoverable torn tail. A complete frame with an invalid CRC is corruption. A separate `verify` operation checks every data block.

`snapshot` captures a checkpoint and copies its required files to a separate WAL-free directory. Writes can continue after the file set has been captured. `restore_snapshot` creates a database in a new directory. JSONL export/import transfers the catalog and available raw points; a physical snapshot also preserves aggregate archives.

For shutdown, the application stops its producers, calls `stop_admission`, waits for accepted batches and their checkpoint through `drain`, releases query streams and explicitly calls `close`. The close result reports errors that `Drop` cannot return. An old query stream can retain the directory lock after the workers have stopped.

## Source map

| Module | Responsibility |
| --- | --- |
| [engine](../src/engine/) | admission, writes, reads, planning, publication and background roles |
| [wal.rs](../src/wal.rs), [manifest.rs](../src/manifest.rs) | log and published state |
| [segment](../src/segment/) | blocks, codecs, indexes, metadata pages and location directories |
| [catalog.rs](../src/catalog.rs) | spaces, buckets, policies and write modes |
| [aggregate.rs](../src/aggregate.rs), [aggregate_archive.rs](../src/aggregate_archive.rs) | aggregate summaries and retained aggregates |
| [query.rs](../src/query.rs), [admission.rs](../src/admission.rs) | query limits and temporary memory |
| [cache.rs](../src/cache.rs), [file_pool.rs](../src/file_pool.rs) | managed memory, caches and files |
| [snapshot.rs](../src/snapshot.rs), [diagnostics.rs](../src/diagnostics.rs), [exchange.rs](../src/exchange.rs) | copying, verification, recovery and exchange |
| [lib.rs](../src/lib.rs) | public API |

This design covers one embedded database. Network protocols, authorization, metric labels and distribution across databases belong to the application using the engine.
