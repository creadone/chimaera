<p align="center">
  <img src="assets/chimaera-logo.png" alt="Chimaera" width="640">
</p>

[Русский](README.ru.md)

# Chimæra

Chimaera is an embedded engine for storing and querying metrics in Rust applications. It acknowledges writes after synchronizing the write-ahead log and computes exact aggregates. Memory usage and query latency remained stable in the tested workloads.

## Status

* **Current version:** `0.2.0`
* **Stability:** active development.
* **Direction:** the engine is being developed as the core of a future database, not as a standalone product.

## Features

* **MVCC and LSM storage.** Each query reads its own snapshot while writes and background compactions continue. Data is stored in immutable segments, with L0/L1/L2 compaction running in the background.

* **GroupCommit, WAL, and crash recovery.** Writes are acknowledged after the WAL is synchronized and the data is published for reading. After a crash, the engine replays WAL records not yet covered by persisted data files. A timeout or a dropped ticket does not cancel an accepted write.

* **Columnar storage and compression.** By default, timestamps use delta-of-delta encoding; hot values are stored as raw `f64`, and cold values use Gorilla XOR. A storage policy can enable additional Zstd-3 block compression. In a local test with constant values, Zstd reduced the logical size of the whole database by 79.8% compared with delta-of-delta + Gorilla without Zstd. Savings depend on the data.

* **Queries.** Point lookups, range scans, grouping, and aggregates include count, sum, minimum, maximum, mean, variance, and more. Multiple series can be aggregated separately or combined into a single result.

* **Retention and archives.** Buckets can use different retention periods, partition spans, and codecs. Aggregate archives can outlive raw points. Deleting a series removes its entire history, including archives.

* **Resource limits and overload handling.** Memory, caches, and queues have budgets. Under overload, a write may wait for resources or return an error. Queries have limits on work, memory, and execution time, with cooperative cancellation.

## Limitations

* **Strict timestamp order.** Timestamps must increase within each series. Updates, duplicate timestamps, and out-of-order points cause the entire batch to be rejected.

* **Numeric series only.** Values are finite `f64` numbers; timestamps are Unix microseconds. Strings and arbitrary value types are not supported.

* **Aggregation within one storage group.** A query can combine series only from the same space and bucket.

* **Background work affects query latency.** Large segment compactions and backups can increase query execution time.

* **The memory budget is not a process-wide limit.** Actual memory consumption can exceed the engine's configured budget.

* **Disk space is reclaimed later.** Deleted data remains in files until background cleanup runs. Existing queries can delay reclamation further.

## Architecture

Chimaera runs inside the application and manages time-series storage: disk writes, data lookup, aggregate computation, and expired-history cleanup. Applications call the engine through its Rust API; no separate server is required.

Storage follows an LSM design. New points first enter the write-ahead log and memory, then move into immutable files. Background workers merge these files and build summaries for fast aggregate queries. History is partitioned by time, and indexes locate the relevant data. Writes, reads, and maintenance run concurrently; consistent snapshots let readers continue while the engine changes the physical layout.

| Component | Responsibility |
|---|---|
| Write admission | Reserves memory and places batches in a bounded queue |
| Writer and GroupCommit | One writer orders writes. Multiple batches share a WAL synchronization while retaining their individual atomicity |
| WAL | Supports crash recovery. Writes are acknowledged after log synchronization and publication for reading |
| Memory | Holds recent points. Full generations are frozen and flushed to disk |
| Immutable files | Contain point blocks, indexes, and summaries. Multiple logical segments are packed into a shared file |
| Disk catalog | An index tree locates segments by series and time. A bounded page cache is kept in memory |
| Query executor | Combines relevant data from memory and files and computes the result |
| Background maintenance | Compacts data, builds summaries, cleans up expired data, and reclaims replaced files |

Files move through three levels:

- **L0:** recently flushed data.
- **L1:** data in the active window after compactions reduce overlap.
- **L2:** sealed historical windows. Historical maintenance is scoped to the relevant windows rather than the entire accumulated database.

**Reads use MVCC snapshots.** A query captures the state of memory and files. Background workers can create new versions, while old versions remain available as long as readers need them. The catalog and manifest publish a consistent new set of files.

**Aggregates use exact summaries.** For fully covered blocks, the engine combines precomputed aggregate states; it reads recent points and partial range boundaries directly. This avoids rescanning the entire series history for each query.

Database growth does not require keeping the entire catalog in RAM or visiting every segment for each query. Compactions and reads still share CPU and disk resources; queues, caches, and working memory are bounded by budgets.

## Data model

Chimaera stores numeric time series: each series is a sequence of timestamp–value pairs. For example, one server's CPU temperature is one series, and the same server's memory usage is another.

| Level | Meaning |
|---|---|
| Space | A named group of buckets, such as a project |
| Bucket | A group of series sharing a storage policy: retention, compression, and the hot-data window |
| Series | A sequence of points identified by a `u64` key unique within its bucket |
| Point | An `i64` timestamp in Unix microseconds and an `f64` value |

A series is identified by `(space_id, bucket_id, key)`. A point is identified by the same key plus its timestamp. `NaN` and infinity are rejected.

Data rules:

- Duplicate or earlier timestamps are rejected. Intervals may vary, and gaps are allowed.
- A batch is atomic and can contain points from multiple series. One invalid point causes the entire batch to be rejected.
- Each point has one numeric value. The application defines metric names, units, tags, and what the value represents: a counter, temperature, percentage, or something else.
- Summaries are derived data. The engine builds them to accelerate aggregates; minute, hour, and day archives can be retained longer than raw points.
- Reads use consistent MVCC snapshots. Concurrent writes or deletions do not change an existing query's view.
- A whole series or a set of series can be deleted, including their summaries. A new history can then begin under the same key.

By default, queries over multiple series compute aggregates separately for each series. `Combined` mode merges the selected series into one result.

## API and queries

Writes and reads go through `Storage`. This complete example uses a new database: it writes two series, reads an individual point and a range, and computes minute aggregates.

For simplicity, it uses numeric keys without registering a space or bucket in the catalog.

`write_batch` accepts a batch of points and returns a write receipt. Points in each series are five seconds apart in this example.

```rust
use chimaera::{
    AggregateSet, GroupRequest, MINUTE_US, OpenOptions,
    Point, Result, SeriesKey, Storage,
};

fn main() -> Result<()> {
    let storage = Storage::open("./example-db", OpenOptions::default())?;

    let cpu = SeriesKey::new(1, 1, 101);
    let memory = SeriesKey::new(1, 1, 102);

    let start = 1_700_000_000_000_000_i64;
    let end = start + MINUTE_US;

    // Write points for both series in one atomic batch.
    let receipt = storage.write_batch(vec![
        Point::new(cpu, start, 10.0)?,
        Point::new(cpu, start + 5_000_000, 14.0)?,
        Point::new(memory, start, 100.0)?,
        Point::new(memory, start + 5_000_000, 120.0)?,
    ])?;

    println!("Written points: {}", receipt.point_count);

    // Read a point at an exact timestamp.
    if let Some(point) = storage.point(cpu, start)? {
        println!("CPU value: {}", point.value);
    }

    // Read all points in [start, end).
    let points = storage
        .scan(cpu, start, end)?
        .collect::<Result<Vec<_>>>()?;

    println!("CPU points: {points:?}");

    // Calculate one-minute aggregates separately for each series.
    let request = GroupRequest::fixed(
        vec![cpu, memory],
        start,
        end,
        MINUTE_US,
        start,
    )?
    .with_aggregates(AggregateSet::MEAN | AggregateSet::MAX);

    let result = storage.group(request)?;

    for row in result.rows {
        println!(
            "Series: {:?}, count: {}, mean: {:?}, max: {:?}",
            row.series, row.count, row.mean, row.max,
        );
    }

    storage.close()?;
    Ok(())
}
```

The following read methods are available:

| Method | Result |
|---|---|
| `point` | The point at the specified timestamp, or `None` if it is absent |
| `scan` | A stream of raw points from one series in ascending timestamp order |
| `group` | A materialized result of aggregation into time buckets |
| `group_stream` | The same aggregate result, returned incrementally |

Ranges use **`[start, end)`**: the start is inclusive, and the end is exclusive.

`GroupRequest::fixed` takes the series, range, bucket width, and bucket-grid origin. In this example, the one-minute grid starts at `start`. The result is:

| Series | Point count | Mean | Maximum |
|---|---:|---:|---:|
| CPU — 101 | 2 | 12 | 14 |
| Memory — 102 | 2 | 110 | 120 |

Aggregate flags are combined with `|` and computed in one traversal. `count` and `sum` are always present. Each series is aggregated separately by default; use `GroupScope::Combined` for a combined result across the selected series.

For large results, consume `scan` and `group_stream` incrementally instead of collecting the whole response into a `Vec`. Check errors on every iteration: the result is complete only after the stream reaches its end.

## Performance

* A 72-hour run on October 3–6, 2026 demonstrated stability at 1,000,000 points and 100 reads per second concurrently. The engine revision was `2370438`.
* Memory and CPU figures are interval averages. Latency figures in both tables are mean / p95 / p99 in milliseconds, including queueing time.
* Test configuration: Linux, 8 vCPU, 10,000 series, batches of 10,000 points, four writers and four readers, Zstd-3, and 30-day retention. Data timestamps advance in five-second steps with jitter; data time is explicitly accelerated by a factor of 500. The first minute of warmup is excluded.
* Five query types run in equal proportions: latest point, mean/count, rate, max/mean, and mean/stddev. Aggregate windows end four hours of data time before the latest acknowledged write. Once retention takes effect, the database holds 30 days of history; this is not a test of unbounded database growth.

### Resource stability

| Hours | Writes, points/s | Reads/s | RSS, MiB | CPU, cores | Batch acknowledgment, ms |
|---|---:|---:|---:|---:|---|
| 0–6 | 1,000,000 | 100.00 | 590.9 | 3.69 | 7.73 / 13.41 / 18.37 |
| 6–12 | 1,000,000 | 100.00 | 590.3 | 3.71 | 7.48 / 13.46 / 18.41 |
| 12–18 | 1,000,000 | 100.00 | 593.6 | 3.72 | 7.44 / 13.50 / 18.45 |
| 18–24 | 1,000,000 | 100.00 | 594.6 | 3.70 | 7.40 / 13.44 / 18.37 |
| 24–30 | 1,000,000 | 100.00 | 592.9 | 3.69 | 7.37 / 13.38 / 18.35 |
| 30–36 | 1,000,000 | 100.00 | 593.9 | 3.70 | 7.39 / 13.43 / 18.37 |
| 36–42 | 1,000,000 | 100.00 | 594.0 | 3.71 | 7.40 / 13.44 / 18.37 |
| 42–48 | 1,000,000 | 100.00 | 592.2 | 3.71 | 7.43 / 13.49 / 18.38 |
| 48–54 | 1,000,000 | 100.00 | 593.0 | 3.72 | 7.42 / 13.46 / 18.37 |
| 54–60 | 1,000,000 | 100.00 | 592.6 | 3.71 | 7.40 / 13.43 / 18.37 |
| 60–66 | 1,000,000 | 100.00 | 591.9 | 3.69 | 7.39 / 13.40 / 18.35 |
| 66–72 | 999,997 | 100.00 | 589.8 | 3.70 | 7.39 / 13.42 / 18.37 |

### Aggregate query latency

| Hours | Mean/count | Rate | Max/mean | Mean/stddev |
|---|---|---|---|---|
| 0–6 | 15.84 / 21.33 / 26.34 | 16.11 / 22.08 / 26.67 | 16.15 / 22.00 / 25.17 | 15.42 / 20.70 / 23.76 |
| 6–12 | 15.91 / 21.44 / 26.53 | 16.18 / 22.22 / 26.88 | 16.21 / 22.08 / 25.26 | 15.49 / 20.80 / 23.95 |
| 12–18 | 15.91 / 21.45 / 26.51 | 16.18 / 22.21 / 26.86 | 16.20 / 22.08 / 25.25 | 15.48 / 20.82 / 23.92 |
| 18–24 | 15.82 / 21.36 / 26.41 | 16.09 / 22.09 / 26.73 | 16.11 / 22.02 / 25.18 | 15.40 / 20.70 / 23.84 |
| 24–30 | 15.85 / 21.42 / 26.34 | 16.11 / 22.13 / 26.70 | 16.15 / 22.02 / 25.17 | 15.41 / 20.73 / 23.79 |
| 30–36 | 15.94 / 21.55 / 26.59 | 16.20 / 22.27 / 26.93 | 16.22 / 22.16 / 25.36 | 15.50 / 20.86 / 24.00 |
| 36–42 | 15.93 / 21.50 / 26.50 | 16.19 / 22.24 / 26.85 | 16.22 / 22.14 / 25.30 | 15.48 / 20.80 / 23.97 |
| 42–48 | 15.93 / 21.49 / 26.50 | 16.19 / 22.21 / 26.77 | 16.21 / 22.09 / 25.26 | 15.48 / 20.78 / 23.87 |
| 48–54 | 15.95 / 21.52 / 26.56 | 16.21 / 22.22 / 26.82 | 16.24 / 22.13 / 25.28 | 15.50 / 20.82 / 23.92 |
| 54–60 | 15.94 / 21.54 / 26.56 | 16.20 / 22.24 / 26.85 | 16.23 / 22.16 / 25.33 | 15.50 / 20.85 / 23.95 |
| 60–66 | 15.90 / 21.49 / 26.53 | 16.16 / 22.19 / 26.80 | 16.20 / 22.09 / 25.26 | 15.46 / 20.78 / 23.86 |
| 66–72 | 15.87 / 21.39 / 26.37 | 16.13 / 22.13 / 26.69 | 16.16 / 22.02 / 25.21 | 15.43 / 20.73 / 23.84 |

All aggregate queries select 300 series over 30 minutes, grouped into one-minute buckets.

## Comparison with other engines

The comparison ran on Linux with 8 AMD EPYC 9554 vCPUs. The initial database contained 10,000 series and 3.6 million points. Batches contained 1,000 points, with four readers and no rate cap.

Each case ran twice for 60 seconds. Throughput is shown as forward / reverse execution order, RSS as mean / highest observed peak, and CPU as the average number of cores used by the process. Aggregate queries selected 300 series over 30 minutes, grouped into one-minute buckets.

The tables use Chimaera revision `d329320`, before the subsequent read optimization. Before measurement, the database was reopened and warmed up with queries. Five query types ran in equal proportions: point lookup, mean/count, rate, max/mean, and mean/stddev. Mixed workloads read a fixed, preloaded window while new points were written after it. Measurements include the public API and adapter; each engine wrote at its own maximum rate. This is a short comparison of one workload profile.

### Read-only

| Engine | Queries/s | RSS, MiB | CPU, cores |
|---|---:|---:|---:|
| Chimaera | 301.65 / 300.05 | 38 / 219 | 3.95 |
| tsink | 641.00 / 563.92 | 178 / 742 | 4.00 |
| chDB | 69.02 / 68.60 | 374 / 400 | 3.43 |
| SQLite | 22.97 / 22.88 | 69 / 70 | 2.74 |
| Ugnos | 408.95 / 411.30 | 62 / 747 | 4.00 |
| NanoTS | 20.53 / 20.22 | 32 / 33 | 2.32 |
| tstorage | 0.93 / 0.98 | 958 / 1486 | 3.30 |

### Write-only

| Engine | Writers | Points/s | RSS, MiB | CPU, cores |
|---|---:|---:|---:|---:|
| Chimaera | 1 | 769,967 / 749,900 | 270 / 370 | 1.20 |
| Chimaera | 4 | **1,601,517 / 1,600,000** | 416 / 527 | 2.36 |
| tsink | 1 | 46,333 / 44,817 | 759 / 1022 | 0.95 |
| tsink | 4 | 80,017 / 86,783 | 755 / 1035 | 1.13 |
| chDB | 1 | 165,233 / 165,633 | 352 / 396 | 0.99 |
| chDB | 4 | 296,583 / 298,033 | 406 / 451 | 1.85 |
| SQLite | 1 | 10,983 / 10,967 | 69 / 70 | 0.15 |
| SQLite\* | 4 | 17,983 / — | 92 / 116 | 0.25 |
| Ugnos† | 1 | 877,000 / 853,317 | 2569 / 4814 | 1.14 |
| Ugnos† | 4 | 680,217 / 658,867 | 2020 / 3578 | 1.93 |
| Talna | 1 | 512,333 / 514,917 | 115 / 171 | 1.35 |
| Talna | 4 | 238,850 / 243,067 | 95 / 117 | 1.59 |

### Concurrent reads and writes

| Engine | Writers | Points/s | Queries/s | RSS, MiB | CPU, cores |
|---|---:|---:|---:|---:|---:|
| Chimaera | 1 | 545,183 / 545,950 | 377.23 / 378.48 | 294 / 418 | 4.97 |
| Chimaera | 4 | **1,025,433 / 1,005,700** | **366.07 / 368.88** | 385 / 479 | 5.74 |
| tsink | 1 | 40,817 / 44,883 | 452.87 / 441.33 | 768 / 1038 | 4.43 |
| tsink | 4 | 70,133 / 72,100 | 394.15 / 419.47 | 782 / 1058 | 4.46 |
| chDB | 1 | 118,883 / 117,150 | 65.47 / 65.33 | 417 / 456 | 4.23 |
| chDB | 4 | 210,617 / 215,500 | 56.85 / 56.70 | 463 / 500 | 4.59 |
| SQLite | 1 | 53,983 / 54,350 | 19.60 / 20.08 | 161 / 196 | 3.52 |
| SQLite\* | 4 | 56,300 / — | 19.38 / — | 200 / 250 | 3.56 |
| Ugnos† | 1 | 681,133 / 680,017 | 711.70 / 744.37 | 2070 / 3741 | 5.12 |
| Ugnos† | 4 | 539,850 / 537,533 | 640.50 / 678.78 | 1781 / 2917 | 5.59 |

- † Ugnos: an API return did not guarantee that the whole batch had been persisted to disk. Its write throughput reflects weaker durability guarantees.
- \* SQLite: the second run with four writers failed with `database is locked`. Only the successful run is shown.
- Talna: the required multi-series aggregates were unavailable through the selected API. A separate point-lookup-only test achieved 877,256 / 855,139 queries/s; it is not comparable to the query mix above.
- NanoTS and tstorage: write-only and mixed workloads were not measured because preliminary checks failed.
- chDB: batches of 1,000 points and `max_threads=1` per query; the results apply to those settings.
- Monotone: aggregate validation failed after reopening the database. An adapter issue is possible; no valid throughput measurements are available.

## Reliability, integrity, and guarantees

Chimaera acknowledges writes after synchronizing the WAL to disk and publishing the data for reading. Segment flushes and compactions happen later. Before a checkpoint, acknowledged writes are protected by the WAL; after a checkpoint, recovery uses the published files and the remaining log tail.

| Mechanism | Guarantee |
|---|---|
| Atomic batch writes | A batch is applied in full, including points from different series. One invalid point causes the entire batch to be rejected |
| GroupCommit | Multiple batches share one WAL synchronization while retaining their individual atomicity and acknowledgments |
| Crash recovery | On open, the engine recovers from published files and the WAL portion not yet covered by a checkpoint |
| Consistent reads through MVCC | A query sees one snapshot. Concurrent writes, deletions, and compactions do not change that view |
| Safe file replacement | New files are synchronized before publication. Old files are reclaimed after readers using them finish |
| Integrity checks | Checksums detect corruption. A full `verify` scans data, CRCs, and WAL; normal open performs a narrower recovery check |
| Atomic series deletion | After acknowledgment, new queries cannot see the deleted history or its summaries. Disk reclamation runs in the background |
| Consistent backups | `snapshot` captures a database state and copies the files it pins. `restore_snapshot` restores the copy into a separate directory |
| Database directory lock | Prevents two independent engine instances from managing the same files concurrently |

These guarantees have limits:

- A timeout does not cancel a write. A batch may have been persisted even if the application did not receive its acknowledgment. For `OutcomeUnknown`, recover and reconcile the state; blind retries are unsafe.
- Exactly-once delivery is not provided. There is no idempotency token or API to look up a write outcome by request ID.
- Atomicity applies to one batch. Arbitrary transactions spanning multiple read and write calls are not supported.
- Corruption can be detected without being repairable. Missing required files or invalid CRCs cause an error. An external rollback of the WAL to an older valid boundary may go undetected.
- Durability depends on correct `fsync`, filesystem, and storage-device behavior. The target platform is Linux x86-64 with a local filesystem. Process crashes and I/O errors have been tested; actual power cuts have not.

## Installation and build

Chimaera is a Rust library built together with its host application.

> **The application chooses the allocator.** The default `jemalloc` feature provides allocator statistics but does not install a global allocator. The application also controls its build profile through its own `Cargo.toml`.

Building requires Rust 1.96 or newer, Cargo, a C/C++ compiler, and Make. Linux x86-64 is the primary deployment platform; macOS ARM64 is used for development and functional checks.

Add a dependency to your application's `Cargo.toml`, pointing to the source checkout:

```toml
[dependencies]
chimaera = { path = "../chimaera" }
```

Build the application in release mode:

```sh
cargo build --release
```

Cargo builds Chimaera and its dependencies automatically. For reproducibility, keep the application's `Cargo.lock` and pin the engine source revision.

To build the library separately, run this from the Chimaera source directory:

```sh
cargo build --locked --release --lib
```

Two examples are included:

```sh
cargo run --locked --release --example embedding
cargo run --locked --release --example operations
```

`embedding` checks writing, reading, and reopening a database. `operations` demonstrates aggregates, deletion, backups, and other operations. Both examples use temporary directories and remove them when finished.

Build the administration utility separately:

```sh
cargo build --locked --release --features cli --bin chimaera-admin
./target/release/chimaera-admin --help
```
