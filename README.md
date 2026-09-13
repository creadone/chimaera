<p align="center">
  <img src="assets/chimaera-logo.png" alt="Chimaera" width="640">
</p>

[Русский](README.ru.md)

# Chimaera

Chimaera is an embedded time-series storage engine written in Rust. It runs inside your application, accepts atomic batches of points and reads data from consistent snapshots.

- WAL-backed writes with `Sync`, `GroupCommit` and `Async` durability.
- Point lookups, streaming range scans and multi-series grouping.
- Background L0/L1/L2 compaction and exact aggregate summaries.
- Raw and aggregate retention, snapshot/restore, verification and JSONL exchange.

[Engine architecture → DESIGN.md](docs/DESIGN.md)

## Quickstart

Install Rust 1.96 or newer and a C toolchain. From the source directory:

```sh
cargo build --locked --bins
cargo run --quiet --locked --example quickstart
```

The [example](examples/quickstart.rs) creates a temporary database, writes two points, reads them and checks reopening. Its temporary data is removed on completion.

```text
point: 10
group: count=2, sum=22.5
reopen: 12.5
```

To use a local checkout beside your application:

```toml
[dependencies]
chimaera = { path = "../chimaera" }
```

## Usage examples

With the dependency above, each example can be used as your application's `src/main.rs`. Run the first with a new `metrics-db` directory; the other two read that database from the same working directory.

### Write a batch and read a point

A series key contains `(space_id, bucket_id, key)`. Timestamps use Unix microseconds. `Sync` acknowledges the batch after synchronizing the WAL.

```rust
use chimaera::{Durability, OpenOptions, Point, SeriesKey, Storage};

fn main() -> chimaera::Result<()> {
    let storage = Storage::open("metrics-db", OpenOptions::default())?;
    let sensor_a = SeriesKey::new(1, 1, 1);
    let sensor_b = SeriesKey::new(1, 1, 2);
    let start_us = 1_788_220_800_000_000;

    storage.write_batch(
        vec![
            Point::new(sensor_a, start_us, 10.0)?,
            Point::new(sensor_a, start_us + 1_000_000, 20.0)?,
            Point::new(sensor_b, start_us, 30.0)?,
            Point::new(sensor_b, start_us + 1_000_000, 40.0)?,
        ],
        Durability::Sync,
    )?;

    if let Some(point) = storage.point(sensor_a, start_us)? {
        println!("point: {}", point.value);
    }
    storage.close()
}
```

Output: `point: 10`.

### Stream a time range

`scan` returns points in timestamp order. The interval includes its lower bound and excludes its upper bound: `[from_us, to_us)`. Each iteration reads the next point without collecting the entire result.

```rust
use chimaera::{OpenOptions, SeriesKey, Storage};

fn main() -> chimaera::Result<()> {
    let storage = Storage::open(
        "metrics-db",
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let series = SeriesKey::new(1, 1, 1);
    let start_us = 1_788_220_800_000_000;

    for point in storage.scan(series, start_us, start_us + 2_000_000)? {
        let point = point?;
        println!("{}: {}", point.timestamp_us, point.value);
    }
    storage.close()
}
```

This reads the two points with values `10` and `20`.

### Group multiple series by minute

`Combined` combines both series into one result per interval. This request counts points and sums their values in one-minute intervals.

```rust
use chimaera::{GroupRequest, GroupScope, OpenOptions, SeriesKey, Storage};

fn main() -> chimaera::Result<()> {
    let storage = Storage::open(
        "metrics-db",
        OpenOptions {
            create_if_missing: false,
            ..OpenOptions::default()
        },
    )?;
    let start_us = 1_788_220_800_000_000;
    let minute_us = 60_000_000;
    let request = GroupRequest::fixed(
        vec![SeriesKey::new(1, 1, 1), SeriesKey::new(1, 1, 2)],
        start_us,
        start_us + minute_us,
        minute_us,
        start_us,
    )?
    .with_scope(GroupScope::Combined);

    for row in storage.group_stream(request)? {
        let row = row?;
        println!("count: {}, sum: {}", row.count, row.sum);
    }
    storage.close()
}
```

Output: `count: 4, sum: 100`.

## Reads under 200K points/s ingestion

![Two-hour Linux test: 200,995 acknowledged points/s, 31 completed queries/s; read p99 from 11.10 to 133.21 ms](assets/benchmark-200k-30qps.svg)

Linux, 8 AMD EPYC 9554 vCPUs, 15.6 GiB RAM. `GroupCommit`, `StrictlyIncreasing`, batches of 1,000 points; four write clients, four read clients and four pack workers. Scheduled load: 201,000 points/s and 31 queries/s. Wide reads account for 20% of queries, with 300 series per request: a fixed one-day window, a random 14-day window and a hot-weighted 90-day window.

The chart shows full query p99 from scheduled start to complete result over two hours after the first minute of warm-up. Verification checked **1,771,814,706 operations**. [Run data and configuration](assets/benchmark-200k-30qps.json).

## API and development

A series is identified by `(space_id, bucket_id, key)`, timestamps use Unix microseconds and values are finite `f64`. New policies use `StrictlyIncreasing`; select `Mutable` for replacements and deletes. Applications explicitly call `Storage::close` and handle its result.

```sh
cargo doc --no-deps --open
cargo test --locked
```

- [Engine design](docs/DESIGN.md)
- [Contributing](CONTRIBUTING.md)
- [Repository](https://github.com/creadone/chimaera)

Run `chimaera-admin --help` for available commands.