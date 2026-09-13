# Contributing to Chimaera

Start with [the engine design](docs/DESIGN.md). Use Rust 1.96 or newer and a C toolchain.

Keep changes focused. Describe the problem, the resulting behavior and how you
checked it. Add a regression test when changing behavior, then run:

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

Use `cargo doc --no-deps --locked` when editing API documentation. Run examples
after changing them and keep the Russian and English documents in sync.

Code from another project must retain its license notices and source attribution.
Do not commit generated databases, build artifacts, test logs, secrets or customer data.
