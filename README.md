# iroh-fake-store

[![Crates.io](https://img.shields.io/crates/v/iroh-fake-store.svg)](https://crates.io/crates/iroh-fake-store)
[![Documentation](https://docs.rs/iroh-fake-store/badge.svg)](https://docs.rs/iroh-fake-store)
[![License](https://img.shields.io/crates/l/iroh-fake-store.svg)](LICENSE)

fake `iroh-blobs` store for testing. generates data on-the-fly without storing anything in RAM or disk.

compatible with `iroh-blobs` 0.98 and `iroh` 0.96.

## what it does

for testing with large blobs (like 2TB) when you don't care about actual content. features:

- zero allocation: generates data on-the-fly
- deterministic: same config = same hashes
- configurable: zeros, ones, or pseudo-random data (per-store or per-blob)
- safe limits: 10GB default max to prevent accidents
- full protocol support: implements complete `iroh-blobs` store protocol
- bandwidth throttling: simulate slow peers
- real data round-trip: blobs added via `add_bytes`/`add_byte_stream` serve back their original data

## installation

```toml
[dev-dependencies]
iroh-fake-store = "0.1"
```

## usage

### basic

```rust
use iroh_fake_store::FakeStore;

#[tokio::main]
async fn main() {
    let store = FakeStore::new([
        1024,               // 1KB
        1024 * 1024,        // 1MB
        1024 * 1024 * 1024, // 1GB
    ]);

    let hashes = store.blobs().list().hashes().await.unwrap();
    for hash in hashes {
        let status = store.blobs().status(hash).await.unwrap();
        println!("blob {} status: {:?}", hash, status);
    }
}
```

### with builder

```rust
use iroh_fake_store::{FakeStore, DataStrategy};

let store = FakeStore::builder()
    .strategy(DataStrategy::PseudoRandom { seed: 42 })
    .max_blob_size(Some(100 * 1024 * 1024)) // 100MB max
    .with_blob(1024)
    .with_blob(2048)
    .build();
```

### unique blobs with distinct hashes

when you need many same-sized blobs that are distinguishable by hash:

```rust
let store = FakeStore::builder()
    .with_unique_blobs(100, 1024 * 1024) // 100 x 1MB blobs, each with a unique seed
    .build();
```

### bandwidth throttling

simulate slow peers by throttling read bandwidth:

```rust
let store = FakeStore::builder()
    .with_throttle(1024 * 1024) // 1MB/s
    .with_blob(10 * 1024 * 1024)
    .build();
```

### data strategies

- `DataStrategy::Zeros`: all zeros (default, most efficient)
- `DataStrategy::Ones`: all ones (0xFF)
- `DataStrategy::PseudoRandom { seed }`: deterministic pseudo-random

the strategy can be set globally via the builder, or per-blob using `with_unique_blobs` which assigns each blob a different `PseudoRandom` seed.

```rust
// zeros (default)
let store = FakeStore::new([1024]);

// pseudo-random for more realistic testing
let store = FakeStore::builder()
    .strategy(DataStrategy::PseudoRandom { seed: 12345 })
    .with_blob(1024 * 1024)
    .build();
```

### dynamic blob addition

blobs added at runtime via `add_bytes` or `add_byte_stream` store and serve back their real data:

```rust
let store = FakeStore::new([]);
let tag = store.blobs().add_bytes(b"hello world".to_vec()).tag().await?;
// reading this blob back returns the original "hello world" bytes
```

## testing large blobs

test behavior with huge blobs without allocating memory:

```rust
use iroh_fake_store::FakeStore;

#[tokio::test]
async fn test_large_blob_transfer() {
    // 2TB blob (doesn't actually allocate 2TB!)
    let store = FakeStore::builder()
        .max_blob_size(None) // remove size limit
        .with_blob(2 * 1024 * 1024 * 1024 * 1024)
        .build();

    // test your transfer logic here...
}
```

## safety features

### size limits

default 10GB max to prevent accidents:

```rust
// this will panic!
let store = FakeStore::builder()
    .with_blob(100 * 1024 * 1024 * 1024) // exceeds 10GB limit
    .build();

// remove limit explicitly if you want huge blobs
let store = FakeStore::builder()
    .max_blob_size(None)
    .with_blob(100 * 1024 * 1024 * 1024) // now ok
    .build();
```

## how it works

1. **hash computation**: hashes computed once at store creation via BAO tree
2. **outboard storage**: BAO tree outboard stored in memory (small, ~O(log(size)))
3. **data generation**: blob data generated on-demand during reads using the configured strategy
4. **zero allocation**: no blob data stored for pre-configured blobs, generated as needed
5. **real data storage**: blobs added dynamically via `add_bytes`/`add_byte_stream` keep their original data in memory so it can be served back correctly

### memory usage

- per blob overhead: ~O(log(size)) for BAO tree outboard
- during reads: temporary allocation only for requested range
- example: 1GB blob uses ~50KB of memory for metadata

## api compatibility

implements full `iroh-blobs` store protocol:

- list blobs
- status checks
- observe operations
- export (BAO, ranges, path)
- import (BAO, bytes, streams)
- tag management (create, list, set, rename, delete)

## license

dual-licensed under MIT or Apache-2.0
