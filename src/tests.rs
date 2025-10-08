use super::*;
use bao_tree::io::mixed::EncodedItem;
use iroh_blobs::api::proto::ExportRangesItem;
use n0_future::stream::StreamExt;

type TestResult<T> = anyhow::Result<T>;

const INTERESTING_SIZES: [u64; 8] = [
    0,               // empty blob
    1,               // less than 1 chunk
    1024,            // exactly 1 chunk
    1024 * 16 - 1,   // less than 1 chunk group
    1024 * 16,       // exactly 1 chunk group
    1024 * 16 + 1,   // more than 1 chunk group
    1024 * 256,      // multiple chunk groups
    1024 * 1024 * 8, // 8MB
];

#[tokio::test]
async fn test_list_and_status() -> TestResult<()> {
    let store = FakeStore::new([1024, 2048]);

    let blobs = store.blobs().list().hashes().await?;
    assert_eq!(blobs.len(), 2);

    for hash in blobs {
        let status = store.blobs().status(hash).await?;
        match status {
            BlobStatus::Complete { size } => {
                assert!(size == 1024 || size == 2048);
            }
            _ => panic!("Expected complete status"),
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_observe() -> TestResult<()> {
    let store = FakeStore::new(INTERESTING_SIZES);

    for &size in &INTERESTING_SIZES {
        let hash = compute_hash_for_strategy(size, DataStrategy::Zeros);
        let bitfield = store.blobs().observe(hash).await?;
        assert_eq!(bitfield.size(), size);
        assert!(bitfield.is_complete());
    }
    Ok(())
}

#[tokio::test]
async fn test_export_bao_all_ranges() -> TestResult<()> {
    for &size in &INTERESTING_SIZES {
        let store = FakeStore::new([size]);
        let hash = compute_hash_for_strategy(size, DataStrategy::Zeros);

        let stream = store.blobs().export_bao(hash, ChunkRanges::all()).stream();
        let mut items = vec![];
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            items.push(item);
        }

        if size > 0 {
            assert!(!items.is_empty(), "Expected items for size {}", size);
        }

        for item in items {
            match item {
                EncodedItem::Size(_)
                | EncodedItem::Parent(_)
                | EncodedItem::Leaf(_)
                | EncodedItem::Done => {}
                EncodedItem::Error(e) => panic!("Got error: {:?}", e),
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_export_bao_specific_ranges() -> TestResult<()> {
    let size = 1024 * 256;
    let store = FakeStore::new([size]);
    let hash = compute_hash_for_strategy(size, DataStrategy::Zeros);

    let ranges = ChunkRanges::chunks(0..8);
    let stream = store.blobs().export_bao(hash, ranges).stream();
    let mut count = 0;
    tokio::pin!(stream);
    while let Some(item) = stream.next().await {
        count += 1;
        match item {
            EncodedItem::Size(_)
            | EncodedItem::Parent(_)
            | EncodedItem::Leaf(_)
            | EncodedItem::Done => {}
            EncodedItem::Error(e) => panic!("Got error: {:?}", e),
        }
    }

    assert!(count > 0, "Expected to receive some items");
    Ok(())
}

#[tokio::test]
async fn test_export_ranges() -> TestResult<()> {
    for &size in &INTERESTING_SIZES {
        if size == 0 {
            continue;
        }

        let store = FakeStore::new([size]);
        let hash = compute_hash_for_strategy(size, DataStrategy::Zeros);

        let ranges = 0..size.min(1024);
        let stream = store.blobs().export_ranges(hash, ranges.clone()).stream();
        let mut total_bytes = 0u64;
        tokio::pin!(stream);

        while let Some(item) = stream.next().await {
            match item {
                ExportRangesItem::Size(_) => {}
                ExportRangesItem::Data(leaf) => {
                    assert!(
                        leaf.data.iter().all(|&b| b == 0),
                        "Expected all zeros at offset {}",
                        leaf.offset
                    );
                    total_bytes += leaf.data.len() as u64;
                }
                ExportRangesItem::Error(e) => panic!("Got error: {:?}", e),
            }
        }

        assert!(
            total_bytes > 0,
            "Expected to receive bytes for size {}",
            size
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_export_path() -> TestResult<()> {
    use std::io::Read;

    let tempdir = tempfile::tempdir()?;

    for &size in &INTERESTING_SIZES {
        let store = FakeStore::new([size]);
        let hash = compute_hash_for_strategy(size, DataStrategy::Zeros);

        let out_path = tempdir.path().join(format!("out-{size}"));
        store.blobs().export(hash, &out_path).await?;

        let metadata = std::fs::metadata(&out_path)?;
        assert_eq!(metadata.len(), size);

        let mut file = std::fs::File::open(&out_path)?;
        let mut buf = vec![0u8; size.min(4096) as usize];
        let n = file.read(&mut buf)?;
        assert!(buf[..n].iter().all(|&b| b == 0), "Expected all zeros");
    }
    Ok(())
}

#[tokio::test]
async fn test_hash_computation() -> TestResult<()> {
    for &size in &INTERESTING_SIZES {
        let hash1 = compute_hash_for_strategy(size, DataStrategy::Zeros);
        let hash2 = compute_hash_for_strategy(size, DataStrategy::Zeros);
        assert_eq!(
            hash1, hash2,
            "Hash should be deterministic for size {}",
            size
        );

        let data = vec![0u8; size as usize];
        let expected_hash = Hash::new(&data);
        assert_eq!(
            hash1, expected_hash,
            "Hash should match actual zero data for size {}",
            size
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_data_strategy_zeros() -> TestResult<()> {
    let store = FakeStore::builder()
        .strategy(DataStrategy::Zeros)
        .with_blob(1024)
        .build();

    let hashes = store.blobs().list().hashes().await?;
    let hash = hashes[0];

    let data = store.blobs().export_ranges(hash, 0..1024).stream();
    tokio::pin!(data);

    while let Some(item) = data.next().await {
        match item {
            ExportRangesItem::Data(leaf) => {
                assert!(leaf.data.iter().all(|&b| b == 0), "Expected all zeros");
            }
            ExportRangesItem::Error(e) => panic!("Got error: {:?}", e),
            _ => {}
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_data_strategy_real_data() -> TestResult<()> {
    let store = FakeStore::builder()
        .strategy(DataStrategy::RealData { data: vec![0xFA, 0xFB, 0xFC] })
        .with_blob(1024)
        .build();

    let hashes = store.blobs().list().hashes().await?;
    let hash = hashes[0];

    let data = store.blobs().export_ranges(hash, 0..1024).stream();
    tokio::pin!(data);

    while let Some(item) = data.next().await {
        match item {
            ExportRangesItem::Data(leaf) => {
                let mut iter = leaf.data.iter();
                assert!(
                    iter.next() == Some(&0xFA),
                    "Expected first byte to be 0xFA"
                );
                assert!(
                    iter.next() == Some(&0xFB),
                    "Expected second byte to be 0xFB"
                );
                assert!(
                    iter.next() == Some(&0xFC),
                    "Expected third byte to be 0xFC"
                );
                assert!(
                    iter.all(|&b| b == 0x00),
                    "Expected all rest of the bytes to be 0x00"
                );
            }
            ExportRangesItem::Error(e) => panic!("Got error: {:?}", e),
            _ => {}
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_data_strategy_ones() -> TestResult<()> {
    let store = FakeStore::builder()
        .strategy(DataStrategy::Ones)
        .with_blob(1024)
        .build();

    let hashes = store.blobs().list().hashes().await?;
    let hash = hashes[0];

    let data = store.blobs().export_ranges(hash, 0..1024).stream();
    tokio::pin!(data);

    while let Some(item) = data.next().await {
        match item {
            ExportRangesItem::Data(leaf) => {
                assert!(
                    leaf.data.iter().all(|&b| b == 0xFF),
                    "Expected all ones (0xFF)"
                );
            }
            ExportRangesItem::Error(e) => panic!("Got error: {:?}", e),
            _ => {}
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_data_strategy_pseudo_random() -> TestResult<()> {
    let seed = 42;
    let size = 1024;

    let store = FakeStore::builder()
        .strategy(DataStrategy::PseudoRandom { seed })
        .with_blob(size)
        .build();

    let hashes = store.blobs().list().hashes().await?;
    let hash = hashes[0];

    let mut exported = Vec::new();
    let data = store.blobs().export_ranges(hash, 0..size).stream();
    tokio::pin!(data);

    while let Some(item) = data.next().await {
        match item {
            ExportRangesItem::Data(leaf) => {
                exported.extend_from_slice(&leaf.data);
            }
            ExportRangesItem::Error(e) => panic!("Got error: {:?}", e),
            _ => {}
        }
    }

    assert!(
        !exported.iter().all(|&b| b == 0),
        "Data should not be all zeros"
    );
    assert!(
        !exported.iter().all(|&b| b == 0xFF),
        "Data should not be all ones"
    );

    let mut exported2 = Vec::new();
    let data = store.blobs().export_ranges(hash, 0..size).stream();
    tokio::pin!(data);

    while let Some(item) = data.next().await {
        match item {
            ExportRangesItem::Data(leaf) => {
                exported2.extend_from_slice(&leaf.data);
            }
            ExportRangesItem::Error(e) => panic!("Got error: {:?}", e),
            _ => {}
        }
    }

    assert_eq!(exported, exported2, "Data should be deterministic");

    let bao_data = store
        .blobs()
        .export_bao(hash, ChunkRanges::all())
        .bao_to_vec()
        .await?;

    assert!(!bao_data.is_empty(), "BAO data should not be empty");

    Ok(())
}

#[tokio::test]
async fn test_pseudo_random_different_seeds() -> TestResult<()> {
    let size = 1024;

    let store1 = FakeStore::builder()
        .strategy(DataStrategy::PseudoRandom { seed: 42 })
        .with_blob(size)
        .build();

    let store2 = FakeStore::builder()
        .strategy(DataStrategy::PseudoRandom { seed: 99 })
        .with_blob(size)
        .build();

    let hash1 = store1.blobs().list().hashes().await?[0];
    let hash2 = store2.blobs().list().hashes().await?[0];

    assert_ne!(
        hash1, hash2,
        "Different seeds should produce different hashes"
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_blobs() -> TestResult<()> {
    let sizes = vec![100, 1024, 10000, 100000];
    let store = FakeStore::new(sizes.iter().copied());

    let hashes = store.blobs().list().hashes().await?;
    assert_eq!(hashes.len(), sizes.len());

    for &size in &sizes {
        let expected_hash = compute_hash_for_strategy(size, DataStrategy::Zeros);
        assert!(
            hashes.contains(&expected_hash),
            "Hash for size {} should be in list",
            size
        );

        let status = store.blobs().status(expected_hash).await?;
        match status {
            BlobStatus::Complete { size: s } => {
                assert_eq!(s, size, "Size mismatch for hash");
            }
            _ => panic!("Expected complete status"),
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_nonexistent_blob() -> TestResult<()> {
    let store = FakeStore::new([1024]);
    let fake_hash = Hash::from([0xFFu8; 32]);

    let status = store.blobs().status(fake_hash).await?;
    assert!(matches!(status, BlobStatus::NotFound));
    Ok(())
}

#[tokio::test]
async fn test_large_blob() -> TestResult<()> {
    let size = 1024 * 1024 * 100; // 100MB
    let store = FakeStore::new([size]);
    let hash = compute_hash_for_strategy(size, DataStrategy::Zeros);

    let status = store.blobs().status(hash).await?;
    match status {
        BlobStatus::Complete { size: s } => {
            assert_eq!(s, size);
        }
        _ => panic!("Expected complete status"),
    }

    // verify we don't allocate the full blob
    let ranges = 0..1024u64;
    let stream = store.blobs().export_ranges(hash, ranges).stream();
    let mut total_bytes = 0u64;
    tokio::pin!(stream);

    while let Some(item) = stream.next().await {
        match item {
            ExportRangesItem::Size(_) => {}
            ExportRangesItem::Data(leaf) => {
                total_bytes += leaf.data.len() as u64;
            }
            ExportRangesItem::Error(e) => panic!("Got error: {:?}", e),
        }
    }

    assert!(total_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn test_empty_blob() -> TestResult<()> {
    let store = FakeStore::new([0]);
    let hash = compute_hash_for_strategy(0, DataStrategy::Zeros);

    let status = store.blobs().status(hash).await?;
    match status {
        BlobStatus::Complete { size } => {
            assert_eq!(size, 0);
        }
        _ => panic!("Expected complete status"),
    }

    let bitfield = store.blobs().observe(hash).await?;
    assert_eq!(bitfield.size(), 0);
    assert!(bitfield.is_complete());

    Ok(())
}

#[tokio::test]
async fn test_export_bao_to_vec() -> TestResult<()> {
    for &size in &INTERESTING_SIZES {
        if size == 0 {
            continue;
        }

        let store = FakeStore::new([size]);
        let hash = compute_hash_for_strategy(size, DataStrategy::Zeros);

        let exported = store
            .blobs()
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;

        assert!(!exported.is_empty(), "Expected data for size {}", size);

        let size_bytes = &exported[0..8];
        let encoded_size = u64::from_le_bytes(size_bytes.try_into().unwrap());
        assert_eq!(encoded_size, size, "Size mismatch in encoding");
    }
    Ok(())
}

#[tokio::test]
async fn test_wait_idle() -> TestResult<()> {
    let store = FakeStore::new([1024, 2048]);

    store.wait_idle().await?;
    store.wait_idle().await?;

    Ok(())
}

#[tokio::test]
async fn test_concurrent_operations() -> TestResult<()> {
    let store = FakeStore::new([1024, 2048, 4096]);

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move {
                let hashes = store.blobs().list().hashes().await?;
                for hash in hashes {
                    let _ = store.blobs().status(hash).await?;
                    let _ = store.blobs().observe(hash).await?;
                }
                TestResult::Ok(())
            })
        })
        .collect();

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

/// end-to-end test with actual iroh protocol using pseudo-random data
/// tests: provider serves prng data, downloader fetches & validates,
/// hash validation works (proves our data gen is consistent)
#[tokio::test]
async fn test_e2e_transfer_with_pseudo_random() -> TestResult<()> {
    use iroh::{Endpoint, Watcher, protocol::Router};
    use iroh_blobs::BlobsProtocol;

    tracing_subscriber::fmt::try_init().ok();

    let provider_store = FakeStore::builder()
        .strategy(DataStrategy::PseudoRandom { seed: 42 })
        .with_blob(1024 * 1024)
        .build();

    let hash = provider_store.blobs().list().hashes().await?[0];
    println!("provider created blob with hash: {}", hash);

    let provider_endpoint = Endpoint::builder().bind().await?;
    let provider_blobs = BlobsProtocol::new(&provider_store, provider_endpoint.clone(), None);
    let provider_router = Router::builder(provider_endpoint.clone())
        .accept(iroh_blobs::ALPN, provider_blobs)
        .spawn();

    let provider_addr = provider_router.endpoint().node_addr().initialized().await;
    println!("provider listening at: {:?}", provider_addr);

    use iroh_blobs::store::mem::MemStore;
    let receiver_store = MemStore::new();

    let receiver_endpoint = Endpoint::builder().bind().await?;
    receiver_endpoint.add_node_addr(provider_addr.clone())?;

    let downloader = receiver_store.downloader(&receiver_endpoint);
    println!("starting download...");

    let mut progress = downloader
        .download(hash, Some(provider_addr.node_id))
        .stream()
        .await?;

    while let Some(event) = progress.next().await {
        println!("download progress: {:?}", event);
    }

    println!("download complete!");

    let receiver_hashes = receiver_store.blobs().list().hashes().await?;
    assert!(
        receiver_hashes.contains(&hash),
        "downloaded blob should be in receiver store"
    );

    let status = receiver_store.blobs().status(hash).await?;
    match status {
        BlobStatus::Complete { size } => {
            assert_eq!(size, 1024 * 1024, "downloaded blob size should match");
        }
        _ => panic!("expected complete status for downloaded blob"),
    }

    let downloaded_bytes = receiver_store.blobs().get_bytes(hash).await?;
    use range_collections::RangeSet2;
    let mut provider_ranges = provider_store
        .blobs()
        .export_ranges(hash, RangeSet2::<u64>::all())
        .stream();
    let mut provider_data = Vec::new();
    while let Some(item) = provider_ranges.next().await {
        if let ExportRangesItem::Data(leaf) = item {
            provider_data.extend_from_slice(&leaf.data);
        }
    }

    assert_eq!(
        provider_data, downloaded_bytes,
        "downloaded data should match provider data"
    );

    assert!(
        !provider_data.iter().all(|&b| b == 0),
        "data should be pseudo-random, not all zeros"
    );

    provider_router.shutdown().await?;
    receiver_endpoint.close().await;

    println!("e2e transfer with prng data works (◕‿◕)");

    Ok(())
}

/// e2e test with both stores being FakeStore using downloader
#[tokio::test]
async fn test_e2e_both_fake_stores_downloader() -> TestResult<()> {
    use iroh::{Endpoint, Watcher, protocol::Router};
    use iroh_blobs::BlobsProtocol;

    tracing_subscriber::fmt::try_init().ok();

    // provider with 1MB pseudo-random blob
    let provider_store = FakeStore::builder()
        .strategy(DataStrategy::PseudoRandom { seed: 42 })
        .with_blob(1024 * 1024)
        .build();

    let hash = provider_store.blobs().list().hashes().await?[0];
    println!("provider has blob: {}", hash);

    // setup provider
    let provider_endpoint = Endpoint::builder().bind().await?;
    let provider_blobs = BlobsProtocol::new(&provider_store, provider_endpoint.clone(), None);
    let provider_router = Router::builder(provider_endpoint.clone())
        .accept(iroh_blobs::ALPN, provider_blobs)
        .spawn();

    let provider_addr = provider_router.endpoint().node_addr().initialized().await;

    // receiver is also FakeStore with same strategy, starts empty
    let receiver_store = FakeStore::builder()
        .strategy(DataStrategy::PseudoRandom { seed: 42 })
        .build();

    let receiver_endpoint = Endpoint::builder().bind().await?;
    receiver_endpoint.add_node_addr(provider_addr.clone())?;

    // download
    let downloader = receiver_store.downloader(&receiver_endpoint);
    let mut progress = downloader
        .download(hash, Some(provider_addr.node_id))
        .stream()
        .await?;

    while let Some(event) = progress.next().await {
        println!("download event: {:?}", event);
    }

    // verify blob is now in receiver store
    let receiver_hashes = receiver_store.blobs().list().hashes().await?;
    assert!(
        receiver_hashes.contains(&hash),
        "receiver should have the blob after download"
    );

    // verify status
    let status = receiver_store.blobs().status(hash).await?;
    match status {
        BlobStatus::Complete { size } => {
            assert_eq!(size, 1024 * 1024);
        }
        _ => panic!("expected complete status"),
    }

    // cleanup
    provider_router.shutdown().await?;
    receiver_endpoint.close().await;

    println!("both stores are fake, downloader works, no memory allocated ᕕ( ᐛ )ᕗ");

    Ok(())
}

/// test that you can dynamically add blobs to a FakeStore via add_bytes
#[tokio::test]
async fn test_dynamic_blob_addition() -> TestResult<()> {
    // start with empty store
    let store = FakeStore::builder()
        .strategy(DataStrategy::PseudoRandom { seed: 42 })
        .build();

    // verify it's empty
    let hashes = store.blobs().list().hashes().await?;
    assert_eq!(hashes.len(), 0);

    // add a blob dynamically via add_bytes
    // the actual data doesn't matter - we just compute hash and store metadata
    let data = vec![0xABu8; 1024 * 1024]; // 1MB of 0xAB
    let result = store.blobs().add_bytes(data).await?;
    let hash = result.hash;

    println!("added blob with hash: {}", hash);

    // verify it's now in the store
    let hashes = store.blobs().list().hashes().await?;
    assert_eq!(hashes.len(), 1);
    assert!(hashes.contains(&hash));

    // verify we can query its status
    let status = store.blobs().status(hash).await?;
    match status {
        BlobStatus::Complete { size } => {
            assert_eq!(size, 1024 * 1024);
        }
        _ => panic!("expected complete status"),
    }

    // verify we can read it back
    // it will be served as pseudo-random data based on the strategy, not the original 0xAB
    use range_collections::RangeSet2;
    let mut ranges = store
        .blobs()
        .export_ranges(hash, RangeSet2::<u64>::all())
        .stream();

    let mut data = Vec::new();
    while let Some(item) = ranges.next().await {
        if let ExportRangesItem::Data(leaf) = item {
            data.extend_from_slice(&leaf.data);
        }
    }

    // data should be pseudo-random, not all 0xAB
    assert_eq!(data.len(), 1024 * 1024);
    assert!(
        data.iter().any(|&b| b != 0xAB),
        "data should be pseudo-random, not original data"
    );

    println!("blob was added and can be read back as fake data ᕕ( ᐛ )ᕗ");

    Ok(())
}

#[tokio::test]
async fn test_tag_create_and_list() -> TestResult<()> {
    let store = FakeStore::new([1024]);
    let hash = compute_hash_for_strategy(1024, DataStrategy::Zeros);

    // create a tag
    let tag = store
        .tags()
        .create(HashAndFormat {
            hash,
            format: BlobFormat::Raw,
        })
        .await?;

    // list tags should include our new tag
    let mut tags_stream = store.tags().list().await?;
    let mut tags = Vec::new();
    while let Some(tag_info) = tags_stream.next().await {
        tags.push(tag_info?);
    }

    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].name, tag);
    assert_eq!(tags[0].hash, hash);
    assert_eq!(tags[0].format, BlobFormat::Raw);

    Ok(())
}

#[tokio::test]
async fn test_tag_set() -> TestResult<()> {
    let store = FakeStore::new([1024, 2048]);
    let hashes = store.blobs().list().hashes().await?;
    let hash1 = hashes[0];
    let hash2 = hashes[1];

    // create a tag
    let tag = store
        .tags()
        .create(HashAndFormat {
            hash: hash1,
            format: BlobFormat::Raw,
        })
        .await?;

    // set it to point to a different hash
    store
        .tags()
        .set(
            tag.clone(),
            HashAndFormat {
                hash: hash2,
                format: BlobFormat::Raw,
            },
        )
        .await?;

    // list tags to verify it points to hash2 now
    let mut tags_stream = store.tags().list().await?;
    let mut tags = Vec::new();
    while let Some(tag_info) = tags_stream.next().await {
        tags.push(tag_info?);
    }

    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].name, tag);
    assert_eq!(tags[0].hash, hash2);

    Ok(())
}

#[tokio::test]
async fn test_tag_rename() -> TestResult<()> {
    let store = FakeStore::new([1024]);
    let hash = compute_hash_for_strategy(1024, DataStrategy::Zeros);

    // create a tag
    let tag = store
        .tags()
        .create(HashAndFormat {
            hash,
            format: BlobFormat::Raw,
        })
        .await?;

    // rename it
    let new_name = api::Tag::from("renamed-tag");
    store.tags().rename(tag.clone(), new_name.clone()).await?;

    // list tags to verify rename
    let mut tags_stream = store.tags().list().await?;
    let mut tags = Vec::new();
    while let Some(tag_info) = tags_stream.next().await {
        tags.push(tag_info?);
    }

    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].name, new_name);
    assert_eq!(tags[0].hash, hash);

    // old tag name should not exist
    assert!(!tags.iter().any(|t| t.name == tag));

    Ok(())
}

#[tokio::test]
async fn test_tag_delete() -> TestResult<()> {
    let store = FakeStore::new([1024]);
    let hash = compute_hash_for_strategy(1024, DataStrategy::Zeros);

    // create two tags
    let tag1 = store
        .tags()
        .create(HashAndFormat {
            hash,
            format: BlobFormat::Raw,
        })
        .await?;

    let tag2 = store
        .tags()
        .create(HashAndFormat {
            hash,
            format: BlobFormat::Raw,
        })
        .await?;

    // verify we have 2 tags
    let mut tags_stream = store.tags().list().await?;
    let mut tags = Vec::new();
    while let Some(tag_info) = tags_stream.next().await {
        tags.push(tag_info?);
    }
    assert_eq!(tags.len(), 2);

    // delete one tag by name
    store.tags().delete(tag1.clone()).await?;

    // verify we have 1 tag left
    let mut tags_stream = store.tags().list().await?;
    let mut tags = Vec::new();
    while let Some(tag_info) = tags_stream.next().await {
        tags.push(tag_info?);
    }
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].name, tag2);

    // delete the other
    store.tags().delete(tag2.clone()).await?;

    // verify empty
    let mut tags_stream = store.tags().list().await?;
    let mut tags = Vec::new();
    while let Some(tag_info) = tags_stream.next().await {
        tags.push(tag_info?);
    }
    assert_eq!(tags.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_tag_multiple_tags_same_hash() -> TestResult<()> {
    let store = FakeStore::new([1024]);
    let hash = compute_hash_for_strategy(1024, DataStrategy::Zeros);

    // create multiple tags for same hash
    let tag1 = store
        .tags()
        .create(HashAndFormat {
            hash,
            format: BlobFormat::Raw,
        })
        .await?;

    let tag2 = store
        .tags()
        .create(HashAndFormat {
            hash,
            format: BlobFormat::Raw,
        })
        .await?;

    // verify both exist
    let mut tags_stream = store.tags().list().await?;
    let mut tags = Vec::new();
    while let Some(tag_info) = tags_stream.next().await {
        tags.push(tag_info?);
    }
    assert_eq!(tags.len(), 2);

    let tag_names: Vec<_> = tags.iter().map(|t| &t.name).collect();
    assert!(tag_names.contains(&&tag1));
    assert!(tag_names.contains(&&tag2));

    Ok(())
}
