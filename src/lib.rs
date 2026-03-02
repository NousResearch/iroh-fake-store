//! fake iroh-blobs store for testing.
//!
//! generates data on-the-fly, stores nothing. serves deterministic data (zeros or
//! pseudo-random) at specific lengths for testing big blob transfers.
//!
//! # Examples
//!
//! ```
//! use iroh_fake_store::FakeStore;
//!
//! # tokio_test::block_on(async {
//! let store = FakeStore::builder()
//!     .with_blob(1024)           // 1KB blob
//!     .with_blob(1024 * 1024)    // 1MB blob
//!     .build();
//!
//! let hashes = store.blobs().list().hashes().await.unwrap();
//! assert_eq!(hashes.len(), 2);
//! # });
//! ```
//!
//! generates on-the-fly without storing, deterministic hashes, configurable patterns,
//! has safety limits to prevent accidentally making huge blobs

use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bao_tree::{
    BaoTree, ChunkRanges,
    io::{
        Leaf,
        mixed::{EncodedItem, ReadBytesAt, traverse_ranges_validated},
        outboard::PreOrderMemOutboard,
        sync::ReadAt,
    },
};
use bytes::Bytes;
use iroh_blobs::{
    BlobFormat, Hash, HashAndFormat,
    api::{
        self, Store, TempTag,
        blobs::{Bitfield, ExportProgressItem},
        proto::{
            self, BlobStatus, Command, ExportBaoMsg, ExportBaoRequest, ExportPathMsg,
            ExportPathRequest, ExportRangesItem, ExportRangesMsg, ExportRangesRequest,
            ImportBaoMsg, ImportByteStreamMsg, ImportBytesMsg, ObserveMsg, ObserveRequest,
            WaitIdleMsg,
        },
    },
    protocol::ChunkRangesExt,
    store::IROH_BLOCK_SIZE,
};
use irpc::channel::mpsc;
use range_collections::range_set::RangeSetRange;
use ref_cast::RefCast;
use tokio::task::{JoinError, JoinSet};

/// data generation strategy for fake blobs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataStrategy {
    /// all zeros (default, most efficient)
    Zeros,
    /// all ones
    Ones,
    /// deterministic pseudo-random based on seed
    PseudoRandom { seed: u64 },
    /// real data
    RealData(Bytes),
}

impl Default for DataStrategy {
    fn default() -> Self {
        Self::Zeros
    }
}

#[derive(Debug, Clone)]
pub struct FakeStoreConfig {
    pub strategy: DataStrategy,
    /// max blob size (prevents accidents)
    pub max_blob_size: Option<u64>,
    /// throttle read bandwidth to this many bytes per second (None = unlimited)
    pub throttle_bytes_per_sec: Option<u64>,
}

impl Default for FakeStoreConfig {
    fn default() -> Self {
        Self {
            strategy: DataStrategy::Zeros,
            // 10GB limit to prevent accidents
            max_blob_size: Some(10 * 1024 * 1024 * 1024),
            throttle_bytes_per_sec: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeStoreBuilder {
    config: FakeStoreConfig,
    /// Each blob can have its own strategy override
    blobs: Vec<(u64, Option<DataStrategy>)>,
}

impl FakeStoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn strategy(mut self, strategy: DataStrategy) -> Self {
        self.config.strategy = strategy;
        self
    }

    /// None for unlimited
    pub fn max_blob_size(mut self, max: Option<u64>) -> Self {
        self.config.max_blob_size = max;
        self
    }

    pub fn with_blob(mut self, size: u64) -> Self {
        self.blobs.push((size, None));
        self
    }

    pub fn with_blobs(mut self, sizes: impl IntoIterator<Item = u64>) -> Self {
        self.blobs.extend(sizes.into_iter().map(|s| (s, None)));
        self
    }

    /// Create `count` blobs of the same size, each with a unique PseudoRandom seed
    /// so they have distinct hashes. Use this when you need many same-sized blobs.
    pub fn with_unique_blobs(mut self, count: usize, size: u64) -> Self {
        for i in 0..count {
            self.blobs
                .push((size, Some(DataStrategy::PseudoRandom { seed: i as u64 })));
        }
        self
    }

    /// Throttle read bandwidth to the given bytes per second.
    /// This simulates slow peers by adding delays during BAO export.
    pub fn with_throttle(mut self, bytes_per_sec: u64) -> Self {
        self.config.throttle_bytes_per_sec = Some(bytes_per_sec);
        self
    }

    /// # Panics
    /// panics if any blob size exceeds the configured max
    pub fn build(self) -> FakeStore {
        if let Some(max) = self.config.max_blob_size {
            for &(size, _) in &self.blobs {
                assert!(size <= max, "Blob size {} exceeds maximum {}", size, max);
            }
        }

        let blob_specs: Vec<(u64, DataStrategy)> = self
            .blobs
            .into_iter()
            .map(|(size, strategy)| {
                (
                    size,
                    strategy.unwrap_or_else(|| self.config.strategy.clone()),
                )
            })
            .collect();

        FakeStore::new_with_config(blob_specs, self.config)
    }
}

/// fake iroh-blobs store for testing
///
/// generates data on-the-fly, stores nothing. for testing big blobs when you don't
/// care about content.
///
/// # Examples
///
/// ```
/// use iroh_fake_store::{FakeStore, DataStrategy};
///
/// # tokio_test::block_on(async {
/// let store = FakeStore::new([1024, 2048]);
///
/// let store = FakeStore::builder()
///     .strategy(DataStrategy::Zeros)
///     .max_blob_size(Some(1024 * 1024 * 100)) // 100MB max
///     .with_blob(1024)
///     .build();
/// # });
/// ```
#[derive(Debug, Clone)]
pub struct FakeStore {
    store: Store,
}

impl std::ops::Deref for FakeStore {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

/// wrapper around mpsc::Sender<EncodedItem> that impls bao_tree::io::mixed::Sender
#[derive(RefCast)]
#[repr(transparent)]
struct BaoTreeSender(mpsc::Sender<EncodedItem>);

impl bao_tree::io::mixed::Sender for BaoTreeSender {
    type Error = irpc::channel::SendError;
    async fn send(&mut self, item: EncodedItem) -> std::result::Result<(), Self::Error> {
        self.0.send(item).await
    }
}

#[derive(Debug, Clone)]
struct BlobMetadata {
    size: u64,
    outboard: Bytes,
    strategy: DataStrategy,
}

struct Actor {
    commands: tokio::sync::mpsc::Receiver<proto::Command>,
    tasks: JoinSet<()>,
    idle_waiters: Vec<irpc::channel::oneshot::Sender<()>>,
    blobs: Arc<Mutex<HashMap<Hash, BlobMetadata>>>,
    strategy: DataStrategy,
    tags: BTreeMap<api::Tag, HashAndFormat>,
    throttle_bytes_per_sec: Option<u64>,
}

impl Actor {
    fn new(
        commands: tokio::sync::mpsc::Receiver<proto::Command>,
        blobs: HashMap<Hash, BlobMetadata>,
        strategy: DataStrategy,
        throttle_bytes_per_sec: Option<u64>,
    ) -> Self {
        Self {
            blobs: Arc::new(Mutex::new(blobs)),
            commands,
            tasks: JoinSet::new(),
            idle_waiters: Vec::new(),
            strategy,
            tags: BTreeMap::new(),
            throttle_bytes_per_sec,
        }
    }

    async fn handle_command(&mut self, cmd: Command) -> Option<irpc::channel::oneshot::Sender<()>> {
        match cmd {
            Command::ImportBao(msg) => {
                self.handle_import_bao(msg).await;
            }
            Command::WaitIdle(WaitIdleMsg { tx, .. }) => {
                if self.tasks.is_empty() {
                    tx.send(()).await.ok();
                } else {
                    self.idle_waiters.push(tx);
                }
            }
            Command::ImportBytes(msg) => {
                self.handle_import_bytes(msg).await;
            }
            Command::ImportByteStream(msg) => {
                self.handle_import_byte_stream(msg).await;
            }
            Command::ImportPath(msg) => {
                msg.tx
                    .send(io::Error::other("import path not supported").into())
                    .await
                    .ok();
            }
            Command::Observe(ObserveMsg {
                inner: ObserveRequest { hash },
                tx,
                ..
            }) => {
                let size = self.blobs.lock().unwrap().get(&hash).map(|x| x.size);
                self.tasks.spawn(async move {
                    if let Some(size) = size {
                        tx.send(Bitfield::complete(size)).await.ok();
                    } else {
                        tx.send(Bitfield::empty()).await.ok();
                    };
                });
            }
            Command::ExportBao(ExportBaoMsg {
                inner: ExportBaoRequest { hash, ranges, .. },
                tx,
                ..
            }) => {
                let metadata = self.blobs.lock().unwrap().get(&hash).cloned();
                let throttle = self.throttle_bytes_per_sec;
                self.tasks
                    .spawn(export_bao(hash, metadata, ranges, tx, throttle));
            }
            Command::ExportPath(ExportPathMsg {
                inner: ExportPathRequest { hash, target, .. },
                tx,
                ..
            }) => {
                let metadata = self.blobs.lock().unwrap().get(&hash).cloned();
                self.tasks.spawn(export_path(metadata, target, tx));
            }
            Command::Batch(_cmd) => {}
            Command::ClearProtected(cmd) => {
                cmd.tx.send(Ok(())).await.ok();
            }
            Command::CreateTag(cmd) => {
                use api::proto::CreateTagRequest;
                use std::time::SystemTime;

                let CreateTagRequest { value } = cmd.inner;
                let tag = api::Tag::auto(SystemTime::now(), |t| self.tags.contains_key(t));
                self.tags.insert(tag.clone(), value);
                cmd.tx.send(Ok(tag)).await.ok();
            }
            Command::CreateTempTag(cmd) => {
                cmd.tx.send(TempTag::new(cmd.inner.value, None)).await.ok();
            }
            Command::RenameTag(cmd) => {
                use api::proto::RenameTagRequest;
                let RenameTagRequest { from, to } = cmd.inner;

                if let Some(value) = self.tags.remove(&from) {
                    self.tags.insert(to, value);
                    cmd.tx.send(Ok(())).await.ok();
                } else {
                    cmd.tx
                        .send(Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "tag not found",
                        )
                        .into()))
                        .await
                        .ok();
                }
            }
            Command::DeleteTags(cmd) => {
                use api::proto::DeleteTagsRequest;
                use std::ops::Bound;
                let DeleteTagsRequest { from, to } = cmd.inner;

                // delete all tags in the range [from, to)
                let start: Bound<&api::Tag> = from
                    .as_ref()
                    .map_or(Bound::Unbounded, |t| Bound::Included(t));
                let end: Bound<&api::Tag> =
                    to.as_ref().map_or(Bound::Unbounded, |t| Bound::Excluded(t));

                let to_delete: Vec<_> = self
                    .tags
                    .range::<api::Tag, _>((start, end))
                    .map(|(k, _)| k.clone())
                    .collect();

                for tag in &to_delete {
                    self.tags.remove(tag);
                }
                cmd.tx.send(Ok(to_delete.len() as u64)).await.ok();
            }
            Command::DeleteBlobs(cmd) => {
                cmd.tx
                    .send(Err(io::Error::other("delete blobs not supported").into()))
                    .await
                    .ok();
            }
            Command::ListBlobs(cmd) => {
                let hashes: Vec<Hash> = self.blobs.lock().unwrap().keys().cloned().collect();
                self.tasks.spawn(async move {
                    for hash in hashes {
                        cmd.tx.send(Ok(hash)).await.ok();
                    }
                });
            }
            Command::BlobStatus(cmd) => {
                let hash = cmd.inner.hash;
                let metadata = self.blobs.lock().unwrap().get(&hash).cloned();
                let status = if let Some(metadata) = metadata {
                    BlobStatus::Complete {
                        size: metadata.size,
                    }
                } else {
                    BlobStatus::NotFound
                };
                cmd.tx.send(status).await.ok();
            }
            Command::ListTags(cmd) => {
                use api::proto::TagInfo;
                let tags: Vec<_> = self
                    .tags
                    .iter()
                    .map(|(name, value)| {
                        Ok(TagInfo {
                            name: name.clone(),
                            hash: value.hash,
                            format: value.format,
                        })
                    })
                    .collect();
                cmd.tx.send(tags).await.ok();
            }
            Command::SetTag(cmd) => {
                use api::proto::SetTagRequest;
                let SetTagRequest { name, value } = cmd.inner;

                self.tags.insert(name, value);
                cmd.tx.send(Ok(())).await.ok();
            }
            Command::ListTempTags(cmd) => {
                cmd.tx.send(Vec::new()).await.ok();
            }
            Command::SyncDb(cmd) => {
                cmd.tx.send(Ok(())).await.ok();
            }
            Command::Shutdown(cmd) => {
                return Some(cmd.tx);
            }
            Command::ExportRanges(cmd) => {
                let metadata = self.blobs.lock().unwrap().get(&cmd.inner.hash).cloned();
                self.tasks.spawn(export_ranges(cmd, metadata));
            }
        }
        None
    }

    fn log_unit_task(&self, res: Result<(), JoinError>) {
        if let Err(e) = res {
            eprintln!("task failed: {e}");
        }
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.commands.recv() => {
                    if let Some(shutdown) = self.handle_command(cmd).await {
                        shutdown.send(()).await.ok();
                        break;
                    }
                },
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    self.log_unit_task(res);
                    if self.tasks.is_empty() {
                        for tx in self.idle_waiters.drain(..) {
                            tx.send(()).await.ok();
                        }
                    }
                },
                else => break,
            }
        }
    }

    async fn handle_import_bytes(&mut self, msg: ImportBytesMsg) {
        use bao_tree::io::outboard::PreOrderMemOutboard;
        use iroh_blobs::api::blobs::AddProgressItem;

        let ImportBytesMsg {
            inner: proto::ImportBytesRequest { data, .. },
            tx,
            ..
        } = msg;

        let size = data.len() as u64;

        if tx.send(AddProgressItem::Size(size)).await.is_err() {
            return;
        }
        if tx.send(AddProgressItem::CopyDone).await.is_err() {
            return;
        }

        // compute hash from the actual data
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let hash = Hash::from(outboard.root);

        // store metadata with real data so it can be served back correctly
        self.blobs.lock().unwrap().insert(
            hash,
            BlobMetadata {
                size,
                outboard: outboard.data.into(),
                strategy: DataStrategy::RealData(Bytes::copy_from_slice(&data)),
            },
        );

        // send completion with the hash
        let temp_tag = api::TempTag::new(
            HashAndFormat {
                hash,
                format: BlobFormat::Raw,
            },
            None,
        );
        if tx.send(AddProgressItem::Done(temp_tag)).await.is_err() {}
    }

    async fn handle_import_byte_stream(&mut self, msg: ImportByteStreamMsg) {
        use bao_tree::io::outboard::PreOrderMemOutboard;
        use iroh_blobs::api::blobs::AddProgressItem;
        use proto::ImportByteStreamUpdate;

        let ImportByteStreamMsg { tx, mut rx, .. } = msg;

        // collect all bytes to compute hash
        let mut data = Vec::new();
        loop {
            match rx.recv().await {
                Ok(Some(ImportByteStreamUpdate::Bytes(chunk))) => {
                    data.extend_from_slice(&chunk);
                    if tx
                        .send(AddProgressItem::CopyProgress(data.len() as u64))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(Some(ImportByteStreamUpdate::Done)) => {
                    break;
                }
                Ok(None) | Err(_) => {
                    tx.send(AddProgressItem::Error(io::Error::other(
                        "stream ended unexpectedly",
                    )))
                    .await
                    .ok();
                    return;
                }
            }
        }

        let size = data.len() as u64;

        if tx.send(AddProgressItem::CopyDone).await.is_err() {
            return;
        }

        // compute hash
        let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let hash = Hash::from(outboard.root);

        // store metadata with real data for dynamically-imported blobs
        self.blobs.lock().unwrap().insert(
            hash,
            BlobMetadata {
                size,
                outboard: outboard.data.into(),
                strategy: DataStrategy::RealData(Bytes::from(data)),
            },
        );

        // send completion
        let temp_tag = api::TempTag::new(
            HashAndFormat {
                hash,
                format: BlobFormat::Raw,
            },
            None,
        );
        if tx.send(AddProgressItem::Done(temp_tag)).await.is_err() {}
    }

    async fn handle_import_bao(&mut self, msg: ImportBaoMsg) {
        use proto::ImportBaoRequest;

        let ImportBaoMsg {
            inner: ImportBaoRequest { hash, size },
            tx,
            mut rx,
            ..
        } = msg;

        let size_u64 = size.get();
        let strategy = self.strategy.clone();
        let blobs = self.blobs.clone();

        self.tasks.spawn(async move {
            // consume all incoming BAO chunks without storing them
            while let Ok(Some(_item)) = rx.recv().await {
                // just drain them, don't store
            }

            // store minimal metadata (empty outboard - we won't re-export from this store)
            // This avoids generating the full blob data just to compute the outboard,
            // which would defeat the purpose of FakeStore for large blob imports.
            blobs.lock().unwrap().insert(
                hash,
                BlobMetadata {
                    size: size_u64,
                    outboard: Bytes::new(),
                    strategy,
                },
            );

            tx.send(Ok(())).await.ok();
        });
    }
}

/// generates data on-the-fly based on strategy and offset
#[derive(Clone)]
struct DataReader {
    strategy: DataStrategy,
}

impl DataReader {
    fn new(strategy: DataStrategy) -> Self {
        Self { strategy }
    }

    fn from_metadata(metadata: &BlobMetadata) -> Self {
        Self::new(metadata.strategy.clone())
    }

    fn byte_at(&self, offset: u64) -> u8 {
        match &self.strategy {
            DataStrategy::Zeros => 0,
            DataStrategy::Ones => 0xFF,
            DataStrategy::PseudoRandom { seed } => {
                let seed = *seed;
                let mut x = seed.wrapping_add(offset);
                x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
                x = x ^ (x >> 31);
                (x >> 24) as u8
            }
            DataStrategy::RealData(data) => {
                let idx = offset as usize;
                assert!(
                    idx < data.len(),
                    "RealData offset {idx} out of bounds (len {})",
                    data.len()
                );
                data[idx]
            }
        }
    }
}

impl ReadAt for DataReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // For RealData, use bulk copy for efficiency
        if let DataStrategy::RealData(ref data) = self.strategy {
            let start = offset as usize;
            let end = start + buf.len();
            assert!(
                end <= data.len(),
                "RealData read_at out of bounds: {start}..{end} but len is {}",
                data.len()
            );
            buf.copy_from_slice(&data[start..end]);
            return Ok(buf.len());
        }
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = self.byte_at(offset + i as u64);
        }
        Ok(buf.len())
    }
}

impl ReadBytesAt for DataReader {
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        if let DataStrategy::RealData(ref data) = self.strategy {
            let start = offset as usize;
            let end = start + size;
            assert!(
                end <= data.len(),
                "RealData read_bytes_at out of bounds: {start}..{end} but len is {}",
                data.len()
            );
            return Ok(data.slice(start..end));
        }
        let mut data = vec![0u8; size];
        self.read_at(offset, &mut data)?;
        Ok(Bytes::from(data))
    }
}

fn generate_data_for_strategy(size: u64, strategy: &DataStrategy) -> Vec<u8> {
    let reader = DataReader::new(strategy.clone());
    let mut data = vec![0u8; size as usize];
    reader.read_at(0, &mut data).expect("read should succeed");
    data
}

fn compute_hash_for_strategy(size: u64, strategy: &DataStrategy) -> Hash {
    let data = generate_data_for_strategy(size, strategy);
    let outboard = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    Hash::from(outboard.root)
}

/// A sender wrapper that throttles bandwidth by sleeping between sends.
struct ThrottledBaoTreeSender {
    inner: mpsc::Sender<EncodedItem>,
    bytes_per_sec: u64,
}

impl bao_tree::io::mixed::Sender for ThrottledBaoTreeSender {
    type Error = irpc::channel::SendError;
    async fn send(&mut self, item: EncodedItem) -> std::result::Result<(), Self::Error> {
        // Calculate bytes in this item for throttling
        let bytes = match &item {
            EncodedItem::Leaf(Leaf { data, .. }) => data.len() as u64,
            _ => 0,
        };
        // Sleep proportionally to simulate limited bandwidth
        if bytes > 0 && self.bytes_per_sec > 0 {
            let sleep_secs = bytes as f64 / self.bytes_per_sec as f64;
            tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_secs)).await;
        }
        self.inner.send(item).await
    }
}

async fn export_bao(
    hash: Hash,
    metadata: Option<BlobMetadata>,
    ranges: ChunkRanges,
    mut sender: mpsc::Sender<EncodedItem>,
    throttle_bytes_per_sec: Option<u64>,
) {
    let metadata = match metadata {
        Some(metadata) => metadata,
        None => {
            sender
                .send(EncodedItem::Error(bao_tree::io::EncodeError::Io(
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "export task ended unexpectedly",
                    ),
                )))
                .await
                .ok();
            return;
        }
    };

    let size = metadata.size;
    let data = DataReader::from_metadata(&metadata);

    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let outboard = PreOrderMemOutboard {
        root: hash.into(),
        tree,
        data: metadata.outboard,
    };

    if let Some(bps) = throttle_bytes_per_sec {
        let mut throttled = ThrottledBaoTreeSender {
            inner: sender,
            bytes_per_sec: bps,
        };
        traverse_ranges_validated(data, outboard, &ranges, &mut throttled)
            .await
            .ok();
    } else {
        let sender = BaoTreeSender::ref_cast_mut(&mut sender);
        traverse_ranges_validated(data, outboard, &ranges, sender)
            .await
            .ok();
    }
}

async fn export_ranges(mut cmd: ExportRangesMsg, metadata: Option<BlobMetadata>) {
    let Some(metadata) = metadata else {
        cmd.tx
            .send(ExportRangesItem::Error(api::Error::io(
                io::ErrorKind::NotFound,
                "hash not found",
            )))
            .await
            .ok();
        return;
    };
    if let Err(cause) = export_ranges_impl(cmd.inner, &mut cmd.tx, metadata).await {
        cmd.tx
            .send(ExportRangesItem::Error(cause.into()))
            .await
            .ok();
    }
}

async fn export_ranges_impl(
    cmd: ExportRangesRequest,
    tx: &mut mpsc::Sender<ExportRangesItem>,
    metadata: BlobMetadata,
) -> io::Result<()> {
    let ExportRangesRequest { ranges, .. } = cmd;
    let size = metadata.size;
    let bitfield = Bitfield::complete(size);

    for range in ranges.iter() {
        let range = match range {
            RangeSetRange::Range(range) => size.min(*range.start)..size.min(*range.end),
            RangeSetRange::RangeFrom(range) => size.min(*range.start)..size,
        };
        let requested = ChunkRanges::bytes(range.start..range.end);
        if !bitfield.ranges.is_superset(&requested) {
            return Err(io::Error::other(format!(
                "missing range: {requested:?}, present: {bitfield:?}",
            )));
        }
        let reader = DataReader::from_metadata(&metadata);
        let bs = 1024;
        let mut offset = range.start;
        loop {
            let end: u64 = (offset + bs).min(range.end);
            let chunk_size = (end - offset) as usize;
            let data = reader.read_bytes_at(offset, chunk_size)?;
            tx.send(Leaf { offset, data }.into()).await?;
            offset = end;
            if offset >= range.end {
                break;
            }
        }
    }
    Ok(())
}

impl FakeStore {
    /// uses zeros strategy. for more control use [`FakeStore::builder()`]
    pub fn new(sizes: impl IntoIterator<Item = u64>) -> Self {
        let config = FakeStoreConfig::default();
        let blob_specs: Vec<(u64, DataStrategy)> = sizes
            .into_iter()
            .map(|s| (s, config.strategy.clone()))
            .collect();
        Self::new_with_config(blob_specs, config)
    }

    pub fn builder() -> FakeStoreBuilder {
        FakeStoreBuilder::new()
    }

    fn new_with_config(
        blob_specs: impl IntoIterator<Item = (u64, DataStrategy)>,
        config: FakeStoreConfig,
    ) -> Self {
        let mut blobs = HashMap::new();
        for (size, strategy) in blob_specs {
            let hash = compute_hash_for_strategy(size, &strategy);
            let data = generate_data_for_strategy(size, &strategy);
            let outboard_data = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
            blobs.insert(
                hash,
                BlobMetadata {
                    size,
                    outboard: outboard_data.data.into(),
                    strategy,
                },
            );
        }

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let actor = Actor::new(
            receiver,
            blobs,
            config.strategy,
            config.throttle_bytes_per_sec,
        );
        tokio::spawn(actor.run());

        // Store is #[repr(transparent)] so we can use RefCast
        let local = irpc::LocalSender::from(sender);
        let client = local.into();

        Self {
            store: Store::ref_cast(&client).clone(),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }
}

async fn export_path(
    metadata: Option<BlobMetadata>,
    target: PathBuf,
    mut tx: mpsc::Sender<ExportProgressItem>,
) {
    let Some(metadata) = metadata else {
        tx.send(api::Error::io(io::ErrorKind::NotFound, "hash not found").into())
            .await
            .ok();
        return;
    };
    match export_path_impl(metadata, target, &mut tx).await {
        Ok(()) => tx.send(ExportProgressItem::Done).await.ok(),
        Err(cause) => tx.send(api::Error::from(cause).into()).await.ok(),
    };
}

async fn export_path_impl(
    metadata: BlobMetadata,
    target: PathBuf,
    tx: &mut mpsc::Sender<ExportProgressItem>,
) -> io::Result<()> {
    let size = metadata.size;
    let mut file = std::fs::File::create(&target)?;
    tx.send(ExportProgressItem::Size(size)).await?;

    let reader = DataReader::from_metadata(&metadata);
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = reader.read_bytes_at(offset, len)?;
        file.write_all(&buf)?;
        tx.try_send(ExportProgressItem::CopyProgress(offset))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
