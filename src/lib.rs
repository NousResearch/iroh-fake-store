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
    blake3,
    io::{
        mixed::{traverse_ranges_validated, EncodedItem, ReadBytesAt},
        outboard::PreOrderMemOutboard,
        sync::{outboard, OutboardMut, ReadAt},
        Leaf,
    },
    BaoTree, ChunkRanges,
};
use bytes::Bytes;
use iroh_blobs::{
    api::{
        self,
        blobs::{Bitfield, ExportProgressItem},
        proto::{
            self, BlobStatus, Command, ExportBaoMsg, ExportBaoRequest, ExportPathMsg,
            ExportPathRequest, ExportRangesItem, ExportRangesMsg, ExportRangesRequest,
            ImportBaoMsg, ImportByteStreamMsg, ImportBytesMsg, ObserveMsg, ObserveRequest,
            WaitIdleMsg,
        },
        Store, TempTag,
    },
    protocol::ChunkRangesExt,
    store::IROH_BLOCK_SIZE,
    BlobFormat, Hash, HashAndFormat,
};
use irpc::channel::mpsc;
use rand::{Rng, SeedableRng};
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
    /// padded real data
    RealData { data: Vec<u8> },
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
}

impl Default for FakeStoreConfig {
    fn default() -> Self {
        Self {
            strategy: DataStrategy::Zeros,
            // 10GB limit to prevent accidents
            max_blob_size: Some(10 * 1024 * 1024 * 1024),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FakeStoreBuilder {
    config: FakeStoreConfig,
    sizes: Vec<u64>,
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
        self.sizes.push(size);
        self
    }

    pub fn with_blobs(mut self, sizes: impl IntoIterator<Item = u64>) -> Self {
        self.sizes.extend(sizes);
        self
    }

    /// # Panics
    /// panics if any blob size exceeds the configured max
    pub fn build(self) -> FakeStore {
        if let Some(max) = self.config.max_blob_size {
            for &size in &self.sizes {
                assert!(size <= max, "Blob size {} exceeds maximum {}", size, max);
            }
        }

        FakeStore::new_with_config(self.sizes, self.config)
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

pub type Nonce = u64;
pub const NONCE_SIZE: u64 = size_of::<Nonce>() as u64;

#[derive(Debug, Clone)]
struct BlobMetadata {
    size: u64,
    outboard: Bytes,
    strategy: DataStrategy,
    nonce: Nonce,
}

struct Actor {
    commands: tokio::sync::mpsc::Receiver<proto::Command>,
    tasks: JoinSet<()>,
    idle_waiters: Vec<irpc::channel::oneshot::Sender<()>>,
    blobs: Arc<Mutex<HashMap<Hash, BlobMetadata>>>,
    strategy: DataStrategy,
    tags: BTreeMap<api::Tag, HashAndFormat>,
    rng: rand::rngs::StdRng,
}

impl Actor {
    fn new(
        commands: tokio::sync::mpsc::Receiver<proto::Command>,
        blobs: HashMap<Hash, BlobMetadata>,
        strategy: DataStrategy,
    ) -> Self {
        Self {
            blobs: Arc::new(Mutex::new(blobs)),
            commands,
            tasks: JoinSet::new(),
            idle_waiters: Vec::new(),
            strategy,
            tags: BTreeMap::new(),
            rng: rand::rngs::StdRng::from_entropy(),
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
                self.tasks.spawn(export_bao(hash, metadata, ranges, tx));
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
                let start: Bound<&api::Tag> =
                    from.as_ref().map_or(Bound::Unbounded, Bound::Included);
                let end: Bound<&api::Tag> = to.as_ref().map_or(Bound::Unbounded, Bound::Excluded);

                let to_delete: Vec<_> = self
                    .tags
                    .range::<api::Tag, _>((start, end))
                    .map(|(k, _)| k.clone())
                    .collect();

                for tag in to_delete {
                    self.tags.remove(&tag);
                }
                cmd.tx.send(Ok(())).await.ok();
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
                        size: metadata.size + NONCE_SIZE,
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

        // generate random nonce to make each blob unique
        let nonce = self.rng.gen::<u64>();

        // compute hash from strategy-generated data + nonce
        let hash = compute_hash_streaming_with_nonce(size, self.strategy.clone(), nonce);

        // store metadata (we don't store the actual data, just remember the size)
        self.blobs.lock().unwrap().insert(
            hash,
            BlobMetadata {
                size,
                outboard: Bytes::new(),
                strategy: self.strategy.clone(),
                nonce,
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

        // generate random nonce to make each blob unique
        let nonce = self.rng.gen::<u64>();

        // compute hash using streaming approach with nonce
        let hash = compute_hash_streaming_with_nonce(size, self.strategy.clone(), nonce);

        // store metadata
        self.blobs.lock().unwrap().insert(
            hash,
            BlobMetadata {
                size: size + 8,      // internal size with nonce
                external_size: size, // original size without nonce
                outboard: Bytes::new(),
                strategy: self.strategy.clone(),
                nonce,
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
            inner: ImportBaoRequest { hash: _, size },
            tx,
            mut rx,
            ..
        } = msg;

        let size_u64 = size.get();
        let strategy = self.strategy.clone();
        let blobs = self.blobs.clone();
        let nonce = self.rng.gen::<u64>();

        self.tasks.spawn(async move {
            // consume all incoming chunks
            while let Ok(Some(_item)) = rx.recv().await {
                // just drain them, don't store
            }

            // once all chunks consumed, compute outboard using streaming with nonce
            let computed_hash =
                compute_hash_streaming_with_nonce(size_u64, strategy.clone(), nonce);
            let outboard_data = Bytes::new(); // empty since we don't actually store the outboard

            // store metadata using computed hash from strategy + nonce
            blobs.lock().unwrap().insert(
                computed_hash,
                BlobMetadata {
                    size: size_u64 + 8,      // internal size with nonce
                    external_size: size_u64, // original size without nonce
                    outboard: outboard_data,
                    strategy,
                    nonce,
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
    position: u64,
    nonce: Option<u64>, // if Some, prepend 8 bytes of nonce data
}

impl DataReader {
    fn new(strategy: DataStrategy) -> Self {
        Self {
            strategy,
            position: 0,
            nonce: None,
        }
    }

    fn new_with_nonce(strategy: DataStrategy, nonce: u64) -> Self {
        Self {
            strategy,
            position: 0,
            nonce: Some(nonce),
        }
    }

    fn byte_at(&self, offset: u64) -> u8 {
        // handle nonce prefix first 8 bytes
        if let Some(nonce) = self.nonce {
            if offset < 8 {
                return (nonce >> (8 * (7 - offset))) as u8;
            }
            // adjust offset to account for nonce prefix
            let strategy_offset = offset - 8;
            return self.strategy_byte_at(strategy_offset);
        }

        self.strategy_byte_at(offset)
    }

    fn strategy_byte_at(&self, offset: u64) -> u8 {
        match self.strategy.clone() {
            DataStrategy::Zeros => 0,
            DataStrategy::Ones => 0xFF,
            DataStrategy::PseudoRandom { seed } => {
                // offset-based hashing for independent byte generation
                // simpler than LCG jump-ahead, still deterministic
                // simple mixing function (SplitMix64-inspired)
                let mut x = seed.wrapping_add(offset);
                x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
                x = x ^ (x >> 31);
                (x >> 24) as u8
            }
            DataStrategy::RealData { data } => {
                if offset < data.len() as u64 {
                    data[offset as usize]
                } else {
                    0
                }
            }
        }
    }
}

impl ReadAt for DataReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = self.byte_at(offset + i as u64);
        }
        Ok(buf.len())
    }
}

impl io::Read for DataReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = self.byte_at(self.position + i as u64);
        }
        self.position += buf.len() as u64;
        Ok(buf.len())
    }
}

impl ReadBytesAt for DataReader {
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        let mut data = vec![0u8; size];
        self.read_at(offset, &mut data)?;
        Ok(Bytes::from(data))
    }
}

/// generate outboard data on-demand for export operations by creating a streaming PreOrderMemOutboard
fn generate_outboard_data(size: u64, strategy: DataStrategy, nonce: u64) -> Bytes {
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let data = DataReader::new_with_nonce(strategy, nonce);

    // create empty outboard with correct size
    let outboard_size = tree
        .outboard_size()
        .try_into()
        .expect("outboard size fits in usize");
    let outboard_data = vec![0u8; outboard_size];

    // use a mutable outboard that writes to our buffer
    struct VecOutboard {
        tree: BaoTree,
        data: Vec<u8>,
    }

    impl OutboardMut for VecOutboard {
        fn save(
            &mut self,
            node: bao_tree::TreeNode,
            hash_pair: &(blake3::Hash, blake3::Hash),
        ) -> io::Result<()> {
            if let Some(offset) = self.tree.pre_order_offset(node) {
                let offset = (offset * 64) as usize;
                let mut content = [0u8; 64];
                content[0..32].copy_from_slice(hash_pair.0.as_bytes());
                content[32..64].copy_from_slice(hash_pair.1.as_bytes());
                self.data[offset..offset + 64].copy_from_slice(&content);
            }
            Ok(())
        }

        fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut vec_outboard = VecOutboard {
        tree,
        data: outboard_data,
    };

    // compute outboard using streaming approach
    let _root =
        outboard(data, tree, &mut vec_outboard).expect("outboard computation should succeed");

    Bytes::from(vec_outboard.data)
}

/// dummy outboard that just ignores saves
struct NoOpOutboard;

impl OutboardMut for NoOpOutboard {
    fn save(
        &mut self,
        _node: bao_tree::TreeNode,
        _hash_pair: &(blake3::Hash, blake3::Hash),
    ) -> io::Result<()> {
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn compute_hash_for_strategy(size: u64, strategy: DataStrategy) -> Hash {
    compute_hash_streaming(size, strategy)
}

/// compute hash using streaming approach to avoid memory allocation
fn compute_hash_streaming(size: u64, strategy: DataStrategy) -> Hash {
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
    let data = DataReader::new(strategy);
    let mut dummy_outboard = NoOpOutboard;

    // use the public outboard function from bao_tree
    match outboard(data, tree, &mut dummy_outboard) {
        Ok(hash) => Hash::from(hash),
        Err(_) => panic!("hash computation should not fail"),
    }
}

/// compute hash using streaming approach with nonce prefix
fn compute_hash_streaming_with_nonce(size: u64, strategy: DataStrategy, nonce: u64) -> Hash {
    let total_size = size + NONCE_SIZE; // add bytes for nonce
    let tree = BaoTree::new(total_size, IROH_BLOCK_SIZE);
    let data = DataReader::new_with_nonce(strategy, nonce);
    let mut dummy_outboard = NoOpOutboard;

    match outboard(data, tree, &mut dummy_outboard) {
        Ok(hash) => Hash::from(hash),
        Err(_) => panic!("hash computation should not fail"),
    }
}

/// compute hash of data without nonce prefix (for exports)
fn compute_hash_streaming_stripped(size: u64, strategy: DataStrategy, nonce: u64) -> Hash {
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);

    struct NonceStrippingReader {
        inner: DataReader,
        position: u64,
    }

    impl ReadAt for NonceStrippingReader {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.read_at(offset + 8, buf)
        }
    }

    impl io::Read for NonceStrippingReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            // read from inner at current position + 8 (to skip nonce)
            for (i, byte_ref) in buf.iter_mut().enumerate() {
                *byte_ref = self.inner.byte_at(self.position + i as u64 + 8);
            }
            self.position += buf.len() as u64;
            Ok(buf.len())
        }
    }

    let data = NonceStrippingReader {
        inner: DataReader::new_with_nonce(strategy, nonce),
        position: 0,
    };
    let mut dummy_outboard = NoOpOutboard;

    match outboard(data, tree, &mut dummy_outboard) {
        Ok(hash) => Hash::from(hash),
        Err(_) => panic!("hash computation should not fail"),
    }
}

/// generate outboard data for stripped data (without nonce prefix)
fn generate_outboard_data_stripped(size: u64, strategy: DataStrategy, nonce: u64) -> Bytes {
    let tree = BaoTree::new(size, IROH_BLOCK_SIZE);

    // create nonce-stripping reader
    struct NonceStrippingReader {
        inner: DataReader,
        position: u64,
    }

    impl ReadAt for NonceStrippingReader {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.read_at(offset + 8, buf)
        }
    }

    impl io::Read for NonceStrippingReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            // read from inner at current position + 8 (to skip nonce)
            for (i, byte_ref) in buf.iter_mut().enumerate() {
                *byte_ref = self.inner.byte_at(self.position + i as u64 + 8);
            }
            self.position += buf.len() as u64;
            Ok(buf.len())
        }
    }

    let mut data = NonceStrippingReader {
        inner: DataReader::new_with_nonce(strategy, nonce),
        position: 0,
    };

    // create empty outboard with correct size
    let outboard_size = tree
        .outboard_size()
        .try_into()
        .expect("outboard size fits in usize");
    let outboard_data = vec![0u8; outboard_size];

    // use a mutable outboard that writes to our buffer
    struct VecOutboard {
        tree: BaoTree,
        data: Vec<u8>,
    }

    impl OutboardMut for VecOutboard {
        fn save(
            &mut self,
            node: bao_tree::TreeNode,
            hash_pair: &(blake3::Hash, blake3::Hash),
        ) -> io::Result<()> {
            if let Some(offset) = self.tree.pre_order_offset(node) {
                let offset = (offset * 64) as usize;
                let mut content = [0u8; 64];
                content[0..32].copy_from_slice(hash_pair.0.as_bytes());
                content[32..64].copy_from_slice(hash_pair.1.as_bytes());
                self.data[offset..offset + 64].copy_from_slice(&content);
            }
            Ok(())
        }

        fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut vec_outboard = VecOutboard {
        tree,
        data: outboard_data,
    };

    // compute outboard using streaming approach
    let _root =
        outboard(data, tree, &mut vec_outboard).expect("outboard computation should succeed");

    Bytes::from(vec_outboard.data)
}

async fn export_bao(
    hash: Hash,
    metadata: Option<BlobMetadata>,
    ranges: ChunkRanges,
    mut sender: mpsc::Sender<EncodedItem>,
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

    let external_size = metadata.external_size; // size user should see

    // create a nonce-stripping data reader
    struct NonceStrippingReader {
        inner: DataReader,
    }

    impl ReadAt for NonceStrippingReader {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            // read from inner with +8 offset to skip nonce
            self.inner.read_at(offset + 8, buf)
        }
    }

    impl ReadBytesAt for NonceStrippingReader {
        fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
            self.inner.read_bytes_at(offset + 8, size)
        }
    }

    let data = NonceStrippingReader {
        inner: DataReader::new_with_nonce(metadata.strategy.clone(), metadata.nonce),
    };

    let tree = BaoTree::new(external_size, IROH_BLOCK_SIZE);

    // generate outboard data for external size (stripped data)
    let outboard_data = if metadata.outboard.is_empty() {
        generate_outboard_data_stripped(external_size, metadata.strategy.clone(), metadata.nonce)
    } else {
        // stored outboard is for internal size, regenerate for external size
        generate_outboard_data_stripped(external_size, metadata.strategy.clone(), metadata.nonce)
    };

    // compute hash of external data (without nonce)
    let external_hash =
        compute_hash_streaming_stripped(external_size, metadata.strategy.clone(), metadata.nonce);

    let outboard = PreOrderMemOutboard {
        root: external_hash.into(),
        tree,
        data: outboard_data,
    };

    let sender = BaoTreeSender::ref_cast_mut(&mut sender);
    traverse_ranges_validated(data, outboard, &ranges, sender)
        .await
        .ok();
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
    let external_size = metadata.external_size; // size without nonce (user-visible)
    let bitfield = Bitfield::complete(external_size);

    for range in ranges.iter() {
        let range = match range {
            RangeSetRange::Range(range) => {
                external_size.min(*range.start)..external_size.min(*range.end)
            }
            RangeSetRange::RangeFrom(range) => external_size.min(*range.start)..external_size,
        };
        let requested = ChunkRanges::bytes(range.start..range.end);
        if !bitfield.ranges.is_superset(&requested) {
            return Err(io::Error::other(format!(
                "missing range: {requested:?}, present: {bitfield:?}",
            )));
        }
        let reader = DataReader::new_with_nonce(metadata.strategy.clone(), metadata.nonce);
        let bs = 1024;
        let mut offset = range.start;
        loop {
            let end: u64 = (offset + bs).min(range.end);
            let chunk_size = (end - offset) as usize;
            // create a buffer for the data without nonce
            let mut buf = vec![0u8; chunk_size];
            // read from reader at (offset + 8) to skip nonce, but manually extract just the data part
            for (i, byte_ref) in buf.iter_mut().enumerate() {
                *byte_ref = reader.byte_at((offset + i as u64) + 8);
            }
            let data = Bytes::from(buf);
            tx.send(Leaf { offset, data }.into()).await?;
            offset = end;
            if offset >= range.end {
                break;
            }
        }
    }
    Ok(())
}

impl Default for FakeStore {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl FakeStore {
    pub fn builder() -> FakeStoreBuilder {
        FakeStoreBuilder::new()
    }

    fn new(config: FakeStoreConfig) -> Self {
        let mut blobs = HashMap::new();

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let actor = Actor::new(receiver, blobs, config.strategy);
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

    let buf = [0u8; 1024 * 64];
    for offset in (0..size).step_by(1024 * 64) {
        let len = std::cmp::min(size - offset, 1024 * 64) as usize;
        let buf = &buf[..len];
        file.write_all(buf)?;
        tx.try_send(ExportProgressItem::CopyProgress(offset))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
