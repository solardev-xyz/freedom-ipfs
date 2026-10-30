use cid::Cid;
use freedom_ipfs_core::{
    btree_map_bytes, Block, BlockProvider, FixedPool, PoolShare, Reservation, CODEC_DAG_PB,
    CODEC_RAW,
};
use prost::Message;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

/// The most entries of each kind (decoded nodes, resolved paths, file
/// sizes) a resolver's metadata cache keeps by default.
pub const DEFAULT_UNIXFS_METADATA_CACHE_CAPACITY: usize = 256;
/// A resolver's metadata cache's byte capacity by default, for all its
/// entries together (design 0007, "Budgets"): its fixed reservation. As the
/// cache counts conservatively, it holds the default 256 resolved paths and
/// 256 file sizes in about 145 KiB and 75 KiB of it, and in the remaining
/// 800 KiB or so, 256 directory or file nodes of up to about 20 links each
/// (some 3 KiB with their share of the maps), or fewer larger ones.
pub const DEFAULT_UNIXFS_METADATA_CACHE_MAX_BYTES: usize = 1024 * 1024;

const HAMT_MURMUR3_X64_64: u64 = 0x22;
/// The HAMT fanouts read, as boxo reads them: the powers of two from 8 to
/// 1024.
const HAMT_FANOUTS: std::ops::RangeInclusive<u64> = 8..=1024;
const HAMT_MAX_SHARDS_VISITED: usize = 1024;
const UNIXFS_METADATA_CACHE_MAX_BLOCK_BYTES: usize = 64 * 1024;
const UNIXFS_PATH_CACHE_MAX_PATH_BYTES: usize = 1024;
const UNIXFS_RANGE_BATCH_MAX_BLOCKS: usize = 4;
/// The most links one dag-pb node may have, and the most block sizes its
/// UnixFS data may list. They are counted on the wire before anything is
/// decoded: prost decodes each link into a 64-byte struct (and each block
/// size into 8 bytes) whatever it holds, so a 2 MiB block of empty links
/// decoded to 1,048,576 links, 64 MiB. A block is at most 2 MiB, and a link
/// naming a real CID takes about 40 bytes of it, so no real node comes near
/// this; a HAMT shard has at most 256.
pub const MAX_DAG_PB_LINKS: usize = 65_536;
/// The deepest a file's DAG may go below its root: reading it, or summing
/// its size, walks it one call per level, and a chain of nested file nodes
/// any deeper is refused before it can exhaust a thread's stack. Real
/// files stay far shallower: a balanced DAG of 174 links per node needs 7
/// levels for 2^64 bytes, and a trickle DAG under 20.
pub const MAX_FILE_DAG_DEPTH: usize = 128;

#[derive(Debug, Error)]
pub enum UnixfsError {
    #[error("block not found: {0}")]
    NotFound(Cid),
    #[error("path segment not found: {0}")]
    PathNotFound(String),
    #[error("unsupported codec {0}")]
    UnsupportedCodec(u64),
    #[error("unsupported unixfs node type {0}")]
    UnsupportedNodeType(i32),
    #[error("path resolves to a directory")]
    IsDirectory,
    #[error("path requires a directory but found file")]
    NotDirectory,
    #[error("invalid dag-pb: {0}")]
    InvalidDagPb(String),
    #[error("block provider: {0}")]
    Provider(String),
    /// The provider couldn't reserve memory for a block: a host maps it to
    /// busy.
    #[error("memory budget: {0}")]
    MemoryBudget(freedom_ipfs_core::BudgetError),
}

/// A provider's error as UnixFS reports it: a memory refusal stays one.
fn provider_error(err: freedom_ipfs_core::CoreError) -> UnixfsError {
    match err {
        freedom_ipfs_core::CoreError::MemoryBudget(refusal) => UnixfsError::MemoryBudget(refusal),
        other => UnixfsError::Provider(other.to_string()),
    }
}

pub type Result<T> = std::result::Result<T, UnixfsError>;

#[derive(Clone, PartialEq, Message)]
struct PbNode {
    #[prost(bytes = "vec", optional, tag = "1")]
    data: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "2")]
    links: Vec<PbLink>,
}

#[derive(Clone, PartialEq, Message)]
struct PbLink {
    #[prost(bytes = "vec", optional, tag = "1")]
    hash: Option<Vec<u8>>,
    #[prost(string, optional, tag = "2")]
    name: Option<String>,
    #[prost(uint64, optional, tag = "3")]
    tsize: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct UnixfsData {
    #[prost(enumeration = "DataType", optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(bytes = "vec", optional, tag = "2")]
    data: Option<Vec<u8>>,
    #[prost(uint64, optional, tag = "3")]
    filesize: Option<u64>,
    #[prost(uint64, repeated, tag = "4")]
    blocksizes: Vec<u64>,
    #[prost(uint64, optional, tag = "5")]
    hash_type: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    fanout: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum DataType {
    Raw = 0,
    Directory = 1,
    File = 2,
    Metadata = 3,
    Symlink = 4,
    HamtShard = 5,
}

#[derive(Debug, Clone)]
pub struct ResolvedNode {
    pub cid: Cid,
    pub kind: NodeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Raw,
    File,
    Directory,
    HamtShard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub name: String,
    pub cid: Cid,
    pub size: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LinkTarget {
    cid: Cid,
    size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct UnixfsResolver {
    metadata_cache: Arc<UnixfsMetadataCache>,
}

impl UnixfsResolver {
    /// A resolver whose metadata cache has the default limits:
    /// [`DEFAULT_UNIXFS_METADATA_CACHE_CAPACITY`] entries of each kind in
    /// [`DEFAULT_UNIXFS_METADATA_CACHE_MAX_BYTES`].
    pub fn new() -> Self {
        Self::with_metadata_cache_capacity(DEFAULT_UNIXFS_METADATA_CACHE_CAPACITY)
    }

    pub fn without_metadata_cache() -> Self {
        Self::with_metadata_cache_limits(0, 0)
    }

    /// At most `capacity` entries of each kind, in the default byte
    /// capacity.
    pub fn with_metadata_cache_capacity(capacity: usize) -> Self {
        Self::with_metadata_cache_limits(capacity, DEFAULT_UNIXFS_METADATA_CACHE_MAX_BYTES)
    }

    /// At most `capacity` entries of each kind, all of them together in at
    /// most `max_bytes` of heap, counted conservatively: the cache's fixed
    /// capacity ([`UnixfsResolver::metadata_cache_max_bytes`]). Either at
    /// zero, or a `max_bytes` too small for the cache's maps, caches
    /// nothing.
    pub fn with_metadata_cache_limits(capacity: usize, max_bytes: usize) -> Self {
        Self {
            metadata_cache: Arc::new(UnixfsMetadataCache::new(capacity, max_bytes)),
        }
    }

    /// The metadata cache's fixed capacity in bytes, which a host reserves
    /// for it ([`UnixfsResolver::metadata_cache_fixed_bytes`]).
    pub fn metadata_cache_max_bytes(&self) -> usize {
        self.metadata_cache.max_bytes()
    }

    /// The fixed capacity of a metadata cache built with these limits
    /// ([`UnixfsResolver::with_metadata_cache_limits`]): `max_bytes`, but at
    /// least the cache's own structures, which even one that caches nothing
    /// keeps (a few hundred bytes).
    pub fn metadata_cache_fixed_bytes(capacity: usize, max_bytes: usize) -> usize {
        metadata_cache_fixed_bytes(capacity, max_bytes)
    }

    /// Ties `lease`, the fixed reservation the metadata cache is reserved
    /// by, to its memory ([`FixedPool::keep_lease`]): kept for as long as
    /// the cache lives, and given back with it.
    pub fn keep_metadata_cache_lease(&self, lease: Arc<Reservation>) -> bool {
        self.metadata_cache.pool.keep_lease(lease)
    }

    /// Whether `other` shares this resolver's metadata cache, as its clones
    /// do.
    pub fn shares_metadata_cache_with(&self, other: &UnixfsResolver) -> bool {
        Arc::ptr_eq(&self.metadata_cache, &other.metadata_cache)
    }

    pub fn metadata_cache_stats(&self) -> UnixfsMetadataCacheStats {
        self.metadata_cache.stats()
    }

    pub fn clear_metadata_cache(&self) {
        self.metadata_cache.clear();
    }

    pub fn resolve_path(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<ResolvedNode> {
        UnixfsContext::cached(provider, &self.metadata_cache).resolve_path(root, path)
    }

    pub fn resolve_path_with_raw_link_tsize_hint(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<(ResolvedNode, Option<u64>)> {
        UnixfsContext::cached(provider, &self.metadata_cache)
            .resolve_path_with_raw_link_tsize_hint(root, path)
    }

    pub fn read_file(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<Vec<u8>> {
        UnixfsContext::cached(provider, &self.metadata_cache).read_file(root, path)
    }

    pub fn list_directory(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
    ) -> Result<Vec<DirectoryEntry>> {
        UnixfsContext::cached(provider, &self.metadata_cache).list_directory(root, path)
    }

    pub fn file_size(&self, provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<u64> {
        UnixfsContext::cached(provider, &self.metadata_cache).file_size(root, path)
    }

    pub fn file_size_cid(&self, provider: &dyn BlockProvider, cid: &Cid) -> Result<u64> {
        UnixfsContext::cached(provider, &self.metadata_cache).file_size_cid(cid)
    }

    pub fn read_file_range(
        &self,
        provider: &dyn BlockProvider,
        root: &Cid,
        path: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>> {
        UnixfsContext::cached(provider, &self.metadata_cache)
            .read_file_range(root, path, start, end)
    }

    pub fn read_file_cid_range(
        &self,
        provider: &dyn BlockProvider,
        cid: &Cid,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>> {
        UnixfsContext::cached(provider, &self.metadata_cache).read_file_cid_range(cid, start, end)
    }
}

impl Default for UnixfsResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnixfsMetadataCacheStats {
    /// The most entries of each kind.
    pub capacity: usize,
    /// The byte capacity, for all entries together (zero when it caches
    /// nothing).
    pub max_bytes: usize,
    /// The bytes it holds, conservatively counted, of `max_bytes`.
    pub bytes: usize,
    pub len: usize,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub oversized_skips: u64,
    pub path_len: usize,
    pub path_hits: u64,
    pub path_misses: u64,
    pub path_inserts: u64,
    pub path_evictions: u64,
    pub path_oversized_skips: u64,
    pub file_size_len: usize,
    pub file_size_hits: u64,
    pub file_size_misses: u64,
    pub file_size_inserts: u64,
    pub file_size_evictions: u64,
}

/// The metadata cache: decoded dag-pb nodes, resolved paths and file sizes,
/// each kind at most `capacity` entries, all of them together within a
/// fixed byte capacity (design 0007, "Budgets").
///
/// The byte capacity is a [`FixedPool`], which a host reserves as fixed.
/// What the cache holds counts against it by heap footprint, conservatively:
/// the cache itself, its maps' nodes ([`btree_map_bytes`], which bounds
/// them by entry count alone), and every entry's own heap, by capacity (a
/// decoded node's data and its links, their hashes and names; a path key's
/// path). Room is taken before an entry is copied in, oldest entries of any
/// kind making way for it, and given back when one goes.
#[derive(Debug)]
struct UnixfsMetadataCache {
    pool: FixedPool,
    inner: Mutex<UnixfsMetadataCacheInner>,
}

/// What the cache's own structures take besides its maps' nodes: the cache
/// in its `Arc`, and its pool's counter in its own. All a cache that caches
/// nothing takes.
const UNIXFS_METADATA_CACHE_OWN_BYTES: usize = std::mem::size_of::<UnixfsMetadataCache>()
    + 2 * std::mem::size_of::<usize>()
    + FixedPool::HEAP_BYTES;

/// The fixed capacity of a metadata cache with these limits: `max_bytes`,
/// but at least the cache's own structures, which a cache that caches
/// nothing (`capacity` zero) takes alone.
fn metadata_cache_fixed_bytes(capacity: usize, max_bytes: usize) -> usize {
    if capacity == 0 {
        UNIXFS_METADATA_CACHE_OWN_BYTES
    } else {
        max_bytes.max(UNIXFS_METADATA_CACHE_OWN_BYTES)
    }
}

impl UnixfsMetadataCache {
    fn new(capacity: usize, max_bytes: usize) -> Self {
        let pool = FixedPool::new(metadata_cache_fixed_bytes(capacity, max_bytes));
        // Its own structures count from the start.
        let room = pool
            .share(UNIXFS_METADATA_CACHE_OWN_BYTES)
            .expect("a cache's capacity holds its own structures");
        let mut inner = UnixfsMetadataCacheInner {
            capacity,
            max_bytes: pool.capacity(),
            enabled: capacity > 0 && max_bytes > 0,
            clock: 0,
            room,
            heap: 0,
            nodes: CachedMap::default(),
            paths: CachedMap::default(),
            file_sizes: CachedMap::default(),
        };
        // Then room for its maps: a pool too small for them caches nothing.
        if inner.enabled && !inner.true_room() {
            inner.enabled = false;
        }
        Self {
            pool,
            inner: Mutex::new(inner),
        }
    }

    /// The fixed capacity a host reserves for it.
    fn max_bytes(&self) -> usize {
        self.pool.capacity()
    }

    fn get(&self, cid: &Cid) -> Option<DecodedDagPb> {
        let mut inner = self.lock_inner();
        let tick = inner.tick();
        inner.nodes.get(cid, tick).cloned()
    }

    fn get_path(&self, root: &Cid, path: &str) -> Option<ResolvedNode> {
        if path.len() > UNIXFS_PATH_CACHE_MAX_PATH_BYTES {
            return None;
        }

        let key = UnixfsPathCacheKey::new(root, path);
        let mut inner = self.lock_inner();
        let tick = inner.tick();
        inner.paths.get(&key, tick).cloned()
    }

    fn get_file_size(&self, cid: &Cid) -> Option<u64> {
        let mut inner = self.lock_inner();
        let tick = inner.tick();
        inner.file_sizes.get(cid, tick).copied()
    }

    /// Caches a copy of `decoded`, made once its room is taken.
    fn insert(&self, cid: &Cid, encoded_len: usize, decoded: &DecodedDagPb) {
        let mut inner = self.lock_inner();
        if !inner.enabled {
            return;
        }
        if encoded_len > UNIXFS_METADATA_CACHE_MAX_BLOCK_BYTES {
            inner.nodes.oversized_skips = inner.nodes.oversized_skips.saturating_add(1);
            return;
        }
        inner.insert(
            |inner| &mut inner.nodes,
            |nodes, tick| nodes.touch(cid, tick),
            decoded.heap_bytes(),
            || (*cid, decoded.clone()),
            |_, decoded| decoded.heap_bytes(),
        );
    }

    fn insert_path(&self, root: &Cid, path: &str, resolved: ResolvedNode) {
        let mut inner = self.lock_inner();
        if !inner.enabled {
            return;
        }
        if path.len() > UNIXFS_PATH_CACHE_MAX_PATH_BYTES {
            inner.paths.oversized_skips = inner.paths.oversized_skips.saturating_add(1);
            return;
        }

        // The key is made only once its room is taken.
        inner.insert(
            |inner| &mut inner.paths,
            |paths, tick| paths.touch_where(|key| key.root == *root && key.path == path, tick),
            path.len(),
            || (UnixfsPathCacheKey::new(root, path), resolved),
            |key, _| key.heap_bytes(),
        );
    }

    fn insert_file_size(&self, cid: &Cid, size: u64) {
        let mut inner = self.lock_inner();
        if !inner.enabled {
            return;
        }
        inner.insert(
            |inner| &mut inner.file_sizes,
            |sizes, tick| sizes.touch(cid, tick),
            0,
            || (*cid, size),
            |_, _| 0,
        );
    }

    fn clear(&self) {
        let mut inner = self.lock_inner();
        inner.nodes.entries.clear();
        inner.paths.entries.clear();
        inner.file_sizes.entries.clear();
        inner.heap = 0;
        inner.true_room();
    }

    fn stats(&self) -> UnixfsMetadataCacheStats {
        let inner = self.lock_inner();
        let (max_bytes, bytes) = (self.pool.capacity(), inner.room.bytes());
        UnixfsMetadataCacheStats {
            capacity: inner.capacity,
            max_bytes,
            bytes,
            len: inner.nodes.entries.len(),
            hits: inner.nodes.hits,
            misses: inner.nodes.misses,
            inserts: inner.nodes.inserts,
            evictions: inner.nodes.evictions,
            oversized_skips: inner.nodes.oversized_skips,
            path_len: inner.paths.entries.len(),
            path_hits: inner.paths.hits,
            path_misses: inner.paths.misses,
            path_inserts: inner.paths.inserts,
            path_evictions: inner.paths.evictions,
            path_oversized_skips: inner.paths.oversized_skips,
            file_size_len: inner.file_sizes.entries.len(),
            file_size_hits: inner.file_sizes.hits,
            file_size_misses: inner.file_sizes.misses,
            file_size_inserts: inner.file_sizes.inserts,
            file_size_evictions: inner.file_sizes.evictions,
        }
    }

    fn lock_inner(&self) -> MutexGuard<'_, UnixfsMetadataCacheInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug)]
struct UnixfsMetadataCacheInner {
    /// The most entries of each kind.
    capacity: usize,
    /// Its pool's capacity: the most it holds.
    max_bytes: usize,
    /// Whether it caches anything: not when its limits are zero, or too
    /// small for its maps.
    enabled: bool,
    clock: u64,
    /// The pool's room for all the cache holds ([`Self::footprint`]), its
    /// own structures alone when it caches nothing.
    room: PoolShare,
    /// The heap its entries own.
    heap: usize,
    nodes: CachedMap<Cid, DecodedDagPb>,
    paths: CachedMap<UnixfsPathCacheKey, ResolvedNode>,
    file_sizes: CachedMap<Cid, u64>,
}

impl Drop for UnixfsMetadataCacheInner {
    /// The cached entries go first: the room, and with the last of the
    /// pool's handles its lease, go only after them, so the reservation is
    /// given back only once what it stands for is freed. (Fields would drop
    /// in their order, the room before the maps.) The cache's own structure
    /// and its pool's counter are freed in the same drop, right after.
    fn drop(&mut self) {
        self.nodes.entries.clear();
        self.paths.entries.clear();
        self.file_sizes.entries.clear();
    }
}

impl UnixfsMetadataCacheInner {
    fn tick(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    /// What the cache holds, at most: its own structures, its maps' nodes
    /// and its entries' heap. One that caches nothing never allocates its
    /// maps' nodes.
    fn footprint(&self) -> usize {
        if !self.enabled {
            return UNIXFS_METADATA_CACHE_OWN_BYTES;
        }
        UNIXFS_METADATA_CACHE_OWN_BYTES
            .saturating_add(self.nodes.node_bytes(0))
            .saturating_add(self.paths.node_bytes(0))
            .saturating_add(self.file_sizes.node_bytes(0))
            .saturating_add(self.heap)
    }

    /// Sizes the room to the footprint: `false` if it can't grow to it.
    fn true_room(&mut self) -> bool {
        let footprint = self.footprint();
        self.room.try_resize(footprint)
    }

    /// Evicts the least recently used entry of any kind: `false` if there
    /// is none.
    fn evict_oldest(&mut self) -> bool {
        let oldest = [
            self.nodes.oldest(),
            self.paths.oldest(),
            self.file_sizes.oldest(),
        ];
        let Some(kind) = (0..oldest.len())
            .filter(|kind| oldest[*kind].is_some())
            .min_by_key(|kind| oldest[*kind])
        else {
            return false;
        };
        let freed = match kind {
            0 => self.nodes.evict_oldest(),
            1 => self.paths.evict_oldest(),
            _ => self.file_sizes.evict_oldest(),
        };
        self.heap = self.heap.saturating_sub(freed.unwrap_or(0));
        true
    }

    /// Caches the entry `make` makes in the map `select` picks, unless
    /// `cached` finds it there already (and touches it, at the tick it is
    /// given): a key names its content, so that is the same entry. Room for
    /// it (its heap, at most `heap`, and its map's nodes with it) is taken
    /// first, evicting the oldest entries of any kind, and only then is
    /// `make` called to copy it in; the room is then trued to what the
    /// cache holds.
    fn insert<K: Ord, V>(
        &mut self,
        select: fn(&mut Self) -> &mut CachedMap<K, V>,
        cached: impl FnOnce(&mut CachedMap<K, V>, u64) -> bool,
        heap: usize,
        make: impl FnOnce() -> (K, V),
        heap_of: fn(&K, &V) -> usize,
    ) {
        let tick = self.tick();
        if cached(select(self), tick) {
            return;
        }
        // The cache emptied but for this entry: if even that is past the
        // capacity, nothing is evicted for it.
        let alone = UNIXFS_METADATA_CACHE_OWN_BYTES
            .saturating_add(btree_map_bytes::<Cid, Cached<DecodedDagPb>>(0))
            .saturating_add(btree_map_bytes::<UnixfsPathCacheKey, Cached<ResolvedNode>>(
                0,
            ))
            .saturating_add(btree_map_bytes::<Cid, Cached<u64>>(0))
            .saturating_add(btree_map_bytes::<K, Cached<V>>(1) - btree_map_bytes::<K, Cached<V>>(0))
            .saturating_add(heap);
        if alone > self.max_bytes {
            let map = select(self);
            map.oversized_skips = map.oversized_skips.saturating_add(1);
            return;
        }
        // Each kind's entry cap, as before the byte capacity.
        while select(self).entries.len() >= self.capacity {
            let Some(freed) = select(self).evict_oldest() else {
                break;
            };
            self.heap = self.heap.saturating_sub(freed);
        }
        loop {
            let growth = select(self).node_bytes(1) - select(self).node_bytes(0);
            let needed = self.footprint().saturating_add(growth).saturating_add(heap);
            if self.room.try_resize(needed) {
                break;
            }
            if !self.evict_oldest() {
                self.true_room();
                return;
            }
        }
        let (key, value) = make();
        let held = heap_of(&key, &value);
        // A copy owns no more than its source; were it ever to, it stays
        // only if the pool has room for the difference.
        if held > heap && !self.room.try_grow(held - heap) {
            drop((key, value));
            let map = select(self);
            map.oversized_skips = map.oversized_skips.saturating_add(1);
            self.true_room();
            return;
        }
        let map = select(self);
        map.entries.insert(
            key,
            Cached {
                value,
                last_used: tick,
                heap: held,
            },
        );
        map.inserts = map.inserts.saturating_add(1);
        self.heap = self.heap.saturating_add(held);
        self.true_room();
    }
}

/// One kind of entry, with its counters.
#[derive(Debug)]
struct CachedMap<K, V> {
    entries: BTreeMap<K, Cached<V>>,
    hits: u64,
    misses: u64,
    inserts: u64,
    evictions: u64,
    oversized_skips: u64,
}

impl<K, V> Default for CachedMap<K, V> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            hits: 0,
            misses: 0,
            inserts: 0,
            evictions: 0,
            oversized_skips: 0,
        }
    }
}

#[derive(Debug)]
struct Cached<V> {
    value: V,
    last_used: u64,
    /// The heap the entry owns, its key's included.
    heap: usize,
}

impl<K: Ord, V> CachedMap<K, V> {
    /// What the map's nodes take, at most, with `extra` more entries.
    fn node_bytes(&self, extra: usize) -> usize {
        btree_map_bytes::<K, Cached<V>>(self.entries.len().saturating_add(extra))
    }

    fn get(&mut self, key: &K, tick: u64) -> Option<&V> {
        match self.entries.get_mut(key) {
            Some(entry) => {
                self.hits = self.hits.saturating_add(1);
                entry.last_used = tick;
                Some(&entry.value)
            }
            None => {
                self.misses = self.misses.saturating_add(1);
                None
            }
        }
    }

    /// Marks `key`'s entry used at `tick`: whether there is one.
    fn touch(&mut self, key: &K, tick: u64) -> bool {
        self.entries
            .get_mut(key)
            .map(|entry| entry.last_used = tick)
            .is_some()
    }

    /// Marks the entry whose key `matches` used at `tick`, without making a
    /// key to look it up by: whether there is one.
    fn touch_where(&mut self, matches: impl Fn(&K) -> bool, tick: u64) -> bool {
        self.entries
            .iter_mut()
            .find(|(key, _)| matches(key))
            .map(|(_, entry)| entry.last_used = tick)
            .is_some()
    }

    /// When the least recently used entry was last used.
    fn oldest(&self) -> Option<u64> {
        self.entries.values().map(|entry| entry.last_used).min()
    }

    /// Evicts the least recently used entry, returning the heap it owned.
    fn evict_oldest(&mut self) -> Option<usize> {
        let (last_used, heap) = self
            .entries
            .values()
            .map(|entry| (entry.last_used, entry.heap))
            .min()?;
        // Ticks are unique: this removes that one entry.
        self.entries.retain(|_, entry| entry.last_used != last_used);
        self.evictions = self.evictions.saturating_add(1);
        Some(heap)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct UnixfsPathCacheKey {
    root: Cid,
    path: String,
}

impl UnixfsPathCacheKey {
    fn new(root: &Cid, path: &str) -> Self {
        Self {
            root: *root,
            path: path.to_string(),
        }
    }

    fn heap_bytes(&self) -> usize {
        self.path.capacity()
    }
}

#[derive(Clone, Debug)]
struct DecodedDagPb {
    /// The node's links. Its data is decoded into `data` and not kept.
    node: PbNode,
    data: UnixfsData,
    kind: DataType,
}

impl DecodedDagPb {
    /// The heap it owns, by capacity: what a clone of it owns at most.
    fn heap_bytes(&self) -> usize {
        let vec = |bytes: &Option<Vec<u8>>| bytes.as_ref().map_or(0, Vec::capacity);
        let links = self
            .node
            .links
            .iter()
            .map(|link| {
                vec(&link.hash).saturating_add(link.name.as_ref().map_or(0, String::capacity))
            })
            .fold(
                self.node
                    .links
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PbLink>()),
                usize::saturating_add,
            );
        vec(&self.node.data)
            .saturating_add(links)
            .saturating_add(vec(&self.data.data))
            .saturating_add(
                self.data
                    .blocksizes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }
}

struct UnixfsContext<'a> {
    provider: &'a dyn BlockProvider,
    metadata_cache: Option<&'a UnixfsMetadataCache>,
}

impl<'a> UnixfsContext<'a> {
    fn uncached(provider: &'a dyn BlockProvider) -> Self {
        Self {
            provider,
            metadata_cache: None,
        }
    }

    fn cached(provider: &'a dyn BlockProvider, metadata_cache: &'a UnixfsMetadataCache) -> Self {
        Self {
            provider,
            metadata_cache: Some(metadata_cache),
        }
    }

    fn get_block(&self, cid: &Cid) -> Result<Block> {
        self.provider
            .get_block(cid)
            .map_err(provider_error)?
            .ok_or(UnixfsError::NotFound(*cid))
    }

    fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Vec<u8>> {
        self.provider
            .get_block_range(cid, start, end)
            .map_err(provider_error)?
            .ok_or(UnixfsError::NotFound(*cid))
    }

    fn get_block_ranges(&self, ranges: &[(Cid, u64, u64)]) -> Result<Vec<Vec<u8>>> {
        self.provider
            .get_block_ranges(ranges)
            .map_err(provider_error)?
            .into_iter()
            .zip(ranges.iter())
            .map(|(bytes, (cid, _, _))| bytes.ok_or(UnixfsError::NotFound(*cid)))
            .collect()
    }

    fn dag_pb(&self, cid: &Cid) -> Result<DecodedDagPb> {
        if let Some(cache) = self.metadata_cache {
            if let Some(decoded) = cache.get(cid) {
                return Ok(decoded);
            }
        }

        let block = self.get_block(cid)?;
        if block.codec() != CODEC_DAG_PB {
            return Err(UnixfsError::UnsupportedCodec(block.codec()));
        }

        let mut node = decode_pb_node(block.data())?;
        let data = decode_unixfs_data(&node)?;
        // Decoded into `data`: a second copy of it would only take room.
        node.data = None;
        let kind = data_type(&data)?;
        let decoded = DecodedDagPb { node, data, kind };
        if let Some(cache) = self.metadata_cache {
            cache.insert(cid, block.data().len(), &decoded);
        }
        Ok(decoded)
    }

    fn resolve_path(&self, root: &Cid, path: &str) -> Result<ResolvedNode> {
        if let Some(cache) = self.metadata_cache {
            if let Some(resolved) = cache.get_path(root, path) {
                return Ok(resolved);
            }
        }

        let resolved = self.resolve_path_uncached(root, path)?;
        if let Some(cache) = self.metadata_cache {
            cache.insert_path(root, path, resolved.clone());
        }
        Ok(resolved)
    }

    fn resolve_path_with_raw_link_tsize_hint(
        &self,
        root: &Cid,
        path: &str,
    ) -> Result<(ResolvedNode, Option<u64>)> {
        let mut current = LinkTarget {
            cid: *root,
            size: None,
        };
        let mut segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .peekable();

        if segments.peek().is_none() {
            return self.classify_with_raw_link_tsize_hint(&current);
        }

        for segment in segments {
            if current.cid.codec() != CODEC_DAG_PB {
                self.get_block(&current.cid)?;
                return Err(UnixfsError::NotDirectory);
            }

            let decoded = self.dag_pb(&current.cid)?;
            current = match decoded.kind {
                DataType::Directory => find_link_target(&decoded.node, segment)?
                    .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?,
                DataType::HamtShard => self
                    .find_hamt_link_target(&decoded.node, &decoded.data, segment)?
                    .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?,
                _ => return Err(UnixfsError::NotDirectory),
            };
        }

        self.classify_with_raw_link_tsize_hint(&current)
    }

    fn resolve_path_uncached(&self, root: &Cid, path: &str) -> Result<ResolvedNode> {
        let mut current = *root;
        let mut segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .peekable();

        if segments.peek().is_none() {
            return self.classify(&current);
        }

        for segment in segments {
            if current.codec() != CODEC_DAG_PB {
                self.get_block(&current)?;
                return Err(UnixfsError::NotDirectory);
            }

            let decoded = self.dag_pb(&current)?;
            match decoded.kind {
                DataType::Directory => {
                    current = find_link(&decoded.node, segment)?
                        .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?;
                }
                DataType::HamtShard => {
                    current = self
                        .find_hamt_link(&decoded.node, &decoded.data, segment)?
                        .ok_or_else(|| UnixfsError::PathNotFound(segment.to_string()))?;
                }
                _ => return Err(UnixfsError::NotDirectory),
            }
        }

        self.classify(&current)
    }

    fn read_file(&self, root: &Cid, path: &str) -> Result<Vec<u8>> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
            NodeKind::Raw | NodeKind::File => {}
        }
        self.read_file_cid(&resolved.cid)
    }

    fn list_directory(&self, root: &Cid, path: &str) -> Result<Vec<DirectoryEntry>> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => self.list_directory_cid(&resolved.cid),
            NodeKind::Raw | NodeKind::File => Err(UnixfsError::NotDirectory),
        }
    }

    fn file_size(&self, root: &Cid, path: &str) -> Result<u64> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
            NodeKind::Raw | NodeKind::File => {}
        }
        self.file_size_cid(&resolved.cid)
    }

    fn read_file_range(&self, root: &Cid, path: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        let resolved = self.resolve_path(root, path)?;
        match resolved.kind {
            NodeKind::Directory | NodeKind::HamtShard => return Err(UnixfsError::IsDirectory),
            NodeKind::Raw | NodeKind::File => {}
        }
        self.read_file_cid_range(&resolved.cid, start, end)
    }
}

pub fn resolve_path(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<ResolvedNode> {
    UnixfsContext::uncached(provider).resolve_path(root, path)
}

pub fn read_file(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<Vec<u8>> {
    UnixfsContext::uncached(provider).read_file(root, path)
}

pub fn list_directory(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
) -> Result<Vec<DirectoryEntry>> {
    UnixfsContext::uncached(provider).list_directory(root, path)
}

pub fn file_size(provider: &dyn BlockProvider, root: &Cid, path: &str) -> Result<u64> {
    UnixfsContext::uncached(provider).file_size(root, path)
}

pub fn read_file_range(
    provider: &dyn BlockProvider,
    root: &Cid,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>> {
    UnixfsContext::uncached(provider).read_file_range(root, path, start, end)
}

impl UnixfsContext<'_> {
    fn list_directory_cid(&self, cid: &Cid) -> Result<Vec<DirectoryEntry>> {
        if cid.codec() != CODEC_DAG_PB {
            return Err(UnixfsError::NotDirectory);
        }

        let decoded = self.dag_pb(cid)?;
        let mut entries = match decoded.kind {
            DataType::Directory => directory_entries(&decoded.node.links)?,
            DataType::HamtShard => self.hamt_directory_entries(&decoded.node, &decoded.data)?,
            _ => return Err(UnixfsError::NotDirectory),
        };
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    fn hamt_directory_entries(
        &self,
        node: &PbNode,
        data: &UnixfsData,
    ) -> Result<Vec<DirectoryEntry>> {
        let root = validate_hamt(data)?;
        let mut entries = Vec::new();
        let mut pending = Vec::new();
        collect_hamt_entries(&node.links, root.prefix_len, &mut entries, &mut pending)?;

        let mut visited = 0usize;
        while let Some(cid) = pending.pop() {
            visited += 1;
            if visited > HAMT_MAX_SHARDS_VISITED {
                return Err(UnixfsError::InvalidDagPb(format!(
                    "HAMT traversal exceeded {HAMT_MAX_SHARDS_VISITED} shards"
                )));
            }
            if cid.codec() != CODEC_DAG_PB {
                return Err(UnixfsError::NotDirectory);
            }

            let shard = self.dag_pb(&cid)?;
            if shard.kind != DataType::HamtShard {
                return Err(UnixfsError::InvalidDagPb(
                    "HAMT bucket link did not resolve to a HAMT shard".into(),
                ));
            }
            let layout = validate_hamt(&shard.data)?;
            collect_hamt_entries(
                &shard.node.links,
                layout.prefix_len,
                &mut entries,
                &mut pending,
            )?;
        }

        Ok(entries)
    }

    fn read_file_cid(&self, cid: &Cid) -> Result<Vec<u8>> {
        self.read_file_cid_at(cid, 0)
    }

    fn read_file_cid_at(&self, cid: &Cid, depth: usize) -> Result<Vec<u8>> {
        check_file_depth(depth)?;
        match cid.codec() {
            CODEC_RAW => Ok(self.get_block(cid)?.data().to_vec()),
            CODEC_DAG_PB => {
                let decoded = self.dag_pb(cid)?;
                match decoded.kind {
                    DataType::Raw | DataType::File => {
                        let mut out = decoded.data.data.unwrap_or_default();
                        for link in &decoded.node.links {
                            let child = link_cid(link)?;
                            out.extend_from_slice(&self.read_file_cid_at(&child, depth + 1)?);
                        }
                        Ok(out)
                    }
                    DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                    other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
                }
            }
            _ => {
                let block = self.get_block(cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn file_size_cid(&self, cid: &Cid) -> Result<u64> {
        self.file_size_cid_at(cid, 0)
    }

    fn file_size_cid_at(&self, cid: &Cid, depth: usize) -> Result<u64> {
        if let Some(cache) = self.metadata_cache {
            if let Some(size) = cache.get_file_size(cid) {
                return Ok(size);
            }
        }

        let size = self.file_size_cid_uncached(cid, depth)?;
        if let Some(cache) = self.metadata_cache {
            cache.insert_file_size(cid, size);
        }
        Ok(size)
    }

    fn file_size_cid_uncached(&self, cid: &Cid, depth: usize) -> Result<u64> {
        check_file_depth(depth)?;
        match cid.codec() {
            CODEC_RAW => Ok(self.get_block(cid)?.data().len() as u64),
            CODEC_DAG_PB => {
                let decoded = self.dag_pb(cid)?;
                match decoded.kind {
                    DataType::Raw | DataType::File => {
                        if let Some(filesize) = decoded.data.filesize {
                            return Ok(filesize);
                        }
                        let mut size = decoded
                            .data
                            .data
                            .as_ref()
                            .map_or(0, |bytes| bytes.len() as u64);
                        for link in &decoded.node.links {
                            size = size.saturating_add(
                                self.file_size_cid_at(&link_cid(link)?, depth + 1)?,
                            );
                        }
                        Ok(size)
                    }
                    DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                    other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
                }
            }
            _ => {
                let block = self.get_block(cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn read_file_cid_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Vec<u8>> {
        self.read_file_cid_range_at(cid, start, end, 0)
    }

    fn read_file_cid_range_at(
        &self,
        cid: &Cid,
        start: u64,
        end: u64,
        depth: usize,
    ) -> Result<Vec<u8>> {
        check_file_depth(depth)?;
        match cid.codec() {
            CODEC_RAW => self.get_block_range(cid, start, end),
            CODEC_DAG_PB => {
                let decoded = self.dag_pb(cid)?;
                match decoded.kind {
                    DataType::Raw | DataType::File => {
                        let mut out = Vec::new();
                        let mut offset = 0u64;
                        if let Some(inline) = decoded.data.data.as_deref() {
                            append_intersection(&mut out, inline, offset, start, end);
                            offset = offset.saturating_add(inline.len() as u64);
                        }

                        let mut child_ranges = Vec::new();
                        for (index, link) in decoded.node.links.iter().enumerate() {
                            let child = link_cid(link)?;
                            let child_size = decoded
                                .data
                                .blocksizes
                                .get(index)
                                .copied()
                                .map(Ok)
                                .unwrap_or_else(|| self.file_size_cid_at(&child, depth + 1))?;
                            if child_size == 0 {
                                continue;
                            }
                            let child_end = offset.saturating_add(child_size - 1);
                            if ranges_intersect(offset, child_end, start, end) {
                                let range_start = start.saturating_sub(offset);
                                let range_end = end.min(child_end).saturating_sub(offset);
                                child_ranges.push((child, range_start, range_end));
                            }
                            offset = offset.saturating_add(child_size);
                            if offset > end {
                                break;
                            }
                        }
                        let mut raw_batch = Vec::new();
                        for (child, range_start, range_end) in child_ranges {
                            if child.codec() == CODEC_RAW {
                                raw_batch.push((child, range_start, range_end));
                                if raw_batch.len() >= UNIXFS_RANGE_BATCH_MAX_BLOCKS {
                                    for bytes in self.get_block_ranges(&raw_batch)? {
                                        out.extend_from_slice(&bytes);
                                    }
                                    raw_batch.clear();
                                }
                            } else {
                                if !raw_batch.is_empty() {
                                    for bytes in self.get_block_ranges(&raw_batch)? {
                                        out.extend_from_slice(&bytes);
                                    }
                                    raw_batch.clear();
                                }
                                out.extend_from_slice(&self.read_file_cid_range_at(
                                    &child,
                                    range_start,
                                    range_end,
                                    depth + 1,
                                )?);
                            }
                        }
                        if !raw_batch.is_empty() {
                            for bytes in self.get_block_ranges(&raw_batch)? {
                                out.extend_from_slice(&bytes);
                            }
                        }
                        Ok(out)
                    }
                    DataType::Directory | DataType::HamtShard => Err(UnixfsError::IsDirectory),
                    other => Err(UnixfsError::UnsupportedNodeType(other as i32)),
                }
            }
            _ => {
                let block = self.get_block(cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn classify(&self, cid: &Cid) -> Result<ResolvedNode> {
        let kind = match cid.codec() {
            CODEC_RAW => {
                self.get_block(cid)?;
                NodeKind::Raw
            }
            CODEC_DAG_PB => match self.dag_pb(cid)?.kind {
                DataType::Raw | DataType::File => NodeKind::File,
                DataType::Directory => NodeKind::Directory,
                DataType::HamtShard => NodeKind::HamtShard,
                other => return Err(UnixfsError::UnsupportedNodeType(other as i32)),
            },
            _ => {
                let block = self.get_block(cid)?;
                return Err(UnixfsError::UnsupportedCodec(block.codec()));
            }
        };

        Ok(ResolvedNode { cid: *cid, kind })
    }

    fn classify_with_raw_link_tsize_hint(
        &self,
        target: &LinkTarget,
    ) -> Result<(ResolvedNode, Option<u64>)> {
        match target.cid.codec() {
            CODEC_RAW => {
                if let Some(size) = target.size.filter(|size| *size > 0) {
                    return Ok((
                        ResolvedNode {
                            cid: target.cid,
                            kind: NodeKind::Raw,
                        },
                        Some(size),
                    ));
                }

                self.get_block(&target.cid)?;
                Ok((
                    ResolvedNode {
                        cid: target.cid,
                        kind: NodeKind::Raw,
                    },
                    None,
                ))
            }
            CODEC_DAG_PB => {
                let kind = match self.dag_pb(&target.cid)?.kind {
                    DataType::Raw | DataType::File => NodeKind::File,
                    DataType::Directory => NodeKind::Directory,
                    DataType::HamtShard => NodeKind::HamtShard,
                    other => return Err(UnixfsError::UnsupportedNodeType(other as i32)),
                };
                Ok((
                    ResolvedNode {
                        cid: target.cid,
                        kind,
                    },
                    None,
                ))
            }
            _ => {
                let block = self.get_block(&target.cid)?;
                Err(UnixfsError::UnsupportedCodec(block.codec()))
            }
        }
    }

    fn find_hamt_link(&self, node: &PbNode, data: &UnixfsData, name: &str) -> Result<Option<Cid>> {
        self.find_hamt_entry(node, data, name)?
            .as_ref()
            .map(link_cid)
            .transpose()
    }

    fn find_hamt_link_target(
        &self,
        node: &PbNode,
        data: &UnixfsData,
        name: &str,
    ) -> Result<Option<LinkTarget>> {
        self.find_hamt_entry(node, data, name)?
            .as_ref()
            .map(link_target)
            .transpose()
    }

    /// `name`'s link in the HAMT whose root shard is `node`, found as the
    /// UnixFS spec's HAMT lookup does (and boxo's): the name's
    /// murmur3-x64-64 hash, read from its most significant end, gives each
    /// shard log2(fanout) bits, its bucket. The shard's bitfield says
    /// whether the bucket is occupied, and its link is the one at the
    /// bucket's rank among the occupied buckets. That link's name starts
    /// with the bucket's index in uppercase hex, as many digits as
    /// fanout - 1 has: named by the index alone, it is the next level's
    /// shard; by the index and `name`, the entry. An unoccupied bucket, or
    /// a link named otherwise, means `name` isn't in the directory. A
    /// lookup reads one shard a level, until the 64-bit hash has too few
    /// bits left for a bucket: at most 21 levels, at fanout 8.
    fn find_hamt_entry(
        &self,
        node: &PbNode,
        data: &UnixfsData,
        name: &str,
    ) -> Result<Option<PbLink>> {
        let hash = murmur3_x64_64(name.as_bytes());
        let mut used = 0;
        let mut next: Option<DecodedDagPb> = None;
        loop {
            let (links, data) = next.as_ref().map_or((&node.links, data), |shard| {
                (&shard.node.links, &shard.data)
            });
            let shard = validate_hamt(data)?;
            if used + shard.bits > u64::BITS {
                return Err(UnixfsError::InvalidDagPb(
                    "HAMT deeper than its 64-bit hash".into(),
                ));
            }
            let bucket = (hash << used >> (u64::BITS - shard.bits)) as usize;
            used += shard.bits;
            if !shard.occupied(bucket) {
                return Ok(None);
            }
            let link = links.get(shard.rank(bucket)).ok_or_else(|| {
                UnixfsError::InvalidDagPb("HAMT bitfield has more buckets than links".into())
            })?;
            // boxo takes the prefix's length and doesn't compare it; a link
            // at this rank under another bucket's prefix is malformed, and
            // the name is absent (as freedom-core's reader has it).
            let prefix = format!("{bucket:0width$X}", width = shard.prefix_len);
            let Some(rest) = link
                .name
                .as_deref()
                .and_then(|link_name| link_name.strip_prefix(prefix.as_str()))
            else {
                return Ok(None);
            };
            if !rest.is_empty() {
                return Ok((rest == name).then(|| link.clone()));
            }
            let cid = link_cid(link)?;
            if cid.codec() != CODEC_DAG_PB {
                return Err(UnixfsError::NotDirectory);
            }
            let shard = self.dag_pb(&cid)?;
            if shard.kind != DataType::HamtShard {
                return Err(UnixfsError::InvalidDagPb(
                    "HAMT bucket link did not resolve to a HAMT shard".into(),
                ));
            }
            next = Some(shard);
        }
    }
}

/// Refuses a file node `depth` levels below its file's root past
/// [`MAX_FILE_DAG_DEPTH`].
fn check_file_depth(depth: usize) -> Result<()> {
    if depth > MAX_FILE_DAG_DEPTH {
        return Err(UnixfsError::InvalidDagPb(format!(
            "file DAG deeper than {MAX_FILE_DAG_DEPTH} levels"
        )));
    }
    Ok(())
}

fn directory_entries(links: &[PbLink]) -> Result<Vec<DirectoryEntry>> {
    let mut entries = Vec::new();
    for link in links {
        let Some(name) = link.name.as_ref().filter(|name| !name.is_empty()) else {
            continue;
        };
        entries.push(DirectoryEntry {
            name: name.clone(),
            cid: link_cid(link)?,
            size: link.tsize,
        });
    }
    Ok(entries)
}

fn collect_hamt_entries(
    links: &[PbLink],
    prefix_len: usize,
    entries: &mut Vec<DirectoryEntry>,
    pending: &mut Vec<Cid>,
) -> Result<()> {
    for link in links {
        let Some(link_name) = link.name.as_deref() else {
            continue;
        };
        let link_name = link_name.as_bytes();
        if link_name.len() == prefix_len {
            pending.push(link_cid(link)?);
        } else if link_name.len() > prefix_len {
            entries.push(DirectoryEntry {
                name: String::from_utf8_lossy(&link_name[prefix_len..]).to_string(),
                cid: link_cid(link)?,
                size: link.tsize,
            });
        }
    }
    Ok(())
}

fn append_intersection(out: &mut Vec<u8>, bytes: &[u8], offset: u64, start: u64, end: u64) {
    if bytes.is_empty() {
        return;
    }
    let block_end = offset.saturating_add(bytes.len() as u64 - 1);
    if !ranges_intersect(offset, block_end, start, end) {
        return;
    }
    out.extend_from_slice(&slice_bytes(
        bytes,
        start.saturating_sub(offset),
        end.min(block_end).saturating_sub(offset),
    ));
}

fn slice_bytes(bytes: &[u8], start: u64, end: u64) -> Vec<u8> {
    if bytes.is_empty() || start > end || start >= bytes.len() as u64 {
        return Vec::new();
    }
    let start = start.min(bytes.len() as u64) as usize;
    let end = end.min(bytes.len() as u64 - 1) as usize;
    bytes[start..=end].to_vec()
}

fn ranges_intersect(a_start: u64, a_end: u64, b_start: u64, b_end: u64) -> bool {
    a_start <= b_end && b_start <= a_end
}

/// A dag-pb block, decoded as the dag-pb spec requires
/// (ipld.io/specs/codecs/dag-pb, "Strictness"), and as go-codec-dagpb
/// (Kubo's) and `@ipld/dag-pb` decode it:
/// - **PBNode:** only Links (field 2) and Data (field 1), each
///   length-delimited; Data at most once, before the first Link or after
///   the last (the spec allows both orders, and IPFS data has both), never
///   between two;
/// - **PBLink:** only Hash (1, bytes), Name (2, a string) and Tsize (3, a
///   varint), each at most once and in that order; Hash present, and a
///   CID; Name UTF-8;
/// - nothing else: no other field, no group, no wrong wire type, no byte
///   past the last field.
///
/// The links are counted first, without decoding ([`MAX_DAG_PB_LINKS`]).
fn decode_pb_node(data: &[u8]) -> Result<PbNode> {
    // PBNode's links are field 2, each length-delimited.
    check_repeated_count(data, 2, false, "links")?;
    let invalid = |what: &str| UnixfsError::InvalidDagPb(format!("PBNode: {what}"));
    let mut rest = data;
    let mut node = PbNode::default();
    // Set when Data ends a run of Links: no Link may follow.
    let mut links_ended = false;
    while !rest.is_empty() {
        let (field, value) = length_delimited_field(&mut rest).map_err(invalid)?;
        match field {
            1 if node.data.is_some() => return Err(invalid("Data more than once")),
            1 => {
                links_ended = !node.links.is_empty();
                node.data = Some(value.to_vec());
            }
            2 if links_ended => return Err(invalid("Links on both sides of Data")),
            2 => node.links.push(decode_pb_link(value)?),
            _ => return Err(invalid("a field other than Links and Data")),
        }
    }
    Ok(node)
}

/// One PBLink, decoded strictly (see [`decode_pb_node`]).
fn decode_pb_link(bytes: &[u8]) -> Result<PbLink> {
    let invalid = |what: &str| UnixfsError::InvalidDagPb(format!("PBLink: {what}"));
    let mut rest = bytes;
    let mut link = PbLink::default();
    let mut last = 0;
    while !rest.is_empty() {
        let key = read_varint(&mut rest).ok_or_else(|| invalid("a truncated key"))?;
        let (field, wire_type) = (key >> 3, key & 7);
        if field <= last {
            return Err(invalid(if field == last {
                "a field more than once"
            } else {
                "fields out of order (Hash, Name, Tsize)"
            }));
        }
        last = field;
        match (field, wire_type) {
            (1, 2) => {
                let hash =
                    take_length_delimited(&mut rest).ok_or_else(|| invalid("a truncated Hash"))?;
                cid_from_bytes(hash).ok_or_else(|| invalid("a Hash that isn't a CID"))?;
                link.hash = Some(hash.to_vec());
            }
            (2, 2) => {
                let name =
                    take_length_delimited(&mut rest).ok_or_else(|| invalid("a truncated Name"))?;
                let name =
                    std::str::from_utf8(name).map_err(|_| invalid("a Name that isn't UTF-8"))?;
                link.name = Some(name.to_string());
            }
            (3, 0) => {
                link.tsize =
                    Some(read_varint(&mut rest).ok_or_else(|| invalid("a truncated Tsize"))?);
            }
            (1..=3, _) => return Err(invalid("a field of the wrong wire type")),
            _ => return Err(invalid("a field other than Hash, Name and Tsize")),
        }
    }
    if link.hash.is_none() {
        return Err(invalid("no Hash"));
    }
    Ok(link)
}

/// A length-delimited field off the front of `rest`: its number and value.
fn length_delimited_field<'a>(
    rest: &mut &'a [u8],
) -> std::result::Result<(u64, &'a [u8]), &'static str> {
    let key = read_varint(rest).ok_or("a truncated key")?;
    if key & 7 != 2 {
        return Err("a field that isn't length-delimited");
    }
    let value = take_length_delimited(rest).ok_or("a truncated field")?;
    Ok((key >> 3, value))
}

/// A length and that many bytes off the front of `rest`.
fn take_length_delimited<'a>(rest: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = usize::try_from(read_varint(rest)?).ok()?;
    let value = rest.get(..len)?;
    *rest = &rest[len..];
    Some(value)
}

/// A CID's bytes, all of them, in the CID's one encoding: CIDv0 (a bare
/// SHA-256 multihash) or CIDv1. `Cid::read_bytes` alone also takes a
/// varint that isn't minimal, or one past 64 bits (it keeps the low bits),
/// as a different CID; Kubo refuses both.
fn cid_from_bytes(bytes: &[u8]) -> Option<Cid> {
    let mut cursor = Cursor::new(bytes);
    let cid = Cid::read_bytes(&mut cursor).ok()?;
    (cursor.position() as usize == bytes.len() && cid.to_bytes() == bytes).then_some(cid)
}

fn decode_unixfs_data(node: &PbNode) -> Result<UnixfsData> {
    let data = node
        .data
        .as_deref()
        .ok_or_else(|| UnixfsError::InvalidDagPb("missing UnixFS data".into()))?;
    // UnixFS Data's blocksizes are field 4, varints, packed or not.
    check_repeated_count(data, 4, true, "block sizes")?;
    UnixfsData::decode(data).map_err(|err| UnixfsError::InvalidDagPb(err.to_string()))
}

/// Refuses `message` when prost would decode more than [`MAX_DAG_PB_LINKS`]
/// values into its repeated `field`, counted on the wire without decoding,
/// or when its wire format is broken (prost refuses that too).
fn check_repeated_count(message: &[u8], field: u64, varints: bool, what: &str) -> Result<()> {
    let count = repeated_count(message, field, varints)
        .ok_or_else(|| UnixfsError::InvalidDagPb("malformed protobuf".into()))?;
    if count > MAX_DAG_PB_LINKS {
        return Err(UnixfsError::InvalidDagPb(format!(
            "more than {MAX_DAG_PB_LINKS} {what}"
        )));
    }
    Ok(())
}

/// How many values prost would decode into the repeated `field` of the
/// protobuf `message`, read off the wire as prost reads it: each
/// length-delimited occurrence of a message field, or for a varint field
/// (`varints`) each varint occurrence and each varint in a packed one.
/// Fields inside groups are skipped, as prost skips them. `None` when the
/// message's own wire format is broken, as prost would find it: the values'
/// contents (a link's own fields) are prost's to check when it decodes them.
fn repeated_count(message: &[u8], field: u64, varints: bool) -> Option<usize> {
    // prost's recursion limit: inside the 100th open group, nothing but
    // that group's end is read.
    const MAX_GROUP_DEPTH: usize = 100;
    let mut rest = message;
    let mut groups = Vec::new();
    let mut count = 0usize;
    while !rest.is_empty() {
        let key = read_varint(&mut rest)?;
        let (number, wire_type) = (key >> 3, key & 7);
        if number == 0 {
            return None;
        }
        if groups.len() >= MAX_GROUP_DEPTH && wire_type != 4 {
            return None;
        }
        let counted = groups.is_empty() && number == field;
        match wire_type {
            0 => {
                read_varint(&mut rest)?;
                if counted && varints {
                    count += 1;
                }
            }
            1 => rest = rest.get(8..)?,
            2 => {
                let len = usize::try_from(read_varint(&mut rest)?).ok()?;
                let value = rest.get(..len)?;
                rest = &rest[len..];
                if counted {
                    count += if varints {
                        // A packed run: each varint ends on a byte below 0x80.
                        value.iter().filter(|byte| **byte < 0x80).count()
                    } else {
                        1
                    };
                }
            }
            3 => groups.push(number),
            4 => {
                if groups.pop() != Some(number) {
                    return None;
                }
            }
            5 => rest = rest.get(4..)?,
            _ => return None,
        }
    }
    groups.is_empty().then_some(count)
}

/// A protobuf varint off the front of `bytes`.
fn read_varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for (index, &byte) in bytes.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte < 0x80 {
            if index == 9 && byte > 1 {
                return None;
            }
            *bytes = &bytes[index + 1..];
            return Some(value);
        }
    }
    None
}

fn data_type(data: &UnixfsData) -> Result<DataType> {
    let value = data.r#type.unwrap_or(DataType::Raw as i32);
    DataType::try_from(value).map_err(|_| UnixfsError::UnsupportedNodeType(value))
}

fn find_link(node: &PbNode, name: &str) -> Result<Option<Cid>> {
    node.links
        .iter()
        .find(|link| link.name.as_deref() == Some(name))
        .map(link_cid)
        .transpose()
}

fn find_link_target(node: &PbNode, name: &str) -> Result<Option<LinkTarget>> {
    node.links
        .iter()
        .find(|link| link.name.as_deref() == Some(name))
        .map(link_target)
        .transpose()
}

/// A HAMT shard's layout, read from its UnixFS data as boxo's `makeShard`
/// reads it.
struct HamtLayout<'a> {
    /// The hash bits a bucket takes: log2(fanout).
    bits: u32,
    /// The hex digits of a link's bucket prefix: as many as fanout - 1 has.
    prefix_len: usize,
    /// The occupied buckets: a big-endian integer whose bit `i` is bucket
    /// `i`, its leading zero bytes possibly left out.
    bitfield: &'a [u8],
}

impl HamtLayout<'_> {
    fn occupied(&self, bucket: usize) -> bool {
        let bytes = self.bitfield.len();
        bucket / 8 < bytes && self.bitfield[bytes - 1 - bucket / 8] >> (bucket % 8) & 1 == 1
    }

    /// The occupied buckets below `bucket`: the index of its link.
    fn rank(&self, bucket: usize) -> usize {
        let bytes = self.bitfield.len();
        let whole = (bucket / 8).min(bytes);
        let mut rank: u32 = self.bitfield[bytes - whole..]
            .iter()
            .map(|byte| byte.count_ones())
            .sum();
        if bucket / 8 < bytes {
            let below = (1u8 << (bucket % 8)) - 1;
            rank += (self.bitfield[bytes - 1 - bucket / 8] & below).count_ones();
        }
        rank as usize
    }
}

/// A HAMT shard's layout, or an error when boxo wouldn't read it: a hash
/// other than murmur3-x64-64, a fanout other than a power of two from 8 to
/// 1024, a bitfield wider than fanout / 8 bytes, or file fields.
fn validate_hamt(data: &UnixfsData) -> Result<HamtLayout<'_>> {
    let fanout = data
        .fanout
        .filter(|fanout| fanout.is_power_of_two() && HAMT_FANOUTS.contains(fanout));
    let (Some(HAMT_MURMUR3_X64_64), Some(fanout)) = (data.hash_type, fanout) else {
        return Err(UnixfsError::InvalidDagPb(format!(
            "unsupported HAMT parameters hashType={:?} fanout={:?}",
            data.hash_type, data.fanout
        )));
    };
    if data.filesize.is_some() || !data.blocksizes.is_empty() {
        return Err(UnixfsError::InvalidDagPb(
            "HAMT shard carried file-only UnixFS fields".into(),
        ));
    }
    let bitfield = data.data.as_deref().unwrap_or_default();
    if bitfield.len() as u64 > fanout / 8 {
        return Err(UnixfsError::InvalidDagPb(format!(
            "HAMT bitfield of {} bytes, more than fanout {fanout}'s {}",
            bitfield.len(),
            fanout / 8
        )));
    }
    Ok(HamtLayout {
        bits: fanout.trailing_zeros(),
        prefix_len: format!("{:X}", fanout - 1).len(),
        bitfield,
    })
}

/// MurmurHash3 x64 128 (Austin Appleby's), its first 64 bits (`h1`): the
/// UnixFS HAMT's hash function, multicodec murmur3-x64-64 (0x22), as
/// go-unixfs and boxo take it from `murmur3.Sum64`.
fn murmur3_x64_64(key: &[u8]) -> u64 {
    murmur3_x64_128(key, 0).0
}

/// MurmurHash3 x64 128 of `key` with `seed`: `(h1, h2)`.
fn murmur3_x64_128(key: &[u8], seed: u32) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    let mix_k1 = |k1: u64| k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
    let mix_k2 = |k2: u64| k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
    let (mut h1, mut h2) = (u64::from(seed), u64::from(seed));
    let mut blocks = key.chunks_exact(16);
    for block in &mut blocks {
        let (low, high) = block.split_at(8);
        let k1 = u64::from_le_bytes(low.try_into().unwrap_or_default());
        let k2 = u64::from_le_bytes(high.try_into().unwrap_or_default());
        h1 ^= mix_k1(k1);
        h1 = h1
            .rotate_left(27)
            .wrapping_add(h2)
            .wrapping_mul(5)
            .wrapping_add(0x52dc_e729);
        h2 ^= mix_k2(k2);
        h2 = h2
            .rotate_left(31)
            .wrapping_add(h1)
            .wrapping_mul(5)
            .wrapping_add(0x3849_5ab5);
    }
    let tail = blocks.remainder();
    let (mut k1, mut k2) = (0u64, 0u64);
    for (index, &byte) in tail.iter().enumerate() {
        if index < 8 {
            k1 |= u64::from(byte) << (8 * index);
        } else {
            k2 |= u64::from(byte) << (8 * (index - 8));
        }
    }
    if tail.len() > 8 {
        h2 ^= mix_k2(k2);
    }
    if !tail.is_empty() {
        h1 ^= mix_k1(k1);
    }
    let len = key.len() as u64;
    h1 ^= len;
    h2 ^= len;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix64(h1);
    h2 = fmix64(h2);
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    (h1, h2)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

fn link_target(link: &PbLink) -> Result<LinkTarget> {
    Ok(LinkTarget {
        cid: link_cid(link)?,
        size: link.tsize,
    })
}

fn link_cid(link: &PbLink) -> Result<Cid> {
    let bytes = link
        .hash
        .as_deref()
        .ok_or_else(|| UnixfsError::InvalidDagPb("link is missing hash".into()))?;
    // A decoded link's Hash is a CID (decode_pb_node checked it).
    cid_from_bytes(bytes).ok_or_else(|| UnixfsError::InvalidDagPb("link hash isn't a CID".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use freedom_ipfs_core::{
        block_data_range, cid_from_data, Block, Result as CoreResult, CODEC_DAG_PB, CODEC_RAW,
    };
    use freedom_ipfs_store::SqliteBlockStore;
    use multihash::Multihash;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    fn unixfs_data(kind: DataType, data: &[u8]) -> Vec<u8> {
        UnixfsData {
            r#type: Some(kind as i32),
            data: Some(data.to_vec()),
            filesize: Some(data.len() as u64),
            blocksizes: Vec::new(),
            hash_type: None,
            fanout: None,
        }
        .encode_to_vec()
    }

    fn pb_file(data: &[u8], links: Vec<PbLink>) -> Vec<u8> {
        pb_file_with_metadata(data, links, data.len() as u64, Vec::new())
    }

    fn pb_file_with_metadata(
        data: &[u8],
        links: Vec<PbLink>,
        filesize: u64,
        blocksizes: Vec<u64>,
    ) -> Vec<u8> {
        PbNode {
            data: Some(
                UnixfsData {
                    r#type: Some(DataType::File as i32),
                    data: Some(data.to_vec()),
                    filesize: Some(filesize),
                    blocksizes,
                    hash_type: None,
                    fanout: None,
                }
                .encode_to_vec(),
            ),
            links,
        }
        .encode_to_vec()
    }

    fn pb_directory(links: Vec<PbLink>) -> Vec<u8> {
        PbNode {
            data: Some(unixfs_data(DataType::Directory, &[])),
            links,
        }
        .encode_to_vec()
    }

    fn pb_hamt(links: Vec<PbLink>) -> Vec<u8> {
        pb_hamt_at(256, links)
    }

    /// A HAMT shard at `fanout` as boxo writes one: each link's bucket read
    /// from its name's hex prefix, the links in bucket order, and a
    /// bitfield of exactly their buckets, its leading zero bytes left out.
    fn pb_hamt_at(fanout: u64, mut links: Vec<PbLink>) -> Vec<u8> {
        let width = format!("{:X}", fanout - 1).len();
        let bucket_of = |link: &PbLink| {
            usize::from_str_radix(&link.name.as_deref().unwrap()[..width], 16).unwrap()
        };
        links.sort_by_key(bucket_of);
        let buckets: Vec<usize> = links.iter().map(bucket_of).collect();
        let mut bitfield = hamt_bitfield(&buckets, (fanout / 8) as usize);
        let zeros = bitfield.iter().take_while(|&&byte| byte == 0).count();
        bitfield.drain(..zeros);
        hamt_shard(fanout, bitfield, links)
    }

    /// A HAMT shard at `fanout` with exactly this bitfield and these links.
    fn hamt_shard(fanout: u64, bitfield: Vec<u8>, links: Vec<PbLink>) -> Vec<u8> {
        PbNode {
            data: Some(
                UnixfsData {
                    r#type: Some(DataType::HamtShard as i32),
                    data: Some(bitfield),
                    filesize: None,
                    blocksizes: Vec::new(),
                    hash_type: Some(HAMT_MURMUR3_X64_64),
                    fanout: Some(fanout),
                }
                .encode_to_vec(),
            ),
            links,
        }
        .encode_to_vec()
    }

    /// A `bytes`-byte HAMT bitfield with these buckets set: a big-endian
    /// integer whose bit `i` is bucket `i`.
    fn hamt_bitfield(buckets: &[usize], bytes: usize) -> Vec<u8> {
        let mut bitfield = vec![0; bytes];
        for &bucket in buckets {
            bitfield[bytes - 1 - bucket / 8] |= 1 << (bucket % 8);
        }
        bitfield
    }

    fn link(name: &str, cid: &Cid) -> PbLink {
        PbLink {
            hash: Some(cid.to_bytes()),
            name: Some(name.to_string()),
            tsize: None,
        }
    }

    type RangeBatchCalls = Arc<Mutex<Vec<Vec<(Cid, u64, u64)>>>>;

    #[derive(Clone)]
    struct CountingProvider {
        blocks: Arc<HashMap<Cid, Vec<u8>>>,
        calls: Arc<Mutex<HashMap<Cid, usize>>>,
        range_calls: Arc<Mutex<HashMap<Cid, usize>>>,
        range_batch_calls: RangeBatchCalls,
    }

    impl CountingProvider {
        fn new(blocks: HashMap<Cid, Vec<u8>>) -> Self {
            Self {
                blocks: Arc::new(blocks),
                calls: Arc::new(Mutex::new(HashMap::new())),
                range_calls: Arc::new(Mutex::new(HashMap::new())),
                range_batch_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn call_count(&self, cid: &Cid) -> usize {
            *self
                .calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(cid)
                .unwrap_or(&0)
        }

        fn range_call_count(&self, cid: &Cid) -> usize {
            *self
                .range_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(cid)
                .unwrap_or(&0)
        }

        fn range_batch_calls(&self) -> Vec<Vec<(Cid, u64, u64)>> {
            self.range_batch_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl BlockProvider for CountingProvider {
        fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
            if let Some(data) = self.blocks.get(cid) {
                let mut calls = self
                    .calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *calls.entry(*cid).or_default() += 1;
                return Ok(Some(Block::unchecked(*cid, data.clone())));
            }
            Ok(None)
        }

        fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> CoreResult<Option<Vec<u8>>> {
            if let Some(data) = self.blocks.get(cid) {
                let mut calls = self
                    .range_calls
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *calls.entry(*cid).or_default() += 1;
                return Ok(Some(block_data_range(data, start, end)));
            }
            Ok(None)
        }

        fn get_block_ranges(&self, ranges: &[(Cid, u64, u64)]) -> CoreResult<Vec<Option<Vec<u8>>>> {
            self.range_batch_calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(ranges.to_vec());
            ranges
                .iter()
                .map(|(cid, start, end)| self.get_block_range(cid, *start, *end))
                .collect()
        }
    }

    #[test]
    fn reads_raw_block_as_file() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"raw leaf";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        assert_eq!(read_file(&store, &cid, "").unwrap(), data);
    }

    #[test]
    fn resolves_directory_and_reads_linked_file() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        let leaf_data = b"linked bytes";
        let leaf_cid = cid_from_data(CODEC_RAW, leaf_data);
        store.put_block(&leaf_cid, leaf_data).unwrap();

        let file_data = pb_file(b"prefix ", vec![link("leaf", &leaf_cid)]);
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        store.put_block(&file_cid, &file_data).unwrap();

        let dir_data = pb_directory(vec![link("index.html", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        store.put_block(&dir_cid, &dir_data).unwrap();

        assert_eq!(
            read_file(&store, &dir_cid, "index.html").unwrap(),
            b"prefix linked bytes"
        );
    }

    #[test]
    fn lists_directory_entries_without_reading_child_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        let alpha_data = b"alpha";
        let alpha_cid = cid_from_data(CODEC_RAW, alpha_data);
        let beta_data = b"beta";
        let beta_cid = cid_from_data(CODEC_RAW, beta_data);

        let dir_data = pb_directory(vec![
            PbLink {
                tsize: Some(5),
                ..link("beta.txt", &beta_cid)
            },
            PbLink {
                tsize: Some(4),
                ..link("alpha.txt", &alpha_cid)
            },
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        store.put_block(&dir_cid, &dir_data).unwrap();

        let entries = list_directory(&store, &dir_cid, "").unwrap();

        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.name.as_str(), entry.cid, entry.size))
                .collect::<Vec<_>>(),
            vec![
                ("alpha.txt", alpha_cid, Some(4)),
                ("beta.txt", beta_cid, Some(5))
            ]
        );
    }

    #[test]
    fn raw_link_tsize_hint_resolves_without_reading_raw_leaf() {
        let leaf_data = b"linked raw leaf";
        let leaf_cid = cid_from_data(CODEC_RAW, leaf_data);
        let dir_data = pb_directory(vec![PbLink {
            tsize: Some(leaf_data.len() as u64),
            ..link("app.js", &leaf_cid)
        }]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);
        let provider = CountingProvider::new(HashMap::from([
            (leaf_cid, leaf_data.to_vec()),
            (dir_cid, dir_data),
        ]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        let (resolved, size_hint) = resolver
            .resolve_path_with_raw_link_tsize_hint(&provider, &dir_cid, "app.js")
            .unwrap();

        assert_eq!(resolved.cid, leaf_cid);
        assert_eq!(resolved.kind, NodeKind::Raw);
        assert_eq!(size_hint, Some(leaf_data.len() as u64));
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&leaf_cid), 0);
        assert_eq!(
            resolver
                .read_file_cid_range(&provider, &leaf_cid, 0, 5)
                .unwrap(),
            b"linked"
        );
        assert_eq!(provider.range_call_count(&leaf_cid), 1);
    }

    #[test]
    fn cached_resolver_reuses_directory_metadata_across_sibling_paths() {
        let alpha_data = b"alpha";
        let alpha_cid = cid_from_data(CODEC_RAW, alpha_data);
        let beta_data = b"beta";
        let beta_cid = cid_from_data(CODEC_RAW, beta_data);

        let dir_data = pb_directory(vec![
            link("alpha.txt", &alpha_cid),
            link("beta.txt", &beta_cid),
        ]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);

        let provider = CountingProvider::new(HashMap::from([
            (alpha_cid, alpha_data.to_vec()),
            (beta_cid, beta_data.to_vec()),
            (dir_cid, dir_data),
        ]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver
                .file_size(&provider, &dir_cid, "alpha.txt")
                .unwrap(),
            alpha_data.len() as u64
        );
        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "beta.txt").unwrap(),
            beta_data.len() as u64
        );

        assert_eq!(provider.call_count(&dir_cid), 1);
        let stats = resolver.metadata_cache_stats();
        assert!(
            stats.bytes > 0 && stats.bytes <= stats.max_bytes,
            "{stats:?}"
        );
        assert_eq!(
            stats,
            UnixfsMetadataCacheStats {
                capacity: 8,
                max_bytes: DEFAULT_UNIXFS_METADATA_CACHE_MAX_BYTES,
                bytes: stats.bytes,
                len: 1,
                hits: 1,
                misses: 1,
                inserts: 1,
                evictions: 0,
                oversized_skips: 0,
                path_len: 2,
                path_hits: 0,
                path_misses: 2,
                path_inserts: 2,
                path_evictions: 0,
                path_oversized_skips: 0,
                file_size_len: 2,
                file_size_hits: 0,
                file_size_misses: 2,
                file_size_inserts: 2,
                file_size_evictions: 0,
            }
        );
    }

    #[test]
    fn cached_resolver_reuses_file_metadata_across_ranges() {
        let file_data = pb_file(b"abcdef", Vec::new());
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);

        let provider = CountingProvider::new(HashMap::from([(file_cid, file_data)]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver
                .read_file_range(&provider, &file_cid, "", 0, 1)
                .unwrap(),
            b"ab"
        );
        assert_eq!(
            resolver
                .read_file_range(&provider, &file_cid, "", 2, 4)
                .unwrap(),
            b"cde"
        );

        assert_eq!(provider.call_count(&file_cid), 1);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.inserts, 1);
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.path_misses, 1);
        assert_eq!(stats.path_inserts, 1);
        assert_eq!(stats.path_hits, 1);
    }

    #[test]
    fn cached_resolver_reuses_raw_file_sizes() {
        let data = b"raw file size";
        let cid = cid_from_data(CODEC_RAW, data);
        let provider = CountingProvider::new(HashMap::from([(cid, data.to_vec())]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(resolver.file_size(&provider, &cid, "").unwrap(), 13);
        assert_eq!(resolver.file_size(&provider, &cid, "").unwrap(), 13);

        assert_eq!(provider.call_count(&cid), 2);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.file_size_len, 1);
        assert_eq!(stats.file_size_misses, 1);
        assert_eq!(stats.file_size_inserts, 1);
        assert_eq!(stats.file_size_hits, 1);
    }

    #[test]
    fn cached_resolver_reuses_path_resolution_across_repeated_reads() {
        let file_data = pb_file(b"abcdef", Vec::new());
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        let dir_data = pb_directory(vec![link("file.txt", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);

        let provider =
            CountingProvider::new(HashMap::from([(file_cid, file_data), (dir_cid, dir_data)]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "file.txt").unwrap(),
            6
        );
        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "file.txt").unwrap(),
            6
        );

        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&file_cid), 1);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.path_len, 1);
        assert_eq!(stats.path_misses, 1);
        assert_eq!(stats.path_inserts, 1);
        assert_eq!(stats.path_hits, 1);
        assert_eq!(stats.file_size_len, 1);
        assert_eq!(stats.file_size_misses, 1);
        assert_eq!(stats.file_size_inserts, 1);
        assert_eq!(stats.file_size_hits, 1);
    }

    #[test]
    fn cid_direct_range_reads_skip_path_resolution_cache() {
        let file_data = pb_file(b"abcdef", Vec::new());
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        let dir_data = pb_directory(vec![link("file.txt", &file_cid)]);
        let dir_cid = cid_from_data(CODEC_DAG_PB, &dir_data);

        let provider =
            CountingProvider::new(HashMap::from([(file_cid, file_data), (dir_cid, dir_data)]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(8);

        assert_eq!(
            resolver.file_size(&provider, &dir_cid, "file.txt").unwrap(),
            6
        );
        let before = resolver.metadata_cache_stats();

        assert_eq!(
            resolver
                .read_file_cid_range(&provider, &file_cid, 1, 3)
                .unwrap(),
            b"bcd"
        );

        let after = resolver.metadata_cache_stats();
        assert_eq!(after.path_hits, before.path_hits);
        assert_eq!(after.path_misses, before.path_misses);
        assert_eq!(provider.call_count(&dir_cid), 1);
        assert_eq!(provider.call_count(&file_cid), 1);
    }

    #[test]
    fn raw_cid_range_uses_provider_range_read() {
        let data = b"0123456789".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        let provider = CountingProvider::new(HashMap::from([(cid, data)]));
        let resolver = UnixfsResolver::default();

        assert_eq!(
            resolver.read_file_cid_range(&provider, &cid, 2, 5).unwrap(),
            b"2345"
        );

        assert_eq!(provider.call_count(&cid), 0);
        assert_eq!(provider.range_call_count(&cid), 1);
    }

    #[test]
    fn file_range_batches_adjacent_raw_child_ranges() {
        let first_data = b"abcdef";
        let first_cid = cid_from_data(CODEC_RAW, first_data);
        let second_data = b"ghijkl";
        let second_cid = cid_from_data(CODEC_RAW, second_data);
        let third_data = b"mnopqr";
        let third_cid = cid_from_data(CODEC_RAW, third_data);
        let file_data = pb_file_with_metadata(
            b"",
            vec![
                link("first", &first_cid),
                link("second", &second_cid),
                link("third", &third_cid),
            ],
            (first_data.len() + second_data.len() + third_data.len()) as u64,
            vec![
                first_data.len() as u64,
                second_data.len() as u64,
                third_data.len() as u64,
            ],
        );
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        let provider = CountingProvider::new(HashMap::from([
            (first_cid, first_data.to_vec()),
            (second_cid, second_data.to_vec()),
            (third_cid, third_data.to_vec()),
            (file_cid, file_data),
        ]));

        assert_eq!(
            read_file_range(&provider, &file_cid, "", 4, 9).unwrap(),
            b"efghij"
        );

        assert_eq!(
            provider.range_batch_calls(),
            vec![vec![(first_cid, 4, 5), (second_cid, 0, 3)]]
        );
    }

    #[test]
    fn metadata_cache_evicts_when_capacity_is_exceeded() {
        let first_data = pb_file(b"first", Vec::new());
        let first_cid = cid_from_data(CODEC_DAG_PB, &first_data);
        let second_data = pb_file(b"second", Vec::new());
        let second_cid = cid_from_data(CODEC_DAG_PB, &second_data);

        let provider = CountingProvider::new(HashMap::from([
            (first_cid, first_data),
            (second_cid, second_data),
        ]));
        let resolver = UnixfsResolver::with_metadata_cache_capacity(1);

        assert_eq!(resolver.file_size(&provider, &first_cid, "").unwrap(), 5);
        assert_eq!(resolver.file_size(&provider, &second_cid, "").unwrap(), 6);
        assert_eq!(resolver.file_size(&provider, &first_cid, "").unwrap(), 5);

        assert_eq!(provider.call_count(&first_cid), 2);
        assert_eq!(provider.call_count(&second_cid), 1);
        let stats = resolver.metadata_cache_stats();
        assert_eq!(stats.capacity, 1);
        assert_eq!(stats.len, 1);
        assert_eq!(stats.evictions, 2);
        assert_eq!(stats.path_len, 1);
        assert_eq!(stats.path_evictions, 2);
        assert_eq!(stats.file_size_len, 1);
        assert_eq!(stats.file_size_evictions, 2);
    }

    #[test]
    fn reads_file_range_across_inline_and_linked_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        let leaf_data = b"linked bytes";
        let leaf_cid = cid_from_data(CODEC_RAW, leaf_data);
        store.put_block(&leaf_cid, leaf_data).unwrap();

        let file_data = pb_file_with_metadata(
            b"prefix ",
            vec![link("leaf", &leaf_cid)],
            19,
            vec![leaf_data.len() as u64],
        );
        let file_cid = cid_from_data(CODEC_DAG_PB, &file_data);
        store.put_block(&file_cid, &file_data).unwrap();

        assert_eq!(file_size(&store, &file_cid, "").unwrap(), 19);
        assert_eq!(
            read_file_range(&store, &file_cid, "", 3, 12).unwrap(),
            b"fix linked"
        );
    }

    /// A file whose root is `levels` nested file nodes above a raw leaf
    /// holding `x`. No node records a size, so a size or range read walks
    /// every level, as a crafted file's would.
    fn nested_file(levels: usize) -> (CountingProvider, Cid) {
        let mut blocks = HashMap::new();
        let leaf = cid_from_data(CODEC_RAW, b"x");
        blocks.insert(leaf, b"x".to_vec());
        let mut child = leaf;
        for _ in 0..levels {
            let node = PbNode {
                data: Some(
                    UnixfsData {
                        r#type: Some(DataType::File as i32),
                        data: None,
                        filesize: None,
                        blocksizes: Vec::new(),
                        hash_type: None,
                        fanout: None,
                    }
                    .encode_to_vec(),
                ),
                links: vec![link("", &child)],
            }
            .encode_to_vec();
            child = cid_from_data(CODEC_DAG_PB, &node);
            blocks.insert(child, node);
        }
        (CountingProvider::new(blocks), child)
    }

    /// Each way to walk a file, on a thread with tokio's 2 MiB worker
    /// stack: the bytes read, or the error.
    fn walk_nested_file(levels: usize) -> Vec<std::result::Result<Vec<u8>, String>> {
        let (provider, root) = nested_file(levels);
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let resolver = UnixfsResolver::new();
                let size = |result: Result<u64>| result.map(|size| size.to_string().into_bytes());
                [
                    read_file(&provider, &root, ""),
                    size(file_size(&provider, &root, "")),
                    size(resolver.file_size_cid(&provider, &root)),
                    resolver.read_file_cid_range(&provider, &root, 0, 0),
                    read_file_range(&provider, &root, "", 0, 0),
                ]
                .into_iter()
                .map(|result| result.map_err(|err| err.to_string()))
                .collect()
            })
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn file_dags_are_walked_to_the_depth_bound_and_refused_past_it() {
        // The leaf sits MAX_FILE_DAG_DEPTH levels below the root: read.
        for result in walk_nested_file(MAX_FILE_DAG_DEPTH) {
            let bytes = result.unwrap();
            assert!(bytes == b"x" || bytes == b"1", "{bytes:?}");
        }
        // One level more, and far deeper chains, are refused with an error.
        // Unbounded, a chain of a few hundred levels (debug) or about 1,500
        // (release) overflowed this stack and aborted the process.
        for levels in [MAX_FILE_DAG_DEPTH + 1, 10_000] {
            for result in walk_nested_file(levels) {
                assert_eq!(
                    result.unwrap_err(),
                    format!("invalid dag-pb: file DAG deeper than {MAX_FILE_DAG_DEPTH} levels"),
                    "{levels} levels"
                );
            }
        }
    }

    /// The dag-pb spec's strictness (ipld.io/specs/codecs/dag-pb), as
    /// go-codec-dagpb (Kubo's) and `@ipld/dag-pb` decode: each form the
    /// rules allow decodes, and each way to break them is refused.
    #[test]
    fn dag_pb_is_decoded_strictly() {
        let cid = cid_from_data(CODEC_RAW, b"x").to_bytes();
        let v0 = Cid::new_v0(Multihash::<64>::wrap(0x12, &[7; 32]).unwrap())
            .unwrap()
            .to_bytes();
        let bytes_field = |field: u64, value: &[u8]| {
            let mut out = key(field, 2);
            prost::encoding::encode_varint(value.len() as u64, &mut out);
            out.extend_from_slice(value);
            out
        };
        let varint_field = |field: u64, value: u64| {
            let mut out = key(field, 0);
            prost::encoding::encode_varint(value, &mut out);
            out
        };
        let link = |fields: &[Vec<u8>]| bytes_field(2, &fields.concat());
        let hash = bytes_field(1, &cid);
        let name = bytes_field(2, b"a.txt");
        let tsize = varint_field(3, 9);
        let data = bytes_field(1, b"\x08\x01");

        for (allowed, what) in [
            (Vec::new(), "an empty node"),
            (data.clone(), "Data alone"),
            (
                link(std::slice::from_ref(&hash)),
                "a link with only its Hash",
            ),
            (
                link(&[hash.clone(), tsize.clone()]),
                "Hash and Tsize, no Name",
            ),
            (
                link(&[bytes_field(1, &v0), name.clone(), tsize.clone()]),
                "a CIDv0 Hash",
            ),
            (
                [
                    link(&[hash.clone(), name.clone(), tsize.clone()]),
                    link(std::slice::from_ref(&hash)),
                    data.clone(),
                ]
                .concat(),
                "Links, then Data",
            ),
            (
                [
                    data.clone(),
                    link(&[hash.clone(), name.clone(), tsize.clone()]),
                    link(std::slice::from_ref(&hash)),
                ]
                .concat(),
                "Data, then Links",
            ),
            (
                [bytes_field(1, &[]), link(std::slice::from_ref(&hash))].concat(),
                "an empty Data, then a Link",
            ),
            (bytes_field(1, &[]), "an empty Data"),
        ] {
            assert!(decode_pb_node(&allowed).is_ok(), "{what}");
        }

        for (broken, error) in [
            (
                [
                    link(std::slice::from_ref(&hash)),
                    data.clone(),
                    link(std::slice::from_ref(&hash)),
                ]
                .concat(),
                "PBNode: Links on both sides of Data",
            ),
            (
                [data.clone(), data.clone()].concat(),
                "PBNode: Data more than once",
            ),
            (
                [
                    data.clone(),
                    link(std::slice::from_ref(&hash)),
                    data.clone(),
                ]
                .concat(),
                "PBNode: Data more than once",
            ),
            (
                bytes_field(3, b"x"),
                "PBNode: a field other than Links and Data",
            ),
            (
                varint_field(1, 1),
                "PBNode: a field that isn't length-delimited",
            ),
            (
                [key(9, 3), key(9, 4)].concat(),
                "PBNode: a field that isn't length-delimited",
            ),
            // Caught first by the link count's scan of the wire.
            (vec![0x12, 0x05, 0x0a], "malformed protobuf"),
            (vec![0x12], "malformed protobuf"),
            (link(&[]), "PBLink: no Hash"),
            (link(std::slice::from_ref(&name)), "PBLink: no Hash"),
            (
                link(&[name.clone(), hash.clone()]),
                "PBLink: fields out of order (Hash, Name, Tsize)",
            ),
            (
                link(&[tsize.clone(), hash.clone()]),
                "PBLink: fields out of order (Hash, Name, Tsize)",
            ),
            (
                link(&[hash.clone(), tsize.clone(), name.clone()]),
                "PBLink: fields out of order (Hash, Name, Tsize)",
            ),
            (
                link(&[hash.clone(), hash.clone()]),
                "PBLink: a field more than once",
            ),
            (
                link(&[hash.clone(), name.clone(), name.clone()]),
                "PBLink: a field more than once",
            ),
            (
                link(&[hash.clone(), vec![0x18, 0x01, 0x18, 0x02]]),
                "PBLink: a field more than once",
            ),
            (
                link(&[hash.clone(), bytes_field(4, b"x")]),
                "PBLink: a field other than Hash, Name and Tsize",
            ),
            (
                link(&[hash.clone(), bytes_field(3, b"9")]),
                "PBLink: a field of the wrong wire type",
            ),
            (
                link(&[varint_field(1, 1)]),
                "PBLink: a field of the wrong wire type",
            ),
            (
                link(&[bytes_field(1, &[0x00, 0x00])]),
                "PBLink: a Hash that isn't a CID",
            ),
            (
                link(&[bytes_field(1, &[&cid[..], &[0]].concat())]),
                "PBLink: a Hash that isn't a CID",
            ),
            (
                link(&[hash.clone(), bytes_field(2, &[0xff])]),
                "PBLink: a Name that isn't UTF-8",
            ),
            (link(&[vec![0x0a, 0x05, 0x01]]), "PBLink: a truncated Hash"),
        ] {
            assert_eq!(
                decode_pb_node(&broken).unwrap_err().to_string(),
                format!("invalid dag-pb: {error}"),
                "{broken:02x?}"
            );
        }

        // A Hash whose CID has a varint that isn't minimal, or one past 64
        // bits, at each of its four: version, codec, multihash code and
        // digest length. Kubo refuses each ("varint not minimally
        // encoded", "varints larger than uint63 not supported").
        let mut bad_cids = Vec::new();
        for at in 0..4 {
            let padded = [cid[at] | 0x80, 0x00];
            let mut past_64_bits = vec![cid[at] | 0x80];
            past_64_bits.extend([0x80; 8]);
            past_64_bits.push(0x02);
            for varint in [&padded[..], &past_64_bits] {
                bad_cids.push(link(&[bytes_field(
                    1,
                    &[&cid[..at], varint, &cid[at + 1..]].concat(),
                )]));
            }
        }
        // A padded CIDv0 multihash code.
        bad_cids.push(link(&[bytes_field(
            1,
            &[&[0x92, 0x00][..], &v0[1..]].concat(),
        )]));
        for broken in bad_cids {
            assert_eq!(
                decode_pb_node(&broken).unwrap_err().to_string(),
                "invalid dag-pb: PBLink: a Hash that isn't a CID",
                "{broken:02x?}"
            );
        }
    }

    /// A file read through the public API, its root encoded in each order
    /// the spec allows: Data first (prost's encoder, and older IPFS data)
    /// and Links first (Kubo's).
    #[test]
    fn a_file_reads_with_data_before_or_after_its_links() {
        let leaves = [
            cid_from_data(CODEC_RAW, b"x"),
            cid_from_data(CODEC_RAW, b"y"),
        ];
        let data_first = pb_file_with_metadata(
            b"",
            vec![link("", &leaves[0]), link("", &leaves[1])],
            2,
            vec![1, 1],
        );
        let node = decode_pb_node(&data_first).unwrap();
        let mut links_first = Vec::new();
        for link in &node.links {
            prost::encoding::bytes::encode(2, &link.encode_to_vec(), &mut links_first);
        }
        prost::encoding::bytes::encode(1, node.data.as_ref().unwrap(), &mut links_first);
        assert_eq!((data_first[0], links_first[0]), (0x0a, 0x12));

        for root_block in [data_first, links_first] {
            let root = cid_from_data(CODEC_DAG_PB, &root_block);
            let provider = CountingProvider::new(HashMap::from([
                (root, root_block),
                (leaves[0], b"x".to_vec()),
                (leaves[1], b"y".to_vec()),
            ]));
            assert_eq!(read_file(&provider, &root, "").unwrap(), b"xy");
        }
    }

    /// A protobuf key.
    fn key(field: u64, wire_type: u64) -> Vec<u8> {
        let mut out = Vec::new();
        let mut value = field << 3 | wire_type;
        while value >= 0x80 {
            out.push(value as u8 | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
        out
    }

    #[test]
    fn a_block_of_empty_links_is_refused_before_it_is_decoded() {
        // 2 MiB of empty PBLinks (`0x12 0x00`): 1,048,576 links, which
        // prost decoded to 64 MiB of link structs.
        let block = [0x12u8, 0x00].repeat(1024 * 1024);
        assert_eq!(
            decode_pb_node(&block).unwrap_err().to_string(),
            format!("invalid dag-pb: more than {MAX_DAG_PB_LINKS} links")
        );
        // Through the resolver too: the error, not the allocation.
        let cid = cid_from_data(CODEC_DAG_PB, &block);
        let provider = CountingProvider::new(HashMap::from([(cid, block)]));
        assert!(matches!(
            list_directory(&provider, &cid, ""),
            Err(UnixfsError::InvalidDagPb(_))
        ));
        // At the bound a node decodes, one link more is refused: links each
        // naming the smallest CID there is (identity, no bytes), as a
        // strict dag-pb link must name one.
        let smallest = PbLink {
            hash: Some(Cid::new_v1(CODEC_RAW, Multihash::<64>::wrap(0, &[]).unwrap()).to_bytes()),
            name: None,
            tsize: None,
        };
        let node = |count| PbNode {
            data: None,
            links: vec![smallest.clone(); count],
        };
        let at = node(MAX_DAG_PB_LINKS).encode_to_vec();
        assert_eq!(decode_pb_node(&at).unwrap().links.len(), MAX_DAG_PB_LINKS);
        let over = node(MAX_DAG_PB_LINKS + 1).encode_to_vec();
        assert_eq!(
            decode_pb_node(&over).unwrap_err().to_string(),
            format!("invalid dag-pb: more than {MAX_DAG_PB_LINKS} links")
        );
    }

    #[test]
    fn unixfs_block_sizes_are_bounded_packed_or_not() {
        let node = |data: Vec<u8>| PbNode {
            data: Some(data),
            links: Vec::new(),
        };
        let packed = |count: usize| {
            let mut data = key(1, 0);
            data.push(DataType::File as u8);
            data.extend(key(4, 2));
            let mut len = count;
            while len >= 0x80 {
                data.push(len as u8 | 0x80);
                len >>= 7;
            }
            data.push(len as u8);
            data.extend(std::iter::repeat_n(1u8, count));
            data
        };
        let unpacked = |count: usize| {
            let mut data = key(1, 0);
            data.push(DataType::File as u8);
            for _ in 0..count {
                data.extend(key(4, 0));
                data.push(1);
            }
            data
        };
        for data in [packed(MAX_DAG_PB_LINKS), unpacked(MAX_DAG_PB_LINKS)] {
            let decoded = decode_unixfs_data(&node(data)).unwrap();
            assert_eq!(decoded.blocksizes.len(), MAX_DAG_PB_LINKS);
        }
        for data in [
            packed(MAX_DAG_PB_LINKS + 1),
            unpacked(MAX_DAG_PB_LINKS + 1),
            // 2 MiB of one-byte block sizes: 16 MiB decoded.
            packed(2 * 1024 * 1024 - 16),
        ] {
            assert_eq!(
                decode_unixfs_data(&node(data)).unwrap_err().to_string(),
                format!("invalid dag-pb: more than {MAX_DAG_PB_LINKS} block sizes")
            );
        }
    }

    /// The wire count is the number of links prost decodes, whatever else
    /// the message holds, and a message prost refuses is refused.
    #[test]
    fn the_wire_count_matches_what_prost_decodes() {
        let empty_link = [0x12u8, 0x00];
        let full_link = PbNode {
            data: None,
            links: vec![link("a", &cid_from_data(CODEC_RAW, b"a"))],
        }
        .encode_to_vec();
        let mut cases: Vec<Vec<u8>> = vec![
            Vec::new(),
            full_link.clone(),
            [full_link.clone(), empty_link.to_vec(), full_link.clone()].concat(),
        ];
        // Unknown fields of every wire type around the links.
        let mut unknown = key(9, 0);
        unknown.push(0x96);
        unknown.push(0x01);
        unknown.extend(key(10, 1));
        unknown.extend([0u8; 8]);
        unknown.extend(key(11, 5));
        unknown.extend([0u8; 4]);
        unknown.extend(key(12, 2));
        unknown.push(2);
        unknown.extend(empty_link);
        cases.push([unknown.clone(), empty_link.to_vec(), unknown.clone()].concat());
        // Links inside an unknown group, nested twice: prost skips them.
        let grouped = [
            key(13, 3),
            empty_link.to_vec(),
            key(14, 3),
            empty_link.to_vec(),
            key(14, 4),
            key(13, 4),
            empty_link.to_vec(),
        ]
        .concat();
        cases.push(grouped);
        // Only the empty node and real links are dag-pb: an empty link has
        // no Hash, and unknown fields and groups aren't allowed.
        let strict = [true, true, false, false, false];
        assert_eq!(cases.len(), strict.len());
        for (case, strict) in cases.iter().zip(strict) {
            let decoded = PbNode::decode(case.as_slice()).unwrap();
            assert_eq!(
                repeated_count(case, 2, false),
                Some(decoded.links.len()),
                "{case:02x?}"
            );
            assert_eq!(decode_pb_node(case).is_ok(), strict, "{case:02x?}");
        }
        // Nested groups at prost's recursion limit (review of #63): 99 open
        // groups may hold a field, the 100th only its own end, and a 101st
        // can't open.
        let nested = |depth: usize, inner: &[u8]| {
            let mut wire = Vec::new();
            for _ in 0..depth {
                wire.extend(key(9, 3));
            }
            wire.extend(inner);
            for _ in 0..depth {
                wire.extend(key(9, 4));
            }
            wire
        };
        for (depth, inner, decodes) in [
            (99, &empty_link[..], true),
            (100, &empty_link[..], false),
            (101, &empty_link[..], false),
            (100, &[][..], true),
            (101, &[][..], false),
        ] {
            let wire = nested(depth, inner);
            assert_eq!(
                PbNode::decode(wire.as_slice()).is_ok(),
                decodes,
                "{depth} groups, {inner:02x?}"
            );
            assert_eq!(
                repeated_count(&wire, 2, false),
                decodes.then_some(0),
                "{depth} groups, {inner:02x?}"
            );
            // A group is never dag-pb.
            assert!(decode_pb_node(&wire).is_err(), "{depth} groups");
        }
        // Broken messages: prost refuses them, and so does the count.
        for broken in [
            vec![0x12],
            vec![0x12, 0x05, 0x00],
            vec![0x80],
            vec![0x00],
            key(9, 6),
            key(9, 4),
            key(9, 3),
            [key(9, 3), key(8, 4)].concat(),
            [vec![0x08], vec![0xff; 10]].concat(),
        ] {
            assert!(PbNode::decode(broken.as_slice()).is_err(), "{broken:02x?}");
            assert_eq!(repeated_count(&broken, 2, false), None, "{broken:02x?}");
            assert!(decode_pb_node(&broken).is_err(), "{broken:02x?}");
        }
    }

    /// `name`'s bucket at `level` of a HAMT: two uppercase hex digits of
    /// its hash's byte `level`, most significant first.
    fn bucket(name: &str, level: usize) -> String {
        format!(
            "{:02X}",
            murmur3_x64_64(name.as_bytes()).to_be_bytes()[level]
        )
    }

    /// `count` bits of `name`'s hash from bit `from`, most significant
    /// first, read off the hash written out in binary.
    fn hash_bits(name: &str, from: usize, count: usize) -> usize {
        let binary = format!("{:064b}", murmur3_x64_64(name.as_bytes()));
        usize::from_str_radix(&binary[from..from + count], 2).unwrap()
    }

    /// `name`'s link prefix at `level` of a HAMT at `fanout`: its bucket,
    /// the level's log2(fanout) bits of its hash, in as many uppercase hex
    /// digits as fanout - 1 has.
    fn prefix_at(name: &str, fanout: u64, level: usize) -> String {
        let bits = fanout.trailing_zeros() as usize;
        let width = format!("{:X}", fanout - 1).len();
        format!("{:0width$X}", hash_bits(name, level * bits, bits))
    }

    /// A chain of HAMT shards at `fanout` along `name`'s buckets at levels
    /// 0 to `levels - 1`, each linking the next, the last linking `bottom`;
    /// the root's CID (`bottom`'s, if `levels` is 0).
    fn hamt_chain(
        blocks: &mut HashMap<Cid, Vec<u8>>,
        fanout: u64,
        name: &str,
        levels: usize,
        bottom: Vec<u8>,
    ) -> Cid {
        let mut block = bottom;
        for level in (0..levels).rev() {
            let cid = cid_from_data(CODEC_DAG_PB, &block);
            blocks.insert(cid, block);
            block = pb_hamt_at(fanout, vec![link(&prefix_at(name, fanout, level), &cid)]);
        }
        let root = cid_from_data(CODEC_DAG_PB, &block);
        blocks.insert(root, block);
        root
    }

    /// Every fanout boxo reads, the powers of two from 8 to 1024: a name's
    /// bucket is the next log2(fanout) bits of its hash at each level, a
    /// link's prefix has as many hex digits as fanout - 1, and a HAMT is
    /// read as deep as the hash has whole buckets for, and no deeper.
    #[test]
    fn hamts_of_every_fanout_from_8_to_1024_are_read() {
        let file = cid_from_data(CODEC_RAW, b"x");
        let name = "entry.txt";
        for (fanout, width, levels) in [
            (8, 1, 21),
            (16, 1, 16),
            (32, 2, 12),
            (64, 2, 10),
            (128, 2, 9),
            (256, 2, 8),
            (512, 3, 7),
            (1024, 3, 6),
        ] {
            assert_eq!(prefix_at(name, fanout, 0).len(), width, "fanout {fanout}");
            let entry_at = |level| {
                let prefix = prefix_at(name, fanout, level);
                pb_hamt_at(fanout, vec![link(&format!("{prefix}{name}"), &file)])
            };
            let mut blocks = HashMap::from([(file, b"x".to_vec())]);
            let one = hamt_chain(&mut blocks, fanout, name, 0, entry_at(0));
            let two = hamt_chain(&mut blocks, fanout, name, 1, entry_at(1));
            // The entry at the last level with a whole bucket, and a shard
            // one level below it.
            let deepest = hamt_chain(&mut blocks, fanout, name, levels - 1, entry_at(levels - 1));
            let empty = pb_hamt_at(fanout, Vec::new());
            let too_deep = hamt_chain(&mut blocks, fanout, name, levels, empty);
            let provider = CountingProvider::new(blocks);

            for root in [one, two, deepest] {
                assert_eq!(
                    read_file(&provider, &root, name).unwrap(),
                    b"x",
                    "fanout {fanout}"
                );
            }
            assert_eq!(
                read_file(&provider, &too_deep, name)
                    .unwrap_err()
                    .to_string(),
                "invalid dag-pb: HAMT deeper than its 64-bit hash",
                "fanout {fanout}"
            );
            assert!(
                matches!(
                    read_file(&provider, &two, "absent.txt"),
                    Err(UnixfsError::PathNotFound(_))
                ),
                "fanout {fanout}"
            );
            let listed = list_directory(&provider, &two, "").unwrap();
            assert_eq!(
                listed
                    .iter()
                    .map(|entry| entry.name.as_str())
                    .collect::<Vec<_>>(),
                [name],
                "fanout {fanout}"
            );
        }

        // Each shard is read at its own fanout: a root at 8 takes 3 bits,
        // its shard at 1024 the next 10.
        let child = pb_hamt_at(
            1024,
            vec![link(
                &format!("{:03X}{name}", hash_bits(name, 3, 10)),
                &file,
            )],
        );
        let child_cid = cid_from_data(CODEC_DAG_PB, &child);
        let root = pb_hamt_at(8, vec![link(&prefix_at(name, 8, 0), &child_cid)]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root);
        let provider = CountingProvider::new(HashMap::from([
            (root_cid, root),
            (child_cid, child),
            (file, b"x".to_vec()),
        ]));
        assert_eq!(read_file(&provider, &root_cid, name).unwrap(), b"x");

        // Any other fanout, or no fanout, or another hash, is refused.
        let unsupported = |fanout: Option<u64>, hash_type: Option<u64>| {
            let shard = PbNode {
                data: Some(
                    UnixfsData {
                        r#type: Some(DataType::HamtShard as i32),
                        data: None,
                        filesize: None,
                        blocksizes: Vec::new(),
                        hash_type,
                        fanout,
                    }
                    .encode_to_vec(),
                ),
                links: Vec::new(),
            }
            .encode_to_vec();
            let cid = cid_from_data(CODEC_DAG_PB, &shard);
            let provider = CountingProvider::new(HashMap::from([(cid, shard)]));
            let refused = format!(
                "invalid dag-pb: unsupported HAMT parameters hashType={hash_type:?} fanout={fanout:?}"
            );
            assert_eq!(
                read_file(&provider, &cid, name).unwrap_err().to_string(),
                refused
            );
            assert_eq!(
                list_directory(&provider, &cid, "").unwrap_err().to_string(),
                refused
            );
        };
        for fanout in [
            None,
            Some(0),
            Some(1),
            Some(2),
            Some(4),
            Some(12),
            Some(255),
            Some(2048),
        ] {
            unsupported(fanout, Some(HAMT_MURMUR3_X64_64));
        }
        unsupported(Some(256), Some(0x12));
        unsupported(Some(256), None);
    }

    /// The shard's bitfield decides, as boxo's does: an unoccupied bucket
    /// is absent whatever the links say, the bucket's link is the one at
    /// its rank among the occupied buckets, and a bitfield wider than
    /// fanout / 8 bytes, or naming more buckets than there are links, is
    /// refused. Leading zero bytes may be left out.
    #[test]
    fn a_hamt_lookup_follows_the_shards_bitfield() {
        let file = cid_from_data(CODEC_RAW, b"x");
        let other_file = cid_from_data(CODEC_RAW, b"y");
        let name = "entry.txt";
        let bucket = hash_bits(name, 0, 8);
        assert!(bucket > 0, "the fixture needs a bucket above 0");
        let entry = link(&format!("{bucket:02X}{name}"), &file);
        let read = |bitfield: Vec<u8>, links: Vec<PbLink>| {
            let root = hamt_shard(256, bitfield, links);
            let cid = cid_from_data(CODEC_DAG_PB, &root);
            let provider = CountingProvider::new(HashMap::from([
                (cid, root),
                (file, b"x".to_vec()),
                (other_file, b"y".to_vec()),
            ]));
            read_file(&provider, &cid, name).map_err(|err| err.to_string())
        };
        let absent = Err(format!("path segment not found: {name}"));

        // The bucket's bit set: found, the bitfield at its full 32 bytes or
        // with its leading zero bytes left out.
        assert_eq!(
            read(hamt_bitfield(&[bucket], 32), vec![entry.clone()]),
            Ok(b"x".to_vec())
        );
        assert_eq!(
            read(
                hamt_bitfield(&[bucket], bucket / 8 + 1),
                vec![entry.clone()]
            ),
            Ok(b"x".to_vec())
        );
        // The adjacent bucket's bit instead, or no bitfield: absent, though
        // the link names the entry under its own bucket.
        assert_eq!(
            read(hamt_bitfield(&[bucket ^ 1], 32), vec![entry.clone()]),
            absent
        );
        assert_eq!(read(Vec::new(), vec![entry.clone()]), absent);
        // 33 bytes, even with the extra byte zero: refused.
        assert_eq!(
            read(hamt_bitfield(&[bucket], 33), vec![entry.clone()]),
            Err("invalid dag-pb: HAMT bitfield of 33 bytes, more than fanout 256's 32".into())
        );
        // Every bucket set, and one link: the bucket's rank is past it.
        assert_eq!(
            read(vec![0xff; 32], vec![entry.clone()]),
            Err("invalid dag-pb: HAMT bitfield has more buckets than links".into())
        );

        // Two occupied buckets: the link at the bucket's rank is read, so
        // with the links out of bucket order the entry isn't found.
        let other = bucket ^ 0x80;
        let other_link = link(&format!("{other:02X}other.txt"), &other_file);
        let bitfield = hamt_bitfield(&[bucket, other], 32);
        let (in_order, swapped) = if other < bucket {
            (
                vec![other_link.clone(), entry.clone()],
                vec![entry.clone(), other_link],
            )
        } else {
            (
                vec![entry.clone(), other_link.clone()],
                vec![other_link, entry.clone()],
            )
        };
        assert_eq!(read(bitfield.clone(), in_order), Ok(b"x".to_vec()));
        assert_eq!(read(bitfield, swapped), absent);
    }

    /// SMHasher's verification value for MurmurHash3_x64_128: each prefix
    /// of the bytes 0..256 hashed with seed 256 - its length, then the hash
    /// of those hashes, read as four little-endian bytes.
    #[test]
    fn murmur3_matches_smhashers_verification_value() {
        let mut key = Vec::with_capacity(256);
        let mut hashes = Vec::with_capacity(256 * 16);
        for i in 0..256u32 {
            let (h1, h2) = murmur3_x64_128(&key, 256 - i);
            hashes.extend_from_slice(&h1.to_le_bytes());
            hashes.extend_from_slice(&h2.to_le_bytes());
            key.push(i as u8);
        }
        let (h1, _) = murmur3_x64_128(&hashes, 0);
        assert_eq!(h1 as u32, 0x6384_ba69);
        // The UnixFS HAMT's hash is the first 64 bits, with seed 0.
        assert_eq!(murmur3_x64_64(b"hello"), murmur3_x64_128(b"hello", 0).0);
        assert_eq!(murmur3_x64_64(b""), 0);
    }

    /// Finding 6 of design 0009: a lookup scanned every shard until it met
    /// the name, up to 1,024 of them. It now follows the name's hash: one
    /// shard a level, and no other shard is read.
    #[test]
    fn a_hamt_lookup_reads_only_the_shards_on_its_names_path() {
        let name = "target.txt";
        let file = cid_from_data(CODEC_RAW, b"target");
        let mut blocks = HashMap::from([(file, b"target".to_vec())]);
        let mut put = |block: Vec<u8>| {
            let cid = cid_from_data(CODEC_DAG_PB, &block);
            blocks.insert(cid, block);
            cid
        };
        let path = put(pb_hamt(vec![link(
            &format!("{}{name}", bucket(name, 1)),
            &file,
        )]));
        // Every other bucket of the root holds a shard of its own.
        let mut links = vec![link(&bucket(name, 0), &path)];
        let mut others = Vec::new();
        for index in 0..=255u8 {
            let prefix = format!("{index:02X}");
            if prefix != bucket(name, 0) {
                let other = put(pb_hamt(vec![link(
                    &format!("{prefix}other-{index}"),
                    &file,
                )]));
                others.push(other);
                links.push(link(&prefix, &other));
            }
        }
        let root = put(pb_hamt(links));
        let provider = CountingProvider::new(blocks);

        assert_eq!(read_file(&provider, &root, name).unwrap(), b"target");
        assert_eq!(provider.call_count(&path), 1);
        assert!(others.iter().all(|other| provider.call_count(other) == 0));
        // A name whose bucket holds another entry is absent, read at once.
        assert!(matches!(
            read_file(&provider, &root, "missing.txt"),
            Err(UnixfsError::PathNotFound(_))
        ));
        let read: usize = others.iter().map(|other| provider.call_count(other)).sum();
        assert!(read <= 1, "{read} shards read for one missing name");
    }

    /// An entry in a bucket its name doesn't hash to isn't found (the old
    /// scan found it anywhere), and a HAMT deeper than its 64-bit hash is
    /// refused.
    #[test]
    fn a_hamt_entry_is_found_only_in_its_bucket_and_depth_is_bounded() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let file = cid_from_data(CODEC_RAW, b"x");
        store.put_block(&file, b"x").unwrap();
        let name = "misplaced.txt";
        let wrong = format!(
            "{:02X}",
            murmur3_x64_64(name.as_bytes()).to_be_bytes()[0] ^ 1
        );
        let root = pb_hamt(vec![link(&format!("{wrong}{name}"), &file)]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root);
        store.put_block(&root_cid, &root).unwrap();
        assert!(matches!(
            read_file(&store, &root_cid, name),
            Err(UnixfsError::PathNotFound(_))
        ));

        // Shards along every one of the name's eight buckets, and a ninth.
        let name = "deep.txt";
        let mut child = pb_hamt(Vec::new());
        for level in (0..8).rev() {
            let child_cid = cid_from_data(CODEC_DAG_PB, &child);
            store.put_block(&child_cid, &child).unwrap();
            child = pb_hamt(vec![link(&bucket(name, level), &child_cid)]);
        }
        let deep = cid_from_data(CODEC_DAG_PB, &child);
        store.put_block(&deep, &child).unwrap();
        assert_eq!(
            read_file(&store, &deep, name).unwrap_err().to_string(),
            "invalid dag-pb: HAMT deeper than its 64-bit hash"
        );
    }

    #[test]
    fn resolves_single_level_hamt_shard() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hamt index";
        let file_cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&file_cid, data).unwrap();

        let hamt_data = pb_hamt(vec![link(
            &format!("{}index.html", bucket("index.html", 0)),
            &file_cid,
        )]);
        let hamt_cid = cid_from_data(CODEC_DAG_PB, &hamt_data);
        store.put_block(&hamt_cid, &hamt_data).unwrap();

        assert_eq!(read_file(&store, &hamt_cid, "index.html").unwrap(), data);
    }

    #[test]
    fn resolves_nested_hamt_shard() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"nested hamt";
        let file_cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&file_cid, data).unwrap();

        let child_data = pb_hamt(vec![link(
            &format!("{}nested.txt", bucket("nested.txt", 1)),
            &file_cid,
        )]);
        let child_cid = cid_from_data(CODEC_DAG_PB, &child_data);
        store.put_block(&child_cid, &child_data).unwrap();

        let root_data = pb_hamt(vec![link(&bucket("nested.txt", 0), &child_cid)]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root_data);
        store.put_block(&root_cid, &root_data).unwrap();

        assert_eq!(read_file(&store, &root_cid, "nested.txt").unwrap(), data);
    }

    #[test]
    fn lists_nested_hamt_shard_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let alpha_cid = cid_from_data(CODEC_RAW, b"alpha");
        let nested_cid = cid_from_data(CODEC_RAW, b"nested");

        let child_data = pb_hamt(vec![link("CDnested.txt", &nested_cid)]);
        let child_cid = cid_from_data(CODEC_DAG_PB, &child_data);
        store.put_block(&child_cid, &child_data).unwrap();

        let root_data = pb_hamt(vec![
            link("AB", &child_cid),
            link("EFalpha.txt", &alpha_cid),
        ]);
        let root_cid = cid_from_data(CODEC_DAG_PB, &root_data);
        store.put_block(&root_cid, &root_data).unwrap();

        let entries = list_directory(&store, &root_cid, "").unwrap();

        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.name.as_str(), entry.cid))
                .collect::<Vec<_>>(),
            vec![("alpha.txt", alpha_cid), ("nested.txt", nested_cid)]
        );
    }

    /// Counts each thread's live heap bytes, and their peak, so a test can
    /// see what a cache holds on the heap; and can watch one allocation
    /// being freed, to see what was still allocated, and reserved, then.
    mod heap {
        use freedom_ipfs_core::MemoryBudget;
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        thread_local! {
            static LIVE: Cell<isize> = const { Cell::new(0) };
            static PEAK: Cell<isize> = const { Cell::new(0) };
            /// An address inside the allocation watched on this thread.
            static WATCHED: Cell<usize> = const { Cell::new(0) };
            /// The budget to read when it is freed.
            static WATCHED_BUDGET: Cell<*const MemoryBudget> =
                const { Cell::new(std::ptr::null()) };
            /// The live bytes, and the budget's use, just before it was freed.
            static AT_FREE: Cell<Option<(isize, usize)>> = const { Cell::new(None) };
        }

        struct Counting;

        fn add(bytes: isize) {
            let _ = LIVE.try_with(|live| {
                let now = live.get() + bytes;
                live.set(now);
                let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
            });
        }

        /// Before `ptr`'s allocation of `size` bytes is freed: whether it is
        /// the one watched.
        fn freeing(ptr: *mut u8, size: usize) {
            let _ = WATCHED.try_with(|watched| {
                let address = watched.get();
                let start = ptr as usize;
                if address == 0 || address < start || address >= start + size.max(1) {
                    return;
                }
                watched.set(0);
                let budget = WATCHED_BUDGET.with(Cell::get);
                // SAFETY: `watch_free`'s caller keeps the budget alive until
                // it reads the result, which ends the watch.
                let used = (!budget.is_null()).then(|| unsafe { &*budget }.used());
                AT_FREE.with(|at| at.set(Some((LIVE.with(Cell::get), used.unwrap_or(0)))));
            });
        }

        // SAFETY: every call is passed on to the system allocator as is.
        unsafe impl GlobalAlloc for Counting {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                let ptr = unsafe { System.alloc(layout) };
                if !ptr.is_null() {
                    add(layout.size() as isize);
                }
                ptr
            }

            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                let ptr = unsafe { System.alloc_zeroed(layout) };
                if !ptr.is_null() {
                    add(layout.size() as isize);
                }
                ptr
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                freeing(ptr, layout.size());
                unsafe { System.dealloc(ptr, layout) };
                add(-(layout.size() as isize));
            }

            unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                let new = unsafe { System.realloc(ptr, layout, new_size) };
                if !new.is_null() {
                    add(new_size as isize - layout.size() as isize);
                }
                new
            }
        }

        #[global_allocator]
        static COUNTING: Counting = Counting;

        /// This thread's live heap bytes; the peak starts again from them.
        pub(super) fn live() -> isize {
            let live = LIVE.with(Cell::get);
            PEAK.with(|peak| peak.set(live));
            live
        }

        /// The most this thread held since [`live`] was last read.
        #[allow(dead_code)]
        pub(super) fn peak() -> isize {
            PEAK.with(Cell::get)
        }

        /// Watches, on this thread, the allocation that holds `address`:
        /// when it is freed, what was live and what `budget` had reserved.
        #[allow(dead_code)]
        pub(super) fn watch_free(address: *const u8, budget: &MemoryBudget) {
            AT_FREE.with(|at| at.set(None));
            WATCHED_BUDGET.with(|watched| watched.set(budget));
            WATCHED.with(|watched| watched.set(address as usize));
        }

        /// The live bytes, and the budget's use, just before the watched
        /// allocation was freed: `None` if it wasn't. Ends the watch.
        #[allow(dead_code)]
        pub(super) fn watched_free() -> Option<(isize, usize)> {
            WATCHED.with(|watched| watched.set(0));
            WATCHED_BUDGET.with(|watched| watched.set(std::ptr::null()));
            AT_FREE.with(Cell::take)
        }
    }

    /// A decoded directory node of `links` links, as a block decodes to.
    fn decoded_directory(links: usize, seed: usize) -> (Cid, usize, DecodedDagPb) {
        let encoded = pb_directory(
            (0..links)
                .map(|index| {
                    let target = cid_from_data(CODEC_RAW, format!("{seed}/{index}").as_bytes());
                    link(&format!("entry-{seed}-{index}.html"), &target)
                })
                .collect(),
        );
        let cid = cid_from_data(CODEC_DAG_PB, &encoded);
        let mut node = decode_pb_node(&encoded).unwrap();
        let data = decode_unixfs_data(&node).unwrap();
        node.data = None;
        let kind = data_type(&data).unwrap();
        (cid, encoded.len(), DecodedDagPb { node, data, kind })
    }

    /// One insertion's worth of input, made before the measured region.
    enum CacheInput {
        Node(Cid, usize, DecodedDagPb),
        Path(Cid, String, ResolvedNode),
        FileSize(Cid, u64),
    }

    fn cache_inputs(count: usize) -> Vec<CacheInput> {
        let root = cid_from_data(CODEC_DAG_PB, b"root");
        (0..count)
            .map(|index| match index % 3 {
                0 => {
                    let (cid, len, decoded) = decoded_directory(1 + index % 23, index);
                    CacheInput::Node(cid, len, decoded)
                }
                1 => CacheInput::Path(
                    root,
                    // Short paths and long ones, near the longest cached.
                    format!("assets/{index}/{}.js", "x".repeat(index % 7 * 140)),
                    ResolvedNode {
                        cid: cid_from_data(CODEC_RAW, &index.to_le_bytes()),
                        kind: NodeKind::Raw,
                    },
                ),
                _ => CacheInput::FileSize(
                    cid_from_data(CODEC_RAW, &index.to_le_bytes()),
                    index as u64,
                ),
            })
            .collect()
    }

    fn insert_input(cache: &UnixfsMetadataCache, input: &CacheInput) {
        match input {
            CacheInput::Node(cid, len, decoded) => cache.insert(cid, *len, decoded),
            CacheInput::Path(root, path, resolved) => {
                cache.insert_path(root, path, resolved.clone())
            }
            CacheInput::FileSize(cid, size) => cache.insert_file_size(cid, *size),
        }
    }

    #[test]
    fn the_metadata_cache_counts_at_least_the_heap_it_holds_and_copies_only_into_room() {
        const MAX_BYTES: usize = 48 * 1024;
        let inputs = cache_inputs(600);
        let origin = heap::live();
        let cache = Arc::new(UnixfsMetadataCache::new(10_000, MAX_BYTES));
        let mut evicted = false;
        for input in &inputs {
            let before = heap::live();
            insert_input(&cache, input);
            let peak = heap::peak() - origin;
            let held = heap::live() - origin;
            let stats = cache.stats();
            evicted |= stats.evictions + stats.path_evictions + stats.file_size_evictions > 0;
            // What the cache holds on the heap is never more than it counts,
            // which is never more than its capacity...
            assert!(
                held as usize <= stats.bytes,
                "holds {held}, counts {stats:?}"
            );
            assert!(stats.bytes <= MAX_BYTES, "{stats:?}");
            // ...and a copy is made only into room: at no point did the
            // cache hold more than its capacity.
            assert!(
                peak as usize <= MAX_BYTES,
                "peaked at {peak} (from {}), {stats:?}",
                before - origin
            );
        }
        assert!(evicted, "the capacity was reached");
        drop(cache);
        assert_eq!(heap::live(), origin, "all of it was the cache's");
    }

    /// A decoded file node holding `len` bytes inline, as a block decodes to.
    fn decoded_file(len: usize, seed: usize) -> (Cid, usize, DecodedDagPb) {
        let inline: Vec<u8> = (0..len).map(|index| (index + seed) as u8).collect();
        let encoded = pb_file(&inline, Vec::new());
        let cid = cid_from_data(CODEC_DAG_PB, &encoded);
        let mut node = decode_pb_node(&encoded).unwrap();
        let data = decode_unixfs_data(&node).unwrap();
        node.data = None;
        let kind = data_type(&data).unwrap();
        (cid, encoded.len(), DecodedDagPb { node, data, kind })
    }

    #[test]
    fn a_metadata_copy_is_made_only_into_room() {
        const MAX_BYTES: usize = 64 * 1024;
        // Entries whose copies are larger than what the cache's counting
        // leaves spare: a copy made before its room would show.
        let inputs: Vec<_> = (0..12).map(|seed| decoded_file(16 * 1024, seed)).collect();
        let origin = heap::live();
        let cache = UnixfsMetadataCache::new(10_000, MAX_BYTES);
        for (cid, len, decoded) in &inputs {
            heap::live();
            cache.insert(cid, *len, decoded);
            let peak = heap::peak() - origin;
            let stats = cache.stats();
            assert!(peak as usize <= MAX_BYTES, "peaked at {peak}: {stats:?}");
            assert!(cache.get(cid).is_some(), "the newest is cached");
        }
        assert!(cache.stats().evictions > 0);
    }

    #[test]
    fn the_metadata_cache_stays_within_its_byte_capacity_across_its_maps() {
        const MAX_BYTES: usize = 64 * 1024;
        let cache = UnixfsMetadataCache::new(10_000, MAX_BYTES);
        let root = cid_from_data(CODEC_DAG_PB, b"root");
        // Paths and sizes first: far below their entry cap.
        for index in 0..40u64 {
            let cid = cid_from_data(CODEC_RAW, &index.to_le_bytes());
            cache.insert_path(
                &root,
                &format!("dir/{index}.txt"),
                ResolvedNode {
                    cid,
                    kind: NodeKind::Raw,
                },
            );
            cache.insert_file_size(&cid, index);
        }
        let start = cache.stats();
        assert_eq!((start.path_len, start.file_size_len), (40, 40));
        assert!(start.bytes <= MAX_BYTES);
        // Then nodes, until all of it together passes the capacity: the
        // oldest entries go, whatever their kind.
        let mut newest = Vec::new();
        for seed in 0..60 {
            let (cid, len, decoded) = decoded_directory(12, seed);
            cache.insert(&cid, len, &decoded);
            newest.push(cid);
            let stats = cache.stats();
            assert!(stats.bytes <= MAX_BYTES, "{stats:?}");
            assert_eq!(stats.bytes, cache.pool.used());
        }
        let stats = cache.stats();
        assert!(stats.len < 60, "nodes were evicted too: {stats:?}");
        assert_eq!(
            (stats.path_len, stats.file_size_len),
            (0, 0),
            "the older paths and sizes made way: {stats:?}"
        );
        assert_eq!(stats.path_evictions + stats.file_size_evictions, 80);
        assert!(cache.get(newest.last().unwrap()).is_some());
        assert!(cache.get(&newest[0]).is_none());

        // An entry past the whole capacity evicts nothing for itself.
        let (cid, len, big) = decoded_directory(600, 1_000);
        assert!(big.heap_bytes() > MAX_BYTES);
        let before = cache.stats();
        cache.insert(&cid, len.min(UNIXFS_METADATA_CACHE_MAX_BLOCK_BYTES), &big);
        let after = cache.stats();
        assert_eq!(after.oversized_skips, before.oversized_skips + 1);
        assert_eq!((after.len, after.evictions), (before.len, before.evictions));
        assert!(cache.get(&cid).is_none());

        // Cleared, it holds only its own structures.
        cache.clear();
        let cleared = cache.stats();
        assert_eq!(cleared.bytes, cache.pool.used());
        assert!(cleared.bytes < 16 * 1024, "{cleared:?}");
    }

    #[test]
    fn metadata_caches_at_their_capacity_edges_count_and_release_every_heap_byte() {
        let inputs = cache_inputs(500);
        for max_bytes in [1, 4096, 8192, 16_000, 65_536] {
            let origin = heap::live();
            let cache = Arc::new(UnixfsMetadataCache::new(20, max_bytes));
            let capacity = cache.max_bytes();
            assert!(capacity >= max_bytes);
            for input in &inputs {
                heap::live();
                insert_input(&cache, input);
                let peak = heap::peak() - origin;
                let live = heap::live() - origin;
                let counted = cache.stats().bytes;
                assert!(
                    live as usize <= counted,
                    "live {live}, counted {counted} of {max_bytes}"
                );
                assert!(counted <= capacity);
                assert!(peak as usize <= capacity, "peaked at {peak} of {capacity}");
            }
            cache.clear();
            let live = heap::live() - origin;
            assert!(live as usize <= cache.stats().bytes);
            drop(cache);
            assert_eq!(heap::live(), origin);
        }
    }

    #[test]
    fn a_metadata_cache_lease_stays_until_its_cached_heap_is_freed() {
        let encoded = pb_file(&vec![73u8; 32 * 1024], Vec::new());
        let cid = cid_from_data(CODEC_DAG_PB, &encoded);
        let provider = CountingProvider::new(HashMap::from([(cid, encoded)]));
        // The provider's own bookkeeping, before anything is counted.
        UnixfsResolver::without_metadata_cache()
            .file_size(&provider, &cid, "")
            .unwrap();
        let fixed = UnixfsResolver::metadata_cache_fixed_bytes(256, 128 * 1024);
        let budget = freedom_ipfs_core::MemoryBudget::new(fixed);
        for watch_the_lease in [false, true] {
            let origin = heap::live();
            let resolver = UnixfsResolver::with_metadata_cache_limits(256, 128 * 1024);
            let lease = Arc::new(budget.try_reserve(fixed).unwrap());
            assert!(resolver.keep_metadata_cache_lease(lease.clone()));
            let lease_address = Arc::as_ptr(&lease) as *const u8;
            // The cache is the lease's last owner.
            drop(lease);
            assert_eq!(resolver.file_size(&provider, &cid, "").unwrap(), 32 * 1024);
            let payload = {
                let inner = resolver.metadata_cache.lock_inner();
                let cached = &inner.nodes.entries.get(&cid).unwrap().value;
                cached.data.data.as_ref().unwrap().as_ptr()
            };
            heap::watch_free(
                if watch_the_lease {
                    lease_address
                } else {
                    payload
                },
                &budget,
            );
            drop(resolver);
            let (live, reserved) = heap::watched_free().expect("it was freed");
            if watch_the_lease {
                // When the reservation goes, all that is left of the cache
                // is its own structure and pool counter, freed in the same
                // drop, and the lease itself.
                let left = (live - origin) as usize;
                let lease_bytes =
                    std::mem::size_of::<Reservation>() + 2 * std::mem::size_of::<usize>();
                assert!(
                    left <= UNIXFS_METADATA_CACHE_OWN_BYTES + lease_bytes,
                    "{left} bytes still allocated when the reservation went"
                );
            } else {
                assert_eq!(
                    reserved, fixed,
                    "the 32 KiB payload is freed while still reserved"
                );
            }
            assert_eq!(budget.used(), 0);
            assert_eq!(heap::live(), origin);
        }
    }

    #[test]
    fn disabled_and_tiny_metadata_caches_count_their_own_heap() {
        for (entries, max_bytes) in [(0, 0), (0, 1024 * 1024), (256, 1), (256, 64)] {
            let origin = heap::live();
            let cache = Arc::new(UnixfsMetadataCache::new(entries, max_bytes));
            let live = heap::live() - origin;
            let stats = cache.stats();
            // It caches nothing, but counts, and reports as its fixed
            // capacity, the heap it keeps.
            assert!(
                live as usize <= stats.bytes,
                "{entries} entries in {max_bytes}: heap {live}, counted {}",
                stats.bytes
            );
            assert!(stats.bytes <= cache.max_bytes());
            assert_eq!(
                cache.max_bytes(),
                UnixfsResolver::metadata_cache_fixed_bytes(entries, max_bytes)
            );
            insert_input(&cache, &cache_inputs(1).remove(0));
            assert_eq!(cache.stats().len, 0, "caches nothing");
            drop(cache);
            assert_eq!(heap::live(), origin);
        }
    }

    #[test]
    fn a_resolver_reports_its_metadata_cache_capacity() {
        assert_eq!(
            UnixfsResolver::new().metadata_cache_max_bytes(),
            DEFAULT_UNIXFS_METADATA_CACHE_MAX_BYTES
        );
        assert_eq!(
            UnixfsResolver::with_metadata_cache_limits(8, 64 * 1024).metadata_cache_max_bytes(),
            64 * 1024
        );
        // A cache that caches nothing still takes its own structures.
        let own = UNIXFS_METADATA_CACHE_OWN_BYTES;
        assert_eq!(
            UnixfsResolver::without_metadata_cache().metadata_cache_max_bytes(),
            own
        );
        assert_eq!(
            UnixfsResolver::with_metadata_cache_capacity(0).metadata_cache_max_bytes(),
            own
        );
        assert_eq!(
            UnixfsResolver::with_metadata_cache_limits(8, 16).metadata_cache_max_bytes(),
            own
        );
        for (capacity, max_bytes) in [(0, 0), (0, 1 << 20), (8, 16), (8, 64 * 1024)] {
            assert_eq!(
                UnixfsResolver::metadata_cache_fixed_bytes(capacity, max_bytes),
                UnixfsResolver::with_metadata_cache_limits(capacity, max_bytes)
                    .metadata_cache_max_bytes()
            );
        }
    }

    #[test]
    #[ignore = "prints the footprints the default capacity is chosen by"]
    fn print_metadata_cache_footprints() {
        let cache = UnixfsMetadataCache::new(10_000, 64 * 1024 * 1024);
        let root = cid_from_data(CODEC_DAG_PB, b"root");
        let empty = cache.stats().bytes;
        for index in 0..256u64 {
            let cid = cid_from_data(CODEC_RAW, &index.to_le_bytes());
            cache.insert_path(
                &root,
                &format!("assets/img/photo-{index}.jpg"),
                ResolvedNode {
                    cid,
                    kind: NodeKind::Raw,
                },
            );
        }
        let paths = cache.stats().bytes - empty;
        for index in 0..256u64 {
            cache.insert_file_size(&cid_from_data(CODEC_RAW, &index.to_le_bytes()), index);
        }
        let sizes = cache.stats().bytes - empty - paths;
        let mut nodes = Vec::new();
        for links in [1, 5, 10, 20, 50, 100] {
            let before = cache.stats().bytes;
            let (cid, len, decoded) = decoded_directory(links, 10_000 + links);
            cache.insert(&cid, len, &decoded);
            nodes.push((links, len, cache.stats().bytes - before));
        }
        println!("empty {empty}, 256 paths {paths}, 256 sizes {sizes}");
        for (links, encoded, bytes) in nodes {
            println!("directory of {links} links: {encoded} bytes encoded, {bytes} cached");
        }
    }
}
