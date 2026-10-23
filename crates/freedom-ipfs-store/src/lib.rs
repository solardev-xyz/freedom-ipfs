use cid::Cid;
use freedom_ipfs_core::{
    block_data_range, btree_map_bytes, encode_car_v1, parse_car_v1, verify_block, Block,
    BlockProvider, CarBlock, ChargedBytes, CoreError, FixedPool, HeldBytes, PoolShare, Reservation,
    Result as CoreResult,
};
use parking_lot::Mutex;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_HOT_CACHE_BYTES: u64 = 16 * 1024 * 1024;
const HOT_CACHE_TOUCH_INTERVAL: Duration = Duration::from_secs(30);
const MAX_NAME_CACHE_RECORDS: i64 = 128;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("core: {0}")]
    Core(#[from] CoreError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// SQLite can't hold the store to the page cap asked for: the database
    /// already has more pages, or the cap is past SQLite's own limit.
    #[error("page cap {requested} can't be installed (SQLite keeps {installed})")]
    PageCap { requested: u32, installed: i64 },
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// How a store is sized. The default is what [`SqliteBlockStore::open`] and
/// [`SqliteBlockStore::in_memory`] have always done: 256 MiB of blocks, a hot
/// cache of `min(max_bytes, 16 MiB)`, and SQLite's own page limits.
///
/// A host that must bound the store's memory (an in-memory store holds every
/// block twice: in SQLite's pages and, while hot, in the hot cache) sets the
/// hot cache apart from `max_bytes`, and caps SQLite's pages. A write that
/// meets the page cap leaves its entry uncached and succeeds: the block, name
/// or provider record still serves the request that fetched it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreOptions {
    /// Block bytes kept before the least recently used are evicted; 0 means
    /// 256 MiB.
    pub max_bytes: u64,
    /// The verified hot cache's capacity in bytes; `None` keeps
    /// `min(max_bytes, 16 MiB)`, and `Some(0)` turns it off.
    pub hot_cache_bytes: Option<u64>,
    /// SQLite's page cap (`PRAGMA max_page_count`). A write past it fails
    /// with `SQLITE_FULL` inside the store, which leaves the entry uncached.
    pub max_page_count: Option<u32>,
    /// SQLite's page cache in KiB (`PRAGMA cache_size = -N`).
    pub cache_kib: Option<u32>,
    /// Where SQLite keeps temporary tables, indices and sorts that outgrow
    /// the page cache: [`TempStore::Default`], today's, or in memory.
    pub temp_store: TempStore,
}

/// Where SQLite keeps a store's temporary data (`PRAGMA temp_store`): the
/// temporary tables and indices its statements build, and the sorts that
/// outgrow the page cache.
///
/// **What the store's statements can put there:**
/// - The eviction scan (`ORDER BY last_accessed_at, inserted_at`) reads the
///   `last_accessed_at` index and sorts only the right part, one group of
///   blocks with the same second at a time: a CID and a timestamp per row,
///   for the blocks touched within one second.
/// - The name cache's pruning sorts the same way, over at most a few hundred
///   names.
/// - [`SqliteBlockStore::export_car`] sorts every block with its data
///   (`ORDER BY inserted_at`, with no index). In memory, that is a second
///   copy of the store while it runs. It collects every block anyway.
/// - Everything else reads or writes single rows, counts, sums, or deletes by
///   key or by expiry, and needs no temporary data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TempStore {
    /// SQLite's default. A file store writes temporary data that outgrows the
    /// page cache to a temporary file, in the system's temporary directory.
    /// An in-memory store keeps it in memory regardless.
    #[default]
    Default,
    /// In memory (`PRAGMA temp_store = MEMORY`): no temporary file at all.
    /// This is for a host confined to writing only the store's own directory
    /// (freedom-core's design 0010). The memory is the temporary data's size
    /// (above).
    Memory,
}

impl StoreOptions {
    /// The block bytes kept: `max_bytes`, or 256 MiB for 0.
    fn effective_max_bytes(&self) -> u64 {
        if self.max_bytes == 0 {
            DEFAULT_CACHE_BYTES
        } else {
            self.max_bytes
        }
    }

    /// The hot cache's capacity asked for: `hot_cache_bytes`, or
    /// `min(max_bytes, 16 MiB)`.
    fn hot_bytes(&self) -> u64 {
        self.hot_cache_bytes
            .unwrap_or_else(|| hot_cache_bytes(self.effective_max_bytes()))
    }

    /// The hot cache's fixed capacity a store opened with these options has:
    /// what [`SqliteBlockStore::hot_cache_capacity`] reports once it is open,
    /// so a host can reserve it before opening the store. It is at least the
    /// cache's own structures, which even a cache that caches nothing (or is
    /// turned off) keeps.
    pub fn hot_cache_fixed_bytes(&self) -> u64 {
        hot_cache_fixed_bytes(self.hot_bytes()) as u64
    }
}

#[derive(Clone)]
pub struct SqliteBlockStore {
    conn: Arc<Mutex<Connection>>,
    max_bytes: u64,
    hot: Arc<Mutex<VerifiedHotCache>>,
    retained: Arc<Mutex<HashMap<Vec<u8>, usize>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedProviderRecord {
    pub id: Option<String>,
    pub addrs: Vec<String>,
}

impl SqliteBlockStore {
    pub fn open(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self> {
        Self::open_with(
            path,
            StoreOptions {
                max_bytes,
                ..StoreOptions::default()
            },
        )
    }

    pub fn in_memory(max_bytes: u64) -> Result<Self> {
        Self::in_memory_with(StoreOptions {
            max_bytes,
            ..StoreOptions::default()
        })
    }

    /// A store in the file at `path`, sized by `options`.
    pub fn open_with(path: impl AsRef<Path>, options: StoreOptions) -> Result<Self> {
        Self::with_connection(Connection::open(path)?, options)
    }

    /// A store in memory, sized by `options`. Its temporary tables and
    /// indices are kept in memory too (`PRAGMA temp_store = MEMORY`), so it
    /// never writes a temporary file.
    pub fn in_memory_with(options: StoreOptions) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA temp_store = MEMORY;")?;
        Self::with_connection(conn, options)
    }

    fn with_connection(conn: Connection, options: StoreOptions) -> Result<Self> {
        let max_bytes = options.effective_max_bytes();
        let hot_bytes = options.hot_bytes();
        if options.temp_store == TempStore::Memory {
            conn.execute_batch("PRAGMA temp_store = MEMORY;")?;
        }
        if let Some(pages) = options.max_page_count {
            // SQLite answers the cap it installed, which isn't the one asked
            // for when the database already has more pages or the value is
            // past its limit. A cap the host reserved memory by must hold.
            let installed =
                conn.query_row(&format!("PRAGMA max_page_count = {pages}"), [], |row| {
                    row.get::<_, i64>(0)
                })?;
            if installed != i64::from(pages) {
                return Err(StoreError::PageCap {
                    requested: pages,
                    installed,
                });
            }
        }
        if let Some(kib) = options.cache_kib {
            conn.execute_batch(&format!("PRAGMA cache_size = -{kib};"))?;
        }
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
            max_bytes,
            hot: Arc::new(Mutex::new(VerifiedHotCache::new(hot_bytes))),
            retained: Arc::new(Mutex::new(HashMap::new())),
        };
        store.init()?;
        Ok(store)
    }

    /// The bytes SQLite's pages take: `page_count × page_size`. For an
    /// in-memory store, that is the memory the database holds.
    pub fn page_bytes(&self) -> Result<u64> {
        let conn = self.conn.lock();
        let count = conn.query_row("PRAGMA page_count", [], |row| row.get::<_, i64>(0))?;
        let size = conn.query_row("PRAGMA page_size", [], |row| row.get::<_, i64>(0))?;
        Ok((count.max(0) as u64).saturating_mul(size.max(0) as u64))
    }

    fn init(&self) -> Result<()> {
        self.conn.lock().execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS blocks (
                cid BLOB PRIMARY KEY NOT NULL,
                codec INTEGER NOT NULL,
                size INTEGER NOT NULL,
                data BLOB NOT NULL,
                inserted_at INTEGER NOT NULL,
                last_accessed_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS blocks_last_accessed
                ON blocks(last_accessed_at);
            CREATE TABLE IF NOT EXISTS bad_providers (
                peer_or_url TEXT PRIMARY KEY NOT NULL,
                reason TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS provider_cache (
                cid BLOB PRIMARY KEY NOT NULL,
                providers_json TEXT NOT NULL,
                expires_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS name_cache (
                name TEXT PRIMARY KEY NOT NULL,
                resolved_target TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS name_cache_updated_at
                ON name_cache(updated_at);
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            "#,
        )?;
        Ok(())
    }

    /// Closes the database connection now, even while clones of this store
    /// are still alive (for example held by blocking work that outlived its
    /// runtime). The connection is swapped for an empty in-memory database, so
    /// later calls through any clone fail with an SQLite error instead of
    /// touching the file, and the file can be reopened right away. Waits at
    /// most `timeout` for a statement already in progress; returns `false`,
    /// leaving the connection open, if that does not finish in time.
    pub fn close(&self, timeout: Duration) -> bool {
        match self.conn.try_lock_for(timeout) {
            Some(conn) => self.close_locked(conn),
            None => false,
        }
    }

    /// Like [`close`](Self::close), but when the statement in progress is
    /// still running after `timeout`, hands the store to a background thread
    /// that closes it as soon as that statement releases the connection,
    /// instead of leaving it open until the last clone is dropped. Returns
    /// whether the store was closed within `timeout`.
    ///
    /// The background thread has no deadline: it blocks on the connection
    /// lock for exactly as long as the running statement holds it. A statement
    /// that never returns therefore parks one `freedom-ipfs-store-close`
    /// thread (and keeps the database open) for the rest of the process, one
    /// per call that timed out. That is the same lifetime the connection
    /// would have had without this call, plus one idle thread.
    pub fn close_or_close_later(self, timeout: Duration) -> bool {
        if self.close(timeout) {
            return true;
        }
        // If the thread cannot be spawned the store is dropped here and the
        // connection closes with its last clone, as before.
        let _ = std::thread::Builder::new()
            .name("freedom-ipfs-store-close".into())
            .spawn(move || {
                let conn = self.conn.lock();
                self.close_locked(conn);
            });
        false
    }

    fn close_locked(&self, mut conn: parking_lot::MutexGuard<'_, Connection>) -> bool {
        let Ok(closed) = Connection::open_in_memory() else {
            return false;
        };
        drop(std::mem::replace(&mut *conn, closed));
        // No hot cache at all from now on: one a store handle kept after
        // the close would hold heap no reservation stands for. Blocks the
        // old one covers stay covered, by its pool, until they are freed.
        *self.hot.lock() = VerifiedHotCache::closed();
        true
    }

    /// Stores a block. The hot cache, if it keeps the block, keeps a copy
    /// of its own, made only once it has room for it.
    pub fn put_block(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        self.put_verified_block(cid, data, HotInput::Copy(data))
    }

    /// Stores a block from bytes that may carry memory charges. The hot
    /// cache, if it keeps the block, shares those bytes rather than copying
    /// them: it counts them in its own capacity, and takes over their charge
    /// once it holds the last reference (until then they count twice).
    pub fn put_charged(&self, cid: &Cid, data: &ChargedBytes) -> Result<()> {
        self.put_verified_block(cid, data, HotInput::Shared(data.clone()))
    }

    fn put_verified_block(&self, cid: &Cid, data: &[u8], hot_data: HotInput<'_>) -> Result<()> {
        verify_block(cid, data)?;
        let cid_bytes = block_key(cid);
        let now = now_secs();
        let stored = cached_unless_full(self.conn.lock().execute(
            r#"
            INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?5)
            ON CONFLICT(cid) DO UPDATE SET
                codec = excluded.codec,
                size = excluded.size,
                data = excluded.data,
                last_accessed_at = excluded.last_accessed_at
            "#,
            params![
                &cid_bytes,
                cid.codec() as i64,
                data.len() as i64,
                data,
                now as i64
            ],
        ))?;
        if !stored {
            return Ok(());
        }
        let inserted_block_evicted = self.evict_if_needed_tracking(&cid_bytes)?;
        if inserted_block_evicted {
            self.hot.lock().remove(&cid_bytes);
        } else {
            self.hot.lock().put_input(cid_bytes, hot_data, now);
        }
        Ok(())
    }

    /// Stores `block`, sharing its bytes with the hot cache
    /// ([`SqliteBlockStore::put_charged`]).
    pub fn put(&self, block: &Block) -> Result<()> {
        self.put_charged(block.cid(), block.charged())
    }

    /// The hot cache's fixed capacity in bytes: a host that budgets memory
    /// reserves it as the cache's fixed reservation. The cache never covers
    /// more: it counts each block by its allocation's footprint, for as long
    /// as the allocation lives, including after the cache has let it go, and
    /// its own structures (itself, its maps' nodes and keys) besides. Even a
    /// hot cache that caches nothing keeps itself (a few hundred bytes),
    /// which counts.
    pub fn hot_cache_capacity(&self) -> u64 {
        self.hot.lock().pool.capacity() as u64
    }

    /// Ties `lease`, the fixed reservation the hot cache is reserved by, to
    /// its memory ([`FixedPool::keep_lease`]): it goes only once the cache
    /// and every block it covers, one a reader still holds included, are
    /// gone. Closing the store lets go of the cache, but not of such a block,
    /// and leaves no hot cache: a closed store keeps no lease (`false`).
    pub fn keep_hot_cache_lease(&self, lease: Arc<Reservation>) -> bool {
        self.hot.lock().pool.keep_lease(lease)
    }

    /// The bytes the hot cache counts now, of its fixed capacity: the blocks
    /// it covers and its own structures.
    pub fn hot_cache_used(&self) -> u64 {
        self.hot.lock().pool.used() as u64
    }

    pub fn get(&self, cid: &Cid) -> Result<Option<Block>> {
        let cid_bytes = block_key(cid);
        let now = now_secs();
        if let Some(hit) = self.hot.lock().get_verified(&cid_bytes, now) {
            if hit.touch_persistent {
                self.touch_at(cid, now)?;
            }
            return Ok(Some(Block::unchecked_charged(*cid, hit.data)));
        }

        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT data FROM blocks WHERE cid = ?1",
                params![cid_bytes],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        match row {
            Some(data) => {
                verify_block(cid, &data)?;
                self.touch_at(cid, now)?;
                let data = ChargedBytes::from_vec(data);
                self.hot.lock().put_verified(cid_bytes, data.clone(), now);
                Ok(Some(Block::unchecked_charged(*cid, data)))
            }
            None => Ok(None),
        }
    }

    pub fn get_range(&self, cid: &Cid, start: u64, end: u64) -> Result<Option<Vec<u8>>> {
        let cid_bytes = block_key(cid);
        let now = now_secs();
        if let Some(hit) = self
            .hot
            .lock()
            .get_verified_range(&cid_bytes, now, start, end)
        {
            if hit.touch_persistent {
                self.touch_at(cid, now)?;
            }
            return Ok(Some(hit.data));
        }

        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT data FROM blocks WHERE cid = ?1",
                params![&cid_bytes],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        match row {
            Some(data) => {
                verify_block(cid, &data)?;
                self.touch_at(cid, now)?;
                let range = block_data_range(&data, start, end);
                self.hot
                    .lock()
                    .put_verified(cid_bytes, ChargedBytes::from_vec(data), now);
                Ok(Some(range))
            }
            None => Ok(None),
        }
    }

    pub fn import_car(&self, bytes: &[u8]) -> Result<Vec<Cid>> {
        let car = parse_car_v1(bytes)?;
        let mut imported = Vec::with_capacity(car.blocks.len());
        for block in car.blocks {
            self.put_block(&block.cid, &block.data)?;
            imported.push(block.cid);
        }
        Ok(imported)
    }

    pub fn export_car(&self) -> Result<Vec<u8>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT cid, data FROM blocks ORDER BY inserted_at ASC")?;
        let blocks = stmt
            .query_map([], |row| {
                let cid_bytes = row.get::<_, Vec<u8>>(0)?;
                let data = row.get::<_, Vec<u8>>(1)?;
                Ok((cid_bytes, data))
            })?
            .map(|row| {
                let (cid_bytes, data) = row?;
                let cid = Cid::read_bytes(&mut Cursor::new(cid_bytes))
                    .map_err(|err| CoreError::InvalidCid(err.to_string()))?;
                Ok(CarBlock { cid, data })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(encode_car_v1(&blocks))
    }

    pub fn put_provider_records(
        &self,
        cid: &Cid,
        providers: &[CachedProviderRecord],
        ttl: Duration,
    ) -> Result<()> {
        let expires_at = now_secs().saturating_add(ttl.as_secs());
        let providers_json = serde_json::to_string(providers)?;
        let conn = self.conn.lock();
        let stored = cached_unless_full(conn.execute(
            r#"
            INSERT INTO provider_cache(cid, providers_json, expires_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(cid) DO UPDATE SET
                providers_json = excluded.providers_json,
                expires_at = excluded.expires_at
            "#,
            params![provider_cache_key(cid), providers_json, expires_at as i64],
        ))?;
        if !stored {
            // The refused update left the old row: it must not answer for
            // the new one.
            conn.execute(
                "DELETE FROM provider_cache WHERE cid = ?1",
                params![provider_cache_key(cid)],
            )?;
        }
        Ok(())
    }

    pub fn get_provider_records(&self, cid: &Cid) -> Result<Option<Vec<CachedProviderRecord>>> {
        let cid_bytes = provider_cache_key(cid);
        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT providers_json, expires_at FROM provider_cache WHERE cid = ?1",
                params![cid_bytes],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((providers_json, expires_at)) = row else {
            return Ok(None);
        };
        if expires_at <= now_secs() as i64 {
            self.conn.lock().execute(
                "DELETE FROM provider_cache WHERE cid = ?1",
                params![provider_cache_key(cid)],
            )?;
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&providers_json)?))
    }

    pub fn put_name_record(&self, name: &str, resolved_target: &str, ttl: Duration) -> Result<()> {
        if ttl.is_zero() {
            return Ok(());
        }
        let now = now_secs();
        let expires_at = now.saturating_add(ttl.as_secs());
        let conn = self.conn.lock();
        let stored = cached_unless_full(conn.execute(
            r#"
            INSERT INTO name_cache(name, resolved_target, expires_at, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(name) DO UPDATE SET
                resolved_target = excluded.resolved_target,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            "#,
            params![name, resolved_target, expires_at as i64, now as i64],
        ))?;
        if !stored {
            // The refused update left the old target: it must not answer.
            conn.execute("DELETE FROM name_cache WHERE name = ?1", params![name])?;
            return Ok(());
        }
        drop(conn);
        self.prune_name_cache()?;
        Ok(())
    }

    pub fn get_name_record(&self, name: &str) -> Result<Option<String>> {
        let row = self
            .conn
            .lock()
            .query_row(
                "SELECT resolved_target, expires_at FROM name_cache WHERE name = ?1",
                params![name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((resolved_target, expires_at)) = row else {
            return Ok(None);
        };
        if expires_at <= now_secs() as i64 {
            self.conn
                .lock()
                .execute("DELETE FROM name_cache WHERE name = ?1", params![name])?;
            return Ok(None);
        }
        Ok(Some(resolved_target))
    }

    pub fn mark_bad_provider(&self, peer_or_url: &str, reason: &str, ttl: Duration) -> Result<()> {
        let expires_at = now_secs().saturating_add(ttl.as_secs());
        let conn = self.conn.lock();
        let stored = cached_unless_full(conn.execute(
            r#"
            INSERT INTO bad_providers(peer_or_url, reason, expires_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(peer_or_url) DO UPDATE SET
                reason = excluded.reason,
                expires_at = excluded.expires_at
            "#,
            params![peer_or_url, reason, expires_at as i64],
        ))?;
        if !stored {
            // The refused update left the old mark and its expiry.
            conn.execute(
                "DELETE FROM bad_providers WHERE peer_or_url = ?1",
                params![peer_or_url],
            )?;
        }
        Ok(())
    }

    pub fn is_bad_provider(&self, peer_or_url: &str) -> Result<bool> {
        let expires_at = self
            .conn
            .lock()
            .query_row(
                "SELECT expires_at FROM bad_providers WHERE peer_or_url = ?1",
                params![peer_or_url],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let Some(expires_at) = expires_at else {
            return Ok(false);
        };
        if expires_at <= now_secs() as i64 {
            self.conn.lock().execute(
                "DELETE FROM bad_providers WHERE peer_or_url = ?1",
                params![peer_or_url],
            )?;
            return Ok(false);
        }
        Ok(true)
    }

    pub fn total_bytes(&self) -> Result<u64> {
        let total =
            self.conn
                .lock()
                .query_row("SELECT COALESCE(SUM(size), 0) FROM blocks", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        Ok(total.max(0) as u64)
    }

    pub fn block_count(&self) -> Result<u64> {
        let count = self
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM blocks", [], |row| {
                row.get::<_, i64>(0)
            })?;
        Ok(count.max(0) as u64)
    }

    pub fn clear(&self) -> Result<()> {
        self.conn.lock().execute("DELETE FROM blocks", [])?;
        self.conn.lock().execute("DELETE FROM provider_cache", [])?;
        self.conn.lock().execute("DELETE FROM bad_providers", [])?;
        self.conn.lock().execute("DELETE FROM name_cache", [])?;
        self.hot.lock().clear();
        Ok(())
    }

    pub fn clear_provider_metadata(&self) -> Result<()> {
        self.conn.lock().execute("DELETE FROM provider_cache", [])?;
        self.conn.lock().execute("DELETE FROM bad_providers", [])?;
        self.conn.lock().execute("DELETE FROM name_cache", [])?;
        Ok(())
    }

    pub fn trim_blocks_to(&self, max_bytes: u64) -> Result<()> {
        self.evict_until(max_bytes)
    }

    fn touch_at(&self, cid: &Cid, now: u64) -> Result<()> {
        self.conn.lock().execute(
            "UPDATE blocks SET last_accessed_at = ?1 WHERE cid = ?2",
            params![now as i64, block_key(cid)],
        )?;
        Ok(())
    }

    fn evict_if_needed_tracking(&self, tracked_cid_bytes: &[u8]) -> Result<bool> {
        self.evict_until_tracking(self.max_bytes, tracked_cid_bytes)
    }

    fn prune_name_cache(&self) -> Result<()> {
        let now = now_secs() as i64;
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM name_cache WHERE expires_at <= ?1",
            params![now],
        )?;
        conn.execute(
            r#"
            DELETE FROM name_cache
            WHERE name IN (
                SELECT name FROM name_cache
                ORDER BY updated_at DESC, name DESC
                LIMIT -1 OFFSET ?1
            )
            "#,
            params![MAX_NAME_CACHE_RECORDS],
        )?;
        Ok(())
    }

    fn evict_until(&self, max_bytes: u64) -> Result<()> {
        self.evict_until_tracking(max_bytes, &[]).map(|_| ())
    }

    fn evict_until_tracking(&self, max_bytes: u64, tracked_cid_bytes: &[u8]) -> Result<bool> {
        let mut tracked_block_evicted = false;
        loop {
            let total = self.total_bytes()?;
            if total <= max_bytes {
                return Ok(tracked_block_evicted);
            }
            let retained = self.retained.lock().clone();
            let cid_bytes = self.oldest_evictable_cid(&retained)?;
            let Some(cid_bytes) = cid_bytes else {
                return Ok(tracked_block_evicted);
            };
            let deleted = self
                .conn
                .lock()
                .execute("DELETE FROM blocks WHERE cid = ?1", params![&cid_bytes])?;
            if deleted == 0 {
                return Ok(tracked_block_evicted);
            }
            if !tracked_cid_bytes.is_empty() && cid_bytes == tracked_cid_bytes {
                tracked_block_evicted = true;
            }
            self.hot.lock().remove(&cid_bytes);
        }
    }

    fn oldest_evictable_cid(&self, retained: &HashMap<Vec<u8>, usize>) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            r#"
            SELECT cid FROM blocks
            ORDER BY last_accessed_at ASC, inserted_at ASC
            "#,
        )?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let cid_bytes = row.get::<_, Vec<u8>>(0)?;
            if !retained.contains_key(&cid_bytes) {
                return Ok(Some(cid_bytes));
            }
        }
        Ok(None)
    }

    fn retain_cid_bytes(&self, cid_bytes: Vec<u8>) {
        let mut retained = self.retained.lock();
        *retained.entry(cid_bytes).or_insert(0) += 1;
    }

    fn release_cid_bytes(&self, cid_bytes: &[u8]) {
        let mut retained = self.retained.lock();
        let Some(count) = retained.get_mut(cid_bytes) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            retained.remove(cid_bytes);
        }
    }
}

impl BlockProvider for SqliteBlockStore {
    fn get_block(&self, cid: &Cid) -> CoreResult<Option<Block>> {
        self.get(cid)
            .map_err(|err| CoreError::Storage(err.to_string()))
    }

    fn get_block_range(&self, cid: &Cid, start: u64, end: u64) -> CoreResult<Option<Vec<u8>>> {
        self.get_range(cid, start, end)
            .map_err(|err| CoreError::Storage(err.to_string()))
    }

    fn retain_block(&self, cid: &Cid) -> CoreResult<()> {
        self.retain_cid_bytes(block_key(cid));
        Ok(())
    }

    fn release_block(&self, cid: &Cid) {
        self.release_cid_bytes(&block_key(cid));
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn provider_cache_key(cid: &Cid) -> Vec<u8> {
    cid.hash().to_bytes()
}

fn block_key(cid: &Cid) -> Vec<u8> {
    Cid::new_v1(cid.codec(), *cid.hash()).to_bytes()
}

fn hot_cache_bytes(max_bytes: u64) -> u64 {
    max_bytes.min(DEFAULT_HOT_CACHE_BYTES)
}

/// A cache write's outcome: whether the entry was stored. One that met the
/// page cap (`SQLITE_FULL`) wasn't, and that is not an error: the entry is
/// left uncached, and the request it served goes on. SQLite rolls back the
/// failed statement, so nothing of it is left behind; but a refused update
/// leaves the row it would have replaced, which a metadata caller deletes.
fn cached_unless_full(written: rusqlite::Result<usize>) -> Result<bool> {
    match written {
        Ok(_) => Ok(true),
        Err(rusqlite::Error::SqliteFailure(failure, _)) if failure.code == ErrorCode::DiskFull => {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

// Private cache for block bytes that were already verified on put or cold read.
//
// Its capacity is a fixed reservation: a pool that counts each block by its
// allocation's footprint (its backing capacity, not its length, and its
// header) for as long as the allocation lives, so a block a reader still
// holds after the cache evicted it stays counted, and the cache never covers
// more than its capacity. A block shared with the fetch that charged it
// dynamically counts twice until the cache holds the last reference; then
// the cache takes the charge over.
//
// Its own structures count against the same pool, so many tiny blocks can't
// grow it past its capacity either: the cache itself, its maps' nodes
// (`btree_map_bytes`, which bounds them by entry count alone) and their keys'
// heap, by capacity. Room for them is taken before an entry goes in, oldest
// blocks making way.
struct VerifiedHotCache {
    pool: FixedPool,
    /// Whether it caches anything: not when its capacity is zero, or too
    /// small for its maps.
    enabled: bool,
    /// The pool's room for the cache's own structures ([`Self::index_bytes`]),
    /// itself alone when it caches nothing.
    index: PoolShare,
    /// The heap the keys of both maps own.
    key_heap: usize,
    clock: u64,
    entries: BTreeMap<Vec<u8>, VerifiedHotBlock>,
    /// Entries whose bytes still carry a dynamic charge, to hand over once
    /// the cache holds them alone.
    charged: BTreeSet<Vec<u8>>,
}

/// What the hot cache's own structures take besides its maps' nodes and
/// keys: the cache in its store's `Arc<Mutex<_>>`, and its pool's counter in
/// its own `Arc`. All a hot cache that caches nothing takes.
const HOT_CACHE_OWN_BYTES: usize = std::mem::size_of::<Mutex<VerifiedHotCache>>()
    + 2 * std::mem::size_of::<usize>()
    + FixedPool::HEAP_BYTES;

/// The fixed capacity of a hot cache of `max_bytes`: at least its own
/// structures, which even one that caches nothing keeps.
fn hot_cache_fixed_bytes(max_bytes: u64) -> usize {
    usize::try_from(max_bytes)
        .unwrap_or(usize::MAX)
        .max(HOT_CACHE_OWN_BYTES)
}

/// What the hot cache is given: bytes to share, or bytes to copy (once
/// it has room for the copy).
enum HotInput<'a> {
    Shared(ChargedBytes),
    Copy(&'a [u8]),
}

struct VerifiedHotBlock {
    data: HeldBytes,
    last_accessed: u64,
    last_persistent_touch: u64,
}

struct VerifiedHotCacheHit<T> {
    data: T,
    touch_persistent: bool,
}

impl Drop for VerifiedHotCache {
    /// The cached blocks, the maps and their keys go first: the room, and
    /// with the last of the pool's handles or covers its lease, go only
    /// after them, so the reservation is given back only once what it stands
    /// for is freed. (Fields would drop in their order, the room and the
    /// pool before the maps, and the last block's cover before the maps'
    /// last node and the charged set's keys.) The pool's counter is freed in
    /// the same drop, right after.
    fn drop(&mut self) {
        self.entries.clear();
        self.charged.clear();
    }
}

impl VerifiedHotCache {
    /// A closed store's hot cache: none. It holds, counts and allocates
    /// nothing (its pool is the pool of nothing), and keeps no lease.
    fn closed() -> Self {
        let pool = FixedPool::none();
        let index = pool.share(0).expect("a share of nothing fits");
        Self {
            enabled: false,
            index,
            pool,
            key_heap: 0,
            clock: 0,
            entries: BTreeMap::new(),
            charged: BTreeSet::new(),
        }
    }

    /// What its own structures count: nothing once its store is closed.
    fn own_bytes(&self) -> usize {
        if self.pool.capacity() == 0 {
            0
        } else {
            HOT_CACHE_OWN_BYTES
        }
    }

    fn new(max_bytes: u64) -> Self {
        let pool = FixedPool::new(hot_cache_fixed_bytes(max_bytes));
        // Its own structures count from the start.
        let index = pool
            .share(HOT_CACHE_OWN_BYTES)
            .expect("a cache's capacity holds its own structures");
        let mut cache = Self {
            enabled: max_bytes > 0,
            index,
            pool,
            key_heap: 0,
            clock: 0,
            entries: BTreeMap::new(),
            charged: BTreeSet::new(),
        };
        // Then room for its maps: a pool too small for them caches nothing.
        if cache.enabled && !cache.true_index() {
            cache.enabled = false;
        }
        cache
    }

    /// What its structures take, at most, with `entries` and `charged` more
    /// entries in its maps, and `key_heap` more bytes of keys.
    fn index_bytes(&self, entries: usize, charged: usize, key_heap: usize) -> usize {
        if !self.enabled {
            // It never allocates its maps' nodes.
            return self.own_bytes();
        }
        HOT_CACHE_OWN_BYTES
            .saturating_add(btree_map_bytes::<Vec<u8>, VerifiedHotBlock>(
                self.entries.len().saturating_add(entries),
            ))
            .saturating_add(btree_map_bytes::<Vec<u8>, ()>(
                self.charged.len().saturating_add(charged),
            ))
            .saturating_add(self.key_heap)
            .saturating_add(key_heap)
    }

    /// Sizes the room for its structures to what they take now: `false` if
    /// it can't grow to it.
    fn true_index(&mut self) -> bool {
        let bytes = self.index_bytes(0, 0, 0);
        self.index.try_resize(bytes)
    }

    /// Takes over the dynamic charges of the entries the cache now holds
    /// alone.
    fn take_over_charges(&mut self) {
        let Self {
            entries,
            charged,
            key_heap,
            ..
        } = self;
        let before = charged.len();
        charged.retain(|cid| {
            let keep = entries.get(cid).is_some_and(|entry| {
                !entry.data.take_over_if_only_held() && entry.data.bytes().charged_bytes() > 0
            });
            if !keep {
                *key_heap = key_heap.saturating_sub(cid.capacity());
            }
            keep
        });
        if charged.len() != before {
            self.true_index();
        }
    }

    fn touch(&mut self, cid: &[u8], now: u64) -> Option<(&VerifiedHotBlock, bool)> {
        let entry = self.entries.get_mut(cid)?;
        self.clock = self.clock.saturating_add(1);
        entry.last_accessed = self.clock;
        let touch_persistent =
            now.saturating_sub(entry.last_persistent_touch) >= HOT_CACHE_TOUCH_INTERVAL.as_secs();
        if touch_persistent {
            entry.last_persistent_touch = now;
        }
        Some((entry, touch_persistent))
    }

    fn get_verified(&mut self, cid: &[u8], now: u64) -> Option<VerifiedHotCacheHit<ChargedBytes>> {
        self.take_over_charges();
        let (entry, touch_persistent) = self.touch(cid, now)?;
        Some(VerifiedHotCacheHit {
            data: entry.data.bytes().clone(),
            touch_persistent,
        })
    }

    fn get_verified_range(
        &mut self,
        cid: &[u8],
        now: u64,
        start: u64,
        end: u64,
    ) -> Option<VerifiedHotCacheHit<Vec<u8>>> {
        self.take_over_charges();
        let (entry, touch_persistent) = self.touch(cid, now)?;
        Some(VerifiedHotCacheHit {
            data: block_data_range(entry.data.bytes(), start, end),
            touch_persistent,
        })
    }

    fn put_verified(&mut self, cid: Vec<u8>, data: ChargedBytes, now: u64) {
        self.put_input(cid, HotInput::Shared(data), now);
    }

    fn put_input(&mut self, cid: Vec<u8>, input: HotInput<'_>, now: u64) {
        self.take_over_charges();
        self.remove(&cid);
        if !self.enabled {
            return;
        }
        // Bytes to copy (plain input, or bytes another pool covers: one
        // cover per allocation) are copied only once this pool has room for
        // the copy. Make room oldest first: evicting a block a reader still
        // holds frees no room, as it stays covered until it is freed.
        let copy: Option<&[u8]> = match &input {
            HotInput::Copy(data) => Some(data),
            HotInput::Shared(data) if data.is_covered() && !self.pool.covers(data) => {
                Some(data.as_slice())
            }
            HotInput::Shared(_) => None,
        };
        let (footprint, charged) = match (copy, &input) {
            (Some(bytes), _) => (bytes.len().saturating_add(ChargedBytes::HEADER_BYTES), 0),
            (None, HotInput::Shared(data)) if self.pool.covers(data) => {
                (0, usize::from(data.charged_bytes() > 0))
            }
            (None, HotInput::Shared(data)) => {
                (data.footprint(), usize::from(data.charged_bytes() > 0))
            }
            (None, HotInput::Copy(_)) => return,
        };
        // Its key, and the charged set's copy of it.
        let key_heap = cid.capacity().saturating_add(charged * cid.len());
        // Even alone in the cache it wouldn't fit: evict nothing for it.
        let alone = HOT_CACHE_OWN_BYTES
            .saturating_add(btree_map_bytes::<Vec<u8>, VerifiedHotBlock>(1))
            .saturating_add(btree_map_bytes::<Vec<u8>, ()>(charged))
            .saturating_add(key_heap)
            .saturating_add(footprint);
        if alone > self.pool.capacity() {
            return;
        }
        let data = loop {
            // Room for its entry in the maps first, then for its bytes: each
            // taken before anything is allocated for it.
            let index = self.index_bytes(1, charged, key_heap);
            let roomy = self.index.try_resize(index);
            if roomy {
                let held = match (copy, &input) {
                    (Some(bytes), _) => self.pool.hold_copy(bytes.len(), || {
                        #[cfg(test)]
                        tests::HOT_CACHE_COPIES.with(|copies| copies.set(copies.get() + 1));
                        bytes.to_vec()
                    }),
                    (None, HotInput::Shared(data)) => self.pool.hold(data),
                    (None, HotInput::Copy(_)) => None,
                };
                if let Some(held) = held {
                    break held;
                }
            }
            if !self.evict_oldest() {
                self.true_index();
                return;
            }
        };
        self.clock = self.clock.saturating_add(1);
        if data.bytes().charged_bytes() > 0 {
            let key = cid.clone();
            self.key_heap = self.key_heap.saturating_add(key.capacity());
            self.charged.insert(key);
        }
        self.key_heap = self.key_heap.saturating_add(cid.capacity());
        self.entries.insert(
            cid,
            VerifiedHotBlock {
                data,
                last_accessed: self.clock,
                last_persistent_touch: now,
            },
        );
        self.take_over_charges();
        // Its room was taken before it went in, for its key's copy at the
        // key's length, which is all a copy takes: this only shrinks. Were it
        // ever to grow past the pool, the block wouldn't stay.
        if !self.true_index() {
            self.remove_used_at(self.clock);
        }
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(oldest) = self.entries.values().map(|block| block.last_accessed).min() else {
            return false;
        };
        self.remove_used_at(oldest);
        true
    }

    /// Removes the entry last used at `tick` (ticks are unique), its key
    /// going with it rather than copied to look it up by.
    fn remove_used_at(&mut self, tick: u64) {
        let Self {
            entries,
            charged,
            key_heap,
            ..
        } = self;
        entries.retain(|cid, block| {
            if block.last_accessed != tick {
                return true;
            }
            *key_heap = key_heap.saturating_sub(cid.capacity());
            if let Some(key) = charged.take(cid) {
                *key_heap = key_heap.saturating_sub(key.capacity());
            }
            false
        });
        self.true_index();
    }

    fn remove(&mut self, cid: &[u8]) {
        if let Some((key, _)) = self.entries.remove_entry(cid) {
            self.key_heap = self.key_heap.saturating_sub(key.capacity());
        }
        if let Some(key) = self.charged.take(cid) {
            self.key_heap = self.key_heap.saturating_sub(key.capacity());
        }
        self.true_index();
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.charged.clear();
        self.key_heap = 0;
        self.true_index();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freedom_ipfs_core::{cid_from_data, CODEC_DAG_PB, CODEC_RAW};

    fn pragma(store: &SqliteBlockStore, name: &str) -> i64 {
        store
            .conn
            .lock()
            .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn default_options_size_the_store_as_before() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        assert_eq!(store.max_bytes, 1024 * 1024);
        assert_eq!(store.hot_cache_capacity(), 1024 * 1024);
        let store = SqliteBlockStore::in_memory(0).unwrap();
        assert_eq!(store.max_bytes, DEFAULT_CACHE_BYTES);
        assert_eq!(store.hot_cache_capacity(), DEFAULT_HOT_CACHE_BYTES);
    }

    #[test]
    fn an_in_memory_store_keeps_temporary_tables_in_memory() {
        // 2 is MEMORY.
        assert_eq!(
            pragma(&SqliteBlockStore::in_memory(0).unwrap(), "temp_store"),
            2
        );
        let store = SqliteBlockStore::in_memory_with(StoreOptions::default()).unwrap();
        assert_eq!(pragma(&store, "temp_store"), 2);
    }

    #[test]
    fn the_temp_store_option_keeps_temporary_data_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        // Today's default: a file store keeps SQLite's default; an in-memory
        // store keeps its temporary data in memory regardless.
        let file = SqliteBlockStore::open(dir.path().join("default.sqlite3"), 0).unwrap();
        assert_eq!(pragma(&file, "temp_store"), 0);
        assert_eq!(
            pragma(&SqliteBlockStore::in_memory(0).unwrap(), "temp_store"),
            2
        );
        let memory = SqliteBlockStore::open_with(
            dir.path().join("memory.sqlite3"),
            StoreOptions {
                temp_store: TempStore::Memory,
                ..StoreOptions::default()
            },
        )
        .unwrap();
        assert_eq!(pragma(&memory, "temp_store"), 2);
    }

    /// A sort far past the page cache (every block's data, with a page cache
    /// of a few pages) completes with its temporary data in memory.
    #[test]
    fn a_sort_past_the_page_cache_completes_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteBlockStore::open_with(
            dir.path().join("spill.sqlite3"),
            StoreOptions {
                temp_store: TempStore::Memory,
                cache_kib: Some(16),
                ..StoreOptions::default()
            },
        )
        .unwrap();
        // 4 MiB of blocks: past SQLite's 1 MiB minimum before a sort spills.
        let blocks: Vec<Vec<u8>> = (0..512u32)
            .map(|i| {
                let mut data = vec![0u8; 8192];
                data[..4].copy_from_slice(&i.to_be_bytes());
                data
            })
            .collect();
        for data in &blocks {
            let cid = cid_from_data(CODEC_RAW, data);
            store.put_block(&cid, data).unwrap();
        }
        let car = store.export_car().unwrap();
        assert!(car.len() > 512 * 8192, "{}", car.len());
    }

    #[test]
    fn options_set_the_page_cap_and_the_page_cache() {
        let store = SqliteBlockStore::in_memory_with(StoreOptions {
            max_page_count: Some(500),
            cache_kib: Some(1024),
            ..StoreOptions::default()
        })
        .unwrap();
        assert_eq!(pragma(&store, "max_page_count"), 500);
        assert_eq!(pragma(&store, "cache_size"), -1024);
        let size = pragma(&store, "page_size") as u64;
        assert_eq!(
            store.page_bytes().unwrap(),
            pragma(&store, "page_count") as u64 * size
        );
    }

    #[test]
    fn a_page_cap_that_cant_be_installed_fails_the_open() {
        let dir = std::env::temp_dir().join(format!(
            "freedom-ipfs-store-cap-{}-{}",
            std::process::id(),
            now_secs()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("store.sqlite");
        let store = SqliteBlockStore::open(&path, 0).unwrap();
        for i in 0..4u8 {
            let data = vec![i; 64 * 1024];
            store
                .put_block(&cid_from_data(CODEC_RAW, &data), &data)
                .unwrap();
        }
        let pages = pragma(&store, "page_count");
        assert!(store.close(Duration::from_secs(5)));
        drop(store);
        // Below the pages the database already has.
        let refused = SqliteBlockStore::open_with(
            &path,
            StoreOptions {
                max_page_count: Some(1),
                ..StoreOptions::default()
            },
        );
        assert!(
            matches!(refused, Err(StoreError::PageCap { requested: 1, installed }) if installed >= pages),
            "{:?}",
            refused.err()
        );
        // At or above them it holds.
        let held = SqliteBlockStore::open_with(
            &path,
            StoreOptions {
                max_page_count: Some(pages as u32 + 10),
                ..StoreOptions::default()
            },
        )
        .unwrap();
        assert_eq!(pragma(&held, "max_page_count"), pages + 10);
        drop(held);
        // Past SQLite's own limit.
        let past = SqliteBlockStore::in_memory_with(StoreOptions {
            max_page_count: Some(u32::MAX),
            ..StoreOptions::default()
        });
        assert!(
            matches!(past, Err(StoreError::PageCap { .. })),
            "{:?}",
            past.err()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fetched block's bytes as a view into a larger, charged receive
    /// buffer: `len` of `backing` bytes, `backing` reserved from `budget`.
    fn charged_view(
        budget: &freedom_ipfs_core::MemoryBudget,
        data: &[u8],
        backing: usize,
    ) -> ChargedBytes {
        let mut buffer =
            freedom_ipfs_core::ReservedVec::zeroed(backing, budget.try_reserve(backing).unwrap())
                .unwrap();
        buffer.as_mut_slice()[..data.len()].copy_from_slice(data);
        buffer.into_charged().slice(0, data.len())
    }

    thread_local! {
        /// Copies the hot cache made of another pool's bytes, on this thread.
        pub(super) static HOT_CACHE_COPIES: std::cell::Cell<usize> =
            const { std::cell::Cell::new(0) };
    }

    /// What the hot cache counts for its blocks' bytes alone: its pool's use
    /// but for its own structures.
    fn covered(store: &SqliteBlockStore) -> u64 {
        let hot = store.hot.lock();
        (hot.pool.used() - hot.index.bytes()) as u64
    }

    /// A hot cache's capacity for exactly one block of `len` bytes, copied
    /// in (so uncharged), with its entry.
    fn room_for_one(cid: &Cid, len: usize) -> u64 {
        (HOT_CACHE_OWN_BYTES
            + btree_map_bytes::<Vec<u8>, VerifiedHotBlock>(1)
            + btree_map_bytes::<Vec<u8>, ()>(0)
            + block_key(cid).capacity()
            + len
            + ChargedBytes::HEADER_BYTES) as u64
    }

    #[test]
    fn a_full_hot_cache_copies_no_foreign_block_it_can_not_hold() {
        const N: usize = 123_457;
        let capacity = room_for_one(&cid_from_data(CODEC_RAW, b"key"), N);
        let store = || {
            SqliteBlockStore::in_memory_with(StoreOptions {
                max_bytes: 4 << 20,
                hot_cache_bytes: Some(capacity),
                ..StoreOptions::default()
            })
            .unwrap()
        };
        let (a, b) = (store(), store());
        let first = vec![1u8; N];
        let second = vec![2u8; N];
        let (first_cid, second_cid) = (
            cid_from_data(CODEC_RAW, &first),
            cid_from_data(CODEC_RAW, &second),
        );
        a.put_block(&first_cid, &first).unwrap();
        b.put_block(&second_cid, &second).unwrap();
        let from_a = a.get(&first_cid).unwrap().unwrap();
        assert!(from_a.charged().is_covered());
        // A reader pins B's only block: B has no room it can make.
        let pinned = b.get(&second_cid).unwrap().unwrap();
        HOT_CACHE_COPIES.with(|copies| copies.set(0));
        b.put(&from_a).unwrap();
        assert_eq!(
            HOT_CACHE_COPIES.with(std::cell::Cell::get),
            0,
            "no uncovered copy"
        );
        assert_eq!(covered(&b), (N + ChargedBytes::HEADER_BYTES) as u64);
        // With room, it copies once, into room it holds.
        drop(pinned);
        let third = vec![3u8; 1_000];
        b.put_block(&cid_from_data(CODEC_RAW, &third), &third)
            .unwrap();
        HOT_CACHE_COPIES.with(|copies| copies.set(0));
        b.put(&from_a).unwrap();
        assert_eq!(HOT_CACHE_COPIES.with(std::cell::Cell::get), 1);
        assert!(b.hot.lock().entries.contains_key(&block_key(&first_cid)));
        assert!(b.hot_cache_used() <= capacity);
    }

    #[test]
    fn a_full_hot_cache_copies_no_plain_put_it_can_not_hold() {
        const N: usize = 123_457;
        let capacity = room_for_one(&cid_from_data(CODEC_RAW, b"key"), N);
        let store = SqliteBlockStore::in_memory_with(StoreOptions {
            max_bytes: 4 << 20,
            hot_cache_bytes: Some(capacity),
            ..StoreOptions::default()
        })
        .unwrap();
        let (first, second) = (vec![1u8; N], vec![2u8; N]);
        let (first_cid, second_cid) = (
            cid_from_data(CODEC_RAW, &first),
            cid_from_data(CODEC_RAW, &second),
        );
        store.put_block(&first_cid, &first).unwrap();
        // A reader pins the cache's only block: it has no room to make.
        let pinned = store.get(&first_cid).unwrap().unwrap();
        HOT_CACHE_COPIES.with(|copies| copies.set(0));
        store.put_block(&second_cid, &second).unwrap();
        assert_eq!(
            HOT_CACHE_COPIES.with(std::cell::Cell::get),
            0,
            "no uncovered copy"
        );
        assert_eq!(covered(&store), (N + ChargedBytes::HEADER_BYTES) as u64);
        assert!(
            store.get(&second_cid).unwrap().is_some(),
            "stored all the same"
        );
        // With room, the copy is made once, into room it holds.
        drop(pinned);
        let third = vec![3u8; N];
        HOT_CACHE_COPIES.with(|copies| copies.set(0));
        store
            .put_block(&cid_from_data(CODEC_RAW, &third), &third)
            .unwrap();
        assert_eq!(HOT_CACHE_COPIES.with(std::cell::Cell::get), 1);
        assert!(store.hot_cache_used() <= capacity);
    }

    #[test]
    fn the_hot_cache_counts_allocations_and_takes_charges_over_from_the_last_owner() {
        let budget = freedom_ipfs_core::MemoryBudget::new(1 << 20);
        let store = SqliteBlockStore::in_memory_with(StoreOptions {
            max_bytes: 1 << 20,
            hot_cache_bytes: Some(16 * 1024),
            ..StoreOptions::default()
        })
        .unwrap();
        assert_eq!(store.hot_cache_capacity(), 16 * 1024);
        let data = vec![7u8; 3_000];
        let cid = cid_from_data(CODEC_RAW, &data);
        let fetched = charged_view(&budget, &data, 4_000);

        // Shared with the fetch, the block counts twice: its whole 4,000-byte
        // allocation (and its header) in the cache's capacity, and its
        // dynamic charge.
        const FOOTPRINT: u64 = 4_000 + ChargedBytes::HEADER_BYTES as u64;
        store.put_charged(&cid, &fetched).unwrap();
        assert_eq!(covered(&store), FOOTPRINT);
        assert_eq!(budget.used(), 4_000);
        let hit = store.get(&cid).unwrap().unwrap();
        assert_eq!(hit.data(), &data[..]);
        assert_eq!(hit.charged().backing_capacity(), 4_000, "no copy");
        assert_eq!(budget.used(), 4_000);

        // Once the cache holds the last reference, it takes the charge over,
        // as the last other handle is dropped.
        drop(fetched);
        assert_eq!(budget.used(), 4_000);
        drop(hit);
        assert_eq!(budget.used(), 0);
        assert_eq!(covered(&store), FOOTPRINT);
        assert!(store.get(&cid).unwrap().is_some());

        // A reader holds it while the cache evicts it: it stays counted
        // until the reader lets go, so other blocks can't take its room.
        let reader = store.get(&cid).unwrap().unwrap();
        for n in 0..6u8 {
            let other = vec![n; 3_000];
            store
                .put_block(&cid_from_data(CODEC_RAW, &other), &other)
                .unwrap();
        }
        assert!(!store.hot.lock().entries.contains_key(&block_key(&cid)));
        assert!(store.hot_cache_used() <= 16 * 1024);
        let held = covered(&store);
        drop(reader);
        assert_eq!(covered(&store), held - FOOTPRINT);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn the_hot_cache_is_sized_apart_from_max_bytes() {
        let off = SqliteBlockStore::in_memory_with(StoreOptions {
            hot_cache_bytes: Some(0),
            ..StoreOptions::default()
        })
        .unwrap();
        let data = b"not kept twice";
        let cid = cid_from_data(CODEC_RAW, data);
        off.put_block(&cid, data).unwrap();
        assert!(off.hot.lock().entries.is_empty());
        assert_eq!(off.get(&cid).unwrap().unwrap().data(), data);

        let large = SqliteBlockStore::in_memory_with(StoreOptions {
            max_bytes: 1024,
            hot_cache_bytes: Some(64 * 1024 * 1024),
            ..StoreOptions::default()
        })
        .unwrap();
        assert_eq!(large.hot_cache_capacity(), 64 * 1024 * 1024);
    }

    #[test]
    fn a_write_past_the_page_cap_leaves_its_entry_uncached() {
        let store = SqliteBlockStore::in_memory_with(StoreOptions {
            max_page_count: Some(64),
            ..StoreOptions::default()
        })
        .unwrap();
        let cap = 64 * pragma(&store, "page_size") as u64;
        // 64 KiB blocks until one doesn't fit: every put succeeds.
        let mut stored = Vec::new();
        let mut refused = None;
        for i in 0..16u8 {
            let data = vec![i; 64 * 1024];
            let cid = cid_from_data(CODEC_RAW, &data);
            store.put_block(&cid, &data).unwrap();
            if store.get(&cid).unwrap().is_some() {
                stored.push((cid, data));
            } else {
                refused = Some(cid);
                break;
            }
        }
        let refused = refused.expect("the page cap was never met");
        assert!(!stored.is_empty());
        assert!(!store.hot.lock().entries.contains_key(&block_key(&refused)));
        for (cid, data) in &stored {
            assert_eq!(store.get(cid).unwrap().unwrap().data(), &data[..]);
        }
        assert!(store.page_bytes().unwrap() <= cap);

        // Names, provider records and bad providers that need new pages
        // are left uncached too.
        let long = "x".repeat(16 * 1024);
        store
            .put_name_record("example.com", &long, Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_name_record("example.com").unwrap(), None);
        let providers = vec![CachedProviderRecord {
            id: Some("peer".to_string()),
            addrs: vec![long.clone()],
        }];
        store
            .put_provider_records(&refused, &providers, Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&refused).unwrap(), None);
        store
            .mark_bad_provider(&long, "slow", Duration::from_secs(60))
            .unwrap();
        assert!(!store.is_bad_provider(&long).unwrap());

        // A refreshed entry that no longer fits is gone, not left stale.
        // (Seeded first, then the store filled again.)
        store.trim_blocks_to(0).unwrap();
        store
            .put_name_record("seeded.example", "old-target", Duration::from_secs(60))
            .unwrap();
        let seeded = vec![CachedProviderRecord {
            id: Some("old".to_string()),
            addrs: vec![],
        }];
        store
            .put_provider_records(&refused, &seeded, Duration::from_secs(60))
            .unwrap();
        store
            .mark_bad_provider("seeded-peer", "old", Duration::from_secs(60))
            .unwrap();
        assert_eq!(
            store.get_name_record("seeded.example").unwrap().as_deref(),
            Some("old-target")
        );
        assert!(store.get_provider_records(&refused).unwrap().is_some());
        assert!(store.is_bad_provider("seeded-peer").unwrap());
        for i in 0..16u8 {
            let data = vec![i.wrapping_add(100); 64 * 1024];
            let cid = cid_from_data(CODEC_RAW, &data);
            store.put_block(&cid, &data).unwrap();
            if store.get(&cid).unwrap().is_none() {
                break;
            }
        }
        store
            .put_name_record("seeded.example", &long, Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_name_record("seeded.example").unwrap(), None);
        store
            .put_provider_records(&refused, &providers, Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&refused).unwrap(), None);
        let long_reason = "r".repeat(16 * 1024);
        store
            .mark_bad_provider("seeded-peer", &long_reason, Duration::from_secs(60))
            .unwrap();
        assert!(!store.is_bad_provider("seeded-peer").unwrap());

        // Room made, the same block is cached again.
        store.trim_blocks_to(0).unwrap();
        let data = vec![0u8; 64 * 1024];
        let cid = cid_from_data(CODEC_RAW, &data);
        store.put_block(&cid, &data).unwrap();
        assert!(store.get(&cid).unwrap().is_some());
    }

    #[test]
    fn close_releases_the_file_while_clones_are_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.db");
        let store = SqliteBlockStore::open(&path, 0).unwrap();
        let data = b"closed store fixture".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        store.put_block(&cid, &data).unwrap();
        let leaked = store.clone();

        assert!(store.close(Duration::from_secs(1)));
        drop(store);

        // A clone that outlived close() gets errors, not the file or the hot cache.
        assert!(leaked.block_count().is_err());
        assert!(leaked.get(&cid).is_err());
        assert!(leaked.put_block(&cid, &data).is_err());

        let reopened = SqliteBlockStore::open(&path, 0).unwrap();
        assert_eq!(reopened.get(&cid).unwrap().unwrap().data(), data.as_slice());
    }

    #[test]
    fn close_gives_up_after_timeout_when_a_statement_holds_the_connection() {
        let store = SqliteBlockStore::in_memory(0).unwrap();
        let guard = store.conn.lock();
        assert!(!store.close(Duration::from_millis(20)));
        drop(guard);
        assert!(store.block_count().is_ok());
    }

    #[test]
    fn close_or_close_later_closes_once_the_statement_releases_the_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.db");
        let store = SqliteBlockStore::open(&path, 0).unwrap();
        let data = b"close later fixture".to_vec();
        let cid = cid_from_data(CODEC_RAW, &data);
        store.put_block(&cid, &data).unwrap();
        let leaked = store.clone();

        // Stands in for a statement that leaked blocking work is still running.
        let guard = leaked.conn.lock();
        assert!(!store.close_or_close_later(Duration::from_millis(20)));
        drop(guard);

        // Nothing but the background close touches the store from here on;
        // it must close the file even though `leaked` is never dropped.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while leaked.block_count().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "store was not closed after the statement released the connection"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(leaked.get(&cid).is_err());
        assert!(leaked.put_block(&cid, &data).is_err());
        let reopened = SqliteBlockStore::open(&path, 0).unwrap();
        assert_eq!(reopened.get(&cid).unwrap().unwrap().data(), data.as_slice());
    }

    #[test]
    fn stores_and_verifies_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"cached block";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();

        let block = store.get(&cid).unwrap().unwrap();
        assert_eq!(block.data(), data);
        assert_eq!(block.cid(), &cid);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn rejected_put_does_not_populate_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");

        assert!(store.put_block(&cid, b"invalid block").is_err());
        assert!(!store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn evicted_put_does_not_populate_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1).unwrap();
        let data = b"larger than cache budget";
        let cid = cid_from_data(CODEC_RAW, data);

        store.put_block(&cid, data).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
        assert!(!store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn cold_read_verifies_before_populating_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");
        let cid_bytes = block_key(&cid);
        store
            .conn
            .lock()
            .execute(
                r#"
                INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                "#,
                params![
                    &cid_bytes,
                    cid.codec() as i64,
                    13i64,
                    b"invalid block".as_slice(),
                    now_secs() as i64
                ],
            )
            .unwrap();

        let err = store.get(&cid).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Core(CoreError::HashMismatch { .. })
        ));
        assert!(!store.hot.lock().entries.contains_key(&cid_bytes));
    }

    #[test]
    fn range_read_verifies_cold_block_before_populating_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"valid block");
        let cid_bytes = block_key(&cid);
        store
            .conn
            .lock()
            .execute(
                r#"
                INSERT INTO blocks(cid, codec, size, data, inserted_at, last_accessed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?5)
                "#,
                params![
                    &cid_bytes,
                    cid.codec() as i64,
                    13i64,
                    b"invalid block".as_slice(),
                    now_secs() as i64
                ],
            )
            .unwrap();

        let err = store.get_range(&cid, 0, 4).unwrap_err();
        assert!(matches!(
            err,
            StoreError::Core(CoreError::HashMismatch { .. })
        ));
        assert!(!store.hot.lock().entries.contains_key(&cid_bytes));
    }

    #[test]
    fn hot_cache_range_hit_returns_requested_bytes() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"0123456789";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);

        assert_eq!(store.get_range(&cid, 2, 5).unwrap().unwrap(), b"2345");
        assert_eq!(block_last_accessed_at(&store, &cid), 1);
    }

    #[test]
    fn cold_range_read_populates_verified_hot_cache() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"cold range block";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        store.hot.lock().clear();

        assert_eq!(store.get_range(&cid, 5, 9).unwrap().unwrap(), b"range");

        assert!(store.hot.lock().entries.contains_key(&block_key(&cid)));
    }

    #[test]
    fn dag_pb_blocks_are_addressable_by_cidv0_and_cidv1() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"dag-pb alias block";
        let cidv1 = cid_from_data(CODEC_DAG_PB, data);
        let cidv0 = Cid::new_v0(*cidv1.hash()).unwrap();

        store.put_block(&cidv0, data).unwrap();

        assert_eq!(store.get(&cidv0).unwrap().unwrap().data(), data);
        let block = store.get(&cidv1).unwrap().unwrap();
        assert_eq!(block.cid(), &cidv1);
        assert_eq!(block.data(), data);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn cidv0_cidv1_aliases_share_retention_state() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cidv1 = cid_from_data(CODEC_DAG_PB, &first);
        let first_cidv0 = Cid::new_v0(*first_cidv1.hash()).unwrap();
        let second_cid = cid_from_data(CODEC_RAW, &second);

        store.put_block(&first_cidv0, &first).unwrap();
        store.retain_block(&first_cidv1).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert_eq!(store.get(&first_cidv0).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cidv1);
        store.put_block(&second_cid, &second).unwrap();

        assert!(store.get(&first_cidv0).unwrap().is_none());
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);
    }

    #[test]
    fn evicts_lru_blocks_when_over_budget() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert!(store.total_bytes().unwrap() <= 20);
        assert_eq!(store.get(&second_cid).unwrap().unwrap().data(), second);
    }

    #[test]
    fn clear_removes_hot_cache_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache clear";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        store
            .put_name_record(
                "example.test",
                "/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(60),
            )
            .unwrap();
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);
        assert!(store.get_name_record("example.test").unwrap().is_some());

        store.clear().unwrap();

        assert!(store.get(&cid).unwrap().is_none());
        assert!(store.get_name_record("example.test").unwrap().is_none());
    }

    #[test]
    fn hot_cache_hit_skips_redundant_sqlite_touch() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache touch skip";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);

        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        assert_eq!(block_last_accessed_at(&store, &cid), 1);
    }

    #[test]
    fn hot_cache_hit_periodically_refreshes_sqlite_touch() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache touch refresh";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        set_block_last_accessed_at(&store, &cid, 1);
        store
            .hot
            .lock()
            .entries
            .get_mut(&block_key(&cid))
            .unwrap()
            .last_persistent_touch = 0;

        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        assert!(block_last_accessed_at(&store, &cid) > 1);
    }

    #[test]
    fn trim_removes_hot_cache_entries() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"hot cache trim";
        let cid = cid_from_data(CODEC_RAW, data);
        store.put_block(&cid, data).unwrap();
        assert_eq!(store.get(&cid).unwrap().unwrap().data(), data);

        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
        assert!(store.get(&cid).unwrap().is_none());
    }

    #[test]
    fn trims_blocks_to_requested_budget() {
        let store = SqliteBlockStore::in_memory(1024).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        store.trim_blocks_to(20).unwrap();

        assert!(store.total_bytes().unwrap() <= 20);
        assert_eq!(store.block_count().unwrap(), 1);
    }

    #[test]
    fn retained_blocks_are_not_evicted_until_released() {
        let store = SqliteBlockStore::in_memory(20).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let third = vec![3u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        let third_cid = cid_from_data(CODEC_RAW, &third);

        store.put_block(&first_cid, &first).unwrap();
        store.retain_block(&first_cid).unwrap();
        store.put_block(&second_cid, &second).unwrap();

        assert_eq!(store.get(&first_cid).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cid);
        store.put_block(&third_cid, &third).unwrap();

        assert!(store.get(&first_cid).unwrap().is_none());
        assert_eq!(store.get(&third_cid).unwrap().unwrap().data(), third);
    }

    #[test]
    fn trim_skips_retained_blocks() {
        let store = SqliteBlockStore::in_memory(1024).unwrap();
        let first = vec![1u8; 16];
        let second = vec![2u8; 16];
        let first_cid = cid_from_data(CODEC_RAW, &first);
        let second_cid = cid_from_data(CODEC_RAW, &second);
        store.put_block(&first_cid, &first).unwrap();
        store.put_block(&second_cid, &second).unwrap();
        store.retain_block(&first_cid).unwrap();

        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.get(&first_cid).unwrap().unwrap().data(), first);
        assert!(store.get(&second_cid).unwrap().is_none());

        store.release_block(&first_cid);
        store.trim_blocks_to(0).unwrap();

        assert_eq!(store.block_count().unwrap(), 0);
    }

    fn set_block_last_accessed_at(store: &SqliteBlockStore, cid: &Cid, value: i64) {
        store
            .conn
            .lock()
            .execute(
                "UPDATE blocks SET last_accessed_at = ?1 WHERE cid = ?2",
                params![value, block_key(cid)],
            )
            .unwrap();
    }

    fn block_last_accessed_at(store: &SqliteBlockStore, cid: &Cid) -> i64 {
        store
            .conn
            .lock()
            .query_row(
                "SELECT last_accessed_at FROM blocks WHERE cid = ?1",
                params![block_key(cid)],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn caches_name_records_with_ttl() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        store
            .put_name_record(
                "example.test",
                "/ipfs/bafkqaddwgevxmmraojswg33smq",
                Duration::from_secs(60),
            )
            .unwrap();

        assert_eq!(
            store.get_name_record("example.test").unwrap().as_deref(),
            Some("/ipfs/bafkqaddwgevxmmraojswg33smq")
        );
        assert!(store.get_name_record("missing.test").unwrap().is_none());
    }

    #[test]
    fn skips_zero_ttl_name_records() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        store
            .put_name_record("example.test", "/ipfs/bafyroot", Duration::ZERO)
            .unwrap();

        assert!(store.get_name_record("example.test").unwrap().is_none());
    }

    #[test]
    fn bounds_name_cache_records() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();

        for index in 0..(MAX_NAME_CACHE_RECORDS + 2) {
            store
                .put_name_record(
                    &format!("name-{index:03}.test"),
                    "/ipfs/bafyroot",
                    Duration::from_secs(60),
                )
                .unwrap();
        }

        let count = store
            .conn
            .lock()
            .query_row("SELECT COUNT(*) FROM name_cache", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert_eq!(count, MAX_NAME_CACHE_RECORDS);
    }

    #[test]
    fn exports_cache_as_importable_car() {
        let source = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let data = b"car export block";
        let cid = cid_from_data(CODEC_RAW, data);
        source.put_block(&cid, data).unwrap();

        let car = source.export_car().unwrap();
        let target = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let imported = target.import_car(&car).unwrap();

        assert_eq!(imported, vec![cid]);
        assert_eq!(target.get(&cid).unwrap().unwrap().data(), data);
    }

    #[test]
    fn caches_provider_records_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"provider cache key");
        let providers = vec![CachedProviderRecord {
            id: Some("peer".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        }];

        store
            .put_provider_records(&cid, &providers, Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), Some(providers));

        store
            .put_provider_records(
                &cid,
                &[CachedProviderRecord {
                    id: None,
                    addrs: vec![],
                }],
                Duration::ZERO,
            )
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
    }

    #[test]
    fn caches_empty_provider_records_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"empty provider cache key");

        store
            .put_provider_records(&cid, &[], Duration::from_secs(60))
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), Some(Vec::new()));

        store
            .put_provider_records(&cid, &[], Duration::ZERO)
            .unwrap();
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
    }

    #[test]
    fn provider_cache_key_is_cid_representation_independent() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cidv1 = cid_from_data(freedom_ipfs_core::CODEC_DAG_PB, b"provider multihash key");
        let cidv0 = Cid::new_v0(*cidv1.hash()).unwrap();
        let providers = vec![CachedProviderRecord {
            id: Some("peer".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        }];

        store
            .put_provider_records(&cidv1, &providers, Duration::from_secs(60))
            .unwrap();

        assert_eq!(store.get_provider_records(&cidv0).unwrap(), Some(providers));
    }

    #[test]
    fn tracks_bad_providers_until_ttl_expires() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        store
            .mark_bad_provider("peer", "timeout", Duration::from_secs(60))
            .unwrap();
        assert!(store.is_bad_provider("peer").unwrap());

        store
            .mark_bad_provider("peer", "timeout", Duration::ZERO)
            .unwrap();
        assert!(!store.is_bad_provider("peer").unwrap());
    }

    #[test]
    fn clears_provider_metadata_without_removing_blocks() {
        let store = SqliteBlockStore::in_memory(1024 * 1024).unwrap();
        let cid = cid_from_data(CODEC_RAW, b"provider metadata");
        store.put_block(&cid, b"provider metadata").unwrap();
        store
            .put_provider_records(
                &cid,
                &[CachedProviderRecord {
                    id: Some("peer".to_string()),
                    addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
                }],
                Duration::from_secs(60),
            )
            .unwrap();
        store
            .mark_bad_provider("peer", "timeout", Duration::from_secs(60))
            .unwrap();

        store.clear_provider_metadata().unwrap();

        assert!(store.get(&cid).unwrap().is_some());
        assert_eq!(store.get_provider_records(&cid).unwrap(), None);
        assert!(!store.is_bad_provider("peer").unwrap());
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

    #[test]
    fn hot_index_churn_counts_keys_by_capacity_and_takes_room_first() {
        const CAPACITY: usize = 32 * 1024;
        let inputs: Vec<_> = (0..2_000u64)
            .map(|n| {
                let data = vec![n as u8; 1 + n as usize % 301];
                let cid = cid_from_data(CODEC_RAW, &data);
                // Keys with far more capacity than length.
                let mut key = Vec::with_capacity(700 + n as usize % 23);
                key.extend(cid.to_bytes());
                (key, data)
            })
            .collect();
        let origin = heap::live();
        let mut cache = VerifiedHotCache::new(CAPACITY as u64);
        for (key, data) in &inputs {
            // The incoming key is the caller's until the cache takes it.
            let mut incoming = Vec::with_capacity(key.capacity());
            incoming.extend(key);
            let incoming_bytes = incoming.capacity();
            heap::live();
            cache.put_input(incoming, HotInput::Copy(data), 0);
            let peak = heap::peak() - origin;
            let live = heap::live() - origin;
            assert!(
                live as usize <= cache.pool.used(),
                "live {live}, counted {}",
                cache.pool.used()
            );
            assert!(
                peak as usize <= CAPACITY + incoming_bytes,
                "peaked at {peak}"
            );
            assert!(cache.pool.used() <= CAPACITY);
        }
        drop(cache);
        assert_eq!(heap::live(), origin);
    }

    #[test]
    fn a_closed_store_keeps_no_hot_cache_and_no_lease() {
        let budget = freedom_ipfs_core::MemoryBudget::new(1 << 20);
        let store = SqliteBlockStore::in_memory_with(StoreOptions {
            max_bytes: 1 << 20,
            hot_cache_bytes: Some(64 * 1024),
            ..StoreOptions::default()
        })
        .unwrap();
        let data = vec![5u8; 10_000];
        let cid = cid_from_data(CODEC_RAW, &data);
        store.put_block(&cid, &data).unwrap();
        let lease = Arc::new(
            budget
                .try_reserve(store.hot_cache_capacity() as usize)
                .unwrap(),
        );
        assert!(store.keep_hot_cache_lease(lease.clone()));
        drop(lease);
        let reader = store.get(&cid).unwrap().unwrap();
        assert!(store.close(Duration::from_secs(1)));
        // Nothing of a hot cache is left, and none is counted.
        assert_eq!((store.hot_cache_capacity(), store.hot_cache_used()), (0, 0));
        assert!(store.hot.lock().pool.capacity() == 0 && store.hot.lock().entries.is_empty());
        let late = Arc::new(budget.try_reserve(100).unwrap());
        assert!(
            !store.keep_hot_cache_lease(late),
            "a closed store keeps no lease"
        );
        // The old cache's block a reader holds keeps the old lease.
        assert_eq!(budget.used(), 64 * 1024);
        assert_eq!(reader.data(), &data[..]);
        drop(reader);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn a_hot_cache_lease_stays_until_its_heap_is_freed() {
        let budget = freedom_ipfs_core::MemoryBudget::new(64 * 1024);
        let fetches = freedom_ipfs_core::MemoryBudget::new(1 << 20);
        let origin = heap::live();
        let mut cache = VerifiedHotCache::new(64 * 1024);
        let lease = Arc::new(budget.try_reserve(64 * 1024).unwrap());
        assert!(cache.pool.keep_lease(lease.clone()));
        let lease_address = Arc::as_ptr(&lease) as *const u8;
        drop(lease);
        for n in 0..40u32 {
            let data = n.to_le_bytes().repeat(100);
            let key = block_key(&cid_from_data(CODEC_RAW, &data));
            if n % 2 == 0 {
                cache.put_input(key, HotInput::Copy(&data), 0);
            } else {
                // Still charged, so the charged set keeps a copy of its key.
                let fetched = charged_view(&fetches, &data, data.len());
                cache.put_input(key, HotInput::Shared(fetched), 0);
            }
        }
        assert!(!cache.charged.is_empty());
        heap::watch_free(lease_address, &budget);
        drop(cache);
        let (live, _) = heap::watched_free().expect("the lease was freed");
        // When its reservation goes, all that is left of the cache is its
        // pool's counter, freed in the same drop, and the lease itself.
        let left = (live - origin) as usize;
        let lease_bytes = std::mem::size_of::<Reservation>() + 2 * std::mem::size_of::<usize>();
        assert!(
            left <= FixedPool::HEAP_BYTES + lease_bytes,
            "{left} bytes still allocated when the reservation went"
        );
        assert_eq!((budget.used(), fetches.used()), (0, 0));
        assert_eq!(heap::live(), origin);
    }

    #[test]
    fn disabled_and_tiny_hot_caches_count_their_own_heap() {
        for capacity in [0, 1, 64, 256] {
            let origin = heap::live();
            let cache = Arc::new(Mutex::new(VerifiedHotCache::new(capacity)));
            let live = heap::live() - origin;
            let counted = cache.lock().pool.used();
            // It caches nothing, but counts, and reports as its fixed
            // capacity, the heap it keeps.
            assert!(
                live as usize <= counted,
                "capacity {capacity}: heap {live}, counted {counted}"
            );
            assert_eq!(
                cache.lock().pool.capacity(),
                hot_cache_fixed_bytes(capacity)
            );
            assert!(counted <= cache.lock().pool.capacity());
            let data = b"x";
            cache.lock().put_input(
                block_key(&cid_from_data(CODEC_RAW, data)),
                HotInput::Copy(data),
                0,
            );
            assert!(cache.lock().entries.is_empty(), "caches nothing");
            drop(cache);
            assert_eq!(heap::live(), origin);
        }
        let off = SqliteBlockStore::in_memory_with(StoreOptions {
            hot_cache_bytes: Some(0),
            ..StoreOptions::default()
        })
        .unwrap();
        assert_eq!(off.hot_cache_capacity(), HOT_CACHE_OWN_BYTES as u64);
    }

    #[test]
    fn options_tell_the_hot_caches_fixed_capacity_before_the_store_opens() {
        for options in [
            StoreOptions::default(),
            StoreOptions {
                max_bytes: 1,
                ..StoreOptions::default()
            },
            StoreOptions {
                max_bytes: 3 << 20,
                ..StoreOptions::default()
            },
            StoreOptions {
                max_bytes: 1 << 30,
                ..StoreOptions::default()
            },
            StoreOptions {
                max_bytes: 1,
                hot_cache_bytes: Some(4 << 20),
                ..StoreOptions::default()
            },
            StoreOptions {
                hot_cache_bytes: Some(0),
                ..StoreOptions::default()
            },
            StoreOptions {
                hot_cache_bytes: Some(1),
                ..StoreOptions::default()
            },
        ] {
            let store = SqliteBlockStore::in_memory_with(options).unwrap();
            assert_eq!(
                options.hot_cache_fixed_bytes(),
                store.hot_cache_capacity(),
                "{options:?}"
            );
        }
        // A tiny target still keeps the cache's own structures.
        let tiny = StoreOptions {
            max_bytes: 1,
            ..StoreOptions::default()
        };
        assert_eq!(tiny.hot_cache_fixed_bytes(), HOT_CACHE_OWN_BYTES as u64);
        assert!(tiny.hot_cache_fixed_bytes() > tiny.max_bytes);
    }

    #[test]
    fn thousands_of_tiny_blocks_keep_the_hot_cache_within_its_capacity() {
        const CAPACITY: usize = 512 * 1024;
        let budget = freedom_ipfs_core::MemoryBudget::new(1 << 20);
        let blocks: Vec<Vec<u8>> = (0..6_000u32)
            .map(|index| index.to_le_bytes().repeat(1 + index as usize % 16))
            .collect();
        let cids: Vec<Cid> = blocks
            .iter()
            .map(|data| cid_from_data(CODEC_RAW, data))
            .collect();
        // Fetches still holding the last of their blocks, so some entries
        // stay charged a while.
        let mut fetching = std::collections::VecDeque::with_capacity(64);
        let origin = heap::live();
        let mut cache = VerifiedHotCache::new(CAPACITY as u64);
        let mut most = 0;
        for (index, (data, cid)) in blocks.iter().zip(&cids).enumerate() {
            let key = block_key(cid);
            match index % 3 {
                // Copied in, as a plain put is.
                0 => cache.put_input(key, HotInput::Copy(data), 0),
                // Shared, as a cold read is.
                1 => cache.put_verified(key, ChargedBytes::from_vec(data.clone()), 0),
                // Shared with the fetch that charged it.
                _ => {
                    let fetched = charged_view(&budget, data, data.len());
                    cache.put_input(key, HotInput::Shared(fetched.clone()), 0);
                    if fetching.len() == 50 {
                        fetching.pop_front();
                    }
                    fetching.push_back(fetched);
                }
            }
            let held = heap::live() - origin;
            let peak = heap::peak() - origin;
            let counted = cache.pool.used();
            most = most.max(cache.entries.len());
            // The block just put went in, older ones making way for it.
            assert!(cache.entries.contains_key(&block_key(cid)));
            // What it counts stays within its capacity, and what it really
            // holds on the heap (with the fetches' blocks it still covers)
            // within what it counts, at every step.
            assert!(counted <= CAPACITY, "counts {counted}");
            assert!(held as usize <= counted, "holds {held}, counts {counted}");
            assert!(peak as usize <= CAPACITY, "peaked at {peak}");
        }
        assert!(most > 1_000, "thousands of entries: {most}");
        assert!(cache.entries.len() < blocks.len(), "and evictions");
        assert!(!cache.charged.is_empty(), "some still charged");
        fetching.clear();
        cache.clear();
        assert_eq!(cache.entries.len(), 0);
        drop(cache);
        assert_eq!(heap::live(), origin, "all of it was the cache's");
        assert_eq!(budget.used(), 0);
    }
}
