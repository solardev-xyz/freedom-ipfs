use cid::Cid;
use freedom_ipfs_core::{
    block_data_range, encode_car_v1, parse_car_v1, verify_block, Block, BlockProvider, CarBlock,
    ChargedBytes, CoreError, FixedPool, HeldBytes, Result as CoreResult,
};
use parking_lot::Mutex;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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
        let max_bytes = if options.max_bytes == 0 {
            DEFAULT_CACHE_BYTES
        } else {
            options.max_bytes
        };
        let hot_bytes = options
            .hot_cache_bytes
            .unwrap_or_else(|| hot_cache_bytes(max_bytes));
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
        *self.hot.lock() = VerifiedHotCache::new(0);
        true
    }

    /// Stores a block. The hot cache, if it keeps the block, keeps a copy
    /// of its own.
    pub fn put_block(&self, cid: &Cid, data: &[u8]) -> Result<()> {
        self.put_verified_block(cid, data, || ChargedBytes::copy_from_slice(data))
    }

    /// Stores a block from bytes that may carry memory charges. The hot
    /// cache, if it keeps the block, shares those bytes rather than copying
    /// them: it counts them in its own capacity, and takes over their charge
    /// once it holds the last reference (until then they count twice).
    pub fn put_charged(&self, cid: &Cid, data: &ChargedBytes) -> Result<()> {
        self.put_verified_block(cid, data, || data.clone())
    }

    fn put_verified_block(
        &self,
        cid: &Cid,
        data: &[u8],
        hot_data: impl FnOnce() -> ChargedBytes,
    ) -> Result<()> {
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
            self.hot.lock().put_verified(cid_bytes, hot_data(), now);
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
    /// more: it counts each block by its allocation's size, for as long as
    /// the allocation lives, including after the cache has let it go.
    pub fn hot_cache_capacity(&self) -> u64 {
        self.hot.lock().max_bytes
    }

    /// The bytes the hot cache covers now, of its fixed capacity.
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
// allocation's size (not its length) for as long as the allocation lives,
// so a block a reader still holds after the cache evicted it stays counted,
// and the cache never covers more than its capacity. A block shared with
// the fetch that charged it dynamically counts twice until the cache holds
// the last reference; then the cache takes the charge over.
struct VerifiedHotCache {
    max_bytes: u64,
    pool: FixedPool,
    clock: u64,
    entries: HashMap<Vec<u8>, VerifiedHotBlock>,
    /// Entries whose bytes still carry a dynamic charge, to hand over once
    /// the cache holds them alone.
    charged: HashSet<Vec<u8>>,
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

impl VerifiedHotCache {
    fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            pool: FixedPool::new(usize::try_from(max_bytes).unwrap_or(usize::MAX)),
            clock: 0,
            entries: HashMap::new(),
            charged: HashSet::new(),
        }
    }

    /// Takes over the dynamic charges of the entries the cache now holds
    /// alone.
    fn take_over_charges(&mut self) {
        let entries = &self.entries;
        self.charged.retain(|cid| {
            entries.get(cid).is_some_and(|entry| {
                !entry.data.take_over_if_only_held() && entry.data.bytes().charged_bytes() > 0
            })
        });
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
        self.take_over_charges();
        if self.max_bytes == 0 || data.backing_capacity() as u64 > self.max_bytes {
            self.remove(&cid);
            return;
        }
        self.remove(&cid);
        // Bytes another pool covers are copied: one cover per allocation.
        let mut data = data;
        if data.is_covered() && !self.pool.covers(&data) {
            data = ChargedBytes::copy_from_slice(&data);
        }
        // Make room, oldest first. Evicting a block a reader still holds
        // frees no room: it stays covered until it is freed.
        let data = loop {
            if let Some(held) = self.pool.hold(&data) {
                break held;
            }
            if !self.evict_oldest() {
                return;
            }
        };
        self.clock = self.clock.saturating_add(1);
        if data.bytes().charged_bytes() > 0 {
            self.charged.insert(cid.clone());
        }
        self.entries.insert(
            cid,
            VerifiedHotBlock {
                data,
                last_accessed: self.clock,
                last_persistent_touch: now,
            },
        );
        self.take_over_charges();
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(cid) = self
            .entries
            .iter()
            .min_by_key(|(_, block)| block.last_accessed)
            .map(|(cid, _)| cid.clone())
        else {
            return false;
        };
        self.remove(&cid);
        true
    }

    fn remove(&mut self, cid: &[u8]) {
        self.entries.remove(cid);
        self.charged.remove(cid);
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.charged.clear();
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
        assert_eq!(store.hot.lock().max_bytes, 1024 * 1024);
        let store = SqliteBlockStore::in_memory(0).unwrap();
        assert_eq!(store.max_bytes, DEFAULT_CACHE_BYTES);
        assert_eq!(store.hot.lock().max_bytes, DEFAULT_HOT_CACHE_BYTES);
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
        // allocation in the cache's capacity, and its dynamic charge.
        store.put_charged(&cid, &fetched).unwrap();
        assert_eq!(store.hot_cache_used(), 4_000);
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
        assert_eq!(store.hot_cache_used(), 4_000);
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
        let held = store.hot_cache_used();
        drop(reader);
        assert_eq!(store.hot_cache_used(), held - 4_000);
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
        assert_eq!(large.hot.lock().max_bytes, 64 * 1024 * 1024);
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
}
