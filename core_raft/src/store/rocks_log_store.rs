use crate::network::node::{GroupId, TypeConfig};
use crate::store::raft_engine::{MessageExtTyped, create_raft_engine};
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use indexmap::IndexMap;
use lru::LruCache;
use meta::StoreMeta;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::type_config::TypeConfigExt;
use openraft::{Entry, LogState};
use raft_engine::{Engine, LogBatch};
use rand::Rng;
use rand::distributions::Alphanumeric;
use rocksdb::ColumnFamily;
use rocksdb::DB;
use rocksdb::Direction;
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, MutexGuard, RwLock};

const MEM_LOG_SIZE: usize = 2000;
#[derive(Clone)]
pub struct RocksLogStore {
    db: Arc<DB>,
    cache: Arc<Mutex<LruCache<u64, EntryOf<TypeConfig>>>>,
    _p: PhantomData<TypeConfig>,
    engine: Arc<Engine>,
    group_id: GroupId,
}
impl Debug for RocksLogStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksLogStore")
            .field("db", &self.db)
            .field("cache", &self.cache)
            .finish()
    }
}

impl RocksLogStore {
    pub fn new(db: Arc<DB>, group_id: GroupId) -> Self {
        // 明确指定类型
        let cache: LruCache<u64, EntryOf<TypeConfig>> =
            LruCache::new(NonZeroUsize::new(MEM_LOG_SIZE).expect("MEM_LOG_SIZE must be > 0"));
        db.cf_handle("meta")
            .expect("column family `meta` not found");
        db.cf_handle("logs")
            .expect("column family `logs` not found");

        let random_string: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(6)
            .map(char::from)
            .collect();
        let path = format!("E:/tmp/raft/raft-engine/+ {}", random_string);
        let engine = create_raft_engine(path);
        Self {
            db,
            cache: Arc::new(Mutex::new(cache)),
            _p: Default::default(),
            engine: Arc::new(engine),
            group_id,
        }
    }

    /// Try to satisfy the given range from cache.
    ///
    /// Returns Some(vec) if fully hit in cache; None if any element missing or end unbounded.
    async fn try_get_from_cache<RB: RangeBounds<u64> + Clone + Debug>(
        &self,
        range: RB,
    ) -> Option<Vec<EntryOf<TypeConfig>>> {
        // compute start index
        use std::ops::Bound;
        let start: u64 = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => x.saturating_add(1),
            Bound::Unbounded => 0,
        };
        // compute end index; if unbounded -> cannot satisfy from cache reliably
        let end_opt: Option<u64> = match range.end_bound() {
            Bound::Included(x) => Some(*x),
            Bound::Excluded(x) => {
                if *x == 0 {
                    return Some(Vec::new()); // empty range
                } else {
                    Some(x.saturating_sub(1))
                }
            }
            Bound::Unbounded => None,
        };

        let end = match end_opt {
            None => return None, // unbounded end -> fall back to rocksdb
            Some(e) => e,
        };

        if start > end {
            return Some(Vec::new()); // empty range
        }

        // quick check: if requested length is larger than cache capacity -> miss
        let len_needed = end.saturating_sub(start).saturating_add(1);
        if len_needed as usize > MEM_LOG_SIZE {
            return None;
        }

        let mut out = Vec::with_capacity(len_needed as usize);
        let mut cache: MutexGuard<LruCache<u64, Entry<TypeConfig>>> = self.cache.lock().await;
        for idx in start..=end {
            match cache.get(&idx) {
                Some(ent) => out.push(ent.clone()),
                None => return None,
            }
        }
        Some(out)
    }

    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    /// Get a store metadata.
    ///
    /// It returns `None` if the store does not have such a metadata stored.
    fn get_meta<M: StoreMeta<TypeConfig>>(&self) -> Result<Option<M::Value>, io::Error> {
        let bytes = self
            .db
            .get_cf(self.cf_meta(), M::KEY)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let t = bincode2::deserialize(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(t))
    }

    /// Save a store metadata.
    fn put_meta<M: StoreMeta<TypeConfig>>(&self, value: &M::Value) -> Result<(), io::Error> {
        let bin_value = bincode2::serialize(value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, bin_value)
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for RocksLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<TypeConfig as RaftTypeConfig>::Entry>, io::Error> {
        // if let Some(cached) = self.try_get_from_cache(range.clone()).await {
        //     tracing::info!(
        //         "cache hit for range start={:?},end ={:?}",
        //         range.start_bound(),
        //         range.end_bound()
        //     );
        //     return Ok(cached);
        // }
        // tracing::warn!(
        //     "start={:?},end ={:?}",
        //     range.start_bound(),
        //     range.end_bound()
        // );

        let mut start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1, // 排除转换为包含
            Bound::Unbounded => 0,        // 从0开始
        };

        let mut end = match range.end_bound() {
            Bound::Included(&n) => n + 1, // 包含转换为不包含
            Bound::Excluded(&n) => n,
            Bound::Unbounded => u64::MAX, // 到最大值
        };

        let mut res = Vec::new();
        // let it = self.db.iterator_cf(
        //     self.cf_logs(),
        //     rocksdb::IteratorMode::From(&start, Direction::Forward),
        // );
        // for item_res in it {
        //     let (id, val) = item_res.map_err(read_logs_err)?;
        //     let id = bin_to_id(&id);
        //     if !range.contains(&id) {
        //         break;
        //     }
        //     let entry: EntryOf<TypeConfig> = bincode2::deserialize(&val).map_err(read_logs_err)?;
        //     assert_eq!(id, entry.index());
        //     res.push(entry);
        // }
        match self.engine.last_index(self.group_id as u64) {
            None => {
                return Ok(res);
            }
            Some(x) => {
                end = (x + 1).min(end);
                start = (x + 1).min(start);
            }
        }
        self.engine
            .fetch_entries_to::<MessageExtTyped>(self.group_id as u64, start, end, None, &mut res)
            .unwrap();
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        self.get_meta::<meta::Vote>()
    }
}

impl RaftLogStorage<TypeConfig> for RocksLogStore {
    type LogReader = Self;

    //不会在每次提交条目时被调用，但重启等场景会调用
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        // let last = self
        //     .db
        //     .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
        //     .next();
        //
        // let last_log_id = match last {
        //     None => None,
        //     Some(res) => {
        //         let (_log_index, entry_bytes) = res.map_err(read_logs_err)?;
        //         let ent: Entry<TypeConfig> = bincode2::deserialize::<EntryOf<TypeConfig>>(&entry_bytes)
        //             .map_err(read_logs_err)?;
        //         Some(ent.log_id())
        //     }
        // };
        let last_log_id = match self.engine.last_index(self.group_id as u64) {
            None => None, //  只要 last_index 为 None，直接返回 None
            Some(i) => {
                match self
                    .engine
                    .get_entry::<MessageExtTyped>(self.group_id as u64, i)
                    .unwrap()
                {
                    None => None, //  get_entry 为 None 也返回 None
                    Some(entry) => Some(entry.log_id()),
                }
            }
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;
        let last_log_id = match last_log_id {
            None => last_purged_log_id.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        self.put_meta::<meta::Vote>(vote)?;
        // Vote must be persisted to disk before returning.
        let db = self.db.clone();
        TypeConfig::spawn_blocking(move || {
            db.flush_wal(true)
                .map_err(|e| io::Error::other(e.to_string()))
        })
        .await??;
        Ok(())
    }

    //心跳不会走到这里
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
    {
        let start = Instant::now();
        let mut batch = LogBatch::with_capacity(256);
        let x: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        batch
            .add_entries::<MessageExtTyped>(self.group_id as u64, &x)
            .unwrap();

        // for entry in entries {
        //     let id = id_to_bin(entry.index());
        //     self.db
        //         .put_cf(
        //             self.cf_logs(),
        //             id,
        //             bincode2::serialize(&entry)
        //                 .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        //         )
        //         .map_err(|e| io::Error::other(e.to_string()))?;
        // }
        //提前释放
        // 在调用回调函数之前，确保日志已经持久化到磁盘。
        //
        // 但上面的 `pub_cf()` 必须在这个函数中调用，而不能放到另一个任务里。
        // 因为当函数返回时，需要能够读取到这些日志条目。
        // let db = self.db.clone();
        let res = self.engine
            .write(&mut batch, false)
            .map(|_| ())
            .map_err(io::Error::other);
        let engine = self.engine.clone();
        std::thread::spawn(move || {
            let res = engine.sync().map(|_| ())
                .map_err(io::Error::other);
            callback.io_completed(res);
            let elapsed = start.elapsed();
            tracing::info!("rocksdb append elapsed: {:?}", elapsed);
        });
        // Return now, and the callback will be invoked later when IO is done.
        Ok(())
    }

    // 如果follower的日志与leader的日志不匹配，follower会删除冲突的日志
    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        // tracing::info!("truncate_after: ({:?}, +oo)", last_log_id);
        //
        // let start_index = match last_log_id {
        //     Some(log_id) => log_id.index() + 1,
        //     None => 0,
        // };
        //
        // let from = id_to_bin(start_index);
        // let to = id_to_bin(u64::MAX);
        // self.db
        //     .delete_range_cf(self.cf_logs(), &from, &to)
        //     .map_err(|e| io::Error::other(e.to_string()))?;

        // Truncating does not need to be persisted.
        Ok(())
    }

    //日志压缩
    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        tracing::info!("delete_log: [0, {:?}]", log_id);

        // 在清理日志前记录最后清理的日志ID。
        // openraft 将忽略最后清理日志ID及之前的所有日志。
        // 因此，无需在事务中执行此操作
        self.put_meta::<meta::LastPurged>(&log_id)?;

        // let from = id_to_bin(0);
        // let to = id_to_bin(log_id.index() + 1);
        // //删除指定范围内的所有数据
        // self.db
        //     .delete_range_cf(self.cf_logs(), &from, &to)
        //     .map_err(|e| io::Error::other(e.to_string()))?;

        self.engine
            .compact_to(self.group_id as u64, log_id.index + 1);

        // Purging does not need to be persistent.
        Ok(())
    }
}

/// Metadata of a raft-store.
///
/// In raft, except logs and state machine, the store also has to store several piece of metadata.
/// This sub mod defines the key-value pairs of these metadata.
mod meta {
    use openraft::RaftTypeConfig;
    use openraft::alias::LogIdOf;
    use openraft::alias::VoteOf;

    /// Defines metadata key and value
    pub(crate) trait StoreMeta<C>
    where
        C: RaftTypeConfig,
    {
        /// The key used to store in rocksdb
        const KEY: &'static str;

        /// The type of the value to store
        type Value: serde::Serialize + serde::de::DeserializeOwned;
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct Vote {}

    impl<C> StoreMeta<C> for LastPurged
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogIdOf<C>;
    }
    impl<C> StoreMeta<C> for Vote
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "vote";
        type Value = VoteOf<C>;
    }
}

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

fn read_logs_err(e: impl Error + 'static) -> io::Error {
    io::Error::other(e.to_string())
}
