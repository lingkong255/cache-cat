use moka::Expiry;
use moka::sync::Cache;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct MyValue {
    pub data: Arc<Vec<u8>>,
    pub ttl_ms: u64,
}

/// 自定义 Expiry，实现按插入项返回不同过期时间
struct MyExpiry;

impl Expiry<String, MyValue> for MyExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &MyValue,
        _created_at: Instant,
    ) -> Option<Duration> {
        // 根据 value.ttl_ms 返回一个 Duration
        Some(Duration::from_millis(value.ttl_ms))
    }
    fn expire_after_update(
        &self,
        key: &String,
        value: &MyValue,
        updated_at: Instant,
        duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(Duration::from_millis(value.ttl_ms))
    }
}

// 全局静态缓存
static CACHE: OnceLock<Cache<String, MyValue>> = OnceLock::new();

/// 显式初始化缓存（应在程序启动阶段调用一次）
pub fn init_cache() {
    let cache = Cache::builder()
        .max_capacity(10_000)
        .expire_after(MyExpiry)
        .build();
    CACHE
        .set(cache)
        .expect("CACHE has already been initialized");
}

/// 获取已初始化的缓存
pub fn get_cache() -> &'static Cache<String, MyValue> {
    CACHE
        .get()
        .expect("CACHE is not initialized, call init_cache first")
}
