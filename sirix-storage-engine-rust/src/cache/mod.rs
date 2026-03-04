//! Cache and buffer management.
//!
//! Implements:
//! - PageGuard: LeanStore-style guard pattern for preventing eviction
//! - BufferPool: Striped buffer pool for reuse of page-sized allocations
//! - TransactionIntentLog: Epoch-based page modification tracking
//! - PageCache: LeanStore/Umbra-style page cache with clock eviction

pub mod page_guard;
pub mod buffer_pool;
pub mod transaction_intent_log;
pub mod page_cache;

pub use page_guard::PageGuard;
pub use buffer_pool::BufferPool;
pub use transaction_intent_log::TransactionIntentLog;
pub use page_cache::PageCache;
