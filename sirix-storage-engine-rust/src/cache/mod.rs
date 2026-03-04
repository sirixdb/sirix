//! Cache and buffer management.
//!
//! Implements:
//! - PageGuard: LeanStore-style guard pattern for preventing eviction
//! - BufferPool: Striped buffer pool for reuse of page-sized allocations

pub mod page_guard;
pub mod buffer_pool;

pub use page_guard::PageGuard;
pub use buffer_pool::BufferPool;
