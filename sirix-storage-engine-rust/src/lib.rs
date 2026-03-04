//! SirixDB Storage Engine - High-performance temporal storage engine in Rust.
//!
//! This crate implements the core storage engine for SirixDB, providing:
//! - Slotted page layout with bump-allocated heap
//! - Multi-version concurrency control (MVCC) with temporal versioning
//! - Pluggable compression pipeline (LZ4, etc.)
//! - FileChannel-based I/O with striped buffer pools
//! - LeanStore-inspired page guard pattern for cache management

pub mod constants;
pub mod error;
pub mod types;
pub mod page;
pub mod io;
pub mod cache;
pub mod compression;
pub mod revision;
pub mod versioning;

pub use error::StorageError;
pub use error::Result;
