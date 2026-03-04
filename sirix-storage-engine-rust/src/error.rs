//! Error types for the storage engine.

use std::io;

use thiserror::Error;

/// Result type alias for storage engine operations.
pub type Result<T> = std::result::Result<T, StorageError>;

/// Storage engine error variants.
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error from the underlying filesystem.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Page corruption detected (bad checksum, invalid magic, etc.).
    #[error("page corruption: {0}")]
    PageCorruption(String),

    /// Attempted to access a slot that is not occupied.
    #[error("slot {slot} is not occupied on page {page_key}")]
    SlotNotOccupied { page_key: i64, slot: usize },

    /// Page is full - no more heap space for new records.
    #[error("page {page_key} is full: need {needed} bytes, have {available} bytes")]
    PageFull {
        page_key: i64,
        needed: usize,
        available: usize,
    },

    /// Slot index out of range.
    #[error("slot index {slot} out of range (max {max})")]
    SlotOutOfRange { slot: usize, max: usize },

    /// Record exceeds maximum size.
    #[error("record size {size} exceeds maximum {max}")]
    RecordTooLarge { size: usize, max: usize },

    /// Compression/decompression failure.
    #[error("compression error: {0}")]
    Compression(String),

    /// Invalid page kind byte.
    #[error("unknown page kind: {0}")]
    UnknownPageKind(u8),

    /// Invalid binary encoding version.
    #[error("unknown binary encoding version: {0}")]
    UnknownEncodingVersion(u8),

    /// Revision not found.
    #[error("revision {0} not found")]
    RevisionNotFound(i32),

    /// Page guard frame was reused while still held.
    #[error("frame reuse detected: version was {expected}, now {actual}")]
    FrameReused { expected: u32, actual: u32 },

    /// The page has been closed and cannot be accessed.
    #[error("page {page_key} is closed")]
    PageClosed { page_key: i64 },

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Buffer too small for the requested operation.
    #[error("buffer too small: need {needed}, have {available}")]
    BufferTooSmall { needed: usize, available: usize },
}
