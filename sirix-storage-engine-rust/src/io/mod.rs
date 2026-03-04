//! Storage I/O layer - Reader/Writer traits and FileChannel backend.
//!
//! Equivalent to Java's `io.sirix.io` package with `Reader`, `Writer`,
//! and `FileChannelStorage` implementations.

pub mod reader;
pub mod writer;
pub mod file_channel;

pub use reader::StorageReader;
pub use writer::StorageWriter;
pub use file_channel::FileChannelStorage;
