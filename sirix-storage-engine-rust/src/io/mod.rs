//! Storage I/O layer - Reader/Writer traits, FileChannel, MemoryMapped backends.
//!
//! Equivalent to Java's `io.sirix.io` package with `Reader`, `Writer`,
//! `FileChannelStorage`, and `MMFileStorage` implementations.

pub mod reader;
pub mod writer;
pub mod file_channel;
pub mod mmap;

pub use reader::StorageReader;
pub use reader::RevisionFileData;
pub use writer::StorageWriter;
pub use file_channel::FileChannelReader;
pub use file_channel::FileChannelWriter;
pub use mmap::MMFileReader;
pub use mmap::MMFileWriter;
