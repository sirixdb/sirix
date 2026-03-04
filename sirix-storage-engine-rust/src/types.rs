//! Core type definitions for the storage engine.

/// Page kind discriminator stored as the first byte of every serialized page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PageKind {
    KeyValueLeaf = 1,
    Name = 2,
    Uber = 3,
    Indirect = 4,
    RevisionRoot = 5,
}

impl PageKind {
    /// Convert from raw byte. Returns `None` for unknown kinds.
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::KeyValueLeaf),
            2 => Some(Self::Name),
            3 => Some(Self::Uber),
            4 => Some(Self::Indirect),
            5 => Some(Self::RevisionRoot),
            _ => None,
        }
    }
}

/// Binary encoding version for forward/backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BinaryEncodingVersion {
    /// Version 0 - original format, NEVER changes.
    V0 = 0,
}

impl BinaryEncodingVersion {
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::V0),
            _ => None,
        }
    }
}

/// Index type discriminator for different index structures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum IndexType {
    Document = 0,
    ChangedNodes = 1,
    RecordToRevisions = 2,
    PathSummary = 3,
    Name = 4,
    Cas = 5,
    Path = 6,
    DeweyId = 7,
}

impl IndexType {
    #[inline]
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Document),
            1 => Some(Self::ChangedNodes),
            2 => Some(Self::RecordToRevisions),
            3 => Some(Self::PathSummary),
            4 => Some(Self::Name),
            5 => Some(Self::Cas),
            6 => Some(Self::Path),
            7 => Some(Self::DeweyId),
            _ => None,
        }
    }
}

/// Serialization type for different I/O contexts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationType {
    /// Normal data serialization.
    Data,
    /// Transaction intent log serialization.
    TransactionIntentLog,
}

/// Versioning strategy for page snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersioningType {
    /// Full snapshot - every revision stores the complete page.
    Full,
    /// Incremental - only changed slots are stored per revision.
    Incremental,
    /// Differential - changes relative to a base revision.
    Differential,
    /// Sliding snapshot with a configurable window.
    SlidingSnapshot { window_size: u32 },
}

/// A key identifying a page fragment for versioned pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageFragmentKey {
    /// Revision number of this fragment.
    pub revision: i32,
    /// Byte offset in the data file.
    pub offset: i64,
}

/// Variable-length integer encoding utilities matching Java's `Utils.putVarLong`/`getVarLong`.
pub mod varint {
    /// Encode an i64 as a variable-length integer into the buffer.
    /// Returns the number of bytes written.
    #[inline]
    pub fn encode_i64(value: i64, buf: &mut [u8]) -> usize {
        let mut v = ((value as u64) << 1) ^ ((value >> 63) as u64); // zigzag
        let mut i = 0;
        while v >= 0x80 {
            buf[i] = (v as u8) | 0x80;
            v >>= 7;
            i += 1;
        }
        buf[i] = v as u8;
        i + 1
    }

    /// Decode a variable-length i64 from the buffer.
    /// Returns `(value, bytes_consumed)`.
    #[inline]
    pub fn decode_i64(buf: &[u8]) -> (i64, usize) {
        let mut result: u64 = 0;
        let mut shift = 0;
        let mut i = 0;
        loop {
            let b = buf[i];
            result |= ((b & 0x7F) as u64) << shift;
            i += 1;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        // zigzag decode
        let decoded = ((result >> 1) as i64) ^ -((result & 1) as i64);
        (decoded, i)
    }

    /// Maximum bytes needed to encode a varint i64.
    pub const MAX_VARINT_LEN: usize = 10;
}
