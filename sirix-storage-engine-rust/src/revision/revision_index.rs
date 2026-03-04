//! RevisionIndex - fast timestamp-to-revision lookup via binary search.
//!
//! Stores parallel arrays of timestamps and file offsets, enabling
//! O(log n) lookups for point-in-time queries.

use crate::error::Result;
use crate::error::StorageError;

/// Immutable revision index mapping revision numbers to file offsets and timestamps.
///
/// Optimized for binary search on timestamps. Updated atomically when
/// new revisions are committed.
#[derive(Clone)]
pub struct RevisionIndex {
    /// Timestamps in nanoseconds since UNIX epoch, sorted ascending.
    timestamps: Vec<i64>,

    /// Corresponding file offsets for each revision.
    offsets: Vec<i64>,
}

impl RevisionIndex {
    /// Create a new empty revision index.
    pub fn new() -> Self {
        Self {
            timestamps: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Create a revision index from pre-built arrays.
    ///
    /// # Panics
    /// Panics if `timestamps` and `offsets` have different lengths.
    pub fn from_arrays(timestamps: Vec<i64>, offsets: Vec<i64>) -> Self {
        assert_eq!(
            timestamps.len(),
            offsets.len(),
            "timestamps and offsets must have the same length"
        );
        Self {
            timestamps,
            offsets,
        }
    }

    /// Number of revisions in the index.
    #[inline]
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Whether the index is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    /// Get the file offset for a given revision number (0-based).
    #[inline]
    pub fn get_offset(&self, revision: usize) -> Result<i64> {
        self.offsets
            .get(revision)
            .copied()
            .ok_or(StorageError::RevisionNotFound(revision as i32))
    }

    /// Get the timestamp for a given revision number (0-based).
    #[inline]
    pub fn get_timestamp(&self, revision: usize) -> Result<i64> {
        self.timestamps
            .get(revision)
            .copied()
            .ok_or(StorageError::RevisionNotFound(revision as i32))
    }

    /// Find the revision number for a given timestamp using binary search.
    ///
    /// Returns the latest revision whose timestamp is <= the target.
    /// Returns `None` if no revision exists at or before the target timestamp.
    pub fn find_revision_by_timestamp(&self, target_nanos: i64) -> Option<usize> {
        if self.timestamps.is_empty() {
            return None;
        }

        match self.timestamps.binary_search(&target_nanos) {
            Ok(idx) => Some(idx),
            Err(idx) => {
                if idx == 0 {
                    None // all timestamps are after the target
                } else {
                    Some(idx - 1) // latest revision before target
                }
            }
        }
    }

    /// Find the exact revision for a given timestamp.
    /// Returns `None` if no revision matches exactly.
    pub fn find_exact_revision(&self, target_nanos: i64) -> Option<usize> {
        self.timestamps.binary_search(&target_nanos).ok()
    }

    /// Append a new revision entry.
    ///
    /// The timestamp must be >= the last entry's timestamp (monotonic).
    pub fn append(&mut self, timestamp_nanos: i64, offset: i64) -> Result<()> {
        if let Some(&last) = self.timestamps.last() {
            if timestamp_nanos < last {
                return Err(StorageError::Serialization(
                    "revision timestamps must be monotonically increasing".into(),
                ));
            }
        }
        self.timestamps.push(timestamp_nanos);
        self.offsets.push(offset);
        Ok(())
    }

    /// Serialize the revision index.
    ///
    /// Format:
    /// - entry_count (u32, 4 bytes)
    /// - timestamps (entry_count × i64)
    /// - offsets (entry_count × i64)
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        let count = self.timestamps.len() as u32;
        buf.reserve(4 + (count as usize) * 16);
        buf.extend_from_slice(&count.to_le_bytes());
        for &ts in &self.timestamps {
            buf.extend_from_slice(&ts.to_le_bytes());
        }
        for &off in &self.offsets {
            buf.extend_from_slice(&off.to_le_bytes());
        }
    }

    /// Deserialize a revision index.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(StorageError::PageCorruption(
                "revision index too short".into(),
            ));
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let expected_size = 4 + count * 16;
        if data.len() < expected_size {
            return Err(StorageError::PageCorruption(
                "revision index data truncated".into(),
            ));
        }

        let mut timestamps = Vec::with_capacity(count);
        let mut offsets = Vec::with_capacity(count);

        let mut cursor = 4;
        for _ in 0..count {
            timestamps.push(i64::from_le_bytes(
                data[cursor..cursor + 8].try_into().unwrap(),
            ));
            cursor += 8;
        }
        for _ in 0..count {
            offsets.push(i64::from_le_bytes(
                data[cursor..cursor + 8].try_into().unwrap(),
            ));
            cursor += 8;
        }

        Ok(Self {
            timestamps,
            offsets,
        })
    }

    /// Get the latest revision number (0-based), or None if empty.
    #[inline]
    pub fn latest_revision(&self) -> Option<usize> {
        if self.timestamps.is_empty() {
            None
        } else {
            Some(self.timestamps.len() - 1)
        }
    }
}

impl Default for RevisionIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_index() {
        let idx = RevisionIndex::new();
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);
        assert!(idx.find_revision_by_timestamp(1000).is_none());
    }

    #[test]
    fn test_append_and_lookup() {
        let mut idx = RevisionIndex::new();
        idx.append(1000, 0).unwrap();
        idx.append(2000, 4096).unwrap();
        idx.append(3000, 8192).unwrap();

        assert_eq!(idx.len(), 3);
        assert_eq!(idx.get_offset(0).unwrap(), 0);
        assert_eq!(idx.get_offset(1).unwrap(), 4096);
        assert_eq!(idx.get_offset(2).unwrap(), 8192);

        assert_eq!(idx.get_timestamp(0).unwrap(), 1000);
    }

    #[test]
    fn test_binary_search_exact() {
        let mut idx = RevisionIndex::new();
        idx.append(1000, 0).unwrap();
        idx.append(2000, 100).unwrap();
        idx.append(3000, 200).unwrap();

        assert_eq!(idx.find_revision_by_timestamp(2000), Some(1));
        assert_eq!(idx.find_exact_revision(2000), Some(1));
    }

    #[test]
    fn test_binary_search_between() {
        let mut idx = RevisionIndex::new();
        idx.append(1000, 0).unwrap();
        idx.append(3000, 100).unwrap();
        idx.append(5000, 200).unwrap();

        // 2500 is between rev 0 (1000) and rev 1 (3000) -> returns rev 0
        assert_eq!(idx.find_revision_by_timestamp(2500), Some(0));
        // 4000 is between rev 1 (3000) and rev 2 (5000) -> returns rev 1
        assert_eq!(idx.find_revision_by_timestamp(4000), Some(1));
    }

    #[test]
    fn test_binary_search_before_first() {
        let mut idx = RevisionIndex::new();
        idx.append(1000, 0).unwrap();
        assert!(idx.find_revision_by_timestamp(500).is_none());
    }

    #[test]
    fn test_non_monotonic_append_fails() {
        let mut idx = RevisionIndex::new();
        idx.append(2000, 0).unwrap();
        assert!(idx.append(1000, 100).is_err());
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut idx = RevisionIndex::new();
        idx.append(100, 0).unwrap();
        idx.append(200, 4096).unwrap();
        idx.append(300, 8192).unwrap();

        let mut buf = Vec::new();
        idx.serialize(&mut buf);

        let restored = RevisionIndex::deserialize(&buf).unwrap();
        assert_eq!(restored.len(), 3);
        assert_eq!(restored.get_offset(0).unwrap(), 0);
        assert_eq!(restored.get_timestamp(2).unwrap(), 300);
    }

    #[test]
    fn test_from_arrays() {
        let idx = RevisionIndex::from_arrays(vec![10, 20, 30], vec![100, 200, 300]);
        assert_eq!(idx.len(), 3);
        assert_eq!(idx.find_exact_revision(20), Some(1));
    }
}
