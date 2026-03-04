//! Core constants matching the Java SirixDB storage engine.
//!
//! These values define the on-disk format and must remain in sync
//! with the Java implementation for cross-compatibility.

// === File Layout ===

/// Alignment for UberPage writes (bytes).
pub const UBER_PAGE_BYTE_ALIGN: u16 = 512;

/// Alignment for RevisionRootPage writes (bytes).
pub const REVISION_ROOT_PAGE_BYTE_ALIGN: u16 = 256;

/// Alignment for page fragment writes (bytes).
pub const PAGE_FRAGMENT_BYTE_ALIGN: u16 = 8;

/// Offset of the first beacon (UberPage location) in the data file.
/// Computed as `UBER_PAGE_BYTE_ALIGN << 1`.
pub const FIRST_BEACON: u64 = (UBER_PAGE_BYTE_ALIGN as u64) << 1; // 1024

/// Size of subsequent beacon pointers (bytes).
pub const OTHER_BEACON: usize = 4; // size_of::<i32>()

/// Main data file name.
pub const DATA_FILENAME: &str = "sirix.data";

/// Revision offset index file name.
pub const REVISIONS_FILENAME: &str = "sirix.revisions";

// === Slotted Page Structure ===

/// Default initial page size in bytes (64 KiB).
pub const INITIAL_PAGE_SIZE: usize = 64 * 1024;

/// Fixed header size at the start of each page (bytes).
pub const HEADER_SIZE: usize = 32;

/// Bitmap size: 16 × 8 bytes = 128 bytes (1024 bits for slot presence).
pub const BITMAP_SIZE: usize = 128;

/// Number of 64-bit words in the bitmap.
pub const BITMAP_WORDS: usize = 16;

/// On-disk header + bitmap size (never changes across versions).
pub const DISK_HEADER_BITMAP_SIZE: usize = HEADER_SIZE + BITMAP_SIZE; // 160

/// Runtime-only preservation bitmap size (bytes).
pub const PRESERVATION_BITMAP_SIZE: usize = 128;

/// Number of slots per page.
pub const SLOT_COUNT: usize = 1024;

/// Log2 of SLOT_COUNT.
pub const SLOT_COUNT_EXPONENT: u32 = 10;

/// Slot directory size: 1024 × 8 bytes = 8192 bytes.
pub const DIR_SIZE: usize = SLOT_COUNT * DIR_ENTRY_SIZE;

/// Each directory entry is 8 bytes.
pub const DIR_ENTRY_SIZE: usize = 8;

/// Offset where the heap starts in the runtime page layout.
/// `DISK_HEADER_BITMAP_SIZE + PRESERVATION_BITMAP_SIZE + DIR_SIZE`
pub const HEAP_START: usize = DISK_HEADER_BITMAP_SIZE + PRESERVATION_BITMAP_SIZE + DIR_SIZE; // 8480

// === Indirect Page Tree ===

/// Number of references per indirect page.
pub const INP_REFERENCE_COUNT: usize = 1024;

/// Log2 of `INP_REFERENCE_COUNT`.
pub const INP_REFERENCE_COUNT_EXPONENT: u32 = 10;

/// Level-to-key exponents for the 7-level indirect page tree.
/// Level 0 covers keys 0..2^70, level 7 covers keys 0..2^10.
pub const INP_LEVEL_PAGE_COUNT_EXPONENT: [u32; 8] = [70, 60, 50, 40, 30, 20, 10, 0];

/// Number of records (nodes) per data page.
pub const NDP_NODE_COUNT: usize = 1024;

/// Log2 of `NDP_NODE_COUNT`.
pub const NDP_NODE_COUNT_EXPONENT: u32 = 10;

// === Record Sizing ===

/// Maximum size of a single record in bytes.
pub const MAX_RECORD_SIZE: usize = 500;

/// Maximum total size of records on a page.
pub const MAX_PAGE_RECORD_SIZE: usize = 150_000;

/// Maximum DeweyID size in bytes.
pub const MAX_DEWEY_ID_SIZE: usize = 100;

// === Sentinel Values ===

/// Null revision sentinel.
pub const NULL_REVISION_NUMBER: i32 = -1;

/// Null ID sentinel (i64).
pub const NULL_ID_LONG: i64 = -15;

/// Null ID sentinel (i32).
pub const NULL_ID_INT: i32 = -15;

// === I/O Tuning ===

/// Buffered writer flush threshold (bytes).
pub const FLUSH_SIZE: usize = 64_000;

/// Per-stripe direct buffer size (128 KiB).
pub const STRIPE_BUFFER_SIZE: usize = 128 * 1024;

// === Page Flags ===

/// Flag bit: DeweyIDs are stored in this page.
pub const FLAG_DEWEY_IDS_STORED: u8 = 1;

/// Flag bit: FSST symbol table present.
pub const FLAG_HAS_FSST_TABLE: u8 = 2;

// === Page Guard State Flags (HFT-style packed into single AtomicU32) ===

/// Hot bit for clock-based eviction.
pub const STATE_HOT_BIT: u32 = 1;

/// Orphaned bit for cleanup detection.
pub const STATE_ORPHANED_BIT: u32 = 2;

/// Closed bit.
pub const STATE_CLOSED_BIT: u32 = 4;
