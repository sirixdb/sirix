//! Versioning strategies for page snapshots (Full, Incremental, Differential, SlidingSnapshot).
//!
//! Equivalent to Java's `io.sirix.settings.VersioningType` with bitmap-driven O(k) page
//! combination, lazy-copy preservation, and copy-on-write isolation.

pub mod page_container;
pub mod strategies;

pub use page_container::PageContainer;
pub use strategies::combine_record_pages;
pub use strategies::combine_record_pages_for_modification;
pub use strategies::get_revision_roots;
pub use strategies::should_store_full_snapshot;
