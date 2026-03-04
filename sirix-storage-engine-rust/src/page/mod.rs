//! Page types and serialization for the SirixDB storage engine.

pub mod slotted_page;
pub mod page_reference;
pub mod indirect_page;
pub mod serialization;

pub use slotted_page::SlottedPage;
pub use page_reference::PageReference;
pub use indirect_page::IndirectPage;

use crate::error::Result;
use crate::types::SerializationType;

/// Trait for all page types that can be serialized and stored.
pub trait Page: Send + Sync {
    /// Returns the references held by this page (e.g., child page pointers).
    fn references(&self) -> &[PageReference];

    /// Returns a mutable reference to the reference at the given offset.
    /// Creates a new default reference if the slot was empty.
    fn get_or_create_reference(&mut self, offset: usize) -> &mut PageReference;

    /// Serialize this page into the provided buffer.
    fn serialize(&self, buf: &mut Vec<u8>, ser_type: SerializationType) -> Result<()>;
}
