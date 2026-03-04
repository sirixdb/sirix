//! PageGuard - LeanStore/Umbra-style guard for preventing page eviction.
//!
//! While a guard is held, the page's guard count is incremented,
//! preventing the cache from evicting it. On drop, the guard count
//! is decremented and version consistency is checked.

use std::sync::Arc;

use crate::error::Result;
use crate::error::StorageError;
use crate::page::slotted_page::SlottedPage;

/// A guard that pins a `SlottedPage` in the cache.
///
/// Implements the LeanStore buffer frame guard pattern:
/// - Acquiring a guard increments the page's guard count
/// - Releasing (dropping) the guard decrements it
/// - Version is checked on release to detect frame reuse
///
/// When `guard_count > 0`, the cache assigns weight 0 to the page,
/// preventing eviction.
pub struct PageGuard {
    page: Arc<SlottedPage>,
    version_at_fix: u32,
}

impl PageGuard {
    /// Create a new guard, pinning the page.
    pub fn new(page: Arc<SlottedPage>) -> Self {
        let version_at_fix = page.version();
        page.acquire_guard();
        page.set_hot(); // Mark as recently accessed
        Self {
            page,
            version_at_fix,
        }
    }

    /// Access the guarded page.
    #[inline]
    pub fn page(&self) -> &SlottedPage {
        &self.page
    }

    /// Check if the frame has been reused since this guard was created.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.page.version() == self.version_at_fix
    }

    /// Validate that the frame hasn't been reused, returning an error if it has.
    pub fn validate(&self) -> Result<()> {
        let current = self.page.version();
        if current != self.version_at_fix {
            Err(StorageError::FrameReused {
                expected: self.version_at_fix,
                actual: current,
            })
        } else {
            Ok(())
        }
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        self.page.release_guard();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::IndexType;

    #[test]
    fn test_guard_increments_count() {
        let page = Arc::new(SlottedPage::new(1, 0, IndexType::Document));
        assert_eq!(page.guard_count(), 0);

        let guard = PageGuard::new(Arc::clone(&page));
        assert_eq!(page.guard_count(), 1);
        assert!(guard.is_valid());

        drop(guard);
        assert_eq!(page.guard_count(), 0);
    }

    #[test]
    fn test_multiple_guards() {
        let page = Arc::new(SlottedPage::new(1, 0, IndexType::Document));

        let g1 = PageGuard::new(Arc::clone(&page));
        let g2 = PageGuard::new(Arc::clone(&page));
        assert_eq!(page.guard_count(), 2);

        drop(g1);
        assert_eq!(page.guard_count(), 1);

        drop(g2);
        assert_eq!(page.guard_count(), 0);
    }

    #[test]
    fn test_frame_reuse_detection() {
        let page = Arc::new(SlottedPage::new(1, 0, IndexType::Document));
        let guard = PageGuard::new(Arc::clone(&page));

        // Simulate frame reuse
        page.increment_version();

        assert!(!guard.is_valid());
        assert!(guard.validate().is_err());
    }
}
