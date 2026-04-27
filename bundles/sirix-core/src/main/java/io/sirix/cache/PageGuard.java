package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;

/**
 * Auto-closeable guard for page access (LeanStore/Umbra pattern).
 * <p>
 * Manages page lifecycle through scoped guard acquisition and release. Pages can only be evicted
 * when guardCount == 0 and version checks pass.
 * <p>
 * NOTE: Guards protect the PAGE (frame), not the key. This matches LeanStore/Umbra architecture
 * where the frame contains the guard count. No reference to the key is needed since the guard count
 * lives on the page itself.
 * <p>
 * Usage:
 * 
 * <pre>{@code
 * try (PageGuard guard = new PageGuard(page)) {
 *   KeyValueLeafPage p = guard.page();
 *   // Use page...
 * } // Guard automatically released
 * }</pre>
 *
 * @author Johannes Lichtenberger
 */
public final class PageGuard implements AutoCloseable {

  private final KeyValueLeafPage page;
  private final int versionAtFix;
  private boolean closed = false;

  /**
   * Create a new page guard and acquire the guard.
   *
   * @param page the page being guarded
   */
  public PageGuard(KeyValueLeafPage page) {
    this(page, true);
  }

  /**
   * Create a new page guard, optionally acquiring the guard.
   *
   * @param page the page being guarded
   * @param acquireGuard if true, acquire guard; if false, guard must already be acquired
   */
  private PageGuard(KeyValueLeafPage page, boolean acquireGuard) {
    this.page = page;
    if (acquireGuard) {
      page.acquireGuard(); // Guard the PAGE (frame)
    }
    // Capture version AFTER acquireGuard so an in-flight evictor that sees
    // guardCount==0 cannot bump the version between our snapshot and our
    // guard. Before the guard is held the frame can be recycled at any time;
    // once held, the cache's eviction path skips guarded pages. Reversing the
    // order turned a narrow race into a correctness error under pressure —
    // the guard's close-time version check was firing on benign recycles.
    this.versionAtFix = page.getVersion();
  }

  /**
   * Wrap an already-guarded page without re-acquiring the guard. Use this when the guard was acquired
   * inside a compute() block to prevent eviction races.
   *
   * @param page the page that already has an acquired guard
   * @return a new PageGuard wrapper (guard is NOT re-acquired)
   */
  public static PageGuard wrapAlreadyGuarded(KeyValueLeafPage page) {
    return new PageGuard(page, false);
  }

  /**
   * Get the guarded page.
   *
   * @return the page
   */
  public KeyValueLeafPage page() {
    if (closed) {
      throw new IllegalStateException("Cannot access page after guard is closed");
    }
    return page;
  }

  /**
   * Get the version captured when guard was created.
   *
   * @return version at fix time
   */
  public int versionAtFix() {
    return versionAtFix;
  }

  /**
   * Check if this guard has been closed.
   *
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Release the guard. Throws FrameReusedException if the page version changed (indicating the frame
   * was recycled).
   * <p>
   * NOTE: This method is resilient to guards being force-released by cache.clear(). If the page is
   * already closed or has no guards, the release is skipped.
   */
  @Override
  public void close() {
    if (!closed) {
      closed = true; // Mark as closed first to prevent double-close

      // SAFETY CHECK: Don't release if page was already closed or guard was force-released
      // This can happen when cache.clear() force-releases all guards during cleanup
      if (page.isClosed()) {
        // Page was closed (e.g., by cache.clear()) - nothing to release
        return;
      }

      if (page.getGuardCount() <= 0) {
        // Guard was already released (e.g., by cache.clear() force-release)
        // Don't try to release again - that would make guardCount negative
        return;
      }

      // Capture version BEFORE releaseGuard. While we hold the guard, no
      // evictor can bump version. Once we release, another thread can
      // evict + incrementVersion before we read — a race that produced
      // spurious FrameReusedException under severe-pressure eviction.
      final int currentVersion = page.getVersion();
      page.releaseGuard();
      if (currentVersion != versionAtFix) {
        throw new FrameReusedException(
            "Page frame was reused while guard was active: versionAtFix=" + versionAtFix + ", currentVersion="
                + currentVersion + ", pageKey=" + page.getPageKey() + ", revision=" + page.getRevision());
      }
    }
  }
}

