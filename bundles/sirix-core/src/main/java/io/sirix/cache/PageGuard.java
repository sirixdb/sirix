package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;

/**
 * Auto-closeable guard for page access (LeanStore/Umbra pattern).
 * <p>
 * Manages page lifecycle through scoped guard acquisition and release.
 * Pages can only be evicted when guardCount == 0 and version checks pass.
 * <p>
 * Usage:
 * <pre>{@code
 * try (PageGuard guard = new PageGuard(pageRef, page)) {
 *     KeyValueLeafPage page = guard.page();
 *     // Use page...
 * }  // Guard automatically released
 * }</pre>
 *
 * @author Johannes Lichtenberger
 */
public final class PageGuard implements AutoCloseable {
  
  private final PageReference ref;
  private final KeyValueLeafPage page;
  private final int versionAtFix;
  private boolean closed = false;

  /**
   * Create a new page guard.
   *
   * @param ref the page reference
   * @param page the page being guarded
   */
  public PageGuard(PageReference ref, KeyValueLeafPage page) {
    this.ref = ref;
    this.page = page;
    this.versionAtFix = page.getVersion();
    ref.acquireGuard();
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
   * Release the guard.
   * Throws FrameReusedException if the page version changed (indicating the frame was recycled).
   */
  @Override
  public void close() {
    if (!closed) {
      ref.releaseGuard();
      int currentVersion = page.getVersion();
      if (currentVersion != versionAtFix) {
        closed = true;
        throw new FrameReusedException(
            "Page frame was reused while guard was active: versionAtFix=" + versionAtFix +
            ", currentVersion=" + currentVersion +
            ", pageKey=" + page.getPageKey() +
            ", revision=" + page.getRevision());
      }
      closed = true;
    }
  }
}

