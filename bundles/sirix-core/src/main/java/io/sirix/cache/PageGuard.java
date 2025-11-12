package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;

/**
 * Auto-closeable guard for page access (LeanStore/Umbra pattern).
 * <p>
 * Manages page lifecycle through scoped guard acquisition and release.
 * Pages can only be evicted when guardCount == 0 and version checks pass.
 * <p>
 * NOTE: Guards protect the PAGE (frame), not the key. This matches LeanStore/Umbra
 * architecture where the frame contains the guard count. No reference to the key
 * is needed since the guard count lives on the page itself.
 * <p>
 * Usage:
 * <pre>{@code
 * try (PageGuard guard = new PageGuard(page)) {
 *     KeyValueLeafPage p = guard.page();
 *     // Use page...
 * }  // Guard automatically released
 * }</pre>
 *
 * @author Johannes Lichtenberger
 */
public final class PageGuard implements AutoCloseable {
  
  private final KeyValueLeafPage page;
  private final int versionAtFix;
  private boolean closed = false;

  /**
   * Create a new page guard.
   *
   * @param page the page being guarded
   */
  public PageGuard(KeyValueLeafPage page) {
    this.page = page;
    this.versionAtFix = page.getVersion();
    page.acquireGuard();  // Guard the PAGE (frame)
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
      page.releaseGuard();  // Release guard on PAGE
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

