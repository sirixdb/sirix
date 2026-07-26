package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.LongAdder;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(PageGuard.class);

  /**
   * Guard releases that found nothing to release — see {@link #close()}. Non-zero means some holder's
   * guard accounting is wrong; the count is the only durable trace, since the release is skipped
   * rather than thrown.
   */
  private static final LongAdder UNGUARDED_RELEASES = new LongAdder();

  /** Releases that found no guard to release; see {@link #UNGUARDED_RELEASES}. */
  public static long getUnguardedReleaseCount() {
    return UNGUARDED_RELEASES.sum();
  }

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
      // acquireGuard() returns false WITHOUT incrementing when the page is ORPHANED or
      // CLOSED. Ignoring that created an unguarded guard object whose close() then passed
      // the guardCount>0 check on SOMEONE ELSE'S guard and released it — e.g. the cursor's
      // own guard on a freshly-orphaned current page during remove(), freeing the page out
      // from under the cursor.
      if (!page.acquireGuard()) {
        throw new IllegalStateException(
            "Cannot guard page " + page.getPageKey() + ": page is orphaned or closed (revision "
                + page.getRevision() + ")");
      }
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
   * <p><b>The caller MUST have checked that its {@code acquireGuard()} returned true.</b> That call
   * returns false WITHOUT incrementing on a closed or orphaned page, so wrapping an unchecked result
   * produces a guard object backed by nothing — and this class cannot catch that in the case that
   * matters. {@link #close()} detects a zero guard count, but when ANOTHER holder's guard is live the
   * count is positive and the unbacked release is indistinguishable from a real one: it releases, and
   * that holder loses its guard. Prefer {@link #PageGuard(KeyValueLeafPage)}, which acquires and
   * throws on failure, unless the guard genuinely was taken elsewhere under a lock.</p>
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
   *
   * <p>A live guard makes both "already closed" and "guard count is zero" impossible: {@code close()}
   * defers on a guarded page, and nothing may release a guard it did not take. This used to tolerate
   * both silently, because the cache-invalidation and {@code clear()} paths force-released guards they
   * did not own; those drains are gone, so reaching either state now means some page's guard
   * accounting is already wrong — typically an unbacked guard object, from {@code wrapAlreadyGuarded}
   * over an {@code acquireGuard()} whose result was discarded.</p>
   *
   * <p>Reported, NOT thrown. Guards are released from {@code finally} blocks and close paths —
   * {@code NodeStorageEngineReader.closeCurrentPageGuard} catches only {@link FrameReusedException} —
   * so throwing here aborts the rest of a teardown and strands whatever it had left to release. That
   * turns a counting slip into a cascading resource leak, which is strictly worse than the bug it
   * announces. The release is skipped either way, so no one else's guard is taken.</p>
   */
  @Override
  public void close() {
    if (!closed) {
      closed = true; // Mark as closed first to prevent double-close

      if (page.isClosed() || page.getGuardCount() <= 0) {
        UNGUARDED_RELEASES.increment();
        LOGGER.warn("Guard release on an unguarded page {} (revision {}, closed={}, guardCount={}): the "
                + "guard was never acquired, or someone released it on this holder's behalf. Skipping "
                + "the release so another holder's guard is not taken.", page.getPageKey(), page.getRevision(),
            page.isClosed(), page.getGuardCount());
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

