package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.PageGuard;
import io.sirix.index.IndexType;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;

/**
 * Sequential iterator over all {@link KeyValueLeafPage}s in a revision's document index.
 *
 * <p>Resolves page keys via the indirect page trie using {@link StorageEngineReader#getRecordPage}.
 * Each returned page is guarded (reference-counted) to prevent cache eviction during processing.
 * The guard is released when {@link #nextPage()} is called again or when the iterator is closed.</p>
 *
 * <p>Non-existent page keys (sparse trie) are silently skipped. The iterator terminates when
 * all page keys up to {@code maxNodeKey >> NDP_NODE_COUNT_EXPONENT} have been visited.</p>
 *
 * <p>Usage:
 * <pre>{@code
 * try (var iter = new PageScanIterator(reader)) {
 *   KeyValueLeafPage page;
 *   while ((page = iter.nextPage()) != null) {
 *     // Process page — guarded until next nextPage() or close()
 *   }
 * }
 * }</pre></p>
 */
public final class PageScanIterator implements AutoCloseable {

  /**
   * How often one page key is re-read when its cached instance turns out to be mid-teardown. Each
   * retry costs a cache lookup; a page that loses the race three times in a row is not a race.
   */
  private static final int MAX_GUARD_ATTEMPTS = 3;

  private final StorageEngineReader reader;
  private final long maxPageKey;
  private final IndexLogKey reusableKey;
  private long currentPageKey;
  private PageGuard currentGuard;

  /**
   * Create a page scan iterator for the document index of the given reader's revision.
   *
   * @param reader the storage engine reader (must not be closed)
   */
  public PageScanIterator(final StorageEngineReader reader) {
    this.reader = reader;
    final int revisionNumber = reader.getRevisionNumber();
    final long maxNodeKey = reader.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
    this.maxPageKey = maxNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT;
    this.currentPageKey = -1;
    this.reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revisionNumber);
  }

  /**
   * Advance to the next non-null page.
   *
   * <p>The returned page is guarded — it remains valid until {@link #nextPage()} is called
   * again or {@link #close()} is called. The caller may acquire additional guards if the
   * page must outlive the iterator's current position (e.g., for multi-page batch accumulation).</p>
   *
   * @return the next page, or {@code null} when the scan is exhausted
   */
  public KeyValueLeafPage nextPage() {
    releaseCurrentGuard();

    while (++currentPageKey <= maxPageKey) {
      final KeyValueLeafPage guarded = loadAndGuard(currentPageKey);
      if (guarded == null) {
        continue;
      }
      currentGuard = PageGuard.wrapAlreadyGuarded(guarded);
      return guarded;
    }

    return null;
  }

  /**
   * Load the page at one key and acquire our own guard on it.
   *
   * <p>The guard must be our own: the reader's internal guard is released on the next
   * {@link StorageEngineReader#getRecordPage} call, so without one the frame could be recycled while
   * the caller is still reading the page it was handed.</p>
   *
   * <p>{@code acquireGuard()} returns false WITHOUT incrementing on a closed or orphaned page, so
   * wrapping its result unconditionally builds a guard object backed by nothing — and that object's
   * {@code close()} then releases SOMEONE ELSE'S guard, freeing the page under its real holder. The
   * {@link PageGuard} constructor rejects exactly this; {@code wrapAlreadyGuarded} trusts the caller
   * instead, so the check has to happen here.</p>
   *
   * <p>A failed acquire means this instance is being torn down, not that the key has no page: the
   * cache drops the dead mapping, so re-reading the key yields a live instance. Hence retry rather
   * than skip — skipping would silently drop a whole page of nodes from the scan, and this iterator
   * backs query results.</p>
   *
   * @param pageKey the document-index page key to load
   * @return the guarded page, or {@code null} if the trie has no usable page at that key
   */
  private @Nullable KeyValueLeafPage loadAndGuard(final long pageKey) {
    for (int attempt = 0; attempt < MAX_GUARD_ATTEMPTS; attempt++) {
      reusableKey.setRecordPageKey(pageKey);
      final var result = reader.getRecordPage(reusableKey);

      if (result == null || result.page() == null) {
        return null;
      }

      final var kvlPage = (KeyValueLeafPage) result.page();
      if (!kvlPage.acquireGuard()) {
        continue;
      }
      // Checked under our guard: before it, the frame could be recycled between test and use.
      if (kvlPage.getSlottedPage() == null) {
        kvlPage.releaseGuard();
        return null;
      }
      return kvlPage;
    }

    throw new IllegalStateException("Could not guard document page " + pageKey + " after "
        + MAX_GUARD_ATTEMPTS + " attempts: every cached instance was being torn down");
  }

  /**
   * Get the current page key (useful for diagnostics).
   *
   * @return the current page key, or -1 if not yet started
   */
  public long currentPageKey() {
    return currentPageKey;
  }

  private void releaseCurrentGuard() {
    if (currentGuard != null) {
      currentGuard.close();
      currentGuard = null;
    }
  }

  @Override
  public void close() {
    releaseCurrentGuard();
  }
}
