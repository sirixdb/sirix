package io.sirix.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.PageGuard;
import io.sirix.index.IndexType;
import io.sirix.settings.Constants;

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
      reusableKey.setRecordPageKey(currentPageKey);
      final var result = reader.getRecordPage(reusableKey);

      if (result == null || result.page() == null) {
        continue;
      }

      final var kvlPage = (KeyValueLeafPage) result.page();
      if (kvlPage.isClosed() || kvlPage.getSlottedPage() == null) {
        continue;
      }

      // Acquire our own guard — the reader's internal guard will be released
      // on the next getRecordPage() call, but ours keeps the page pinned.
      kvlPage.acquireGuard();
      currentGuard = PageGuard.wrapAlreadyGuarded(kvlPage);
      return kvlPage;
    }

    return null;
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
