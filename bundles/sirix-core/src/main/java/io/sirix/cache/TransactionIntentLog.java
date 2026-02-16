package io.sirix.cache;

import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.Function;

/**
 * Transaction intent log (TIL) for caching all changes made by a read/write transaction.
 * <p>
 * The TIL stores modified pages during a transaction. When the transaction commits,
 * pages are written to storage. On rollback, the TIL is simply cleared.
 * <p>
 * Pages added to the TIL are removed from global caches since they represent
 * uncommitted changes that should not be visible to other transactions.
 * <p>
 * <b>Async Auto-Commit Support:</b>
 * <p>
 * For async auto-commit during bulk imports, the TIL supports <em>rotation</em>:
 * <ul>
 *   <li>{@link #detachAndReset()} freezes the current entries and returns them as a
 *       {@link RotationResult} for background commit processing</li>
 *   <li>The TIL is immediately reset for new inserts with an incremented generation counter</li>
 *   <li>The generation counter enables O(1) membership checking via
 *       {@link PageReference#isInActiveTil(int)}</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class TransactionIntentLog implements AutoCloseable {

  /**
   * Default initial capacity for the entries array.
   */
  private static final int DEFAULT_INITIAL_CAPACITY = 4096;

  /**
   * The collection to hold page containers - using array for O(1) direct access.
   */
  private PageContainer[] entries;

  /**
   * Current size (number of entries in the array).
   */
  private int size;

  /**
   * Identity-based mapping from PageReference to its index for correct lookups.
   * Uses object identity (==) rather than equals() to distinguish refs with same logKey.
   */
  private final IdentityHashMap<PageReference, Integer> refToIndex;

  /**
   * Generation counter for O(1) TIL rotation.
   * Incremented on each {@link #detachAndReset()} call.
   * PageReferences in this TIL have their activeTilGeneration set to this value.
   */
  private volatile int currentGeneration;

  /**
   * The buffer manager.
   */
  private final BufferManager bufferManager;

  /**
   * Optional fallback for looking up pages in the pending snapshot.
   * <p>
   * Set by the writer after TIL rotation to make snapshot pages accessible
   * to the reader's {@code loadPage()} during async auto-commit.
   * When not null, {@link #get(PageReference)} checks this after the active TIL.
   */
  private volatile Function<PageReference, PageContainer> snapshotLookup;

  /**
   * The log key (legacy - kept for compatibility, same as size).
   */
  private int logKey;

  /**
   * Creates a new transaction intent log.
   *
   * @param bufferManager the buffer manager for cache operations
   * @param maxInMemoryCapacity the maximum expected number of modified pages
   */
  public TransactionIntentLog(final BufferManager bufferManager, final int maxInMemoryCapacity) {
    this.bufferManager = bufferManager;
    this.logKey = 0;
    this.size = 0;
    this.currentGeneration = 0;
    final int initialCapacity = Math.max(maxInMemoryCapacity, DEFAULT_INITIAL_CAPACITY);
    this.entries = new PageContainer[initialCapacity];
    this.refToIndex = new IdentityHashMap<>(initialCapacity * 4 / 3);
  }

  /**
   * Set the fallback snapshot lookup function for async auto-commit.
   * <p>
   * When set, {@link #get(PageReference)} checks this function after the active TIL,
   * enabling the reader's trie navigation to find pages from the pending snapshot.
   *
   * @param lookup function mapping PageReference to PageContainer (may return null), or null to clear
   */
  public void setSnapshotLookup(final Function<PageReference, PageContainer> lookup) {
    this.snapshotLookup = lookup;
  }

  /**
   * Retrieves an entry from the cache.<br>
   *
   * @param key the key whose associated value is to be returned.
   * @return the value associated to this key, or {@code null} if no value with this key exists in the
   * cache
   */
  public PageContainer get(final PageReference key) {
    // First check by identity (most reliable for avoiding logKey collisions)
    final Integer index = refToIndex.get(key);
    if (index != null && index < size) {
      return entries[index];
    }
    // LogKey fallback: ONLY if reference belongs to current TIL generation.
    // After rotation, stale logKeys may collide with entries of different page types
    // (e.g., an IndirectPage reference's stale logKey pointing to a KeyValueLeafPage slot).
    if (key.isInActiveTil(currentGeneration)) {
      final var logKeyVal = key.getLogKey();
      if (logKeyVal >= 0 && logKeyVal < this.size) {
        return entries[logKeyVal];
      }
    }
    // Check linked snapshot for async auto-commit
    final var lookup = snapshotLookup;
    if (lookup != null) {
      return lookup.apply(key);
    }
    return null;
  }

  /**
   * Direct array access by logKey index - caller must ensure validity.
   * <p>
   * This is used for fast-path lookups when the caller has already verified
   * the PageReference belongs to this TIL via {@link PageReference#isInActiveTil(int)}.
   *
   * @param logKeyIndex the log key index
   * @return the page container at that index, or null if out of bounds
   */
  public PageContainer getUnchecked(final int logKeyIndex) {
    if (logKeyIndex >= 0 && logKeyIndex < size) {
      return entries[logKeyIndex];
    }
    return null;
  }

  /**
   * Adds an entry to the transaction intent log.
   * <p>
   * The page is removed from global caches as the TIL now owns it exclusively.
   * Guards are released since TIL pages are transaction-private.
   * <p>
   * CRITICAL: Pages removed from caches during this operation are closed if they differ
   * from the pages in the new PageContainer. This prevents memory leaks when a cached
   * combined-page is replaced by newly created pages from combineRecordPagesForModification().
   *
   * @param key the page reference key
   * @param value the page container with complete and modified versions
   */
  public void put(final PageReference key, final PageContainer value) {
    // Clear cached hash before modifying key properties
    key.clearCachedHash();

    // CRITICAL FIX: Close old cached pages that differ from new pages being added to TIL
    // This fixes memory leak where:
    // 1. Read path creates combined page X and caches it
    // 2. Write path creates NEW pages (complete, modified) via combineRecordPagesForModification()  
    // 3. TIL.put() removes page X from cache but TIL owns the new pages, not X
    // 4. Page X becomes orphaned (not in cache, not in TIL)
    KeyValueLeafPage oldCachedPage = bufferManager.getRecordPageCache().get(key);
    
    // Remove from caches - TIL takes exclusive ownership of NEW pages
    bufferManager.getRecordPageCache().remove(key);
    bufferManager.getPageCache().remove(key);
    // Note: RecordPageFragmentCache entries are shared and managed by ClockSweeper
    
    // Close the old cached page if it's different from the pages going into TIL
    // This prevents orphaned pages from leaking memory
    if (oldCachedPage != null && !oldCachedPage.isClosed()) {
      boolean isInNewContainer = (oldCachedPage == value.getComplete() || oldCachedPage == value.getModified());
      if (!isInNewContainer) {
        // Old page is NOT one of the new pages - mark as orphaned first
        // This prevents new guards from being acquired (tryAcquireGuard checks isOrphaned)
        oldCachedPage.markOrphaned();
        // close() internally checks guardCount > 0 and isClosed
        // If guards are active, close() returns without closing
        // The last releaseGuard() will close the page (since isOrphaned is true)
        oldCachedPage.close();
      }
    }

    // Set page reference properties
    key.setKey(Constants.NULL_ID_LONG);
    key.setPage(null);
    key.setLogKey(size);
    // CRITICAL: Mark this ref as belonging to current TIL generation for O(1) lookup
    key.setActiveTilGeneration(currentGeneration);

    // Store in identity map for correct lookups after rotation
    refToIndex.put(key, size);

    // Grow array if needed
    if (size >= entries.length) {
      entries = Arrays.copyOf(entries, entries.length * 2);
    }
    entries[size] = value;
    size++;
    logKey = size; // Keep logKey in sync for backward compatibility

    // Release guards - TIL pages are transaction-private
    if (value.getComplete() instanceof KeyValueLeafPage completePage && completePage.getGuardCount() > 0) {
      completePage.releaseGuard();
    }
    if (value.getModified() instanceof KeyValueLeafPage modifiedPage 
        && modifiedPage != value.getComplete() && modifiedPage.getGuardCount() > 0) {
      modifiedPage.releaseGuard();
    }
  }


  /**
   * Clears the transaction intent log, closing all owned pages.
   * <p>
   * This is typically called on transaction rollback. All pages in the TIL
   * are closed and their memory is released.
   */
  public void clear() {
    logKey = 0;

    // Ensure pending cache operations are complete before closing pages
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();

    // Close all pages owned by TIL
    for (int i = 0; i < size; i++) {
      final PageContainer pageContainer = entries[i];
      if (pageContainer != null) {
        closePage(pageContainer.getComplete());
        if (pageContainer.getModified() != pageContainer.getComplete()) {
          closePage(pageContainer.getModified());
        }
        entries[i] = null; // Help GC
      }
    }
    size = 0;
    refToIndex.clear();
  }

  /**
   * Helper method to release guards and close a page.
   */
  private void closePage(Page page) {
    if (page instanceof KeyValueLeafPage kvPage) {
      while (kvPage.getGuardCount() > 0) {
        kvPage.releaseGuard();
      }
      kvPage.close();
    }
  }

  /**
   * Get a view of the underlying entries as a list.
   * <p>
   * Note: This creates a new list for compatibility. For performance-critical
   * code, prefer using {@link #getUnchecked(int)} with {@link #size()}.
   *
   * @return a new list containing all entries in the cache
   */
  public List<PageContainer> getList() {
    final List<PageContainer> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      result.add(entries[i]);
    }
    return result;
  }

  /**
   * Get the entries array directly (for snapshot creation).
   * <p>
   * WARNING: This returns the internal array. Do not modify!
   *
   * @return the internal entries array
   */
  PageContainer[] getEntriesArray() {
    return entries;
  }

  /**
   * Closes the transaction intent log and releases all owned pages.
   */
  @Override
  public void close() {
    // Ensure pending cache operations are complete
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();

    // Close all pages owned by TIL
    for (int i = 0; i < size; i++) {
      final PageContainer pageContainer = entries[i];
      if (pageContainer != null) {
        closePage(pageContainer.getComplete());
        if (pageContainer.getModified() != pageContainer.getComplete()) {
          closePage(pageContainer.getModified());
        }
        entries[i] = null; // Help GC
      }
    }

    logKey = 0;
    size = 0;
    refToIndex.clear();
  }
  
  /**
   * Get the number of containers in the TIL.
   *
   * @return the number of entries
   */
  public int size() {
    return size;
  }
  
  /**
   * Get the current log key.
   *
   * @return the current log key value
   */
  public int getLogKey() {
    return logKey;
  }

  /**
   * Get the current generation counter.
   * <p>
   * This is used for O(1) TIL membership checking via
   * {@link PageReference#isInActiveTil(int)}.
   *
   * @return the current generation counter
   */
  public int getCurrentGeneration() {
    return currentGeneration;
  }

  /**
   * Rotate the TIL for async commit: freeze current entries and reset for new inserts.
   * <p>
   * This operation is O(1) for the array swap and O(n) for building the identity map.
   * After this call:
   * <ul>
   *   <li>The returned {@link RotationResult} contains all current entries</li>
   *   <li>This TIL is reset with a new empty array and incremented generation</li>
   *   <li>New inserts will have the new generation, old refs retain old generation</li>
   * </ul>
   *
   * @return the rotation result containing frozen entries and identity map
   */
  public RotationResult detachAndReset() {
    // O(1): Increment generation - makes old refs distinguishable from new
    currentGeneration++;

    // Capture current state
    final PageContainer[] snapshotEntries = this.entries;
    final int snapshotSize = this.size;

    // O(1): Swap in new array (don't copy, just allocate fresh)
    final int newCapacity = Math.max(snapshotSize, DEFAULT_INITIAL_CAPACITY);
    this.entries = new PageContainer[newCapacity];
    this.size = 0;
    this.logKey = 0;

    // O(n): Build identity map for snapshot (unavoidable for correct lookups)
    final IdentityHashMap<PageReference, PageContainer> snapshotRefMap =
        new IdentityHashMap<>(snapshotSize * 4 / 3 + 1);
    for (var entry : refToIndex.entrySet()) {
      final PageReference ref = entry.getKey();
      final int index = entry.getValue();
      if (index < snapshotSize && snapshotEntries[index] != null) {
        snapshotRefMap.put(ref, snapshotEntries[index]);
      }
    }

    // Clear for new inserts
    refToIndex.clear();

    return new RotationResult(snapshotEntries, snapshotSize, snapshotRefMap);
  }

  /**
   * Result of TIL rotation containing frozen entries and identity map.
   *
   * @param entries the frozen entries array
   * @param size the number of valid entries
   * @param refToContainer identity-based map from PageReference to PageContainer
   */
  public record RotationResult(
      PageContainer[] entries,
      int size,
      IdentityHashMap<PageReference, PageContainer> refToContainer
  ) {}
}




