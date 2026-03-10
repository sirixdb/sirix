package io.sirix.cache;

import io.sirix.exception.SirixIOException;
import org.jspecify.annotations.Nullable;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Transaction intent log (TIL) for caching all changes made by a read/write transaction.
 * <p>
 * The TIL stores modified pages during a transaction. When the transaction commits, pages are
 * written to storage. On rollback, the TIL is simply cleared.
 * <p>
 * Pages added to the TIL are removed from global caches since they represent uncommitted changes
 * that should not be visible to other transactions.
 * <p>
 * Supports epoch-based O(1) snapshotting for async auto-commit: {@link #snapshot()} swaps the
 * current arrays and increments the generation counter. A background thread can then flush the
 * frozen snapshot while the insert thread continues with a fresh TIL.
 *
 * @author Johannes Lichtenberger
 */
public final class TransactionIntentLog implements AutoCloseable {

  // ==================== CORE STORAGE (replaces ArrayList) ====================

  /** Page containers indexed by logKey. */
  private PageContainer[] entries;

  /** Back-references: entryRefs[i] is the PageReference that maps to entries[i]. */
  private PageReference[] entryRefs;

  /** Number of active entries in the current arrays. */
  private int size;

  // ==================== GENERATION COUNTER ====================

  /** Current generation. Incremented on each snapshot(). Used for O(1) epoch membership. */
  private int currentGeneration;

  // ==================== SNAPSHOT STATE ====================

  /** Frozen entries from the last snapshot() call. Null if no active snapshot. */
  private PageContainer[] snapshotEntries;

  /** Frozen back-references from the last snapshot() call. */
  private PageReference[] snapshotRefs;

  /** Number of entries in the frozen snapshot. */
  private int snapshotSize;

  /** Generation counter at the time of the snapshot. */
  private int snapshotGeneration;

  /** Set to true by the background thread when snapshot flush is complete. */
  private volatile boolean snapshotCommitComplete;

  // ==================== SIDE-CHANNEL FOR BACKGROUND THREAD ====================
  // Background thread NEVER writes to PageReference directly — stores results here.
  // cleanupSnapshot() (insert thread, after semaphore) applies them.

  /** Disk offsets written by background thread. Initialized to NULL_ID_LONG sentinel. */
  private long[] snapshotDiskOffsets;

  /** Page hashes computed by background thread. */
  private byte[][] snapshotHashes;

  // ==================== COMPLETED DISK OFFSETS (stale-reference fix) ====================
  // When cleanupSnapshot() writes KVL disk offsets to the original references, copies of
  // those references (in CoW'd IndirectPages) don't get updated. This map stores
  // (generation << 32 | logKey) → diskOffset so that stale copies can be transparently
  // resolved in get(). Entries are removed on access (auto-pruning).

  /** Completed disk offsets indexed by packed (generation << 32 | logKey). No autoboxing. */
  private final Long2LongOpenHashMap completedDiskOffsets;

  /** Completed page hashes indexed by the same packed key. No autoboxing on key. */
  private final Long2ObjectOpenHashMap<byte[]> completedDiskHashes = new Long2ObjectOpenHashMap<>();

  /** Counter: Layer 3 hits (stale references resolved from completed disk offsets). */
  private long layer3Hits;

  // ==================== BUFFER MANAGER ====================

  /** The buffer manager. */
  private final BufferManager bufferManager;

  /**
   * Creates a new transaction intent log.
   *
   * @param bufferManager the buffer manager for cache operations
   * @param maxInMemoryCapacity the initial capacity (number of expected modified pages)
   */
  public TransactionIntentLog(final BufferManager bufferManager, final int maxInMemoryCapacity) {
    this.bufferManager = bufferManager;
    final int initialCapacity = Math.max(maxInMemoryCapacity, 64);
    entries = new PageContainer[initialCapacity];
    entryRefs = new PageReference[initialCapacity];
    size = 0;
    currentGeneration = 0;
    completedDiskOffsets = new Long2LongOpenHashMap();
    completedDiskOffsets.defaultReturnValue(Constants.NULL_ID_LONG);
  }

  /**
   * Retrieves an entry from the TIL using generation-based 3-layer lookup.
   * <p>
   * Layer 1: Current TIL (fast path — most common during insertion).
   * Layer 2: Active snapshot (if any — for reads of pages not yet cleaned up).
   * Layer 3: Completed disk offsets (for stale reference copies from CoW'd IndirectPages).
   *
   * @param ref the page reference whose associated container is to be returned
   * @return the page container, or {@code null} if not in any TIL layer
   */
  public PageContainer get(final PageReference ref) {
    final int logKey = ref.getLogKey();
    if (logKey < 0) {
      return null;
    }

    // Layer 1: Current TIL (fast path)
    if (ref.getActiveTilGeneration() == currentGeneration && logKey < size) {
      return entries[logKey];
    }

    // Layer 2: Active snapshot (if any)
    if (snapshotEntries != null
        && ref.getActiveTilGeneration() == snapshotGeneration
        && logKey < snapshotSize) {
      return snapshotEntries[logKey];
    }

    // Layer 3: Completed disk offsets — stale reference fix.
    // When an IndirectPage is CoW'd, its child references are copied. If cleanupSnapshot()
    // later applies a disk offset to the ORIGINAL reference, the COPY doesn't get updated.
    // This layer resolves stale copies by applying the disk offset from the completed map.
    if (!completedDiskOffsets.isEmpty()) {
      final long packedKey = ((long) ref.getActiveTilGeneration() << 32) | (logKey & 0xFFFFFFFFL);
      final long diskOffset = completedDiskOffsets.remove(packedKey);
      if (diskOffset != Constants.NULL_ID_LONG) {
        ref.setKey(diskOffset);
        final byte[] hash = completedDiskHashes.remove(packedKey);
        if (hash != null) {
          ref.setHash(hash);
        }
        // Reset logKey/generation so this ref is recognized as a "disk" reference
        ref.setLogKey(Constants.NULL_ID_INT);
        ref.setActiveTilGeneration(-1);
        layer3Hits++;
        return null;  // Page is on disk, not in TIL — caller loads from disk
      }
    }

    return null;
  }

  /**
   * Adds an entry to the transaction intent log.
   * <p>
   * The page is removed from global caches as the TIL now owns it exclusively. Guards are released
   * since TIL pages are transaction-private.
   * <p>
   * CRITICAL: Pages removed from caches during this operation are closed if they differ from the
   * pages in the new PageContainer. This prevents memory leaks when a cached combined-page is
   * replaced by newly created pages from combineRecordPagesForModification().
   *
   * @param ref the page reference key
   * @param value the page container with complete and modified versions
   */
  public void put(final PageReference ref, final PageContainer value) {
    // Clear cached hash before modifying key properties
    ref.clearCachedHash();

    // CRITICAL FIX: Close old cached pages that differ from new pages being added to TIL
    final KeyValueLeafPage oldCachedPage = bufferManager.getRecordPageCache().get(ref);

    // Remove from caches - TIL takes exclusive ownership of NEW pages
    bufferManager.getRecordPageCache().remove(ref);
    bufferManager.getPageCache().remove(ref);

    // Close the old cached page if it's different from the pages going into TIL
    if (oldCachedPage != null && !oldCachedPage.isClosed()) {
      final boolean isInNewContainer =
          (oldCachedPage == value.getComplete() || oldCachedPage == value.getModified());
      if (!isInNewContainer) {
        oldCachedPage.markOrphaned();
        oldCachedPage.close();
      }
    }

    ref.setKey(Constants.NULL_ID_LONG);
    ref.setPage(null);

    final int existingKey = ref.getLogKey();
    if (existingKey != Constants.NULL_ID_INT && existingKey >= 0 && existingKey < size) {
      // Reuse existing logKey — update container in-place.
      // This ensures that PageReference copies (from COW operations) that share
      // the same logKey will resolve to the latest container.
      entries[existingKey] = value;
      entryRefs[existingKey] = ref;
      ref.setActiveTilGeneration(currentGeneration);
    } else {
      // New entry
      ensureCapacity();
      ref.setLogKey(size);
      ref.setActiveTilGeneration(currentGeneration);
      entries[size] = value;
      entryRefs[size] = ref;
      size++;
    }

    // Release guards - TIL pages are transaction-private
    if (value.getComplete() instanceof KeyValueLeafPage completePage && completePage.getGuardCount() > 0) {
      completePage.releaseGuard();
    }
    if (value.getModified() instanceof KeyValueLeafPage modifiedPage && modifiedPage != value.getComplete()
        && modifiedPage.getGuardCount() > 0) {
      modifiedPage.releaseGuard();
    }
  }

  /**
   * Ensure the entries/entryRefs arrays have room for one more element.
   * Doubles capacity when full.
   */
  private void ensureCapacity() {
    if (size == entries.length) {
      final int newCap = entries.length << 1;
      entries = Arrays.copyOf(entries, newCap);
      entryRefs = Arrays.copyOf(entryRefs, newCap);
    }
  }

  /**
   * Get the original PageReference stored at a given logKey. Used to copy disk offsets
   * when a duplicate reference (from HOTIndirectPage COW) resolves to the same TIL entry.
   *
   * @param logKey the log key
   * @return the original PageReference, or null if not found
   */
  public @Nullable PageReference getOriginalRef(final int logKey) {
    if (logKey >= 0 && logKey < size) {
      return entryRefs[logKey];
    }
    return null;
  }

  // ==================== SNAPSHOT (O(1) array swap) ====================

  /**
   * Freeze current entries for background flush. O(1) — array reference swap + generation increment.
   * <p>
   * After this call, the insert thread continues with fresh empty arrays. The frozen arrays
   * are available to the background thread via {@link #getSnapshotEntry(int)} etc.
   *
   * @return snapshotSize (0 = nothing to flush)
   */
  public int snapshot() {
    // Capture current state
    snapshotEntries = entries;
    snapshotRefs = entryRefs;
    snapshotSize = size;
    snapshotGeneration = currentGeneration;
    snapshotCommitComplete = false;

    // Allocate side-channel arrays for background thread disk offsets.
    // Initialize offsets to NULL_ID_LONG sentinel — cleanupSnapshot() validates
    // each KVL entry got a valid offset before applying.
    snapshotDiskOffsets = new long[size];
    Arrays.fill(snapshotDiskOffsets, Constants.NULL_ID_LONG);
    snapshotHashes = new byte[size][];

    // Increment generation AFTER capturing snapshot generation
    currentGeneration++;

    // Allocate fresh arrays for continued insertion
    entries = new PageContainer[snapshotEntries.length];
    entryRefs = new PageReference[snapshotRefs.length];
    size = 0;

    return snapshotSize;
  }

  /**
   * Check if a page reference is in the frozen snapshot zone.
   * Used to trigger CoW when the insert thread needs to modify a frozen page.
   *
   * @param ref the page reference to check
   * @return true if the reference is in the frozen snapshot
   */
  public boolean isFrozen(final PageReference ref) {
    return snapshotEntries != null
        && ref.getActiveTilGeneration() == snapshotGeneration
        && ref.getLogKey() >= 0
        && ref.getLogKey() < snapshotSize;
  }

  /**
   * Mark the snapshot flush as complete. Called by the background thread after all KVL pages
   * have been written to disk and their offsets stored in the side-channel arrays.
   */
  public void markSnapshotCommitComplete() {
    snapshotCommitComplete = true;
  }

  /**
   * Check if the snapshot commit has completed.
   *
   * @return true if the background thread has finished writing all snapshot pages
   */
  public boolean isSnapshotCommitComplete() {
    return snapshotCommitComplete;
  }

  // ==================== SNAPSHOT ACCESSORS (for background thread) ====================

  /**
   * Get snapshot entry at the given index. For background thread iteration.
   */
  public PageContainer getSnapshotEntry(final int index) {
    return snapshotEntries[index];
  }

  /**
   * Get snapshot reference at the given index. For background thread iteration.
   */
  public PageReference getSnapshotRef(final int index) {
    return snapshotRefs[index];
  }

  /**
   * Get the number of entries in the frozen snapshot.
   */
  public int getSnapshotSize() {
    return snapshotSize;
  }

  /**
   * Store a disk offset from the background thread into the side-channel.
   * The background thread NEVER writes to PageReference directly.
   */
  public void setSnapshotDiskOffset(final int index, final long offset) {
    snapshotDiskOffsets[index] = offset;
  }

  /**
   * Store a page hash from the background thread into the side-channel.
   */
  public void setSnapshotHash(final int index, final byte[] hash) {
    snapshotHashes[index] = hash;
  }

  // ==================== SNAPSHOT CLEANUP ====================

  /**
   * Clean up a completed snapshot: apply disk offsets to KVL page refs, close written KVL pages,
   * and promote IndirectPages to the current TIL.
   * <p>
   * MUST be called from the insert thread after the background thread has completed
   * (after semaphore acquire provides happens-before).
   *
   * @throws SirixIOException if any KVL entry is missing a disk offset (background write incomplete)
   */
  public void cleanupSnapshot() {
    if (snapshotEntries == null) {
      return;
    }

    for (int i = 0; i < snapshotSize; i++) {
      final PageContainer container = snapshotEntries[i];
      if (container == null) {
        continue;
      }
      final Page modified = container.getModified();
      final PageReference ref = snapshotRefs[i];

      if (modified instanceof KeyValueLeafPage) {
        // KVL page: already written to disk by background thread.
        // Validate: side-channel offset must be valid (not sentinel).
        final long diskOffset = snapshotDiskOffsets[i];
        if (diskOffset == Constants.NULL_ID_LONG) {
          throw new SirixIOException(
              "Snapshot entry " + i + " has no disk offset — background write incomplete or failed");
        }
        // Apply disk offset from side-channel
        ref.setKey(diskOffset);
        if (snapshotHashes[i] != null) {
          ref.setHash(snapshotHashes[i]);
        }
        // Store in completed map for stale-reference resolution (Layer 3 in get()).
        // When IndirectPages are CoW'd, their child references are COPIES that don't get
        // this disk offset update. The map allows get() to resolve stale copies.
        final long packedKey = ((long) snapshotGeneration << 32) | (i & 0xFFFFFFFFL);
        completedDiskOffsets.put(packedKey, diskOffset);
        if (snapshotHashes[i] != null) {
          completedDiskHashes.put(packedKey, snapshotHashes[i]);
        }
        // Close both complete and modified pages (release MemorySegment)
        closePageContainer(container);
        snapshotEntries[i] = null;
        snapshotRefs[i] = null;
      } else {
        // IndirectPage / structural page: promote to current TIL
        // so final commit traversal can find them via Layer 1 lookup.
        //
        // GUARD: Skip if ref was already moved to currentGeneration.
        // This happens when reAddStructuralPagesToTil() re-added this page
        // (same ref object, generation already updated). Without this check,
        // put() would overwrite the ref's logKey, orphaning the earlier entry.
        if (ref.getActiveTilGeneration() != currentGeneration) {
          put(ref, container);
        }
        snapshotEntries[i] = null;
        snapshotRefs[i] = null;
      }
    }

    // Prune stale entries from completedDiskOffsets/completedDiskHashes that belong
    // to generations older than 2 epochs back. Entries that haven't been accessed by now
    // are from orphaned COW references that will never be read.
    if (!completedDiskOffsets.isEmpty()) {
      final int pruneThreshold = currentGeneration - 2;
      completedDiskOffsets.keySet().removeIf(packedKey -> (int) (packedKey >> 32) < pruneThreshold);
      completedDiskHashes.keySet().removeIf(packedKey -> (int) (packedKey >> 32) < pruneThreshold);
    }

    // Release snapshot arrays for GC
    snapshotEntries = null;
    snapshotRefs = null;
    snapshotDiskOffsets = null;
    snapshotHashes = null;
    snapshotSize = 0;
  }

  // ==================== CLEAR / CLOSE ====================

  /**
   * Clears the transaction intent log, closing all owned pages.
   * <p>
   * This is typically called on transaction rollback. All pages in the TIL are closed and their
   * memory is released. Also clears any active snapshot (best-effort, no offset validation).
   */
  public void clear() {
    // Ensure pending cache operations are complete before closing pages
    bufferManager.getRecordPageCache().cleanUp();
    bufferManager.getRecordPageFragmentCache().cleanUp();
    bufferManager.getPageCache().cleanUp();

    // Close all current TIL pages
    for (int i = 0; i < size; i++) {
      final PageContainer container = entries[i];
      if (container != null) {
        closePageContainer(container);
        entries[i] = null;
        entryRefs[i] = null;
      }
    }
    size = 0;

    // Close snapshot pages unconditionally (best-effort — no offset validation).
    // This handles the error/rollback path where bg thread may have failed mid-write.
    clearSnapshotPages();

    // Clear completed disk offsets map
    completedDiskOffsets.clear();
    completedDiskHashes.clear();
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

    // Close all current TIL pages
    for (int i = 0; i < size; i++) {
      final PageContainer container = entries[i];
      if (container != null) {
        closePageContainer(container);
        entries[i] = null;
        entryRefs[i] = null;
      }
    }
    size = 0;

    // Close snapshot pages unconditionally
    clearSnapshotPages();

    // Clear completed disk offsets map
    completedDiskOffsets.clear();
    completedDiskHashes.clear();
  }

  /**
   * Best-effort cleanup of snapshot pages. Does NOT validate disk offsets — used for
   * rollback/error paths where background writes may be incomplete.
   */
  private void clearSnapshotPages() {
    if (snapshotEntries != null) {
      for (int i = 0; i < snapshotSize; i++) {
        final PageContainer container = snapshotEntries[i];
        if (container != null) {
          closePageContainer(container);
        }
      }
      snapshotEntries = null;
      snapshotRefs = null;
      snapshotDiskOffsets = null;
      snapshotHashes = null;
      snapshotSize = 0;
    }
  }

  /**
   * Close both pages in a container, handling identity (complete == modified).
   */
  private void closePageContainer(final PageContainer container) {
    closePage(container.getComplete());
    if (container.getModified() != container.getComplete()) {
      closePage(container.getModified());
    }
  }

  /**
   * Helper method to release guards and close a page.
   */
  private void closePage(final Page page) {
    if (page instanceof KeyValueLeafPage kvPage) {
      while (kvPage.getGuardCount() > 0) {
        kvPage.releaseGuard();
      }
      kvPage.close();
    }
  }

  // ==================== API COMPATIBILITY ====================

  /**
   * Get a view of the current entries as a List. Used by the commit path
   * ({@code parallelSerializationOfKeyValuePages()}).
   *
   * @return an unmodifiable list view over current TIL entries
   */
  public List<PageContainer> getList() {
    return Collections.unmodifiableList(new ArraySliceList<>(entries, size));
  }

  /**
   * Get the number of containers in the current TIL (not counting snapshot).
   */
  public int size() {
    return size;
  }

  /**
   * Get the current generation counter.
   *
   * @return the current generation value
   */
  public int getCurrentGeneration() {
    return currentGeneration;
  }

  /**
   * Get the snapshot generation counter (for diagnostics).
   */
  public int getSnapshotGeneration() {
    return snapshotGeneration;
  }

  /**
   * Lightweight fixed-size list view over an array prefix. Avoids copying for getList().
   */
  private static final class ArraySliceList<T> extends AbstractList<T> {
    private final T[] array;
    private final int len;

    ArraySliceList(final T[] array, final int len) {
      this.array = array;
      this.len = len;
    }

    @Override
    public T get(final int index) {
      if (index < 0 || index >= len) {
        throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + len);
      }
      return array[index];
    }

    @Override
    public int size() {
      return len;
    }
  }
}
