package io.sirix.cache;

import io.sirix.exception.SirixIOException;
import org.jspecify.annotations.Nullable;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
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
  // resolved in get(). Entries are retained until clear() (#1077).

  /** Completed disk offsets indexed by packed (generation << 32 | logKey). No autoboxing. */
  private final Long2LongOpenHashMap completedDiskOffsets;

  /** Completed page hashes indexed by the same packed key. No autoboxing on key. */
  private final Long2ObjectOpenHashMap<byte[]> completedDiskHashes = new Long2ObjectOpenHashMap<>();

  /** Counter: Layer 3 hits (stale references resolved from completed disk offsets). */
  private long layer3Hits;

  // ==================== FORWARDED ENTRIES (superseded-flush fix, #1077) ====================
  // When a frozen page is CoW'd into the current generation (put() with a prior-generation
  // reference), the old (generation, logKey) identity is SUPERSEDED: the frozen page's flush —
  // whose offset lands in completedDiskOffsets at cleanupSnapshot() — describes an OUTDATED
  // version of the page. Stale reference copies still carrying the old identity (in CoW'd
  // IndirectPages that are never rewalked, e.g. for a leaf page that straddles an epoch
  // boundary during monotonic bulk inserts) must resolve to the NEW entry, not to the stale
  // flush — otherwise the final commit durably serializes the outdated page and every record
  // added after the boundary silently vanishes. This map stores packed(oldGen, oldLogKey) →
  // packed(newGen, newLogKey); get() follows the chain to the terminal identity before
  // consulting the TIL layers. Entries are retained until clear().

  /** Forwarding chain for superseded (generation << 32 | logKey) identities. */
  private final Long2LongOpenHashMap forwardedEntries = new Long2LongOpenHashMap();

  { forwardedEntries.defaultReturnValue(-1L); }

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
    int logKey = ref.getLogKey();
    if (logKey < 0) {
      return null;
    }

    int generation = ref.getActiveTilGeneration();

    // Layer 1: Current TIL (fast path)
    if (generation == currentGeneration && logKey < size) {
      return entries[logKey];
    }

    // Layer 2: Active snapshot (if any)
    if (snapshotEntries != null && generation == snapshotGeneration && logKey < snapshotSize) {
      return snapshotEntries[logKey];
    }

    // Layer 2.5: Forwarding chain for superseded identities (#1077). A frozen page that was
    // CoW'd into a newer generation was re-logged under a new (generation, logKey); this stale
    // copy must resolve to that newer entry — NOT to the frozen page's (outdated) flush in the
    // completed-offsets map. Follow the chain to the terminal identity, then retry the layers.
    if (!forwardedEntries.isEmpty()) {
      long packed = ((long) generation << 32) | (logKey & 0xFFFFFFFFL);
      long forwarded = forwardedEntries.get(packed);
      if (forwarded >= 0) {
        while (true) {
          final long next = forwardedEntries.get(forwarded);
          if (next < 0) {
            break;
          }
          forwarded = next;
        }
        generation = (int) (forwarded >> 32);
        logKey = (int) forwarded;

        if (generation == currentGeneration && logKey < size) {
          // Rebind the stale copy to its terminal identity so later lookups take the fast path.
          ref.setLogKey(logKey);
          ref.setActiveTilGeneration(generation);
          return entries[logKey];
        }
        if (snapshotEntries != null && generation == snapshotGeneration && logKey < snapshotSize) {
          ref.setLogKey(logKey);
          ref.setActiveTilGeneration(generation);
          return snapshotEntries[logKey];
        }
        // Fall through to Layer 3 with the terminal identity (the newest flushed version).
      }
    }

    // Layer 3: Completed disk offsets — stale reference fix.
    // When an IndirectPage is CoW'd, its child references are copied. If cleanupSnapshot()
    // later applies a disk offset to the ORIGINAL reference, the COPY doesn't get updated.
    // This layer resolves stale copies by applying the disk offset from the completed map.
    //
    // Resolution is NON-destructive (#1077): an IndirectPage can be CoW'd more than once, so
    // several stale copies may carry the same (generation, logKey). The former remove() served
    // only the first copy; every later copy missed all three layers and was silently serialized
    // with child key -1 (or resurrected as a brand-new empty page on the write path). Entries
    // stay resolvable until clear() at commit/rollback.
    if (!completedDiskOffsets.isEmpty()) {
      final long packedKey = ((long) generation << 32) | (logKey & 0xFFFFFFFFL);
      final long diskOffset = completedDiskOffsets.get(packedKey);
      if (diskOffset != Constants.NULL_ID_LONG) {
        ref.setKey(diskOffset);
        final byte[] hash = completedDiskHashes.get(packedKey);
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
    // HOT leaf pages: the same instance can back both a TIL container and the shared
    // HOT-leaf cache. Take it out of the cache so the background sweeper and pressure
    // eviction cannot close its off-heap slot while the TIL still owns the page for commit.
    removeHOTLeavesFromCache(value);

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
    // Cross-generation guard: a reference whose activeTilGeneration belongs to a
    // PRIOR (snapshot) generation must NOT reuse its old logKey in the new TIL.
    // Without this check, a CoW of a frozen IndirectPage after
    // asyncIntermediateCommit() can collide with a structural-page slot in the
    // new generation (NamePage / CASPage / ProjectionIndexPage etc. that
    // reAddStructuralPagesToTil packed into low logKeys), silently swapping
    // the container at that index. The visible symptom is a ClassCastException
    // in KeyedTrieWriter.prepareIndirectPage when the trie navigation later
    // walks the structural page's reference and gets back the foreign container.
    final boolean ownsExistingKey = existingKey != Constants.NULL_ID_INT
        && existingKey >= 0
        && existingKey < size
        && ref.getActiveTilGeneration() == currentGeneration;
    if (ownsExistingKey) {
      // Close orphaned HOT leaf pages from the overwritten container.
      // After a leaf split the old complete page (original from disk/copy) is no longer
      // needed — the modified page has completeDump=true and all entries marked dirty.
      final PageContainer oldContainer = entries[existingKey];
      if (oldContainer != null && oldContainer != value) {
        closeOrphanedHOTLeafPages(oldContainer, value, existingKey);
      }
      // Reuse existing logKey — update container in-place.
      // This ensures that PageReference copies (from COW operations) that share
      // the same logKey will resolve to the latest container.
      entries[existingKey] = value;
      entryRefs[existingKey] = ref;
      ref.setActiveTilGeneration(currentGeneration);
    } else {
      // A cross-generation re-put supersedes the frozen entry this reference used to identify:
      // the CoW'd page in the NEW entry is now the authoritative version, and the frozen page's
      // upcoming (or already completed) flush is outdated. Record a forwarding link so stale
      // reference copies that still carry the old identity resolve to the new entry instead of
      // the stale disk offset (#1077).
      final int priorGeneration = ref.getActiveTilGeneration();
      final boolean supersedesPriorEntry =
          existingKey != Constants.NULL_ID_INT && existingKey >= 0 && priorGeneration >= 0
              && priorGeneration != currentGeneration;

      // New entry
      ensureCapacity();
      ref.setLogKey(size);
      ref.setActiveTilGeneration(currentGeneration);
      entries[size] = value;
      entryRefs[size] = ref;

      if (supersedesPriorEntry) {
        final long oldPacked = ((long) priorGeneration << 32) | (existingKey & 0xFFFFFFFFL);
        final long newPacked = ((long) currentGeneration << 32) | (size & 0xFFFFFFFFL);
        forwardedEntries.put(oldPacked, newPacked);
      }

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
   * Remove the HOT leaf pages of a TIL container from the shared HOT-leaf buffer cache.
   *
   * <p>A {@link HOTLeafPage} instance can be referenced by both a {@link PageContainer} in this
   * log and the {@code hotLeafPageCache}. Once a page is in the log it is transaction-private and
   * must survive — unevicted — until commit serializes it. The eviction paths
   * ({@code ClockSweeper.sweep}, {@code ShardedPageCache.evictUnderPressure}) only gate on guard
   * count, so a cache-resident dirty leaf would otherwise have its off-heap slot reclaimed under
   * memory pressure, corrupting the committed page. This mirrors the record-page-cache removal
   * already performed above for {@link KeyValueLeafPage}s.</p>
   */
  private void removeHOTLeavesFromCache(final PageContainer value) {
    if (value == null) {
      return;
    }
    final Cache<PageReference, HOTLeafPage> hotLeafCache = bufferManager.getHOTLeafPageCache();
    if (value.getComplete() instanceof HOTLeafPage complete) {
      hotLeafCache.removePage(complete);
    }
    if (value.getModified() instanceof HOTLeafPage modified && modified != value.getComplete()) {
      hotLeafCache.removePage(modified);
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

    try {
      for (int i = 0; i < snapshotSize; i++) {
        final PageContainer container = snapshotEntries[i];
        if (container == null) {
          continue;
        }
        final Page modified = container.getModified();
        final PageReference ref = snapshotRefs[i];

        // A snapshot slot is SUPERSEDED when its reference object no longer identifies it: page
        // references are shared and mutated in place, so a CoW during the just-finished epoch
        // re-bound this very object to a NEWER container (a fresh clone of the same logical
        // page). Applying this slot's (outdated) state to the re-bound reference would hijack
        // the live trie back to the stale version — the observed symptom was a leaf page that
        // straddled an epoch boundary being committed from its first, nearly-empty flush, with
        // every record added after the boundary silently lost (#1077).
        final boolean refStillIdentifiesSlot =
            ref.getActiveTilGeneration() == snapshotGeneration && ref.getLogKey() == i;

        if (modified instanceof KeyValueLeafPage) {
          // KVL page: already written to disk by background thread.
          // Validate: side-channel offset must be valid (not sentinel).
          final long diskOffset = snapshotDiskOffsets[i];
          if (diskOffset == Constants.NULL_ID_LONG) {
            throw new SirixIOException(
                "Snapshot entry " + i + " has no disk offset — background write incomplete or failed");
          }
          // Apply disk offset from side-channel — but ONLY if the reference still identifies
          // this slot (see above; a re-bound reference identifies a newer version).
          if (refStillIdentifiesSlot) {
            ref.setKey(diskOffset);
            if (snapshotHashes[i] != null) {
              ref.setHash(snapshotHashes[i]);
            }
          }
          // Store in completed map for stale-reference resolution (Layer 3 in get()).
          // When IndirectPages are CoW'd, their child references are COPIES that don't get
          // this disk offset update. The map allows get() to resolve stale copies.
          //
          // Superseded entries excluded (#1077): if this frozen page was CoW'd into a newer
          // generation while the snapshot was active (forwarding entry present), the flush we
          // just completed is an OUTDATED version — storing its offset would let stale copies
          // resolve to it and silently drop every record added after the epoch boundary. The
          // forwarding chain in get() routes such copies to the newer entry instead.
          final long packedKey = ((long) snapshotGeneration << 32) | (i & 0xFFFFFFFFL);
          if (forwardedEntries.get(packedKey) < 0) {
            completedDiskOffsets.put(packedKey, diskOffset);
            if (snapshotHashes[i] != null) {
              completedDiskHashes.put(packedKey, snapshotHashes[i]);
            }
          }
          // Close both complete and modified pages (release MemorySegment)
          closePageContainer(container);
          snapshotEntries[i] = null;
          snapshotRefs[i] = null;
        } else {
          // IndirectPage / structural page: promote to current TIL
          // so final commit traversal can find them via Layer 1 lookup.
          //
          // GUARD: promote ONLY if the reference still identifies this slot. It does not when
          // (a) reAddStructuralPagesToTil() already re-added this page (same ref object,
          // generation already moved to currentGeneration) — re-putting would overwrite the
          // ref's logKey and orphan the earlier entry — or (b) the page was CoW'd during the
          // just-finished epoch (the ref was re-bound to the newer clone) — re-putting would
          // rebind the live trie to the STALE frozen page (#1077, see refStillIdentifiesSlot).
          if (refStillIdentifiesSlot && ref.getActiveTilGeneration() != currentGeneration) {
            put(ref, container);
          }
          snapshotEntries[i] = null;
          snapshotRefs[i] = null;
        }
      }

      // NO generation-age pruning of completedDiskOffsets/completedDiskHashes (#1077). A stale
      // reference copy inside a CoW'd IndirectPage can legitimately stay untouched for many
      // epochs and only be dereferenced by the final commit traversal — the former
      // "currentGeneration - 2" prune deleted exactly the entry that traversal needed, so the
      // parent was serialized with child key -1 and the flushed subtree silently vanished from
      // the committed revision. Entries are retained until clear() (final commit or rollback);
      // the memory bound is one primitive long->long pair (plus an 8-byte hash) per leaf page
      // flushed by an async intermediate commit during this transaction's lifetime.
    } finally {
      // Release snapshot arrays for GC even if processing fails
      snapshotEntries = null;
      snapshotRefs = null;
      snapshotDiskOffsets = null;
      snapshotHashes = null;
      snapshotSize = 0;
    }
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
    forwardedEntries.clear();
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
    forwardedEntries.clear();
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
   * Close HOTLeafPages from an overwritten container that are not reused in the new container.
   * Prevents FrameSlot memory leaks when leaf splits overwrite TIL entries — the old complete
   * page (original from disk) is orphaned and its 65KB off-heap MemorySegment must be released.
   */
  private void closeOrphanedHOTLeafPages(final PageContainer oldContainer,
      final PageContainer newContainer, final int excludeIndex) {
    final Page oldComplete = oldContainer.getComplete();
    final Page oldModified = oldContainer.getModified();
    final Page newComplete = newContainer.getComplete();
    final Page newModified = newContainer.getModified();

    if (oldComplete instanceof HOTLeafPage completeLeaf
        && completeLeaf != newComplete && completeLeaf != newModified
        && !isHOTLeafInOtherEntry(completeLeaf, excludeIndex)
        && !bufferManager.getHOTLeafPageCache().containsPage(completeLeaf)) {
      completeLeaf.close();
    }
    if (oldModified != oldComplete
        && oldModified instanceof HOTLeafPage modifiedLeaf
        && modifiedLeaf != newComplete && modifiedLeaf != newModified
        && !isHOTLeafInOtherEntry(modifiedLeaf, excludeIndex)
        && !bufferManager.getHOTLeafPageCache().containsPage(modifiedLeaf)) {
      modifiedLeaf.close();
    }
  }

  private boolean isHOTLeafInOtherEntry(final HOTLeafPage page, final int excludeIndex) {
    for (int i = 0; i < size; i++) {
      if (i == excludeIndex) continue;
      final PageContainer entry = entries[i];
      if (entry != null && (entry.getComplete() == page || entry.getModified() == page)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Release the off-heap {@code MemorySegment}s of HOT leaf pages that incremental leaf
   * consolidation or a subtree rebuild merged away. Each page is no longer reachable from the
   * trie, so the tree-recursive commit never visits its entry — and a per-reference commit that
   * did reach it would skip it on the {@code isClosed()} guard. Closing here reclaims the 64KB
   * slots instead of pinning them until end-of-transaction {@link #clear()}.
   *
   * <p>Batched on purpose: the sharing guard ("is this leaf still referenced by another TIL
   * entry — a CoW reference copy") is a scan of the whole log. Doing it once for the whole drop
   * list is {@code O(size + orphans)}; doing it per leaf would be {@code O(size * orphans)} —
   * quadratic, since a large transaction's drop list and log both grow with the entry count.
   *
   * <p>A leaf still shared by another TIL entry or held by the HOT-leaf buffer cache is never
   * freed, so no concurrent reader loses its segment.
   *
   * @param orphanRefs references of the merged-away leaves — each carries its TIL log-key
   */
  public void releaseOrphanedHOTLeaves(final List<PageReference> orphanRefs) {
    if (orphanRefs == null || orphanRefs.isEmpty()) {
      return;
    }
    // Map each orphan leaf page to its own TIL index; this set is whittled down to the pages
    // that are safe to close.
    final IdentityHashMap<HOTLeafPage, Integer> closeable = new IdentityHashMap<>();
    for (int r = 0; r < orphanRefs.size(); r++) {
      final PageReference ref = orphanRefs.get(r);
      if (ref == null) {
        continue;
      }
      final int logKey = ref.getLogKey();
      if (logKey < 0 || logKey >= size) {
        continue;
      }
      final PageContainer container = entries[logKey];
      if (container == null) {
        continue;
      }
      if (container.getModified() instanceof HOTLeafPage leaf) {
        closeable.putIfAbsent(leaf, logKey);
      }
      if (container.getComplete() instanceof HOTLeafPage leaf) {
        closeable.putIfAbsent(leaf, logKey);
      }
    }
    if (closeable.isEmpty()) {
      return;
    }
    // One pass over the log: an orphan leaf that also appears at an entry other than its own is
    // shared (a CoW reference copy) and is dropped from the closeable set.
    for (int i = 0; i < size; i++) {
      final PageContainer entry = entries[i];
      if (entry == null) {
        continue;
      }
      unshareIfElsewhere(entry.getComplete(), i, closeable);
      if (entry.getModified() != entry.getComplete()) {
        unshareIfElsewhere(entry.getModified(), i, closeable);
      }
    }
    for (final HOTLeafPage leaf : closeable.keySet()) {
      if (!leaf.isClosed() && !bufferManager.getHOTLeafPageCache().containsPage(leaf)) {
        leaf.close();
      }
    }
  }

  private static void unshareIfElsewhere(final Page page, final int entryIndex,
      final IdentityHashMap<HOTLeafPage, Integer> closeable) {
    if (page instanceof HOTLeafPage leaf) {
      final Integer ownIndex = closeable.get(leaf);
      if (ownIndex != null && ownIndex.intValue() != entryIndex) {
        closeable.remove(leaf);
      }
    }
  }

  /**
   * Helper method to tear down a TIL-owned page without harming concurrent holders.
   */
  private void closePage(final Page page) {
    if (page instanceof KeyValueLeafPage kvPage) {
      if (!kvPage.isClosed()) {
        // NEVER force-release guards this transaction does not own: a concurrent reader may
        // have resolved this very instance through a shared reference and still hold its
        // guard mid-read — draining freed the frame under the reader (use-after-free / torn
        // reads), exactly like the HOTLeafPage case below. markOrphaned() + guard-aware
        // close(): immediate teardown when unguarded, deferred to the LAST releaseGuard()
        // otherwise.
        kvPage.markOrphaned();
        kvPage.close();
      }
    } else if (page instanceof HOTLeafPage hotLeaf && !hotLeaf.isClosed()) {
      // Do NOT free a HOT leaf still owned by the shared HOT-leaf buffer cache — the same
      // instance backs both a TIL PageContainer and the cache, so closing it here would
      // free the off-heap MemorySegment out from under concurrent readers.
      if (!bufferManager.getHOTLeafPageCache().containsPage(hotLeaf)) {
        // NEVER force-release guards this transaction does not own: a concurrent reader may
        // have resolved this very instance from the shared cache BEFORE the TIL's CoW removed
        // it and still holds its guard across the leaf visit — draining it freed the frame
        // under the reader (use-after-free / silently wrong index reads). HOTLeafPage.close()
        // is guard-aware: with live guards it only marks the page orphaned and the LAST
        // releaseGuard() performs the actual teardown.
        hotLeaf.close();
      }
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
