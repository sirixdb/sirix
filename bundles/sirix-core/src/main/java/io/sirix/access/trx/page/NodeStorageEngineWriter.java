/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.access.trx.node.IndexController;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.PageGuard;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.exception.SirixIOException;
import io.sirix.io.SerializationBufferPool;
import io.sirix.node.PooledBytesOut;
import io.sirix.utils.OS;
import io.sirix.index.IndexType;
import io.sirix.io.Writer;
import io.sirix.node.DeletedNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.Node;
import io.sirix.page.*;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.DiagnosticSettings;
import io.sirix.settings.Fixed;
import io.sirix.settings.VersioningType;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Implements the {@link StorageEngineWriter} interface to provide write capabilities to the persistent storage
 * layer.
 * </p>
 *
 * @author Marc Kramis, Seabix AG
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
final class NodeStorageEngineWriter extends AbstractForwardingStorageEngineReader implements StorageEngineWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NodeStorageEngineWriter.class);

  /**
   * Debug flag for memory leak tracking.
   * @see DiagnosticSettings#MEMORY_LEAK_TRACKING
   */
  private static final boolean DEBUG_MEMORY_LEAKS = DiagnosticSettings.MEMORY_LEAK_TRACKING;

  private BytesOut<?> bufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);

  /**
   * Page writer to serialize.
   */
  private final Writer storagePageReaderWriter;

  /**
   * Transaction intent log.
   */
  TransactionIntentLog log;
  
  /**
   * Last reference to the actual revRoot.
   */
  private final RevisionRootPage newRevisionRootPage;

  /**
   * {@link NodeStorageEngineReader} instance.
   */
  private final NodeStorageEngineReader pageRtx;

  /**
   * Determines if transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * Pending fsync future for async durability.
   * For auto-commit mode, fsync runs in background; next commit waits for it.
   */
  private volatile java.util.concurrent.CompletableFuture<Void> pendingFsync;

  /**
   * Pending snapshot being committed asynchronously.
   * <p>
   * Used for layered TIL lookup during async auto-commit:
   * 1. Check active TIL (via generation counter)
   * 2. Check pending snapshot (via identity map or logKey fallback)
   * 3. Fall back to disk
   */
  private volatile CommitSnapshot pendingSnapshot;

  /**
   * Semaphore for backpressure on async commits.
   * Only one async commit can be in flight at a time.
   */
  private final java.util.concurrent.Semaphore commitPermit = new java.util.concurrent.Semaphore(1);

  /**
   * Tracks whether an async intermediate commit is currently in flight.
   * Set to true by {@link #asyncIntermediateCommit()}, cleared by {@link #executeCommitSnapshot}.
   * Used by {@link #awaitPendingAsyncCommit()} to avoid blocking when no async commit is pending.
   */
  private volatile boolean asyncCommitInFlight;

  /**
   * Stores any exception thrown by the background commit thread.
   * Checked and rethrown by {@link #awaitPendingAsyncCommit()}.
   */
  private volatile Throwable asyncCommitError;

  /**
   * Old snapshots whose pages have not yet been closed.
   * <p>
   * Pages from old snapshots may be "borrowed" into the active TIL when
   * {@code combineRecordPagesForModification} reuses a snapshot page as the
   * {@code complete} of a new container. Closing those pages during rotation
   * would destroy memory segments still in use. Instead, old snapshots are
   * kept here and closed during the final sync commit, after {@code log.clear()}
   * has already closed all TIL pages (including borrowed ones). At that point,
   * {@code closePages()} safely skips pages that are already closed.
   */
  private final List<CommitSnapshot> oldSnapshots = new ArrayList<>();

  /**
   * Tracks the previous DOCUMENT page key for page-full detection.
   * When a record is created on a different page, the flag is set so that
   * the next {@code checkAccessAndCommit()} call triggers eager serialization.
   */
  private long lastDocumentRecordPageKey = -1;

  /**
   * Set by {@link #createRecord} when a DOCUMENT page boundary is crossed.
   * Checked and cleared by {@link #eagerSerializePagesIfPageBoundaryCrossed()}.
   */
  private boolean pageBoundaryCrossed;

  /**
   * {@link XmlIndexController} instance.
   */
  private final IndexController<?, ?> indexController;

  /**
   * The indirect page trie writer - manages the trie structure of IndirectPages.
   */
  private final TrieWriter trieWriter;
  
  /**
   * The HOT trie writer - manages HOT (Height Optimized Trie) structure.
   * Used for cache-friendly secondary indexes (PATH, CAS, NAME).
   */
  private final HOTTrieWriter hotTrieWriter;

  /**
   * The revision to represent.
   */
  private final int representRevision;
  
  /**
   * Trie type for routing page operations.
   */
  public enum TrieType {
    /** Traditional bit-decomposed IndirectPage trie (existing approach). */
    KEYED_TRIE,
    /** HOT (Height Optimized Trie) for cache-friendly secondary indexes (new approach). */
    HOT
  }

  /**
   * {@code true} if this page write trx will be bound to a node trx, {@code false} otherwise
   */
  private final boolean isBoundToNodeTrx;

  private record IndexLogKeyToPageContainer(IndexType indexType, long recordPageKey, int indexNumber,
                                            int revisionNumber, PageContainer pageContainer) {
  }

  /**
   * The most recent page container.
   */
  private IndexLogKeyToPageContainer mostRecentPageContainer;

  /**
   * The second most recent page container.
   */
  private IndexLogKeyToPageContainer secondMostRecentPageContainer;

  /**
   * The most recent path summary page container.
   */
  private IndexLogKeyToPageContainer mostRecentPathSummaryPageContainer;

  private final LinkedHashMap<IndexLogKey, PageContainer> pageContainerCache;

  /**
   * Constructor.
   *
   * @param writer            the page writer
   * @param log               the transaction intent log
   * @param revisionRootPage  the revision root page
   * @param pageRtx           the page reading transaction used as a delegate
   * @param indexController   the index controller, which is used to update indexes
   * @param representRevision the revision to represent
   * @param isBoundToNodeTrx  {@code true} if this page write trx will be bound to a node trx,
   *                          {@code false} otherwise
   */
  NodeStorageEngineWriter(final Writer writer, final TransactionIntentLog log,
      final RevisionRootPage revisionRootPage, final NodeStorageEngineReader pageRtx,
      final IndexController<?, ?> indexController, final int representRevision, final boolean isBoundToNodeTrx) {
    this.trieWriter = new TrieWriter();
    this.hotTrieWriter = new HOTTrieWriter();
    storagePageReaderWriter = requireNonNull(writer);
    this.log = requireNonNull(log);
    newRevisionRootPage = requireNonNull(revisionRootPage);
    this.pageRtx = requireNonNull(pageRtx);
    this.indexController = requireNonNull(indexController);
    checkArgument(representRevision >= 0, "The represented revision must be >= 0.");
    this.representRevision = representRevision;
    this.isBoundToNodeTrx = isBoundToNodeTrx;
    mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer = mostRecentPageContainer;
    mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);
    pageContainerCache = new LinkedHashMap<>(100, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<IndexLogKey, PageContainer> eldest) {
        if (size() > 100) {
          // When evicting PageContainer from local cache, ensure pages are properly tracked
          // Pages should be in TIL (will be closed on commit/rollback) or in global cache
          PageContainer container = eldest.getValue();
          if (container != null) {
            // Pages in local cache should already be in TIL (appended via appendLogRecord)
            // No action needed - TIL will handle cleanup
          }
          return true;
        }
        return false;
      }
    };
  }

  // ============================================================================
  // Async Auto-Commit: Layered TIL Lookup
  // ============================================================================

  /**
   * 3-step layered lookup for async auto-commit support.
   * <p>
   * During async auto-commit, pages may be in:
   * <ol>
   *   <li>Active TIL (pages added after rotation)</li>
   *   <li>Pending snapshot (pages being committed in background)</li>
   *   <li>Disk (pages already committed or from previous revisions)</li>
   * </ol>
   * <p>
   * This method checks in that order and returns the first match.
   *
   * @param ref the page reference to look up
   * @return the page container, or null if page should be loaded from disk
   */
  @Nullable
  PageContainer getFromActiveOrPending(final PageReference ref) {
    if (ref == null) {
      return null;
    }

    // STEP 1: FAST PATH - Check if ref was added to active TIL after rotation
    final int currentGen = log.getCurrentGeneration();
    if (ref.isInActiveTil(currentGen)) {
      // This ref belongs to current active TIL
      return log.getUnchecked(ref.getLogKey());
    }

    // STEP 2: Check latest pending snapshot
    final CommitSnapshot snapshot = pendingSnapshot;
    if (snapshot != null) {
      final PageContainer result = getFromSnapshot(ref, snapshot);
      if (result != null) {
        return result;
      }
    }

    // STEP 3: Check older snapshots (pages not yet closed)
    for (int i = oldSnapshots.size() - 1; i >= 0; i--) {
      final PageContainer result = getFromSnapshot(ref, oldSnapshots.get(i));
      if (result != null) {
        return result;
      }
    }

    return null; // Fall back to disk
  }

  /**
   * Promote a page container from the pending snapshot to the active TIL.
   * <p>
   * This MUST be called for leaf-level references (KeyValueLeafPage) after
   * finding them in the snapshot. Without promotion:
   * <ul>
   *   <li>The reference retains stale logKey/generation and becomes unreachable
   *       after the next TIL rotation.</li>
   *   <li>{@link CommitSnapshot#closePages()} would close pages still needed
   *       by the active TIL.</li>
   * </ul>
   * <p>
   * IMPORTANT: Do NOT call this for indirect page references — use the CoW
   * mechanism in {@code prepareIndirectPage} instead.
   *
   * @param ref the leaf page reference to promote
   * @param container the page container from the snapshot
   */
  private void promoteFromSnapshot(final PageReference ref, final PageContainer container) {
    appendLogRecord(ref, container);
    // NOTE: Do NOT clear snapshot entries here! The snapshot entry must remain accessible
    // because future CoW copies of IndirectPages create references with the SAME old logKey.
    // Clearing the entry would make those copied references point to null, causing
    // "Cannot retrieve record" errors. Snapshot cleanup is deferred to the sync commit
    // where closePages() safely checks isClosed() before closing pages.
  }

  /**
   * Get all active snapshots, newest first.
   * <p>
   * Used for trie navigation and page lookups that need to check all snapshots.
   *
   * @return list of all active snapshots (pendingSnapshot first, then oldSnapshots newest-first)
   */
  private List<CommitSnapshot> getAllActiveSnapshots() {
    final CommitSnapshot snap = pendingSnapshot;
    if (snap == null && oldSnapshots.isEmpty()) {
      return List.of();
    }
    final List<CommitSnapshot> result = new ArrayList<>(oldSnapshots.size() + 1);
    if (snap != null) {
      result.add(snap);
    }
    for (int i = oldSnapshots.size() - 1; i >= 0; i--) {
      result.add(oldSnapshots.get(i));
    }
    return result;
  }

  /**
   * Look up a page in the pending snapshot.
   *
   * @param ref the page reference
   * @param snapshot the pending snapshot
   * @return the page container, or null to signal disk load
   */
  @Nullable
  private PageContainer getFromSnapshot(final PageReference ref, final CommitSnapshot snapshot) {
    // STEP 2a: Try identity-based lookup (works for original refs).
    // Always try in-memory lookup first — pages stay alive in the snapshot
    // until the next snapshot rotation closes them on the insert thread.
    PageContainer result = snapshot.getByIdentity(ref);
    if (result != null) {
      return result;
    }

    // STEP 2b: Fallback for cloned RRP refs (8 root references).
    // CRITICAL: Only use logKey fallback when:
    //   (a) the ref hasn't been resolved to disk yet (key == NULL_ID_LONG)
    //   (b) the ref's generation matches this snapshot's generation — prevents cross-snapshot
    //       logKey collisions where a ref from an older snapshot has a stale logKey that
    //       coincidentally matches a different page in this newer snapshot
    final int logKey = ref.getLogKey();
    final int snapshotGen = snapshot.getSnapshotGeneration();
    if (logKey >= 0 && logKey < snapshot.size()
        && ref.getKey() == Constants.NULL_ID_LONG
        && ref.getActiveTilGeneration() == snapshotGen) {
      return snapshot.getEntry(logKey);
    }

    // STEP 2c: If commit is complete, use disk offset mapping for lazy propagation
    if (ref.getKey() == Constants.NULL_ID_LONG && snapshot.isCommitComplete()
        && logKey >= 0 && ref.getActiveTilGeneration() == snapshotGen) {
      final long diskOffset = snapshot.getDiskOffset(logKey);
      if (diskOffset >= 0) {
        ref.setKey(diskOffset);
      }
    }

    return null; // Fall back to disk
  }

  // ============================================================================
  // Async Auto-Commit: Snapshot Management
  // ============================================================================

  /**
   * Acquire permit for async commit (backpressure).
   * <p>
   * Blocks if a previous async commit is still in flight.
   * Call this before {@link #prepareAsyncCommitSnapshot}.
   *
   * @throws InterruptedException if interrupted while waiting
   */
  public void acquireCommitPermit() throws InterruptedException {
    commitPermit.acquire();
  }

  /**
   * Try to acquire permit for async commit without blocking.
   *
   * @return true if permit acquired, false if another commit is in flight
   */
  public boolean tryAcquireCommitPermit() {
    return commitPermit.tryAcquire();
  }

  /**
   * Prepare a snapshot for async commit by rotating the TIL.
   * <p>
   * This method:
   * <ol>
   *   <li>Rotates the TIL (increments generation, swaps arrays)</li>
   *   <li>Creates a deep copy of RevisionRootPage for isolation</li>
   *   <li>Creates an immutable CommitSnapshot</li>
   *   <li>Sets it as the pending snapshot for layered lookup</li>
   * </ol>
   * <p>
   * After this call, inserts continue into the fresh TIL while
   * the snapshot can be committed in background.
   *
   * @param commitMessage optional commit message
   * @param commitTimestamp the commit timestamp
   * @param commitFile the commit file (may be null)
   * @return the commit snapshot for background processing
   */
  public CommitSnapshot prepareAsyncCommitSnapshot(
      final String commitMessage,
      final long commitTimestamp,
      final java.io.File commitFile) {

    pageRtx.assertNotClosed();

    // Rotate TIL: freeze current entries, reset for new inserts
    final TransactionIntentLog.RotationResult rotation = log.detachAndReset();

    // Check for empty snapshot
    if (rotation.size() == 0) {
      // Nothing to commit - release permit immediately
      commitPermit.release();
      return null;
    }

    // The snapshot's generation is the one BEFORE the rotation incremented it.
    // detachAndReset() incremented currentGeneration, so the snapshot's entries
    // belong to (currentGeneration - 1).
    final int snapshotGeneration = log.getCurrentGeneration() - 1;

    // Create snapshot with deep-copied RRP
    final CommitSnapshot snapshot = new CommitSnapshot(
        rotation,
        pageRtx.getUberPage(),
        new PageReference()
            .setDatabaseId(pageRtx.getDatabaseId())
            .setResourceId(pageRtx.getResourceId()),
        newRevisionRootPage,
        newRevisionRootPage.getRevision(),
        commitMessage,
        commitTimestamp,
        commitFile,
        getResourceSession().getResourceConfig(),
        snapshotGeneration
    );

    // Propagate disk offsets from old snapshot to original references.
    // This ensures references from older generations have valid disk keys.
    // IMPORTANT: Do NOT close pages here! Pages from the old snapshot may be "borrowed"
    // into the active TIL (as the 'complete' of a new container created by
    // combineRecordPagesForModification). Closing them would destroy memory segments
    // still in use. Old snapshots are tracked and closed during the sync commit cleanup,
    // after log.clear() has already closed all TIL pages (including borrowed ones).
    final CommitSnapshot oldSnapshot = this.pendingSnapshot;
    if (oldSnapshot != null) {
      oldSnapshot.propagateDiskOffsets();
      oldSnapshots.add(oldSnapshot);
    }

    // Set as pending for layered lookup
    this.pendingSnapshot = snapshot;

    // Make snapshot pages accessible to the reader's loadPage() via TIL fallback.
    // Checks the latest snapshot first, then scans older snapshots in reverse order.
    // Identity-based lookup in each snapshot is O(1), so overall cost is O(snapshots).
    log.setSnapshotLookup(ref -> {
      final CommitSnapshot snap = this.pendingSnapshot;
      if (snap != null) {
        final PageContainer result = getFromSnapshot(ref, snap);
        if (result != null) {
          return result;
        }
      }
      for (int i = oldSnapshots.size() - 1; i >= 0; i--) {
        final PageContainer result = getFromSnapshot(ref, oldSnapshots.get(i));
        if (result != null) {
          return result;
        }
      }
      return null;
    });

    // Re-add root-level page references to the fresh active TIL.
    // These references are put into the TIL ONCE during writer creation (StorageEngineWriterFactory)
    // and their pages are modified in-place during inserts. After rotation, they end up in the
    // snapshot but the sync commit needs them in the active TIL to write the final state.
    reAddRootPageToActiveTil(pageRtx.getUberPage().getRevisionRootReference(),
        PageContainer.getInstance(newRevisionRootPage, newRevisionRootPage), snapshot);
    reAddRootPageToActiveTil(newRevisionRootPage.getNamePageReference(), snapshot);
    reAddRootPageToActiveTil(newRevisionRootPage.getPathSummaryPageReference(), snapshot);
    reAddRootPageToActiveTil(newRevisionRootPage.getCASPageReference(), snapshot);
    reAddRootPageToActiveTil(newRevisionRootPage.getPathPageReference(), snapshot);
    reAddRootPageToActiveTil(newRevisionRootPage.getDeweyIdPageReference(), snapshot);

    // Reset local caches after rotation
    mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer = mostRecentPageContainer;
    mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);
    pageContainerCache.clear();

    return snapshot;
  }

  /**
   * Execute commit of a snapshot in the background.
   * <p>
   * This method performs the actual serialization and disk writing
   * using the snapshot's frozen TIL. It should be called from a
   * background thread.
   * <p>
   * After completion, the snapshot is marked complete and eventually
   * cleared from pending.
   *
   * @param snapshot the snapshot to commit
   */
  public void executeCommitSnapshot(final CommitSnapshot snapshot) {
    if (snapshot == null) {
      return;
    }

    try {
      final var resourceConfig = snapshot.resourceConfig();
      final int snapshotGen = snapshot.getSnapshotGeneration();

      // Allocate a separate buffer for the background thread (Writer's internal byteBufferBytes
      // is safe to reuse since the Semaphore guarantees sequential Writer access).
      final var bgBufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
      try {
        for (final var entry : snapshot.refToContainerEntries()) {
          final PageReference ref = entry.getKey();
          final PageContainer container = entry.getValue();
          if (container == null) {
            continue;
          }
          final Page modified = container.getModified();
          if (!(modified instanceof KeyValueLeafPage)) {
            continue;
          }
          // Skip refs promoted to the active TIL by the insert thread.
          // activeTilGeneration is volatile, so the background thread sees promotions.
          if (ref.getActiveTilGeneration() > snapshotGen) {
            continue;
          }

          final int frozenLogKey = snapshot.getSnapshotLogKey(ref);

          // Write page to disk — sets ref.key and ref.hash on the original PageReference
          storagePageReaderWriter.write(resourceConfig, ref, modified, bgBufferBytes);

          // Record that this entry was written so removeBackgroundWrittenEntries() can skip it
          snapshot.recordDiskOffset(frozenLogKey, ref.getKey());
        }

        // Flush remaining buffered data to the data file
        storagePageReaderWriter.flushBufferedWrites(bgBufferBytes);
      } finally {
        bgBufferBytes.close();
      }

      snapshot.markCommitComplete();
    } catch (final Throwable t) {
      asyncCommitError = t;
    } finally {
      // Release permit for next commit (or for awaitPendingAsyncCommit)
      commitPermit.release();
    }
  }

  /**
   * Re-add a root-level page reference from the snapshot to the active TIL.
   * Retrieves the live container from the snapshot by the ref's current logKey.
   *
   * @param ref the page reference to re-add
   * @param snapshot the snapshot containing the ref's container
   */
  private void reAddRootPageToActiveTil(final PageReference ref, final CommitSnapshot snapshot) {
    if (ref == null) {
      return;
    }
    final int oldLogKey = ref.getLogKey();
    if (oldLogKey >= 0 && oldLogKey < snapshot.size()) {
      final PageContainer container = snapshot.getEntry(oldLogKey);
      if (container != null) {
        log.put(ref, container);
        snapshot.clearEntry(oldLogKey);
      }
    }
  }

  /**
   * Re-add a root-level page reference with an explicit container.
   * Used for the RRP where the container is constructed from the live page.
   *
   * @param ref the page reference to re-add
   * @param container the container to use
   * @param snapshot the snapshot to clear the old entry from
   */
  private void reAddRootPageToActiveTil(final PageReference ref, final PageContainer container,
                                        final CommitSnapshot snapshot) {
    if (ref == null) {
      return;
    }
    final int oldLogKey = ref.getLogKey();
    log.put(ref, container);
    if (oldLogKey >= 0) {
      snapshot.clearEntry(oldLogKey);
    }
  }

  /**
   * Get the pending snapshot (for testing/debugging).
   *
   * @return the pending snapshot, or null if none
   */
  @Nullable
  CommitSnapshot getPendingSnapshot() {
    return pendingSnapshot;
  }

  // ============================================================================
  // Async Auto-Commit Integration
  // ============================================================================

  @Override
  public void asyncIntermediateCommit() {
    // Reset page-full tracking — old TIL entries are going into the snapshot
    lastDocumentRecordPageKey = -1;
    pageBoundaryCrossed = false;

    // Block if another async commit is still in flight (backpressure)
    commitPermit.acquireUninterruptibly();

    final CommitSnapshot snapshot = prepareAsyncCommitSnapshot(
        "autoCommit", System.currentTimeMillis(), null);

    if (snapshot == null) {
      // Nothing to commit (empty TIL) — permit already released by prepareAsyncCommitSnapshot
      return;
    }

    // Mark async commit as in-flight before scheduling
    asyncCommitInFlight = true;

    // Schedule background commit — insert thread continues immediately
    java.util.concurrent.CompletableFuture.runAsync(() -> executeCommitSnapshot(snapshot));
  }

  @Override
  public void awaitPendingAsyncCommit() {
    if (!asyncCommitInFlight) {
      return;
    }
    commitPermit.acquireUninterruptibly();
    commitPermit.release();
    asyncCommitInFlight = false;

    final Throwable error = asyncCommitError;
    if (error != null) {
      asyncCommitError = null;
      throw new SirixIOException("Async intermediate commit failed", error);
    }
  }

  // ============================================================================
  // Eager Serialization for GC Pressure Reduction
  // ============================================================================

  @Override
  public void eagerSerializePages() {
    final var resourceConfig = getResourceSession().getResourceConfig();
    final int tilSize = log.size();
    for (int i = 0; i < tilSize; i++) {
      final PageContainer container = log.getUnchecked(i);
      if (container == null) {
        continue;
      }
      final Page modified = container.getModified();
      if (modified instanceof final KeyValueLeafPage kvPage && !kvPage.isAddedReferences()) {
        kvPage.addReferences(resourceConfig);
        kvPage.clearRecordsForGC();
      }
    }
  }

  @Override
  public void eagerSerializePagesIfPageBoundaryCrossed() {
    if (pageBoundaryCrossed) {
      pageBoundaryCrossed = false;
      eagerSerializePages();
    }
  }

  // ============================================================================
  // Trie Type Routing
  // ============================================================================
  
  /**
   * Determine which trie type to use for the given index.
   * 
   * <p>Currently all indexes use KEYED_TRIE. HOT is available but requires
   * HOTLeafPage to implement setRecord()/getRecord() compatible with existing API.</p>
   * 
   * <p>TODO to enable HOT:</p>
   * <ul>
   *   <li>Implement HOTLeafPage.setRecord() to store DataRecords</li>
   *   <li>Implement HOTLeafPage.getRecord() to retrieve DataRecords</li>
   *   <li>Bridge between HOT key-value storage and DataRecord storage</li>
   * </ul>
   *
   * @param indexType the index type
   * @param indexNumber the index number
   * @return the trie type to use
   */
  @SuppressWarnings("unused") // indexType/indexNumber will be used when HOT is enabled
  private TrieType getTrieType(@NonNull IndexType indexType, int indexNumber) {
    // HOT is not yet fully integrated with DataRecord storage.
    // HOTLeafPage needs to implement setRecord()/getRecord() to be compatible.
    // Once implemented, enable with:
    //   return switch (indexType) {
    //     case PATH, CAS, NAME -> TrieType.HOT;
    //     default -> TrieType.KEYED_TRIE;
    //   };
    return TrieType.KEYED_TRIE;
  }
  
  /**
   * Get the HOT trie writer for HOT index operations.
   * 
   * @return the HOT trie writer
   */
  public HOTTrieWriter getHOTTrieWriter() {
    return hotTrieWriter;
  }

  @Override
  public BytesOut<?> newBufferedBytesInstance() {
    bufferBytes = Bytes.elasticOffHeapByteBuffer(Writer.FLUSH_SIZE);
    return bufferBytes;
  }

  @Override
  public int getRevisionToRepresent() {
    pageRtx.assertNotClosed();
    return representRevision;
  }

  @Override
  public TransactionIntentLog getLog() {
    pageRtx.assertNotClosed();
    return log;
  }

  @Override
  public int getRevisionNumber() {
    pageRtx.assertNotClosed();
    return newRevisionRootPage.getRevision();
  }

  @Override
  public DataRecord prepareRecordForModification(final long recordKey, @NonNull final IndexType indexType,
      final int index) {
    pageRtx.assertNotClosed();
    checkArgument(recordKey >= 0, "recordKey must be >= 0!");
    requireNonNull(indexType);

    final long recordPageKey = pageRtx.pageKey(recordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final var modifiedPage = cont.getModifiedAsKeyValuePage();

    DataRecord record = pageRtx.getValue(modifiedPage, recordKey);
    if (record == null) {
      final var completePage = cont.getCompleteAsKeyValuePage();
      final DataRecord oldRecord = pageRtx.getValue(completePage, recordKey);
      if (oldRecord == null) {
        throw new SirixIOException(
            "Cannot retrieve record from cache: (key: " + recordKey + ") (indexType: " + indexType + ") (index: "
                + index + ")");
      }
      record = oldRecord;
      modifiedPage.setRecord(record);
    }

    return record;
  }

  @Override
  public DataRecord createRecord(@NonNull final DataRecord record, @NonNull final IndexType indexType,
      @NonNegative final int index) {
    pageRtx.assertNotClosed();

    // Allocate record key and increment record count.
    // For RECORD_TO_REVISIONS: Use the record's own nodeKey (document node key) for page allocation.
    // This is critical because later lookups via prepareRecordForModification use the document node key.
    // Using an auto-allocated key would cause a mismatch where the record can't be found.
    // $CASES-OMITTED$
    final long createdRecordKey = switch (indexType) {
      case DOCUMENT -> newRevisionRootPage.incrementAndGetMaxNodeKeyInDocumentIndex();
      case CHANGED_NODES -> newRevisionRootPage.incrementAndGetMaxNodeKeyInChangedNodesIndex();
      case RECORD_TO_REVISIONS -> {
        // CRITICAL FIX: Use the document node key for page allocation, not an auto-allocated key.
        // The RevisionReferencesNode stores document node key as its nodeKey, and we need to 
        // be able to look it up later using that same key in prepareRecordForModification.
        // Still update max key for proper tracking.
        long documentNodeKey = record.getNodeKey();
        long currentMax = newRevisionRootPage.getMaxNodeKeyInRecordToRevisionsIndex();
        if (documentNodeKey > currentMax) {
          newRevisionRootPage.setMaxNodeKeyInRecordToRevisionsIndex(documentNodeKey);
        }
        yield documentNodeKey;
      }
      case PATH_SUMMARY -> {
        // CRITICAL FIX: Use accessor method instead of direct .getPage() call
        // PageReference.getPage() can return null after TIL.put() nulls it
        // Accessor methods use loadPage() which handles TIL lookups
        final PathSummaryPage pathSummaryPage = pageRtx.getPathSummaryPage(newRevisionRootPage);
        yield pathSummaryPage.incrementAndGetMaxNodeKey(index);
      }
      case CAS -> {
        final CASPage casPage = pageRtx.getCASPage(newRevisionRootPage);
        yield casPage.incrementAndGetMaxNodeKey(index);
      }
      case PATH -> {
        final PathPage pathPage = pageRtx.getPathPage(newRevisionRootPage);
        yield pathPage.incrementAndGetMaxNodeKey(index);
      }
      case NAME -> {
        final NamePage namePage = pageRtx.getNamePage(newRevisionRootPage);
        yield namePage.incrementAndGetMaxNodeKey(index);
      }
      default -> throw new IllegalStateException();
    };

    final long recordPageKey = pageRtx.pageKey(createdRecordKey, indexType);

    // Page-full detection: when the DOCUMENT index crosses a page boundary,
    // set a flag so the next checkAccessAndCommit() triggers eager serialization.
    if (indexType == IndexType.DOCUMENT) {
      if (lastDocumentRecordPageKey >= 0 && recordPageKey != lastDocumentRecordPageKey) {
        pageBoundaryCrossed = true;
      }
      lastDocumentRecordPageKey = recordPageKey;
    }

    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final KeyValuePage<DataRecord> modified = cont.getModifiedAsKeyValuePage();
    modified.setRecord(record);
    return record;
  }

  @Override
  public void removeRecord(final long recordKey, @NonNull final IndexType indexType, final int index) {
    pageRtx.assertNotClosed();

    final long recordPageKey = pageRtx.pageKey(recordKey, indexType);
    final PageContainer cont = prepareRecordPage(recordPageKey, index, indexType);
    final DataRecord node = getRecord(recordKey, indexType, index);
    if (node == null) {
      throw new IllegalStateException("Node not found: " + recordKey);
    }

    final Node delNode = new DeletedNode(new NodeDelegate(node.getNodeKey(),
                                                          -1,
                                                          null,
                                                          -1,
                                                          pageRtx.getRevisionNumber(),
                                                          (SirixDeweyID) null));
    cont.getModifiedAsKeyValuePage().setRecord(delNode);
    cont.getCompleteAsKeyValuePage().setRecord(delNode);
  }

  @Override
  public <V extends DataRecord> V getRecord(final long recordKey, @NonNull final IndexType indexType,
      @NonNegative final int index) {
    pageRtx.assertNotClosed();

    checkArgument(recordKey >= Fixed.NULL_NODE_KEY.getStandardProperty());
    requireNonNull(indexType);

    // Calculate page.
    final long recordPageKey = pageRtx.pageKey(recordKey, indexType);

    final PageContainer pageCont = getPageContainer(recordPageKey, index, indexType);

    if (pageCont == null) {
      return pageRtx.getRecord(recordKey, indexType, index);
    } else {
      DataRecord node = pageRtx.getValue(((KeyValueLeafPage) pageCont.getModified()), recordKey);
      if (node == null) {
        node = pageRtx.getValue(((KeyValueLeafPage) pageCont.getComplete()), recordKey);
      }
      return (V) pageRtx.checkItemIfDeleted(node);
    }
  }

  @Override
  public String getName(final int nameKey, @NonNull final NodeKind nodeKind) {
    pageRtx.assertNotClosed();
    final NamePage currentNamePage = getNamePage(newRevisionRootPage);
    return (currentNamePage == null || currentNamePage.getName(nameKey, nodeKind, pageRtx) == null) ? pageRtx.getName(
        nameKey,
        nodeKind) : currentNamePage.getName(nameKey, nodeKind, pageRtx);
  }

  @Override
  public int createNameKey(final @Nullable String name, @NonNull final NodeKind nodeKind) {
    pageRtx.assertNotClosed();
    requireNonNull(nodeKind);
    final String string = name == null ? "" : name;
    final NamePage namePage = getNamePage(newRevisionRootPage);
    return namePage.setName(string, nodeKind, this);
  }

  @Override
  public void commit(final @Nullable PageReference reference) {
    if (reference == null) {
      return;
    }

    PageContainer container = log.get(reference);

    if (container == null) {
      return;
    }

    final var page = container.getModified();

    // Recursively commit indirectly referenced pages and then write self.
    page.commit(this);
    storagePageReaderWriter.write(getResourceSession().getResourceConfig(), reference, page, bufferBytes);

    container.getComplete().close();
    page.close();

    // Remove page reference.
    reference.setPage(null);
  }

  @Override
  public UberPage commit(@Nullable final String commitMessage, @Nullable final Instant commitTimestamp,
                         final boolean isAutoCommitting) {
    pageRtx.assertNotClosed();

    // Wait for any pending async intermediate commit to complete before sync commit
    awaitPendingAsyncCommit();

    // Propagate disk offsets from the last pending snapshot to original references.
    // After this, references to pages written by the background commit have valid disk keys,
    // so the recursive commit correctly skips them (null container from logKey check).
    // IMPORTANT: Do NOT close pages or clear snapshotLookup here! The sync commit's tree
    // traversal needs to find pages that are still in the snapshot (not yet propagated or
    // not written by the background commit). Cleanup happens AFTER the commit writes.
    final CommitSnapshot lastSnapshot = pendingSnapshot;
    if (lastSnapshot != null && lastSnapshot.isCommitComplete()) {
      lastSnapshot.propagateDiskOffsets();
      // Remove background-written leaf pages from the snapshot so the sync commit's
      // tree traversal (Page.commit → logKey check) skips them.
      lastSnapshot.removeBackgroundWrittenEntries();
    }

    pageRtx.resourceSession.getCommitLock().lock();

    try {
      final Path commitFile = pageRtx.resourceSession.getCommitFile();

      commitFile.toFile().deleteOnExit();
      // Issues with windows that it's not created in the first time?
      createIfAbsent(commitFile);

      final PageReference uberPageReference = new PageReference()
          .setDatabaseId(pageRtx.getDatabaseId())
          .setResourceId(pageRtx.getResourceId());
      final UberPage uberPage = pageRtx.getUberPage();
      uberPageReference.setPage(uberPage);

      setUserIfPresent();
      setCommitMessageAndTimestampIfRequired(commitMessage, commitTimestamp);

      // PIPELINING: Serialize pages WHILE previous fsync may still be running
      // This overlaps CPU work (serialization) with IO work (fsync)
      parallelSerializationOfKeyValuePages();

      // Recursively write indirectly referenced pages (serializes to buffers)
      uberPage.commit(this);

      // NOW wait for previous fsync before writing to storage
      // This ensures ordering: previous commit is durable before new data hits disk
      if (pendingFsync != null) {
        pendingFsync.join();
      }

      // Write pages to storage (previous fsync complete, safe to write)
      storagePageReaderWriter.writeUberPageReference(getResourceSession().getResourceConfig(),
                                                     uberPageReference,
                                                     uberPage,
                                                     bufferBytes);

      if (isAutoCommitting) {
        // Auto-commit mode: fsync runs asynchronously for better throughput
        // Next commit's serialization will overlap with this fsync
        pendingFsync = java.util.concurrent.CompletableFuture.runAsync(() -> {
          storagePageReaderWriter.forceAll();
        });
      } else {
        // Regular commit: synchronous fsync for strict durability
        // Data is guaranteed durable when commit() returns
        storagePageReaderWriter.forceAll();
        pendingFsync = null;
      }

      final int revision = uberPage.getRevisionNumber();
      serializeIndexDefinitions(revision);

      // CRITICAL: Release current page guard BEFORE TIL.clear()
      // If guard is on a TIL page, the page won't close (guardCount > 0 check)
      pageRtx.closeCurrentPageGuard();

      // Clear TransactionIntentLog - closes all modified pages
      log.clear();

      // Clear local cache (pages are already handled by log.clear())
      pageContainerCache.clear();
      
      // Null out cache references since pages have been returned to pool
      mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
      secondMostRecentPageContainer = mostRecentPageContainer;
      mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);

      // Clean up all snapshots from async intermediate commits.
      // At this point all pages have been written to disk by the sync commit,
      // and TIL is cleared, so snapshotLookup is no longer needed.
      // log.clear() already closed borrowed pages, so closePages() safely skips them.
      if (lastSnapshot != null) {
        log.setSnapshotLookup(null);
        lastSnapshot.closePages();
        pendingSnapshot = null;
      }
      for (final CommitSnapshot old : oldSnapshots) {
        old.closePages();
      }
      oldSnapshots.clear();

      // Delete commit file which denotes that a commit must write the log in the data file.
      try {
        deleteIfExists(commitFile);
      } catch (final IOException e) {
        throw new SirixIOException("Commit file couldn't be deleted!");
      }
    } finally {
      pageRtx.resourceSession.getCommitLock().unlock();
    }

    return readUberPage();
  }

  private void setCommitMessageAndTimestampIfRequired(@Nullable String commitMessage,
      @Nullable Instant commitTimestamp) {
    if (commitMessage != null) {
      newRevisionRootPage.setCommitMessage(commitMessage);
    }

    if (commitTimestamp != null) {
      newRevisionRootPage.setCommitTimestamp(commitTimestamp);
    }
  }

  private void serializeIndexDefinitions(int revision) {
    if (!indexController.getIndexes().getIndexDefs().isEmpty()) {
      final Path indexes = pageRtx.getResourceSession()
                                  .getResourceConfig().resourcePath.resolve(ResourceConfiguration.ResourcePaths.INDEXES.getPath())
                                                                   .resolve(revision + ".xml");

      try (final OutputStream out = newOutputStream(indexes, CREATE)) {
        indexController.serialize(out);
      } catch (final IOException e) {
        throw new SirixIOException("Index definitions couldn't be serialized!", e);
      }
    }
  }

  /**
   * Threshold for switching from sequential to parallel processing.
   * For small commits, parallel stream overhead exceeds benefits.
   */
  private static final int PARALLEL_SERIALIZATION_THRESHOLD = 4;

  private void parallelSerializationOfKeyValuePages() {
    final var resourceConfig = getResourceSession().getResourceConfig();

    // Filter KeyValueLeafPages first to get accurate count
    var keyValuePages = log.getList()
                           .stream()
                           .map(PageContainer::getModified)
                           .filter(page -> page instanceof KeyValueLeafPage)
                           .toList();

    // Adaptive: use sequential for small commits to avoid parallel stream overhead
    var stream = keyValuePages.size() < PARALLEL_SERIALIZATION_THRESHOLD
                 ? keyValuePages.stream()
                 : keyValuePages.parallelStream();

    stream.forEach(page -> {
      // Use pooled buffer instead of creating new Arena.ofAuto() per page
      var pooledSeg = SerializationBufferPool.INSTANCE.acquire();
      try {
        var bytes = new PooledBytesOut(pooledSeg);
        PageKind.KEYVALUELEAFPAGE.serializePage(resourceConfig, bytes, page, SerializationType.DATA);
      } catch (final Exception e) {
        throw new SirixIOException(e);
      } finally {
        SerializationBufferPool.INSTANCE.release(pooledSeg);
      }
    });
  }

  private UberPage readUberPage() {
    return (UberPage) storagePageReaderWriter.read(storagePageReaderWriter.readUberPageReference(),
                                                   getResourceSession().getResourceConfig());
  }

  private void createIfAbsent(final Path file) {
    while (!Files.exists(file)) {
      try {
        Files.createFile(file);
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
    }
  }

  private void setUserIfPresent() {
    final Optional<User> optionalUser = pageRtx.resourceSession.getUser();
    optionalUser.ifPresent(newRevisionRootPage::setUser);
  }

  @Override
  public UberPage rollback() {
    pageRtx.assertNotClosed();

    // Wait for any pending async intermediate commit to complete before rollback
    awaitPendingAsyncCommit();

    // CRITICAL: Release current page guard BEFORE TIL.clear()
    // If guard is on a TIL page, the page won't close (guardCount > 0 check)
    pageRtx.closeCurrentPageGuard();
    
    // Clear TransactionIntentLog - closes all modified pages
    log.clear();
    
    // Clear local cache and reset references (pages already handled by log.clear())
    pageContainerCache.clear();
    mostRecentPageContainer = new IndexLogKeyToPageContainer(IndexType.DOCUMENT, -1, -1, -1, null);
    secondMostRecentPageContainer = mostRecentPageContainer;
    mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(IndexType.PATH_SUMMARY, -1, -1, -1, null);
    
    return readUberPage();
  }

  @Override
  public void close() {
    if (!isClosed) {
      pageRtx.assertNotClosed();

      // Wait for any pending async intermediate commit to complete before closing
      awaitPendingAsyncCommit();

      // Clear snapshot lookup — no more snapshot pages needed after await
      log.setSnapshotLookup(null);

      // Wait for any pending async fsync to complete before closing
      if (pendingFsync != null) {
        pendingFsync.join();
        pendingFsync = null;
      }

      // Don't clear the cached containers here - they've either been:
      // 1. Already cleared and returned to pool during commit(), or
      // 2. Will be cleared and returned to pool by log.close() below
      // Clearing them here could corrupt pages that have been returned to pool
      // and reused by other transactions.

      final UberPage lastUberPage = readUberPage();

      pageRtx.resourceSession.setLastCommittedUberPage(lastUberPage);

      if (!isBoundToNodeTrx) {
        pageRtx.resourceSession.closePageWriteTransaction(pageRtx.getTrxId());
      }

      // CRITICAL: Close pageRtx FIRST to release guards BEFORE TIL tries to close pages
      // If guards are active when TIL.close() runs, pages won't close (guardCount > 0 check)
      pageRtx.close();
      
      // Now TIL can close pages (guards released)
      log.close();
      
      // CRITICAL FIX: Clear cache AFTER log.close() to avoid OOM
      // log.close() needs the cache entries to properly unpin/close pages
      // Once closed, we must drop references to allow GC
      
      // CRITICAL: Close pages in pageContainerCache that are NOT in TIL or cache
      // This handles pages that were cached but TIL was cleared (e.g., after commit)
      for (PageContainer container : pageContainerCache.values()) {
        closeOrphanedPagesInContainer(container);
      }
      
      pageContainerCache.clear();
      mostRecentPageContainer = null;
      secondMostRecentPageContainer = null;
      mostRecentPathSummaryPageContainer = null;

      // Close pending snapshot's pages and release memory
      if (pendingSnapshot != null) {
        pendingSnapshot.closePages();
        pendingSnapshot = null;
      }
      for (final CommitSnapshot old : oldSnapshots) {
        old.closePages();
      }
      oldSnapshots.clear();

      isClosed = true;
    }
  }
  
  /**
   * Close orphaned pages in a container (pages not in cache).
   * If page is in cache, cache will manage it - we just drop our reference.
   * If page is NOT in cache, we must release guard and close it.
   */
  private void closeOrphanedPagesInContainer(PageContainer container) {
    if (container == null) {
      return;
    }
    
    if (container.getComplete() instanceof KeyValueLeafPage completePage && !completePage.isClosed()) {
      // Check if page is in cache
      PageReference ref = new PageReference()
          .setKey(completePage.getPageKey())
          .setDatabaseId(pageRtx.getDatabaseId())
          .setResourceId(pageRtx.getResourceId());
      KeyValueLeafPage cachedPage = pageRtx.getBufferManager().getRecordPageCache().get(ref);
      
      if (cachedPage != completePage) {
        // Page is NOT in cache - orphaned, must release guard and close
        if (completePage.getGuardCount() > 0) {
          completePage.releaseGuard();
        }
        completePage.close();
      }
      // If page IS in cache, cache will manage it - just drop our reference
    }
    
    if (container.getModified() instanceof KeyValueLeafPage modifiedPage 
        && modifiedPage != container.getComplete() && !modifiedPage.isClosed()) {
      // Check if page is in cache
      PageReference ref = new PageReference()
          .setKey(modifiedPage.getPageKey())
          .setDatabaseId(pageRtx.getDatabaseId())
          .setResourceId(pageRtx.getResourceId());
      KeyValueLeafPage cachedPage = pageRtx.getBufferManager().getRecordPageCache().get(ref);
      
      if (cachedPage != modifiedPage) {
        // Page is NOT in cache - orphaned, must release guard and close
        if (modifiedPage.getGuardCount() > 0) {
          modifiedPage.releaseGuard();
        }
        modifiedPage.close();
      }
      // If page IS in cache, cache will manage it - just drop our reference
    }
  }

  @Override
  public DeweyIDPage getDeweyIDPage(@NonNull RevisionRootPage revisionRoot) {
    // TODO
    return null;
  }

  private PageContainer getPageContainer(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    PageContainer pageContainer =
        getMostRecentPageContainer(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision());
    if (pageContainer != null) {
      return pageContainer;
    }

    return pageContainerCache.computeIfAbsent(new IndexLogKey(indexType,
                                                              recordPageKey,
                                                              indexNumber,
                                                              newRevisionRootPage.getRevision()), _ -> {
      final PageReference pageReference = pageRtx.getPageReference(newRevisionRootPage, indexType, indexNumber);
      final var leafPageReference =
          pageRtx.getLeafPageReference(pageReference, recordPageKey, indexNumber, indexType, newRevisionRootPage);
      // Use layered lookup for async auto-commit support.
      final var leafContainer = getFromActiveOrPending(leafPageReference);
      // Promote leaf-level snapshot entries to the active TIL so they
      // survive the next rotation.
      if (leafContainer != null && !leafPageReference.isInActiveTil(log.getCurrentGeneration())) {
        promoteFromSnapshot(leafPageReference, leafContainer);
      }
      return leafContainer;
    });
  }

  @Nullable
  private PageContainer getMostRecentPageContainer(IndexType indexType, long recordPageKey,
      @NonNegative int indexNumber, @NonNegative int revisionNumber) {
    if (indexType == IndexType.PATH_SUMMARY) {
      return mostRecentPathSummaryPageContainer != null
          && mostRecentPathSummaryPageContainer.indexType == indexType
          && mostRecentPathSummaryPageContainer.indexNumber == indexNumber
          && mostRecentPathSummaryPageContainer.recordPageKey == recordPageKey
          && mostRecentPathSummaryPageContainer.revisionNumber == revisionNumber
          ? mostRecentPathSummaryPageContainer.pageContainer
          : null;
    }

    var pageContainer =
        mostRecentPageContainer != null
            && mostRecentPageContainer.indexType == indexType && mostRecentPageContainer.recordPageKey == recordPageKey
            && mostRecentPageContainer.indexNumber == indexNumber
            && mostRecentPageContainer.revisionNumber == revisionNumber ? mostRecentPageContainer.pageContainer : null;
    if (pageContainer == null) {
      pageContainer = secondMostRecentPageContainer != null
          && secondMostRecentPageContainer.indexType == indexType
          && secondMostRecentPageContainer.recordPageKey == recordPageKey
          && secondMostRecentPageContainer.indexNumber == indexNumber
          && secondMostRecentPageContainer.revisionNumber == revisionNumber
          ? secondMostRecentPageContainer.pageContainer
          : null;
    }
    return pageContainer;
  }

  /**
   * Prepare record page.
   *
   * @param recordPageKey the key of the record page
   * @param indexNumber   the index number if it's a record-page of an index, {@code -1}, else
   * @param indexType     the index type
   * @return {@link PageContainer} instance
   * @throws SirixIOException if an I/O error occurs
   */
  private PageContainer prepareRecordPage(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    assert indexType != null;

    // Route to appropriate trie implementation
    TrieType trieType = getTrieType(indexType, indexNumber);
    if (trieType == TrieType.HOT) {
      return prepareRecordPageViaHOT(recordPageKey, indexNumber, indexType);
    }

    // Traditional KEYED_TRIE path (bit-decomposed)
    return prepareRecordPageViaKeyedTrie(recordPageKey, indexNumber, indexType);
  }
  
  /**
   * Prepare record page using traditional bit-decomposed KEYED_TRIE.
   */
  private PageContainer prepareRecordPageViaKeyedTrie(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {

    PageContainer mostRecentPageContainer1 =
        getMostRecentPageContainer(indexType, recordPageKey, indexNumber, newRevisionRootPage.getRevision());

    if (mostRecentPageContainer1 != null && !mostRecentPageContainer1.getModified().isClosed()
        && !mostRecentPageContainer1.getComplete().isClosed()) {
      return mostRecentPageContainer1;
    }

    final Function<IndexLogKey, PageContainer> fetchPageContainer = _ -> {
      final PageReference pageReference = pageRtx.getPageReference(newRevisionRootPage, indexType, indexNumber);

      // Build list of all active snapshots (newest first) for CoW support.
      final List<CommitSnapshot> snapshots = getAllActiveSnapshots();

      // Get the reference to the unordered key/value page storing the records.
      final PageReference reference = trieWriter.prepareLeafOfTree(this,
                                                                     log,
                                                                     getUberPage().getPageCountExp(indexType),
                                                                     pageReference,
                                                                     recordPageKey,
                                                                     indexNumber,
                                                                     indexType,
                                                                     newRevisionRootPage,
                                                                     snapshots);

      // Use layered lookup for async auto-commit support.
      var pageContainer = getFromActiveOrPending(reference);

      if (pageContainer != null) {
        // Promote leaf-level snapshot entries to the active TIL so they
        // survive the next rotation (stale logKey/generation would make
        // them unreachable otherwise).
        if (!reference.isInActiveTil(log.getCurrentGeneration())) {
          promoteFromSnapshot(reference, pageContainer);
        }
        return pageContainer;
      }

      if (reference.getKey() == Constants.NULL_ID_LONG) {
        // Direct allocation (no pool)
        final MemorySegmentAllocator allocator = OS.isWindows()
            ? WindowsMemorySegmentAllocator.getInstance()
            : LinuxMemorySegmentAllocator.getInstance();

        final KeyValueLeafPage completePage = new KeyValueLeafPage(
            recordPageKey,
            indexType,
            getResourceSession().getResourceConfig(),
            pageRtx.getRevisionNumber(),
            allocator.allocate(SIXTYFOUR_KB),
            getResourceSession().getResourceConfig().areDeweyIDsStored 
                ? allocator.allocate(SIXTYFOUR_KB) 
                : null,
            false  // Memory from allocator - release on close()
        );
        
        final KeyValueLeafPage modifyPage = new KeyValueLeafPage(
            recordPageKey,
            indexType,
            getResourceSession().getResourceConfig(),
            pageRtx.getRevisionNumber(),
            allocator.allocate(SIXTYFOUR_KB),
            getResourceSession().getResourceConfig().areDeweyIDsStored 
                ? allocator.allocate(SIXTYFOUR_KB) 
                : null,
            false  // Memory from allocator - release on close()
        );

        pageContainer = PageContainer.getInstance(completePage, modifyPage);
        appendLogRecord(reference, pageContainer);
        return pageContainer;
      } else {
        pageContainer = dereferenceRecordPageForModification(reference);
        return pageContainer;
      }
    };

    var currPageContainer = pageContainerCache.computeIfAbsent(new IndexLogKey(indexType,
                                                                               recordPageKey,
                                                                               indexNumber,
                                                                               newRevisionRootPage.getRevision()),
                                                               fetchPageContainer);

    if (indexType == IndexType.PATH_SUMMARY) {
      mostRecentPathSummaryPageContainer = new IndexLogKeyToPageContainer(indexType,
                                                                          recordPageKey,
                                                                          indexNumber,
                                                                          newRevisionRootPage.getRevision(),
                                                                          currPageContainer);
    } else {
      secondMostRecentPageContainer = mostRecentPageContainer;
      mostRecentPageContainer = new IndexLogKeyToPageContainer(indexType,
                                                               recordPageKey,
                                                               indexNumber,
                                                               newRevisionRootPage.getRevision(),
                                                               currPageContainer);
    }

    return currPageContainer;
  }
  
  /**
   * Prepare record page using HOT (Height Optimized Trie).
   * 
   * <p>This method uses the HOTTrieWriter for cache-friendly secondary index operations.
   * HOT uses semantic byte[] keys instead of bit-decomposed numeric keys.</p>
   *
   * @param recordPageKey the record page key (used as fallback key encoding)
   * @param indexNumber the index number
   * @param indexType the index type (PATH, CAS, or NAME)
   * @return PageContainer with the HOT leaf page
   */
  private PageContainer prepareRecordPageViaHOT(final @NonNegative long recordPageKey, final int indexNumber,
      final IndexType indexType) {
    
    // Get the root reference for this index
    final PageReference rootRef = pageRtx.getPageReference(newRevisionRootPage, indexType, indexNumber);
    
    // Encode the recordPageKey as a byte[] key for HOT navigation
    // For proper integration, the caller should pass the semantic key directly
    // This is a compatibility bridge for the existing API
    byte[] key = encodeRecordPageKeyForHOT(recordPageKey);
    
    // Use HOT trie writer to navigate and prepare the leaf
    PageContainer container = hotTrieWriter.prepareKeyedLeafForModification(
        this, log, rootRef, key, indexType, indexNumber);
    
    if (container != null) {
      return container;
    }
    
    // Fallback: If HOT navigation fails (e.g., empty tree), 
    // create a new HOT leaf page
    HOTLeafPage newLeaf = new HOTLeafPage(recordPageKey, pageRtx.getRevisionNumber(), indexType);
    container = PageContainer.getInstance(newLeaf, newLeaf);
    
    PageReference newRef = new PageReference();
    newRef.setKey(recordPageKey);
    log.put(newRef, container);
    
    // Update root reference to point to new leaf
    if (rootRef.getKey() == Constants.NULL_ID_LONG) {
      rootRef.setKey(recordPageKey);
      log.put(rootRef, container);
    }
    
    return container;
  }
  
  /**
   * Encode a recordPageKey as a byte[] for HOT navigation.
   * 
   * <p>This is a compatibility bridge. For optimal performance, callers should
   * pass semantic keys directly to HOT methods.</p>
   *
   * @param recordPageKey the numeric page key
   * @return the key as a byte array (big-endian)
   */
  private static byte[] encodeRecordPageKeyForHOT(long recordPageKey) {
    byte[] key = new byte[8];
    key[0] = (byte) (recordPageKey >>> 56);
    key[1] = (byte) (recordPageKey >>> 48);
    key[2] = (byte) (recordPageKey >>> 40);
    key[3] = (byte) (recordPageKey >>> 32);
    key[4] = (byte) (recordPageKey >>> 24);
    key[5] = (byte) (recordPageKey >>> 16);
    key[6] = (byte) (recordPageKey >>> 8);
    key[7] = (byte) recordPageKey;
    return key;
  }

  /**
   * Dereference record page reference.
   *
   * @param reference reference to leaf, that is the record page
   * @return dereferenced page
   */
  @Override
  public PageContainer dereferenceRecordPageForModification(final PageReference reference) {
    final VersioningType versioningType = pageRtx.resourceSession.getResourceConfig().versioningType;
    final int mileStoneRevision = pageRtx.resourceSession.getResourceConfig().maxNumberOfRevisionsToRestore;
    
    // FULL versioning: Release any reader guard before loading for modification
    // This prevents double-guarding when the page is already in RecordPageCache from a read
    if (versioningType == VersioningType.FULL) {
      pageRtx.closeCurrentPageGuard();
    }
    
    final var result = pageRtx.getPageFragments(reference);
    
    // All fragments are guarded by getPageFragments() to prevent eviction during combining
    try {
      return versioningType.combineRecordPagesForModification(result.pages(), mileStoneRevision, this, reference, log);
    } finally {
      // Release guards on ALL fragments after combining
      for (var page : result.pages()) {
        KeyValueLeafPage kvPage = (KeyValueLeafPage) page;
        kvPage.releaseGuard();
        // For FULL versioning, the page might still have guards from concurrent readers
        // (which is fine - they'll release when done). For other versioning types,
        // fragments should have exactly 1 guard from getPageFragments.
        if (versioningType != VersioningType.FULL) {
          assert kvPage.getGuardCount() == 0 : 
              "Fragment should have guardCount=0 after release, but has " + kvPage.getGuardCount();
        }
      }
      // Note: Fragments remain in cache for potential reuse. ClockSweeper will evict them when appropriate.
    }
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    pageRtx.assertNotClosed();
    return newRevisionRootPage;
  }

  @Override
  public @Nullable HOTLeafPage getHOTLeafPage(@NonNull IndexType indexType, int indexNumber) {
    pageRtx.assertNotClosed();
    
    // CRITICAL: Use newRevisionRootPage (not the delegate's rootPage) because
    // HOT pages are stored against the new revision's PathPage/CASPage/NamePage references.
    final RevisionRootPage actualRootPage = newRevisionRootPage;
    
    // Get the root reference for the index from the NEW revision root page
    final PageReference rootRef = switch (indexType) {
      case PATH -> {
        final PathPage pathPage = getPathPage(actualRootPage);
        if (pathPage == null || indexNumber >= pathPage.getReferences().size()) {
          yield null;
        }
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = getCASPage(actualRootPage);
        if (casPage == null || indexNumber >= casPage.getReferences().size()) {
          yield null;
        }
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = getNamePage(actualRootPage);
        if (namePage == null || indexNumber >= namePage.getReferences().size()) {
          yield null;
        }
        yield namePage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
    
    if (rootRef == null) {
      return null;
    }
    
    // Check transaction log for uncommitted pages - using layered lookup for async auto-commit
    final PageContainer container = getFromActiveOrPending(rootRef);
    if (container != null) {
      // Try modified first (the one being written to), then complete
      Page modified = container.getModified();
      if (modified instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
      Page complete = container.getComplete();
      if (complete instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
    }
    
    // Check if page is swizzled (directly on reference)
    if (rootRef.getPage() instanceof HOTLeafPage hotLeaf) {
      return hotLeaf;
    }
    
    // For uncommitted pages with no storage key, we're done
    if (rootRef.getKey() < 0 && rootRef.getLogKey() < 0) {
      return null;
    }
    
    // Try buffer cache or load from storage (for previously committed data)
    return pageRtx.getHOTLeafPage(indexType, indexNumber);
  }
  
  @Override
  public io.sirix.page.interfaces.@Nullable Page loadHOTPage(@NonNull PageReference reference) {
    pageRtx.assertNotClosed();
    
    if (reference == null) {
      return null;
    }
    
    // Check transaction log first for uncommitted pages - using layered lookup for async auto-commit
    final PageContainer container = getFromActiveOrPending(reference);
    if (container != null) {
      Page modified = container.getModified();
      if (modified instanceof HOTLeafPage || modified instanceof HOTIndirectPage) {
        return modified;
      }
      Page complete = container.getComplete();
      if (complete instanceof HOTLeafPage || complete instanceof HOTIndirectPage) {
        return complete;
      }
    }
    
    // Delegate to the reader
    return pageRtx.loadHOTPage(reference);
  }

  @Override
  protected @NonNull StorageEngineReader delegate() {
    return pageRtx;
  }

  @Override
  public StorageEngineReader getStorageEngineReader() {
    return pageRtx;
  }

  @Override
  public StorageEngineWriter appendLogRecord(@NonNull final PageReference reference, @NonNull final PageContainer pageContainer) {
    requireNonNull(pageContainer);
    log.put(reference, pageContainer);
    return this;
  }

  @Override
  public PageContainer getLogRecord(final PageReference reference) {
    requireNonNull(reference);
    // Use layered lookup for async auto-commit support
    return getFromActiveOrPending(reference);
  }

  @Override
  public StorageEngineWriter truncateTo(final int revision) {
    storagePageReaderWriter.truncateTo(this, revision);
    return this;
  }

  @Override
  public int getTrxId() {
    return pageRtx.getTrxId();
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return pageRtx.getCommitCredentials();
  }
  
  @Override
  @SuppressWarnings("deprecation")
  protected void finalize() {
    if (!isClosed && KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
      LOGGER.warn("NodeStorageEngineWriter finalized without close: trxId={} instance={} TIL={} with {} containers in TIL",
          pageRtx.getTrxId(), System.identityHashCode(this), System.identityHashCode(log), log.getList().size());
    }
  }
  
  /**
   * Acquire a guard on the page containing the current node.
   * This is needed when holding a reference to a node across cursor movements.
   *
   * @return a PageGuard that must be closed when done with the node
   */
  public PageGuard acquireGuardForCurrentNode() {
    // The current node is in the pageRtx's currentPageGuard
    // We need to return a new guard on the same page
    // Get the page containing the current node from pageRtx
    final var currentPage = ((NodeStorageEngineReader) pageRtx).getCurrentPage();
    if (currentPage == null) {
      throw new IllegalStateException("No current page - cannot acquire guard");
    }
    return new PageGuard(currentPage);
  }
  
  /**
   * Package-private helper class for managing the trie structure of IndirectPages.
   * 
   * <p>Sirix uses a trie (not a B+tree) to organize IndirectPages. Page keys are
   * decomposed bit-by-bit to determine the path through the trie levels.
   * This enables efficient navigation and modification of the multi-level page structure.</p>
   * 
   * <p>For example, with pageKey=1024 and level exponents [10, 10, 10]:
   * <ul>
   *   <li>Level 0: offset = 1024 >> 10 = 1</li>
   *   <li>Level 1: offset = (1024 - 1024) >> 0 = 0</li>
   * </ul>
   * </p>
   * 
   * <p>This class creates new IndirectPages as needed when navigating to leaves,
   * and manages the transaction intent log for all modified pages.</p>
   *
   * <p>Note: Package-private access is needed for StorageEngineWriterFactory to call preparePreviousRevisionRootPage()
   * before NodeStorageEngineWriter is constructed.</p>
   *
   * @author Johannes Lichtenberger
   */
  static final class TrieWriter {
    
    /**
     * Prepare the previous revision root page and retrieve the next {@link RevisionRootPage}.
     *
     * @param uberPage the uber page
     * @param pageRtx the page reading transaction
     * @param log the transaction intent log
     * @param baseRevision base revision
     * @param representRevision the revision to represent
     * @return new {@link RevisionRootPage} instance
     */
    RevisionRootPage preparePreviousRevisionRootPage(final UberPage uberPage, final NodeStorageEngineReader pageRtx,
        final TransactionIntentLog log, final @NonNegative int baseRevision, final @NonNegative int representRevision) {
      final RevisionRootPage revisionRootPage;

      if (uberPage.isBootstrap()) {
        revisionRootPage = pageRtx.loadRevRoot(baseRevision);
      } else {
        // Prepare revision root nodePageReference.
        revisionRootPage = new RevisionRootPage(pageRtx.loadRevRoot(baseRevision), representRevision + 1);

        // Link the prepared revision root nodePageReference with the prepared indirect tree.
        final var revRootRef = new PageReference()
            .setDatabaseId(pageRtx.getDatabaseId())
            .setResourceId(pageRtx.getResourceId());
        log.put(revRootRef, PageContainer.getInstance(revisionRootPage, revisionRootPage));
      }

      // Return prepared revision root nodePageReference.
      return revisionRootPage;
    }

    /**
     * Prepare the leaf of the trie, navigating through IndirectPages using bit-decomposition.
     *
     * @param pageRtx the page reading transaction
     * @param log the transaction intent log
     * @param inpLevelPageCountExp array which holds the maximum number of indirect page references per trie level
     * @param startReference the reference to start the trie traversal from
     * @param pageKey page key to lookup (decomposed bit-by-bit)
     * @param index the index number or {@code -1} if a regular record page should be prepared
     * @param indexType the index type
     * @param revisionRootPage the revision root page
     * @return {@link PageReference} instance pointing to the leaf page
     */
    PageReference prepareLeafOfTree(final StorageEngineWriter pageRtx, final TransactionIntentLog log,
        final int[] inpLevelPageCountExp, final PageReference startReference, @NonNegative final long pageKey,
        final int index, final IndexType indexType, final RevisionRootPage revisionRootPage) {
      return prepareLeafOfTree(pageRtx, log, inpLevelPageCountExp, startReference, pageKey, index, indexType,
          revisionRootPage, List.of());
    }

    /**
     * Prepare the leaf of the trie with Copy-on-Write support for async auto-commit.
     *
     * @param pageRtx the page reading transaction
     * @param log the transaction intent log
     * @param inpLevelPageCountExp array which holds the maximum number of indirect page references per trie level
     * @param startReference the reference to start the trie traversal from
     * @param pageKey page key to lookup (decomposed bit-by-bit)
     * @param index the index number or {@code -1} if a regular record page should be prepared
     * @param indexType the index type
     * @param revisionRootPage the revision root page
     * @param snapshots all active snapshots (newest first), may be empty
     * @return {@link PageReference} instance pointing to the leaf page
     */
    PageReference prepareLeafOfTree(final StorageEngineWriter pageRtx, final TransactionIntentLog log,
        final int[] inpLevelPageCountExp, final PageReference startReference, @NonNegative final long pageKey,
        final int index, final IndexType indexType, final RevisionRootPage revisionRootPage,
        final List<CommitSnapshot> snapshots) {
      // Initial state pointing to the indirect nodePageReference of level 0.
      PageReference reference = startReference;

      int offset;
      long levelKey = pageKey;

      int maxHeight = pageRtx.getCurrentMaxIndirectPageTreeLevel(indexType, index, revisionRootPage);

      // Check if we need an additional level of indirect pages.
      if (pageKey == (1L << inpLevelPageCountExp[inpLevelPageCountExp.length - maxHeight - 1])) {
        maxHeight = incrementCurrentMaxIndirectPageTreeLevel(pageRtx, revisionRootPage, indexType, index);

        // Add a new indirect page to the top of the trie and to the transaction-log.
        final IndirectPage page = new IndirectPage();

        // Copy ALL properties from the old root reference to slot 0 of the new IndirectPage.
        // CRITICAL: Must use copy constructor to include activeTilGeneration, databaseId, resourceId,
        // etc. Missing activeTilGeneration causes the logKey fallback in getFromSnapshot() to fail
        // after TIL rotation, because the generation check (-1 != snapshotGen) prevents lookup.
        page.setOrCreateReference(0, new PageReference(reference));

        // Create new page reference, add it to the transaction-log and reassign it in the root pages
        // of the trie.
        final PageReference newPageReference = new PageReference()
            .setDatabaseId(pageRtx.getDatabaseId())
            .setResourceId(pageRtx.getResourceId());
        log.put(newPageReference, PageContainer.getInstance(page, page));
        setNewIndirectPage(pageRtx, revisionRootPage, indexType, index, newPageReference);

        reference = newPageReference;
      }

      // Iterate through all levels using bit-decomposition.
      for (int level = inpLevelPageCountExp.length - maxHeight, height = inpLevelPageCountExp.length;
           level < height; level++) {
        offset = (int) (levelKey >> inpLevelPageCountExp[level]);
        levelKey -= (long) offset << inpLevelPageCountExp[level];
        final IndirectPage page = prepareIndirectPage(pageRtx, log, reference, snapshots);
        reference = page.getOrCreateReference(offset);
      }

      // Return reference to leaf of indirect trie.
      return reference;
    }

    /**
     * Prepare indirect page, that is getting the referenced indirect page or creating a new page
     * and putting the whole path into the log.
     * <p>
     * This overload does not support Copy-on-Write from pending snapshot.
     * Use {@link #prepareIndirectPage(StorageEngineReader, TransactionIntentLog, PageReference, CommitSnapshot)}
     * for async auto-commit scenarios.
     *
     * @param pageRtx the page reading transaction
     * @param log the transaction intent log
     * @param reference {@link PageReference} to get the indirect page from or to create a new one
     * @return {@link IndirectPage} reference
     */
    IndirectPage prepareIndirectPage(final StorageEngineReader pageRtx, final TransactionIntentLog log,
        final PageReference reference) {
      return prepareIndirectPage(pageRtx, log, reference, List.of());
    }

    /**
     * Prepare indirect page with Copy-on-Write support for async auto-commit.
     * <p>
     * This method performs a 3-step lookup:
     * <ol>
     *   <li>Check active TIL (via generation counter)</li>
     *   <li>Check all snapshots (newest first) and perform CoW if found</li>
     *   <li>Load from disk if not in any snapshot</li>
     * </ol>
     *
     * @param pageRtx the page reading transaction
     * @param log the transaction intent log
     * @param reference {@link PageReference} to get the indirect page from or to create a new one
     * @param snapshots all active snapshots (newest first), may be empty
     * @return {@link IndirectPage} reference
     */
    IndirectPage prepareIndirectPage(final StorageEngineReader pageRtx, final TransactionIntentLog log,
        final PageReference reference, final List<CommitSnapshot> snapshots) {

      // STEP 1: Check if ref is in active TIL (fast path via generation)
      final int currentGen = log.getCurrentGeneration();
      if (reference.isInActiveTil(currentGen)) {
        final PageContainer cont = log.getUnchecked(reference.getLogKey());
        if (cont != null) {
          return (IndirectPage) cont.getComplete();
        }
      }

      // STEP 2: Check all snapshots and perform CoW if found.
      for (final CommitSnapshot snapshot : snapshots) {
        PageContainer snapshotContainer = snapshot.getByIdentity(reference);
        if (snapshotContainer == null) {
          // Fallback for cloned refs — only if generation matches to avoid cross-snapshot collision
          final int logKey = reference.getLogKey();
          final int snapshotGen = snapshot.getSnapshotGeneration();
          if (logKey >= 0 && logKey < snapshot.size()
              && reference.getActiveTilGeneration() == snapshotGen) {
            snapshotContainer = snapshot.getEntry(logKey);
          }
        }

        if (snapshotContainer != null) {
          final Page snapshotPage = snapshotContainer.getComplete();
          if (snapshotPage instanceof IndirectPage original && !snapshotPage.isClosed()) {
            // COPY-ON-WRITE: Create deep copy for active TIL
            final IndirectPage copy = new IndirectPage(original);
            log.put(reference, PageContainer.getInstance(copy, copy));
            return copy;
          }
        }

        // If commit is complete, try to load via disk offset
        if (snapshot.isCommitComplete()) {
          final int logKey = reference.getLogKey();
          final int snapshotGen = snapshot.getSnapshotGeneration();
          if (logKey >= 0 && reference.getActiveTilGeneration() == snapshotGen) {
            final long diskOffset = snapshot.getDiskOffset(logKey);
            if (diskOffset >= 0) {
              reference.setKey(diskOffset);
              // Fall through to STEP 3 which will load from disk
              break;
            }
          }
        }
      }

      // STEP 3: Not in active TIL or any snapshot - load from disk or create new
      final PageContainer cont = log.get(reference);
      IndirectPage page = cont == null ? null : (IndirectPage) cont.getComplete();
      if (page == null) {
        if (reference.getKey() == Constants.NULL_ID_LONG) {
          page = new IndirectPage();
        } else {
          final IndirectPage indirectPage = pageRtx.dereferenceIndirectPageReference(reference);
          page = new IndirectPage(indirectPage);
        }
        log.put(reference, PageContainer.getInstance(page, page));
      }
      return page;
    }

    /**
     * Set a new indirect page in the appropriate index structure.
     */
    private void setNewIndirectPage(final StorageEngineReader pageRtx, final RevisionRootPage revisionRoot,
        final IndexType indexType, final int index, final PageReference pageReference) {
      // $CASES-OMITTED$
      switch (indexType) {
        case DOCUMENT -> revisionRoot.setOrCreateReference(0, pageReference);
        case CHANGED_NODES -> revisionRoot.setOrCreateReference(1, pageReference);
        case RECORD_TO_REVISIONS -> revisionRoot.setOrCreateReference(2, pageReference);
        case CAS -> pageRtx.getCASPage(revisionRoot).setOrCreateReference(index, pageReference);
        case PATH -> pageRtx.getPathPage(revisionRoot).setOrCreateReference(index, pageReference);
        case NAME -> pageRtx.getNamePage(revisionRoot).setOrCreateReference(index, pageReference);
        case PATH_SUMMARY -> pageRtx.getPathSummaryPage(revisionRoot).setOrCreateReference(index, pageReference);
        default ->
            throw new IllegalStateException("Only defined for node, path summary, text value and attribute value pages!");
      }
    }

    /**
     * Increment the current maximum indirect page trie level for the given index type.
     */
    private int incrementCurrentMaxIndirectPageTreeLevel(final StorageEngineReader pageRtx,
        final RevisionRootPage revisionRoot, final IndexType indexType, final int index) {
      // $CASES-OMITTED$
      return switch (indexType) {
        case DOCUMENT -> revisionRoot.incrementAndGetCurrentMaxLevelOfDocumentIndexIndirectPages();
        case CHANGED_NODES -> revisionRoot.incrementAndGetCurrentMaxLevelOfChangedNodesIndexIndirectPages();
        case RECORD_TO_REVISIONS -> revisionRoot.incrementAndGetCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages();
        case CAS -> pageRtx.getCASPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        case PATH -> pageRtx.getPathPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        case NAME -> pageRtx.getNamePage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        case PATH_SUMMARY ->
            pageRtx.getPathSummaryPage(revisionRoot).incrementAndGetCurrentMaxLevelOfIndirectPages(index);
        default ->
            throw new IllegalStateException("Only defined for node, path summary, text value and attribute value pages!");
      };
    }
  }
}
