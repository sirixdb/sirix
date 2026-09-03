package io.sirix.api;

import io.sirix.cache.PageContainer;
import io.sirix.cache.PageGuard;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.pax.GlobalStringDictionaries;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.util.function.LongFunction;

import java.time.Instant;

/**
 * Storage engine writer interface for writing pages to persistent storage.
 * 
 * <p>
 * This is the write component of the storage engine, extending {@link StorageEngineReader} with
 * modification capabilities. Responsible for:
 * </p>
 * <ul>
 * <li>Creating and modifying pages</li>
 * <li>Managing the transaction intent log (TIL)</li>
 * <li>Writing pages to disk on commit</li>
 * <li>Creating records (nodes) in KeyValueLeafPages</li>
 * <li>Managing the IndirectPage trie structure for writes</li>
 * </ul>
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface StorageEngineWriter extends StorageEngineReader {

  BytesOut<?> newBufferedBytesInstance();

  /**
   * Truncate resource to given revision.
   *
   * @param revision the given revision
   * @return this storage engine writer instance
   */
  StorageEngineWriter truncateTo(int revision);

  /**
   * Put a page into the cache.
   *
   * @param reference the reference
   * @param page the page to put into the cache
   * @return this storage engine writer instance
   */
  StorageEngineWriter appendLogRecord(PageReference reference, PageContainer page);

  /**
   * Create fresh key/record (record must be a record) and prepare key/record-tuple for modifications
   * (CoW). The record might be a node, in this case the key is the node.
   *
   * @param record record to add (usually a node)
   * @param indexType the index type
   * @param index the index number
   * @return unmodified record for convenience
   * @throws SirixIOException if an I/O error occurs
   * @throws NullPointerException if {@code record} or {@code page} is {@code null}
   */
  <V extends DataRecord> V createRecord(V record, IndexType indexType, int index);

  /**
   * Prepare an entry for modification. This is getting the entry from the (persistence) layer,
   * storing the page in the cache and setting up the entry for upcoming modification. The key of the
   * entry might be the node key and the value the node itself.
   *
   * @param key key of the entry to be modified
   * @param indexType the index type
   * @param index the index number
   * @return instance of the class implementing the {@link DataRecord} instance
   * @throws SirixIOException if an I/O-error occurs
   * @throws IllegalArgumentException if {@code recordKey < 0}
   * @throws NullPointerException if {@code page} is {@code null}
   */
  <V extends DataRecord> V prepareRecordForModification(long key, IndexType indexType, int index);

  /**
   * Fast-path variant of {@link #prepareRecordForModification} for the DOCUMENT index type. Skips
   * assertNotClosed(), argument validation, and the IndexType switch in pageKey(). Used on the insert
   * hot path where keys are always valid and indexType is always DOCUMENT.
   *
   * @param key key of the entry to be modified (must be >= 0)
   * @return instance of the class implementing the {@link DataRecord} instance
   */
  @SuppressWarnings("unchecked")
  default <V extends DataRecord> V prepareRecordForModificationDocument(final long key) {
    return prepareRecordForModification(key, IndexType.DOCUMENT, -1);
  }

  /**
   * Persist a mutated record into the TIL's modified page. Ensures the record's page is prepared for
   * modification in the TIL and stores the record in the modified page's records[].
   *
   * <p>
   * This is used by mutation operations (setName, setValue, hash updates) that mutate the current
   * node directly without going through prepareRecordForModification.
   * </p>
   *
   * @param record the mutated record to persist
   * @param indexType the index type
   * @param index the index number
   */
  void persistRecord(DataRecord record, IndexType indexType, int index);

  /**
   * FSST-encode a string value against the resource's current symbol table at insert time, on behalf
   * of a factory writing the record straight to {@code page}'s heap.
   *
   * <p>
   * Returns the exact bytes to store with the record's compressed flag set — the same form
   * commit-time compression produces — or {@code null} when the value must be stored raw: FSST
   * disabled, no table yet (first commit bootstraps through the commit-time pass), value too small,
   * encoding did not shrink it, or {@code page} is bound to a DIFFERENT table than the transaction's
   * (raw is always correct; the commit pass handles it). When encoding succeeds on an unbound page,
   * the page is bound to the table as a side effect, so the caller need only store the returned
   * bytes.
   *
   * @param page the page the record is being written to
   * @param value the raw value bytes
   * @param off offset of the value within {@code value}
   * @param len length of the value
   * @return the encoded bytes to store with the compressed flag, or {@code null} to store raw
   */
  default byte[] encodeStringValueForInsert(KeyValueLeafPage page, byte[] value, int off, int len) {
    return null;
  }

  /**
   * Adds {@code delta} occurrences to an EXISTING interned name's count in one record touch — the
   * batched sibling of the per-occurrence counting in {@code createNameKey}; the parallel bulk
   * importer applies per-chunk occurrence deltas through it before any flush epoch can rotate.
   *
   * @param key the interned name key
   * @param delta additional occurrences; must be positive
   * @param nodeKind the kind selecting the dictionary
   */
  default void addNameCount(int key, int delta, NodeKind nodeKind) {
    throw new UnsupportedOperationException("batched name counting is not supported by this writer");
  }

  /**
   * Adopts an externally BUILT document leaf page into this writer's transaction intent log — the
   * parallel bulk importer's installation seam. The page must cover unwritten territory: its record
   * page key must resolve to a reference with no persisted predecessor and no log entry. The adopted
   * page becomes the container's MODIFIED half (an empty complete twin is created), mirroring exactly
   * what the ordinary fresh-page path produces, so read-back, async flush and commit treat it like
   * any other freshly written page.
   *
   * @param page a fully built page for a contiguous pre-reserved key range
   */
  default void adoptDocumentLeafPage(KeyValueLeafPage page) {
    throw new UnsupportedOperationException("bulk page adoption is not supported by this writer");
  }

  /**
   * Hand this writer the resolver its DOCUMENT record pages encode their string values against.
   *
   * <p>
   * The trie lane's write half. A record page's string region is built at SERIALIZATION time, from
   * the heap, on the flush lane — so the resolver has to be on the page before it gets there, and the
   * only component that sees every page as it is created is this writer.
   * </p>
   *
   * <p>
   * <b>The resolver must be safe to call from many threads at once.</b> Region building runs inside
   * the async snapshot window's parallel {@code forEach}, so {@code idOf} is invoked concurrently.
   * {@code TrieLaneWriteDictionaries} is the intended implementation and is thread-confined for
   * exactly this reason; a resolver that walks the trie through a reader must never be installed
   * here.
   * </p>
   *
   * <p>
   * A no-op default rather than a throw, unlike the bulk-adoption seams above: every writer must
   * tolerate being told about a lane it does not implement, because the caller is a load-time
   * installer that cannot know which writer it got.
   * </p>
   *
   * @param dictionaries the resolver, or {@code null} to encode every tag as bytes
   */
  default void installDocumentStringDictionaries(@Nullable GlobalStringDictionaries dictionaries) {
    // No-op: a writer without the trie lane simply keeps its bytes.
  }

  /**
   * Install a PER-PAGE resolver factory, consulted with each document leaf's record-page key.
   *
   * <p>
   * {@link #installDocumentStringDictionaries} hands ONE resolver to every page, which is right for a
   * resource-wide dictionary and wrong for a segment-scoped one: there the dictionary a page belongs
   * to is a property of the page, and a resolver answering "the segment being filled" would mint a
   * late-flushed page's ids into the wrong one — a coherent wrong answer no downstream check can
   * catch (see {@code SegmentScopedDictionaries}). The factory is therefore consulted where the page
   * is created, on the writer side, in key order, and the view it returns travels with the page.
   * </p>
   *
   * <p>
   * When set it takes precedence over the single resolver; {@code null} restores it. A writer that
   * does not implement the lane ignores both.
   * </p>
   *
   * @param factory record-page key to that page's resolver, or {@code null} to use the single one
   */
  default void installDocumentStringDictionaryFactory(@Nullable LongFunction<GlobalStringDictionaries> factory) {
    // No-op: a writer without the trie lane simply keeps its bytes.
  }

  /**
   * Serialize the heap records a bulk merge left on a LIVE log leaf (the prologue page the importer
   * blits into rather than adopts) and stage every resulting overflow carrier as an immutable side
   * page, exactly as {@link #adoptDocumentLeafPage} does for an adopted leaf — so the background
   * flush defers the leaf one epoch instead of pinning it until final commit.
   *
   * @param page a leaf already owned by this writer's transaction intent log
   */
  default void stageOverflowCarriersOfLiveLeaf(KeyValueLeafPage page) {
    throw new UnsupportedOperationException("bulk page adoption is not supported by this writer");
  }

  /**
   * Resolves the LIVE (CoW-checked) modified half of a document leaf page for direct record blitting
   * — the parallel bulk importer's stitch seam for pages that are already in the intent log (the
   * page-0 prologue and chunk-boundary pages). Goes through the ordinary prepare-record-page
   * machinery, so a frozen-snapshot instance is deep-copied first and caches stay coherent.
   *
   * @param recordPageKey the document record page key
   * @return the modified page, safe for direct writes on the writer thread
   */
  default KeyValueLeafPage prepareDocumentLeafForBlit(long recordPageKey) {
    throw new UnsupportedOperationException("bulk page blitting is not supported by this writer");
  }

  /**
   * Remove an entry from the storage.
   *
   * @param key entry key from entry to be removed
   * @param indexType the index type
   * @param index the index number
   * @throws SirixIOException if the removal fails
   * @throws IllegalArgumentException if {@code recordKey < 0}
   * @throws NullPointerException if {@code indexType} is {@code null}
   */
  void removeRecord(long key, IndexType indexType, int index);

  /**
   * Creating a namekey for a given name.
   *
   * @param name for which the key should be created
   * @param kind kind of node
   * @return an int, representing the namekey
   * @throws SirixIOException if something odd happens while storing the new key
   * @throws NullPointerException if {@code name} or {@code kind} is {@code null}
   */
  int createNameKey(String name, NodeKind kind);

  /**
   * Resolve the name key {@code name} owns in the dictionary for {@code kind}, WITHOUT storing it or
   * counting an occurrence.
   *
   * <p>
   * If the name is already stored this is the key it was given; if it is not, this is the key
   * {@link #createNameKey} would assign to it next. Both answers walk the same probe chain, so a name
   * that lost a hash collision resolves to its own slot rather than to the colliding name's.
   *
   * <p>
   * Exists for the path summary, whose nodes carry a name key but are not records and must not
   * inflate the per-name occurrence count that {@code getNameCount} reports.
   *
   * @param name the name to resolve
   * @param kind kind of node, selecting the dictionary
   * @return the key for the name
   * @throws NullPointerException if {@code kind} is {@code null}
   */
  int keyForName(String name, NodeKind kind);

  /**
   * Commit the transaction, that is persist changes if any and create a new revision.
   *
   * @return UberPage the new revision after commit
   * @throws SirixException if Sirix fails to commit
   */
  default UberPage commit() {
    return commit(null, null);
  }

  /**
   * Commit the transaction, that is persist changes if any and create a new revision. The commit
   * message is going to be persisted as well.
   *
   * @param commitMessage the commit message
   * @return UberPage the revision after commit
   * @throws SirixException if Sirix fails to commit
   */
  default UberPage commit(@Nullable String commitMessage) {
    return commit(commitMessage, null);
  }

  /**
   * Commit the transaction, that is persist changes if any and create a new revision. The commit
   * message is going to be persisted as well.
   *
   * @param commitMessage the commit message
   * @param commitTimeStamp the commit timestamp
   * @return UberPage the revision after commit
   * @throws SirixException if Sirix fails to commit
   */
  default UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp) {
    return commit(commitMessage, commitTimeStamp, false);
  }

  /**
   * Commit the transaction, that is persist changes if any and create a new revision. The commit
   * message is going to be persisted as well.
   *
   * @param commitMessage the commit message
   * @param commitTimeStamp the commit timestamp
   * @param isAutoCommitting if true, fsync runs asynchronously for better throughput; if false,
   *        commit blocks until data is durable (strict sync)
   * @return UberPage the revision after commit
   * @throws SirixException if Sirix fails to commit
   */
  default UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp, boolean isAutoCommitting) {
    return commit(commitMessage, commitTimeStamp, isAutoCommitting, false);
  }

  /**
   * Commit the transaction, that is persist changes if any and create a new revision. The commit
   * message is going to be persisted as well.
   *
   * @param commitMessage the commit message
   * @param commitTimeStamp the commit timestamp
   * @param isAutoCommitting if true, fsync runs asynchronously for better throughput; if false,
   *        commit blocks until data is durable (strict sync)
   * @param isIntermediateCommit if true, this is an intermediate auto-commit during bulk insert;
   *        redundant I/O (e.g. unchanged index definitions) may be skipped
   * @return UberPage the revision after commit
   * @throws SirixException if Sirix fails to commit
   */
  UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp, boolean isAutoCommitting,
      boolean isIntermediateCommit);

  /**
   * Committing a {@link StorageEngineWriter}. This method is recursively invoked by all
   * {@link PageReference}s.
   *
   * @param reference to be commited
   * @throws SirixException if the write fails
   * @throws NullPointerException if {@code reference} is {@code null}
   */
  void commit(PageReference reference);

  /**
   * Stage a fresh immutable {@link io.sirix.page.OverflowPage} for the writer's bounded background
   * append batch.
   *
   * <p>
   * This is intentionally narrower than {@link #commit(PageReference)}: the page must not be
   * reachable from any committed root, must never be mutated, and its owner must tolerate the append
   * becoming an unreachable orphan until transaction rollback reclaims the uncommitted tail.
   * Implementations return {@code false} when their backend cannot reclaim such a tail; the page then
   * stays resident and ordinary recursive commit writes it safely.
   * </p>
   *
   * @param reference fresh unresolved reference whose page is an immutable OverflowPage
   * @return {@code true} if ownership moved into a bounded pending-write batch
   */
  default boolean stageUncommittedOverflowPage(final PageReference reference) {
    return false;
  }

  PageContainer dereferenceRecordPageForModification(PageReference reference);

  /**
   * Functional interface for binding a write-path singleton to a slotted page slot. Set by the node
   * transaction to enable zero-allocation write path.
   */
  @FunctionalInterface
  interface WriteSingletonBinder {
    DataRecord bind(KeyValueLeafPage page, int offset, long nodeKey);
  }

  /**
   * Set the write singleton binder for zero-allocation write path. When set,
   * prepareRecordForModification rebinds factory singletons instead of allocating.
   *
   * @param binder the write singleton binder from the node factory
   */
  default void setWriteSingletonBinder(final WriteSingletonBinder binder) {
    // Default no-op; NodeStorageEngineWriter overrides
  }

  /**
   * Permanently mark this page transaction rollback-only after an already-published structural
   * mutation fails. The first cause is authoritative; later failures must not replace it.
   *
   * <p>
   * This is distinct from an asynchronous-flush failure: the page graph may already contain a
   * partially propagated in-memory splice, so no commit or later mutation may continue on this
   * writer. Rollback closes it and creates a fresh writer with a clear state.
   * </p>
   *
   * @param cause the failure that made the transaction unsafe to commit
   */
  void markTransactionRollbackOnly(Throwable cause);

  /**
   * Reject work on a writer previously marked by {@link #markTransactionRollbackOnly(Throwable)}.
   * Implementations must report the original structural cause.
   */
  void assertTransactionWritable();

  /**
   * Resolve the current revision's transaction-private secondary-index container page.
   *
   * <p>The first call for an exact revision-root slot fully copies the persisted page before it is
   * published to the transaction-intent log. Later calls return that same modified page. This is the
   * only supported mutation gateway for the PATH, CAS, NAME, PROJECTION, and VALIDTIME container
   * pages; callers must never mutate a page obtained from the underlying reader.</p>
   *
   * @param indexType one of PATH, CAS, NAME, PROJECTION, or VALIDTIME
   * @param <P> the concrete container-page type selected by {@code indexType}
   * @return the transaction-private page which may be mutated by this writer
   * @throws IllegalArgumentException if {@code indexType} is not a secondary HOT index type
   * @throws IllegalStateException if the revision-root slot or TIL entry has the wrong page type
   */
  <P extends Page> P prepareSecondaryIndexPage(IndexType indexType);

  /**
   * Allocate a record key and resolve the KVL page for direct-to-heap creation. After this call, read
   * results from {@link #getAllocKvl()}, {@link #getAllocSlotOffset()}, {@link #getAllocNodeKey()}.
   * <p>
   * Only supports DOCUMENT index (the hot path). Other index types use createRecord().
   * </p>
   */
  default void allocateForDocumentCreation() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the KVL page from the last {@link #allocateForDocumentCreation()} call.
   */
  default KeyValueLeafPage getAllocKvl() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the slot offset from the last {@link #allocateForDocumentCreation()} call.
   */
  default int getAllocSlotOffset() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the node key from the last {@link #allocateForDocumentCreation()} call.
   */
  default long getAllocNodeKey() {
    throw new UnsupportedOperationException();
  }

  /**
   * Perform an async intermediate commit: snapshot the current TIL via O(1) array swap and flush KVL
   * pages to disk in a background thread. The insert thread continues immediately.
   *
   * <p>
   * Only supported with {@code AfterCommitState.KEEP_OPEN_ASYNC_FLUSH} on the FILE_CHANNEL or
   * MEMORY_MAPPED backend (both append through the file-channel writer).
   * </p>
   */
  default void asyncFlush() {}

  /**
   * Whether the live transaction-intent-log generation has reached the bounded amount of work for one
   * async-flush epoch.
   *
   * <p>
   * The node transaction samples this only at its existing compound-operation-safe pre-mutation
   * boundary. Implementations that do not support async TIL rotation retain the default {@code false}
   * result.
   * </p>
   *
   * @return {@code true} when the foreground should rotate the current async-flush epoch
   */
  default boolean isAsyncFlushLogBoundaryReached() {
    return false;
  }

  /**
   * Record the duration of one complete foreground async-flush rotation, including index maintenance
   * performed before {@link #asyncFlush()}.
   *
   * @param elapsedNanos non-negative elapsed time in nanoseconds
   */
  default void recordAsyncFlushForegroundNanos(final long elapsedNanos) {}

  /**
   * Phase 1 of a pipelined commit: create the commit marker, serialize and write every modified page
   * from the TIL (assigning all disk keys) through the buffered data channel. After this returns, the
   * revision's page trie is fully addressed and nothing references TIL-only state — but NOTHING is
   * durable yet. Must run on the writer thread (serialization mutates TIL pages).
   *
   * <p>
   * Pair with {@link #hardenCommit(UberPage, boolean)}; {@code commit(...)} is exactly the two phases
   * run back-to-back.
   * </p>
   *
   * @return the canonical in-memory uber page of the pending revision
   */
  default UberPage commitWritePages(@Nullable String commitMessage, @Nullable Instant commitTimeStamp,
      boolean isIntermediateCommit) {
    throw new UnsupportedOperationException();
  }

  /**
   * Phase 2 of a pipelined commit: make the revision durable — index catalogue, buffered-tail flush,
   * data force, uber-page beacons — then clear the TIL and delete the commit marker. Safe to run off
   * the writer thread; performs no page mutation. Does NOT publish {@code lastCommittedUberPage} —
   * the orchestrator decides when the revision becomes visible.
   *
   * @param uberPage the uber page returned by {@link #commitWritePages}
   */
  default void hardenCommit(UberPage uberPage, boolean isIntermediateCommit) {
    throw new UnsupportedOperationException();
  }

  /**
   * Block until any pending async intermediate commit completes, then clean up the snapshot (apply
   * disk offsets, close written KVL pages, promote IndirectPages to current TIL).
   *
   * <p>
   * Must be called before any sync commit to ensure all background-written pages are finalized.
   * </p>
   *
   * @throws io.sirix.exception.SirixIOException if the background async commit failed
   */
  default void awaitPendingAsyncFlush() {}

  /**
   * Get the underlying {@link StorageEngineReader}.
   *
   * @return the {@link StorageEngineReader} reference
   */
  StorageEngineReader getStorageEngineReader();

  /**
   * Rollback all changes done within the page transaction.
   *
   * @return the former uberpage
   */
  UberPage rollback();

  /**
   * Get a transaction intent log record.
   *
   * @param reference the reference parameter
   * @return the page container
   */
  PageContainer getLogRecord(PageReference reference);

  /**
   * Get the transaction intent log.
   *
   * @return the transaction intent log
   */
  TransactionIntentLog getLog();

  /**
   * Get the revision, which this storage engine writer is going to represent in case of a revert.
   *
   * @return the revision to represent
   */
  int getRevisionToRepresent();

  /**
   * Acquire a guard on the page containing the current node. This is needed when holding a reference
   * to a node across cursor movements. The guard prevents the page from being modified or evicted
   * while the node is in use.
   *
   * @return a PageGuard that must be closed when done with the node
   */
  PageGuard acquireGuardForNode(long nodeKey);

  /**
   * Get the current writer revision's {@link KeyValueLeafPage} for a given record page key, or
   * {@code null} when the page is unchanged from the committed base revision. Used by the singleton
   * moveTo path to read from the correct page during write transactions.
   *
   * @param recordPageKey the record page key
   * @param indexType the index type
   * @param index the index number
   * @return the current writer page, or {@code null} when the committed reader can resolve it
   */
  default @Nullable KeyValueLeafPage getModifiedPageForRead(long recordPageKey, IndexType indexType, int index) {
    return null;
  }

  /**
   * Determine whether a page returned by {@link #getModifiedPageForRead(long, IndexType, int)} is a
   * private read-only materialization.
   *
   * @param page the returned page
   * @return {@code true} when callers must detach records before releasing the page
   */
  default boolean isReadOnlyPageForRead(KeyValueLeafPage page) {
    return false;
  }

  /**
   * Read a record whose lazy state no longer depends on the supplied private page.
   *
   * @param page the private read-only page
   * @param recordKey the record key
   * @return the detached record, or {@code null}
   */
  default @Nullable DataRecord getDetachedRecordForRead(KeyValueLeafPage page, long recordKey) {
    return null;
  }

  /**
   * Release a private page returned for current-revision reads.
   *
   * @param page the page, or {@code null}
   */
  default void releasePageForRead(@Nullable KeyValueLeafPage page) {}
}
