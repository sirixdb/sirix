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
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Instant;

/**
 * Storage engine writer interface for writing pages to persistent storage.
 * 
 * <p>This is the write component of the storage engine, extending {@link StorageEngineReader}
 * with modification capabilities. Responsible for:</p>
 * <ul>
 *   <li>Creating and modifying pages</li>
 *   <li>Managing the transaction intent log (TIL)</li>
 *   <li>Writing pages to disk on commit</li>
 *   <li>Creating records (nodes) in KeyValueLeafPages</li>
 *   <li>Managing the IndirectPage trie structure for writes</li>
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
   * @return this page write transaction instance
   */
  StorageEngineWriter truncateTo(int revision);

  /**
   * Put a page into the cache.
   *
   * @param reference the reference
   * @param page      the page to put into the cache
   * @return this page write transaction instance
   */
  StorageEngineWriter appendLogRecord(@NonNull PageReference reference, @NonNull PageContainer page);

  /**
   * Create fresh key/record (record must be a record) and prepare key/record-tuple for modifications
   * (CoW). The record might be a node, in this case the key is the node.
   *
   * @param record     record to add (usually a node)
   * @param indexType the index type
   * @param index     the index number
   * @return unmodified record for convenience
   * @throws SirixIOException     if an I/O error occurs
   * @throws NullPointerException if {@code record} or {@code page} is {@code null}
   */
  <V extends DataRecord> V createRecord(@NonNull V record, @NonNull IndexType indexType, int index);

  /**
   * Prepare an entry for modification. This is getting the entry from the (persistence) layer,
   * storing the page in the cache and setting up the entry for upcoming modification. The key of the
   * entry might be the node key and the value the node itself.
   *
   * @param key       key of the entry to be modified
   * @param indexType the index type
   * @param index     the index number
   * @return instance of the class implementing the {@link DataRecord} instance
   * @throws SirixIOException         if an I/O-error occurs
   * @throws IllegalArgumentException if {@code recordKey < 0}
   * @throws NullPointerException     if {@code page} is {@code null}
   */
  <V extends DataRecord> V prepareRecordForModification(@NonNegative long key, @NonNull IndexType indexType, int index);

  /**
   * Remove an entry from the storage.
   *
   * @param key       entry key from entry to be removed
   * @param indexType the index type
   * @param index     the index number
   * @throws SirixIOException         if the removal fails
   * @throws IllegalArgumentException if {@code recordKey < 0}
   * @throws NullPointerException     if {@code indexType} is {@code null}
   */
  void removeRecord(long key, @NonNull IndexType indexType, int index);

  /**
   * Creating a namekey for a given name.
   *
   * @param name for which the key should be created
   * @param kind kind of node
   * @return an int, representing the namekey
   * @throws SirixIOException     if something odd happens while storing the new key
   * @throws NullPointerException if {@code name} or {@code kind} is {@code null}
   */
  int createNameKey(String name, @NonNull NodeKind kind);

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
   * @param isAutoCommitting if true, fsync runs asynchronously for better throughput;
   *                         if false, commit blocks until data is durable (strict sync)
   * @return UberPage the revision after commit
   * @throws SirixException if Sirix fails to commit
   */
  UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp, boolean isAutoCommitting);

  /**
   * Committing a {@link StorageEngineWriter}. This method is recursively invoked by all {@link PageReference}s.
   *
   * @param reference to be commited
   * @throws SirixException       if the write fails
   * @throws NullPointerException if {@code reference} is {@code null}
   */
  void commit(PageReference reference);

  PageContainer dereferenceRecordPageForModification(PageReference reference);

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
   * Get the revision, which this page trx is going to represent in case of a revert.
   *
   * @return the revision to represent
   */
  int getRevisionToRepresent();
  
  /**
   * Acquire a guard on the page containing the current node.
   * This is needed when holding a reference to a node across cursor movements.
   * The guard prevents the page from being modified or evicted while the node is in use.
   *
   * @return a PageGuard that must be closed when done with the node
   */
  PageGuard acquireGuardForCurrentNode();

  /**
   * Eagerly serialize dirty {@link io.sirix.page.KeyValueLeafPage}s to slotMemory and
   * clear their {@code records[]} arrays so the GC can reclaim node objects.
   * <p>
   * This is a lightweight, in-memory operation: no compression, no disk I/O.
   * It reduces GC tenuring pressure during large batch insertions by keeping
   * the number of live node objects bounded between sub-interval triggers.
   */
  default void eagerSerializePages() {}

  /**
   * Eagerly serialize dirty pages if a DOCUMENT page boundary was crossed since
   * the last call. This is a conditional wrapper around {@link #eagerSerializePages()}
   * that avoids iterating the TIL when no boundary was crossed.
   */
  default void eagerSerializePagesIfPageBoundaryCrossed() {}

  /**
   * Trigger an async intermediate commit with backpressure.
   * <p>
   * Blocks if another async commit is still in flight (backpressure),
   * then rotates the TIL and schedules background page serialization.
   * The calling thread can continue inserting into the fresh TIL immediately
   * after this method returns.
   */
  default void asyncIntermediateCommit() {}

  /**
   * Block until any pending async commit completes.
   * <p>
   * Must be called before a final synchronous commit, close, or rollback
   * to ensure the background thread has finished writing.
   */
  default void awaitPendingAsyncCommit() {}
}
