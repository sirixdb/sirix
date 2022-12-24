package org.sirix.api;

import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Interface for writing pages to disk and to create in-memory records.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface PageTrx extends PageReadOnlyTrx {

  Bytes<ByteBuffer> newBufferedBytesInstance();

  /**
   * Truncate resource to given revision.
   *
   * @param revision the given revision
   * @return this page write transaction instance
   */
  PageTrx truncateTo(int revision);

  /**
   * Put a page into the cache.
   *
   * @param reference the reference
   * @param page      the page to put into the cache
   * @return this page write transaction instance
   */
  PageTrx appendLogRecord(@NonNull PageReference reference, @NonNull PageContainer page);

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
  UberPage commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp);

  /**
   * Committing a {@link PageTrx}. This method is recursively invoked by all {@link PageReference}s.
   *
   * @param reference to be commited
   * @throws SirixException       if the write fails
   * @throws NullPointerException if {@code reference} is {@code null}
   */
  void commit(PageReference reference);

  /**
   * Get inlying {@link PageReadOnlyTrx}.
   *
   * @return the {@link PageReadOnlyTrx} reference
   */
  PageReadOnlyTrx getPageReadOnlyTrx();

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
}
