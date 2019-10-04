package org.sirix.api;

import org.sirix.access.trx.node.Restore;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.KeyValuePage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Interface for writing pages to disk and to create in-memory records.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface PageTrx<K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>>
    extends PageReadOnlyTrx {

  /**
   * Truncate resource to given revision.
   *
   * @param revision the given revision
   * @return this page write transaction instance
   */
  PageTrx<K, V, S> truncateTo(int revision);

  /**
   * Put a page into the cache.
   *
   * @param reference the reference
   * @param page the page to put into the cache
   * @return this page write transaction instance
   */
  PageTrx<K, V, S> appendLogRecord(@Nonnull PageReference reference, @Nonnull PageContainer page);

  /**
   * Create fresh key/value (value must be a record) and prepare key/value-tuple for modifications
   * (CoW). The record might be a node, in this case the key is the node.
   *
   * @param key optional key associated with the record to add (otherwise the record nodeKey is used)
   * @param value value to add (usually a node)
   * @param pageKind kind of subtree the page belongs to
   * @param index the index number
   * @return unmodified record for convenience
   * @throws SirixIOException if an I/O error occurs
   * @throws NullPointerException if {@code record} or {@code page} is {@code null}
   */
  V createEntry(K key, @Nonnull V value, @Nonnull PageKind pageKind, int index);

  /**
   * Prepare an entry for modification. This is getting the entry from the (persistence) layer,
   * storing the page in the cache and setting up the entry for upcoming modification. The key of the
   * entry might be the node key and the value the node itself.
   *
   * @param key key of the entry to be modified
   * @param pageKind the kind of subtree (for instance regular data pages or the kind of index pages)
   * @param index the index number
   * @return instance of the class implementing the {@link Record} instance
   * @throws SirixIOException if an I/O-error occurs
   * @throws IllegalArgumentException if {@code recordKey < 0}
   * @throws NullPointerException if {@code page} is {@code null}
   */
  V prepareEntryForModification(@Nonnegative K key, @Nonnull PageKind pageKind, int index);

  /**
   * Remove an entry from the storage.
   *
   * @param key entry key from entry to be removed
   * @param pageKind denoting the kind of page (that is the subtree root kind)
   * @param index the index number
   * @throws SirixIOException if the removal fails
   * @throws IllegalArgumentException if {@code recordKey < 0}
   * @throws NullPointerException if {@code pageKind} is {@code null}
   */
  void removeEntry(K key, @Nonnull PageKind pageKind, int index);

  /**
   * Creating a namekey for a given name.
   *
   * @param name for which the key should be created
   * @param kind kind of node
   * @return an int, representing the namekey
   * @throws SirixIOException if something odd happens while storing the new key
   * @throws NullPointerException if {@code name} or {@code kind} is {@code null}
   */
  int createNameKey(String name, @Nonnull NodeKind kind);

  /**
   * Commit the transaction, that is persist changes if any and create a new revision.
   *
   * @throws SirixException if Sirix fails to commit
   */
  UberPage commit();

  /**
   * Commit the transaction, that is persist changes if any and create a new revision. The commit
   * message is going to be persisted as well.
   *
   * @param commitMessage the commit message
   * @throws SirixException if Sirix fails to commit
   */
  UberPage commit(String commitMessage);

  /**
   * Committing a {@link PageTrx}. This method is recursively invoked by all {@link PageReference}s.
   *
   * @param reference to be commited
   * @throws SirixException if the write fails
   * @throws NullPointerException if {@code reference} is {@code null}
   */
  void commit(PageReference reference);

  /**
   * Determines if this page write trx must restore a previous failed trx.
   *
   * @param restore determines if this page write trx must restore a previous failed trx
   * @throws NullPointerException if {@code restore} is {@code null}
   */
  void restore(Restore restore);

  /**
   * Get inlying {@link PageReadOnlyTrx}.
   *
   * @return the {@link PageReadOnlyTrx} reference
   */
  PageReadOnlyTrx getPageReadTrx();

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
