package org.sirix.api;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.cache.IndexLogKey;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.Reader;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.util.Optional;

/**
 * Interface for reading pages.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface PageReadOnlyTrx extends AutoCloseable {

  /**
   * Get {@link UberPage}.
   *
   * @return the {@link UberPage} reference
   */
  UberPage getUberPage();

  /**
   * Get the resource manager this transaction is bound to.
   *
   * @return resource manager instance
   */
  ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession();

  /**
   * Get the transaction-ID.
   *
   * @return the transaction-ID.
   */
  long getTrxId();

  /**
   * Get the commit message.
   *
   * @return The commit message.
   */
  CommitCredentials getCommitCredentials();

  /**
   * Get a record from persistent storage.
   *
   * @param key       the unique record-ID
   * @param indexType the index type
   * @param index     the index number
   * @return an {@link Optional} reference usually containing the node reference
   * @throws SirixIOException if an I/O error occurred
   */
  <V extends DataRecord> V getRecord(@NonNull long key, @NonNull IndexType indexType, int index);

  /**
   * Current reference to actual revision-root page.
   *
   * @return the current revision root page
   */
  RevisionRootPage getActualRevisionRootPage();

  /**
   * Getting the name corresponding to the given key.
   *
   * @param nameKey    name key for the term to search
   * @param recordKind kind of record
   * @return the name
   * @throws NullPointerException if {@code kind} is {@code null}
   */
  String getName(int nameKey, @NonNull NodeKind recordKind);

  /**
   * Get the number of references for a name.
   *
   * @param nameKey    name key for the term to search
   * @param recordKind kind of record
   * @return the number of references for a given keyy.
   */
  int getNameCount(int nameKey, @NonNull NodeKind recordKind);

  /**
   * Getting the raw name related to the name key and the record kind.
   *
   * @param nameKey    name key for the term to search
   * @param recordKind kind of record
   * @return a byte array containing the raw name
   * @throws NullPointerException if {@code kind} is {@code null}
   */
  byte[] getRawName(int nameKey, @NonNull NodeKind recordKind);

  /**
   * Close transaction.
   *
   * @throws SirixIOException if something weird happened in the storage
   */
  @Override
  void close();

  /**
   * Get a the record page with the full pages from the page layer, given the
   * unique page key and the page kind.
   *
   * @param indexLogKey it has the key {@code key} of key/value page to get the record from, the index number
   *                    or {@code -1}, if it's a regular record page to lookup and the kind of page to lookup
   * @return {@code the node} or {@code null} if it's not available
   * @throws SirixIOException         if can't read recordPage
   * @throws NullPointerException     if {@code key} is {@code null}
   * @throws NullPointerException     if {@code pageKind} is {@code null}
   * @throws IllegalArgumentException if {@code key} is negative
   */
  Page getRecordPage(@NonNull IndexLogKey indexLogKey);

  /**
   * Determines if transaction is closed or not.
   *
   * @return status whether closed or not
   */
  boolean isClosed();

  /**
   * Get the revision number associated with the transaction.
   *
   * @return the revision number
   */
  int getRevisionNumber();

  /**
   * Calculate record page offset for a given node key.
   *
   * @param key record key to find offset for
   * @return offset into record page
   */
  static int recordPageOffset(long key) {
    return (int) (key - ((key >> Constants.NDP_NODE_COUNT_EXPONENT) << Constants.NDP_NODE_COUNT_EXPONENT));
  }

  /**
   * Calculate record page key from a given record key.
   *
   * @param recordKey record key to find record page key for
   * @param indexType the index type
   * @return record page key
   * @throws IllegalArgumentException if {code recordKey} &lt; 0
   */
  long pageKey(@NonNegative long recordKey, @NonNull IndexType indexType);

  /**
   * Get the {@link NamePage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link NamePage}
   * @return NamePage The associated NamePage
   * @throws SirixIOException if an I/O error occurs
   */
  NamePage getNamePage(@NonNull RevisionRootPage revisionRoot);

  /**
   * Get the {@link PathPage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link PathPage}
   * @return PathPage The associated PathPage
   * @throws SirixIOException if an I/O error occur@NonNull RevisionRootPage revisionRoots
   */
  PathPage getPathPage(@NonNull RevisionRootPage revisionRoot);

  /**
   * Get the {@link CASPage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link CASPage}
   * @return CASPage the associated CASPAGE
   * @throws SirixIOException if an I/O error occurs
   */
  CASPage getCASPage(@NonNull RevisionRootPage revisionRoot);

  /**
   * Get the {@link PathSummaryPage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link PathSummaryPage}
   * @return PathSummaryPage The associated PathSummaryPage
   * @throws SirixIOException if an I/O error occurs
   */
  PathSummaryPage getPathSummaryPage(@NonNull RevisionRootPage revisionRoot);

  /**
   * Get the {@link DeweyIDPage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link DeweyIDPage}
   * @return DeweyIDPage The associated DeweyIDPage
   * @throws SirixIOException if an I/O error occurs
   */
  DeweyIDPage getDeweyIDPage(@NonNull RevisionRootPage revisionRoot);

  /**
   * Get the page reference pointing to the page denoted by {@code pageKey}.
   *
   * @param startReference   the start reference (for instance to the indirect tree or the root-node of
   *                         a BPlusTree)
   * @param pageKey          the unique key of the page to search for
   * @param indexNumber      the index number or {@code -1}
   * @param indexType        the index type
   * @param revisionRootPage the revision root page (may be {@code null})
   * @return {@link PageReference} instance pointing to the page denoted by {@code key}
   * @throws SirixIOException         if an I/O error occurs
   * @throws IllegalArgumentException if {code pageKey} &lt; 0
   */
  PageReference getReferenceToLeafOfSubtree(PageReference startReference, @NonNegative long pageKey, int indexNumber,
      @NonNull IndexType indexType, RevisionRootPage revisionRootPage);

  /**
   * Get the {@link Reader} to read a page from persistent storage if needed.
   *
   * @return the {@link Reader}
   */
  Reader getReader();

  /**
   * Dereference the indirect page reference.
   *
   * @param indirectPageReference The indirect page reference.
   * @return The indirect page.
   */
  IndirectPage dereferenceIndirectPageReference(PageReference indirectPageReference);

  /**
   * Load the revision root.
   *
   * @param lastCommitedRev The revision to load.
   * @return The revision root.
   */
  RevisionRootPage loadRevRoot(int lastCommitedRev);

  /**
   * Get the maximum level of the current indirect page tree.
   *
   * @param indexType        the index type
   * @param index            the index or {@code -1}
   * @param revisionRootPage the revision root page
   * @return The maximum level of the current indirect page tree.
   */
  int getCurrentMaxIndirectPageTreeLevel(IndexType indexType, int index, RevisionRootPage revisionRootPage);
}
