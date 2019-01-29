package org.sirix.api;

import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.CASPage;
import org.sirix.page.IndirectPage;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.PathPage;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.KeyValuePage;

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
  ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> getResourceManager();

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
   * @param key the unique record-ID
   * @param pageKind the page kind from which to fetch the record
   * @param index the index number
   * @return an {@link Optional} reference usually containing the node reference
   * @throws SirixIOException if an I/O error occurred
   */
  Optional<? extends Record> getRecord(final @Nonnegative long key, final PageKind pageKind, final int index);

  /**
   * Current reference to actual revision-root page.
   *
   * @return the current revision root page
   */
  RevisionRootPage getActualRevisionRootPage();

  /**
   * Getting the name corresponding to the given key.
   *
   * @param nameKey name key for the term to search
   * @param recordKind kind of record
   * @return the name
   * @throws NullPointerException if {@code kind} is {@code null}
   */
  String getName(int nameKey, @Nonnull Kind recordKind);

  /**
   * Get the number of references for a name.
   *
   * @param nameKey name key for the term to search
   * @param recordKind kind of record
   * @return the number of references for a given keyy.
   */
  int getNameCount(int nameKey, @Nonnull Kind recordKind);

  /**
   * Getting the raw name related to the name key and the record kind.
   *
   * @param nameKey name key for the term to search
   * @param recordKind kind of record
   * @return a byte array containing the raw name
   * @throws NullPointerException if {@code kind} is {@code null}
   */
  byte[] getRawName(int nameKey, @Nonnull Kind recordKind);

  /**
   * Close transaction.
   *
   * @throws SirixIOException if something weird happened in the storage
   */
  @Override
  void close();

  /**
   * Get a the record page container with the full/modified pages from the page layer, given the
   * unique page key and the page kind.
   *
   * @param key {@code key} of key/value page to get the record from
   * @param index index number or {@code -1}, if it's a regular record page to lookup
   * @param pageKind kind of page to lookup
   * @return {@code the node} or {@code null} if it's not available
   * @throws SirixIOException if can't read recordPage
   * @throws NullPointerException if {@code key} is {@code null}
   * @throws NullPointerException if {@code pageKind} is {@code null}
   * @throws IllegalArgumentException if {@code key} is negative
   */
  <K extends Comparable<? super K>, V extends Record, T extends KeyValuePage<K, V>> PageContainer getRecordPageContainer(
      @Nonnull @Nonnegative Long key, int index, @Nonnull PageKind pageKind);

  /** Determines if transaction is closed or not. */
  boolean isClosed();

  /**
   * Get the revision number associated with the transaction.
   *
   * @return the revision number
   */
  int getRevisionNumber();

  /**
   * Clear the caches.
   */
  void clearCaches();

  /**
   * Close the caches.
   */
  void closeCaches();

  /**
   * Calculate record page key from a given record key.
   *
   * @param recordKey record key to find record page key for
   * @return record page key
   * @throws IllegalArgumentException if {code recordKey} < 0
   */
  long pageKey(@Nonnegative long recordKey);

  /**
   * Get the {@link NamePage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link NamePage}
   * @throws SirixIOException if an I/O error occurs
   */
  NamePage getNamePage(RevisionRootPage revisionRoot);

  /**
   * Get the {@link PathPage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link PathPage}
   * @throws SirixIOException if an I/O error occur@Nonnull RevisionRootPage revisionRoots
   */
  PathPage getPathPage(RevisionRootPage revisionRoot);

  /**
   * Get the {@link CASPage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link CASPage}
   * @throws SirixIOException if an I/O error occurs
   */
  CASPage getCASPage(@Nonnull RevisionRootPage revisionRoot);

  /**
   * Get the {@link PathSummaryPage} associated with the current revision root.
   *
   * @param revisionRoot {@link RevisionRootPage} for which to get the {@link PathSummaryPage}
   * @throws SirixIOException if an I/O error occurs
   */
  PathSummaryPage getPathSummaryPage(RevisionRootPage revisionRoot);

  /**
   * Get the page reference pointing to the page denoted by {@code pageKey}.
   *
   * @param startReference the start reference (for instance to the indirect tree or the root-node of
   *        a BPlusTree)
   * @param pageKey the unique key of the page to search for
   * @param maxNodeKey the maximum node key
   * @param indexNumber the index number or {@code -1}
   * @param pageKind the kind of subtree
   * @return {@link PageReference} instance pointing to the page denoted by {@code key}
   * @throws SirixIOException if an I/O error occurs
   * @throws IllegalArgumentException if {code pageKey} < 0
   */
  PageReference getPageReferenceForPage(PageReference startReference, @Nonnegative long pageKey,
      @Nonnegative long maxNodeKey, int indexNumber, @Nonnull PageKind pageKind);

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
   * @param pageKind the page kind
   * @param index the index or {@code -1}
   * @param revisionRootPage the revision root page
   * @return The maximum level of the current indirect page tree.
   */
  int getCurrentMaxIndirectPageTreeLevel(PageKind pageKind, int index, RevisionRootPage revisionRootPage);
}
