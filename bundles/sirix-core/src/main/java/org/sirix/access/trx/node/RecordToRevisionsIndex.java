package org.sirix.access.trx.node;

import org.sirix.api.PageTrx;
import org.sirix.index.IndexType;
import org.sirix.node.RevisionReferencesNode;

import static java.util.Objects.requireNonNull;

/**
 * Indexes all record changes.
 */
public final class RecordToRevisionsIndex {

  /**
   * The page trx to create index-entries.
   */
  private PageTrx pageTrx;

  /**
   * Constructor
   *
   * @param pageTrx  the page trx to create index-entries.
   */
  public RecordToRevisionsIndex(final PageTrx pageTrx) {
    this.pageTrx = requireNonNull(pageTrx);
  }

  public void setPageTrx(final PageTrx pageTrx) {
    this.pageTrx = pageTrx;
  }

  /**
   * The record to add.
   *
   * @param recordKey the key of the record
   */
  public void addToRecordToRevisionsIndex(long recordKey) {
    // Add to revision index.
    final int[] revisions = { pageTrx.getRevisionNumber() };
    pageTrx.createRecord(recordKey, new RevisionReferencesNode(recordKey, revisions), IndexType.RECORD_TO_REVISIONS, 0);
  }

  /**
   * Add a revision to an existing record.
   *
   * @param recordKey the key of the record
   */
  public void addRevisionToRecordToRevisionsIndex(long recordKey) {
    final RevisionReferencesNode revisionReferencesNode =
        pageTrx.prepareRecordForModification(recordKey, IndexType.RECORD_TO_REVISIONS, 0);
    revisionReferencesNode.addRevision(pageTrx.getRevisionNumber());
  }
}
