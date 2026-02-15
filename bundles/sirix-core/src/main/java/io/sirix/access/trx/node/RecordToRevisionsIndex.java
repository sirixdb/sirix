package io.sirix.access.trx.node;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.RevisionReferencesNode;

import static java.util.Objects.requireNonNull;

/**
 * Indexes all record changes.
 */
public final class RecordToRevisionsIndex {

  /**
   * The page trx to create index-entries.
   */
  private StorageEngineWriter pageTrx;

  /**
   * Constructor
   *
   * @param pageTrx  the page trx to create index-entries.
   */
  public RecordToRevisionsIndex(final StorageEngineWriter pageTrx) {
    this.pageTrx = requireNonNull(pageTrx);
  }

  public void setPageTrx(final StorageEngineWriter pageTrx) {
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
    pageTrx.createRecord(new RevisionReferencesNode(recordKey, revisions), IndexType.RECORD_TO_REVISIONS, 0);
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
    pageTrx.updateRecordSlot(revisionReferencesNode, IndexType.RECORD_TO_REVISIONS, 0);
  }
}
