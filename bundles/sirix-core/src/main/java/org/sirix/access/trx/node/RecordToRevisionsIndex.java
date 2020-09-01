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
  private final PageTrx pageTrx;

  /**
   * The current revision.
   */
  private final int revision;

  /**
   * Constructor
   *
   * @param pageTrx  the page trx to create index-entries.
   * @param revision the current revision to be added
   */
  public RecordToRevisionsIndex(final PageTrx pageTrx, final int revision) {
    this.pageTrx = requireNonNull(pageTrx);
    this.revision = revision;
  }

  /**
   * The record to add.
   *
   * @param recordKey the key of the record
   */
  public void addToRecordToRevisionsIndex(long recordKey) {
    // Add to revision index.
    final int[] revisions = { revision };
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
    revisionReferencesNode.addRevision(revision);
  }
}
