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
   * The storage engine writer to create index-entries.
   */
  private StorageEngineWriter storageEngineWriter;

  /**
   * Constructor
   *
   * @param storageEngineWriter the storage engine writer to create index-entries.
   */
  public RecordToRevisionsIndex(final StorageEngineWriter storageEngineWriter) {
    this.storageEngineWriter = requireNonNull(storageEngineWriter);
  }

  public void setStorageEngineWriter(final StorageEngineWriter storageEngineWriter) {
    this.storageEngineWriter = storageEngineWriter;
  }

  /**
   * The record to add.
   *
   * @param recordKey the key of the record
   */
  public void addToRecordToRevisionsIndex(long recordKey) {
    // Add to revision index.
    final int[] revisions = {storageEngineWriter.getRevisionNumber()};
    storageEngineWriter.createRecord(new RevisionReferencesNode(recordKey, revisions), IndexType.RECORD_TO_REVISIONS, 0);
  }

  /**
   * Add a revision to an existing record.
   *
   * @param recordKey the key of the record
   */
  public void addRevisionToRecordToRevisionsIndex(long recordKey) {
    final RevisionReferencesNode revisionReferencesNode =
        storageEngineWriter.prepareRecordForModification(recordKey, IndexType.RECORD_TO_REVISIONS, 0);
    revisionReferencesNode.addRevision(storageEngineWriter.getRevisionNumber());
    storageEngineWriter.updateRecordSlot(revisionReferencesNode, IndexType.RECORD_TO_REVISIONS, 0);
  }
}
