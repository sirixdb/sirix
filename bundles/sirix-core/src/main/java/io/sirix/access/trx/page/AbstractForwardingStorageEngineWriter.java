package io.sirix.access.trx.page;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.exception.SirixIOException;
import io.sirix.node.interfaces.DataRecord;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingStorageEngineWriter extends AbstractForwardingStorageEngineReader
    implements StorageEngineWriter {

  /**
   * Constructor for use by subclasses.
   */
  protected AbstractForwardingStorageEngineWriter() {}

  @Override
  public int getRevisionToRepresent() {
    return delegate().getRevisionToRepresent();
  }

  @Override
  public void close() throws SirixIOException {
    delegate().close();
  }

  @Override
  public <V extends DataRecord> V createRecord(V record, IndexType indexType,
      int index) {
    return delegate().createRecord(record, indexType, index);
  }

  @Override
  public <V extends DataRecord> V prepareRecordForModification(long recordKey,
      IndexType indexType, int index) {
    return delegate().prepareRecordForModification(recordKey, indexType, index);
  }

  @Override
  public void persistRecord(DataRecord record, IndexType indexType, int index) {
    delegate().persistRecord(record, indexType, index);
  }

  @Override
  public void removeRecord(long recordKey, IndexType indexType, int index) {
    delegate().removeRecord(recordKey, indexType, index);
  }

  @Override
  public int createNameKey(String name, NodeKind kind) {
    return delegate().createNameKey(name, kind);
  }

  @Override
  public UberPage commit() {
    return delegate().commit();
  }

  @Override
  public void commit(PageReference reference) {
    delegate().commit(reference);
  }

  @Override
  protected abstract StorageEngineWriter delegate();
}
