package io.sirix.access.trx.page;

import io.sirix.api.PageTrx;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.exception.SirixIOException;
import io.sirix.node.interfaces.DataRecord;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingPageWriteTrx extends AbstractForwardingPageReadOnlyTrx
    implements PageTrx {

  /**
   * Constructor for use by subclasses.
   */
  protected AbstractForwardingPageWriteTrx() {
  }

  @Override
  public int getRevisionToRepresent() {
    return delegate().getRevisionToRepresent();
  }

  @Override
  public void close() throws SirixIOException {
    delegate().close();
  }

  @Override
  public <V extends DataRecord> V createRecord(@NonNull V record, @NonNull IndexType indexType, @NonNegative int index) {
    return delegate().createRecord(record, indexType, index);
  }

  @Override
  public <V extends DataRecord> V prepareRecordForModification(@NonNegative long recordKey, @NonNull IndexType indexType,
      @NonNegative int index) {
    return delegate().prepareRecordForModification(recordKey, indexType, index);
  }

  @Override
  public void removeRecord(@NonNegative long recordKey, @NonNull IndexType indexType, @NonNegative int index) {
    delegate().removeRecord(recordKey, indexType, index);
  }

  @Override
  public int createNameKey(String name, @NonNull NodeKind kind) {
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
  protected abstract @NonNull PageTrx delegate();
}
