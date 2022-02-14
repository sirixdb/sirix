package org.sirix.access.trx.page;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.trx.node.Restore;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.NodeKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingPageWriteTrx extends AbstractForwardingPageReadOnlyTrx implements PageTrx {

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
  public <K, V> V createRecord(K key, @NonNull V record,
      @NonNull IndexType indexType, @NonNegative int index) {
    return delegate().createRecord(key, record, indexType, index);
  }

  @Override
  public <K, V> V prepareRecordForModification(@NonNegative K recordKey,
      @NonNull IndexType indexType, @NonNegative int index) {
    return delegate().prepareRecordForModification(recordKey, indexType, index);
  }

  @Override
  public <K> void removeRecord(@NonNegative K recordKey, @NonNull IndexType indexType,
      @NonNegative int index) {
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
  public void restore(Restore restore) {
    delegate().restore(restore);
  }

  @Override
  protected abstract PageTrx delegate();
}
