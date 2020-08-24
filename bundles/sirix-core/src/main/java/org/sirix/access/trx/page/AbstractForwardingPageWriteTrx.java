package org.sirix.access.trx.page;

import org.sirix.access.trx.node.Restore;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

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
  public <K, V> V createRecord(K key, @Nonnull V record,
      @Nonnull PageKind pageKind, @Nonnegative int index) {
    return delegate().createRecord(key, record, pageKind, index);
  }

  @Override
  public <K, V> V prepareRecordForModification(@Nonnegative K recordKey,
      @Nonnull PageKind pageKind, @Nonnegative int index) {
    return delegate().prepareRecordForModification(recordKey, pageKind, index);
  }

  @Override
  public <K> void removeRecord(@Nonnegative K recordKey, @Nonnull PageKind pageKind,
      @Nonnegative int index) throws SirixIOException {
    delegate().removeRecord(recordKey, pageKind, index);
  }

  @Override
  public int createNameKey(String name, @Nonnull NodeKind kind) throws SirixIOException {
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
