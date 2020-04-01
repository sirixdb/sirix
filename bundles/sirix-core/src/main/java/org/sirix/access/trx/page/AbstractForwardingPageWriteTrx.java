package org.sirix.access.trx.page;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.sirix.access.trx.node.Restore;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.KeyValuePage;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingPageWriteTrx<K extends Comparable<? super K>, V extends DataRecord, S extends KeyValuePage<K, V>>
    extends AbstractForwardingPageReadOnlyTrx implements PageTrx<K, V, S> {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingPageWriteTrx() {}

  @Override
  public int getRevisionToRepresent() {
    return delegate().getRevisionToRepresent();
  }

  @Override
  public void close() throws SirixIOException {
    delegate().close();
  }

  @Override
  public V createEntry(K key, @Nonnull V record, @Nonnull PageKind pageKind, @Nonnegative int index) {
    return delegate().createEntry(key, record, pageKind, index);
  }

  @Override
  public V prepareEntryForModification(@Nonnegative K recordKey, @Nonnull PageKind pageKind, @Nonnegative int index) {
    return delegate().prepareEntryForModification(recordKey, pageKind, index);
  }

  @Override
  public void removeEntry(@Nonnegative K recordKey, @Nonnull PageKind pageKind, @Nonnegative int index)
      throws SirixIOException {
    delegate().removeEntry(recordKey, pageKind, index);
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
  protected abstract PageTrx<K, V, S> delegate();

}
