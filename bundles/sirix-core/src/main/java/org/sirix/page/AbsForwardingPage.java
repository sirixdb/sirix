package org.sirix.page;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ForwardingObject;
import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.io.ITTSink;
import org.sirix.page.interfaces.IPage;

public abstract class AbsForwardingPage extends ForwardingObject implements IPage {

  /** Constructor for use by subclasses. */
  protected AbsForwardingPage() {
  }

  @Override
  protected abstract IPage delegate();
  
  @Override
  public void commit(final @Nonnull IPageWriteTrx pPageWriteTrx) throws AbsTTException {
    delegate().commit(checkNotNull(pPageWriteTrx));
  }
  
  @Override
  public PageReference[] getReferences() {
    return delegate().getReferences();
  }
  
  @Override
  public long getRevision() {
    return delegate().getRevision();
  }
  
  @Override
  public void serialize(@Nonnull final ITTSink pOut) {
    delegate().serialize(checkNotNull(pOut));
  }
  
}
