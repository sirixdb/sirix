package org.treetank.page;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.treetank.io.ITTSink;
import org.treetank.io.ITTSource;
import org.treetank.page.delegates.PageDelegate;
import org.treetank.page.interfaces.IPage;

public class MetaPage extends AbsForwardingPage {

  /** {@link PageDelegate} instance. */
  private final PageDelegate mDelegate;

  /**
   * Metadata for the revision.
   * 
   * @param pRevision
   *          revision number
   * @throws IllegalArgumentException
   *           if {@code pRevision} < 0
   */
  public MetaPage(@Nonnegative final long pRevision) {
    checkArgument(pRevision >= 0, "pRevision must be >= 0!");
    mDelegate = new PageDelegate(0, pRevision);
  }
  
  /**
   * Read meta page.
   * 
   * @param pIn
   *          input bytes to read from
   */
  protected MetaPage(@Nonnull final ITTSource pIn) {
    mDelegate = new PageDelegate(0, pIn.readLong());
    mDelegate.initialize(pIn);
  }

  @Override
  public void serialize(@Nonnull final ITTSink pOut) {
    mDelegate.serialize(checkNotNull(pOut));
  }

  @Override
  protected IPage delegate() {
    return mDelegate;
  }

}
