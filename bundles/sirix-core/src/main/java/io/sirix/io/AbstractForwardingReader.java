package io.sirix.io;

import com.google.common.collect.ForwardingObject;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingReader extends ForwardingObject implements Reader {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingReader() {}

  @Override
  public Page read(PageReference reference, @Nullable PageReadOnlyTrx pageReadTrx) {
    return delegate().read(reference, pageReadTrx);
  }

  @Override
  public CompletableFuture<? extends Page> readAsync(PageReference reference, @Nullable PageReadOnlyTrx pageReadTrx) {
    return delegate().readAsync(reference, pageReadTrx);
  }

  @Override
  public PageReference readUberPageReference() {
    return delegate().readUberPageReference();
  }

  @Override
  public RevisionRootPage readRevisionRootPage(int revision, PageReadOnlyTrx pageReadTrx) {
    return delegate().readRevisionRootPage(revision, pageReadTrx);
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    return delegate().readRevisionRootPageCommitTimestamp(revision);
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    return delegate().getRevisionFileData(revision);
  }

  @Override
  protected abstract Reader delegate();
}
