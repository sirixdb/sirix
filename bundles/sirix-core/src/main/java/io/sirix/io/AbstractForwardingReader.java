package io.sirix.io;

import io.sirix.utils.ForwardingObject;
import io.sirix.access.ResourceConfiguration;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingReader extends ForwardingObject implements Reader {

  /**
   * Constructor for use by subclasses.
   */
  protected AbstractForwardingReader() {}

  @Override
  public Page read(PageReference reference, @Nullable ResourceConfiguration resourceConfiguration) {
    return delegate().read(reference, resourceConfiguration);
  }

  @Override
  public Page readRecordPageLazily(PageReference reference, @Nullable ResourceConfiguration resourceConfiguration) {
    // Forwarded rather than inherited: the interface default answers eagerly, which would silently
    // strip laziness from every backend reached through a forwarder.
    return delegate().readRecordPageLazily(reference, resourceConfiguration);
  }

  @Override
  public CompletableFuture<? extends Page> readAsync(PageReference reference,
      @Nullable ResourceConfiguration resourceConfiguration) {
    return delegate().readAsync(reference, resourceConfiguration);
  }

  @Override
  public PageReference readUberPageReference() {
    return delegate().readUberPageReference();
  }

  @Override
  public RevisionRootPage readRevisionRootPage(int revision, ResourceConfiguration resourceConfiguration) {
    return delegate().readRevisionRootPage(revision, resourceConfiguration);
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
  public RevisionFileData[] getRevisionFileData(int fromRevision, int count) {
    // Forward explicitly — falling back to the interface default would loop the
    // single-record read and lose the delegate's bulk-read optimization.
    return delegate().getRevisionFileData(fromRevision, count);
  }

  @Override
  public void prefetch(PageReference[] references, int count) {
    // Forward explicitly — the interface default is a no-op, which would silently drop the
    // delegate's batched warm-up.
    delegate().prefetch(references, count);
  }

  @Override
  public Page readHOTLeafFragment(PageReference key, ResourceConfiguration resourceConfiguration) {
    // Forward explicitly — the interface default falls back to the full read, which would strip the
    // delegate's compact fragment decode from every write transaction reached through a forwarder.
    return delegate().readHOTLeafFragment(key, resourceConfiguration);
  }

  @Override
  public Page readHOTLeafFragment(PageReference key, ResourceConfiguration resourceConfiguration,
      long committedExtent) {
    // Forward explicitly — same reason, and the inherited default also discards the chain's
    // already-captured extent bound, re-consulting the file for every scalar fragment.
    return delegate().readHOTLeafFragment(key, resourceConfiguration, committedExtent);
  }

  @Override
  public Page[] readHOTLeafFragments(PageReference[] references, ResourceConfiguration resourceConfiguration) {
    // Forward explicitly — the interface default delegates to the batch read, whose own default is a
    // scalar loop, so a forwarder silently lost both the batch I/O and the compact decode.
    return delegate().readHOTLeafFragments(references, resourceConfiguration);
  }

  @Override
  public long committedDataExtent() {
    // Forward explicitly — the interface default reports "no bound", which makes every scalar
    // fragment read of a chain consult the file size again.
    return delegate().committedDataExtent();
  }

  @Override
  public int preferredPrefetchBatch() {
    return delegate().preferredPrefetchBatch();
  }

  @Override
  public boolean returnsSharedPages() {
    return delegate().returnsSharedPages();
  }

  @Override
  protected abstract Reader delegate();
}
