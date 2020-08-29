package org.sirix.access.trx.page;

import com.google.common.collect.ForwardingObject;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.cache.IndexLogKey;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.Reader;
import org.sirix.node.NodeKind;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingPageReadOnlyTrx
    extends ForwardingObject implements PageReadOnlyTrx {

  /**
   * Constructor for use by subclasses.
   */
  protected AbstractForwardingPageReadOnlyTrx() {
  }

  @Override
  public IndirectPage dereferenceIndirectPageReference(PageReference indirectPageReference) {
    return delegate().dereferenceIndirectPageReference(indirectPageReference);
  }

  @Override
  public int recordPageOffset(long key) {
    return delegate().recordPageOffset(key);
  }

  @Override
  public RevisionRootPage loadRevRoot(int lastCommittedRevision) {
    return delegate().loadRevRoot(lastCommittedRevision);
  }

  @Override
  public PageReference getReferenceToLeafOfSubtree(@Nonnull PageReference startReference, @Nonnegative long pageKey,
      int indexNumber, @Nonnull IndexType indexType) {
    return delegate().getReferenceToLeafOfSubtree(startReference, pageKey, indexNumber, indexType);
  }

  @Override
  public int getCurrentMaxIndirectPageTreeLevel(IndexType indexType, int index, RevisionRootPage revisionRootPage) {
    return delegate().getCurrentMaxIndirectPageTreeLevel(indexType, index, revisionRootPage);
  }

  @Override
  public <K, V> Optional<V> getRecord(@Nonnull K key,
      @Nonnull IndexType indexType, int index) {
    return delegate().getRecord(key, indexType, index);
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return delegate().getCommitCredentials();
  }

  @Override
  public long getTrxId() {
    return delegate().getTrxId();
  }

  @Override
  public ResourceManager<?, ?> getResourceManager() {
    return delegate().getResourceManager();
  }

  @Override
  public CASPage getCASPage(@Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getCASPage(revisionRoot);
  }

  @Override
  public NamePage getNamePage(@Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getNamePage(revisionRoot);
  }

  @Override
  public PathSummaryPage getPathSummaryPage(@Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getPathSummaryPage(revisionRoot);
  }

  @Override
  public PathPage getPathPage(@Nonnull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getPathPage(revisionRoot);
  }

  @Override
  public long pageKey(@Nonnegative long recordKey, @Nonnull IndexType indexType) {
    return delegate().pageKey(recordKey, indexType);
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    return delegate().getActualRevisionRootPage();
  }

  @Override
  public String getName(int nameKey, @Nonnull NodeKind kind) {
    return delegate().getName(nameKey, kind);
  }

  @Override
  public int getNameCount(int nameKey, @Nonnull NodeKind kind) {
    return delegate().getNameCount(nameKey, kind);
  }

  @Override
  public byte[] getRawName(int nameKey, @Nonnull NodeKind kind) {
    return delegate().getRawName(nameKey, kind);
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public Optional<Page> getRecordPage(@Nonnull IndexLogKey indexLogKey) {
    return delegate().getRecordPage(indexLogKey);
  }

  @Override
  public UberPage getUberPage() {
    return delegate().getUberPage();
  }

  @Override
  public boolean isClosed() {
    return delegate().isClosed();
  }

  @Override
  public int getRevisionNumber() {
    return delegate().getRevisionNumber();
  }

  @Override
  public Reader getReader() {
    return delegate().getReader();
  }

  @Override
  protected abstract PageReadOnlyTrx delegate();
}
