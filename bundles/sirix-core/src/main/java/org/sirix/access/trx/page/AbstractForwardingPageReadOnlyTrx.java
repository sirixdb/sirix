package org.sirix.access.trx.page;

import com.google.common.collect.ForwardingObject;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceSession;
import org.sirix.cache.IndexLogKey;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.io.Reader;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingPageReadOnlyTrx extends ForwardingObject
    implements PageReadOnlyTrx {

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
  public RevisionRootPage loadRevRoot(int lastCommittedRevision) {
    return delegate().loadRevRoot(lastCommittedRevision);
  }

  @Override
  public PageReference getReferenceToLeafOfSubtree(@NonNull PageReference startReference, @NonNegative long pageKey,
      int indexNumber, @NonNull IndexType indexType, RevisionRootPage revisionRootPage) {
    return delegate().getReferenceToLeafOfSubtree(startReference, pageKey, indexNumber, indexType, revisionRootPage);
  }

  @Override
  public int getCurrentMaxIndirectPageTreeLevel(IndexType indexType, int index, RevisionRootPage revisionRootPage) {
    return delegate().getCurrentMaxIndirectPageTreeLevel(indexType, index, revisionRootPage);
  }

  @Override
  public <V extends DataRecord> V getRecord(long key, @NonNull IndexType indexType, int index) {
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
  public ResourceSession<?, ?> getResourceSession() {
    return delegate().getResourceSession();
  }

  @Override
  public CASPage getCASPage(@NonNull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getCASPage(revisionRoot);
  }

  @Override
  public NamePage getNamePage(@NonNull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getNamePage(revisionRoot);
  }

  @Override
  public PathSummaryPage getPathSummaryPage(@NonNull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getPathSummaryPage(revisionRoot);
  }

  @Override
  public PathPage getPathPage(@NonNull RevisionRootPage revisionRoot) throws SirixIOException {
    return delegate().getPathPage(revisionRoot);
  }

  @Override
  public long pageKey(@NonNegative long recordKey, @NonNull IndexType indexType) {
    return delegate().pageKey(recordKey, indexType);
  }

  @Override
  public RevisionRootPage getActualRevisionRootPage() {
    return delegate().getActualRevisionRootPage();
  }

  @Override
  public String getName(int nameKey, @NonNull NodeKind kind) {
    return delegate().getName(nameKey, kind);
  }

  @Override
  public int getNameCount(int nameKey, @NonNull NodeKind kind) {
    return delegate().getNameCount(nameKey, kind);
  }

  @Override
  public byte[] getRawName(int nameKey, @NonNull NodeKind kind) {
    return delegate().getRawName(nameKey, kind);
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public Page getRecordPage(@NonNull IndexLogKey indexLogKey) {
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
  protected abstract @NonNull PageReadOnlyTrx delegate();
}
