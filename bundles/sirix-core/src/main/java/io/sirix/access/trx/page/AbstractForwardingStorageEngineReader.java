package io.sirix.access.trx.page;

import com.google.common.collect.ForwardingObject;
import io.sirix.access.trx.node.CommitCredentials;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.ResourceSession;
import io.sirix.cache.BufferManager;
import io.sirix.cache.IndexLogKey;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.Reader;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.*;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingStorageEngineReader extends ForwardingObject
    implements StorageEngineReader {

  /**
   * Constructor for use by subclasses.
   */
  protected AbstractForwardingStorageEngineReader() {
  }

  @Override
  public DataRecord getValue(KeyValueLeafPage page, long nodeKey) {
    return delegate().getValue(page, nodeKey);
  }

  @Override
  public boolean hasTrxIntentLog() {
    return delegate().hasTrxIntentLog();
  }

  @Override
  public BufferManager getBufferManager() {
    return delegate().getBufferManager();
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
  public int getTrxId() {
    return delegate().getTrxId();
  }

  @Override
  public long getDatabaseId() {
    return delegate().getDatabaseId();
  }

  @Override
  public long getResourceId() {
    return delegate().getResourceId();
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
  public NodeStorageEngineReader.PageReferenceToPage getRecordPage(@NonNull IndexLogKey indexLogKey) {
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
  protected abstract @NonNull StorageEngineReader delegate();
}
