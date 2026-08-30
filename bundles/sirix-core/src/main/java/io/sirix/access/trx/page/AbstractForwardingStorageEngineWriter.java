package io.sirix.access.trx.page;

import java.time.Instant;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;
import io.sirix.exception.SirixIOException;
import io.sirix.node.interfaces.DataRecord;
import org.jspecify.annotations.Nullable;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractForwardingStorageEngineWriter extends AbstractForwardingStorageEngineReader
    implements StorageEngineWriter {

  /**
   * Constructor for use by subclasses.
   */
  protected AbstractForwardingStorageEngineWriter() {}

  @Override
  public int getRevisionToRepresent() {
    return delegate().getRevisionToRepresent();
  }

  @Override
  public void close() throws SirixIOException {
    delegate().close();
  }

  @Override
  public <V extends DataRecord> V createRecord(V record, IndexType indexType, int index) {
    return delegate().createRecord(record, indexType, index);
  }

  @Override
  public <V extends DataRecord> V prepareRecordForModification(long recordKey, IndexType indexType, int index) {
    return delegate().prepareRecordForModification(recordKey, indexType, index);
  }

  @Override
  public <V extends DataRecord> V prepareRecordForModificationDocument(final long recordKey) {
    return delegate().prepareRecordForModificationDocument(recordKey);
  }

  @Override
  public void persistRecord(DataRecord record, IndexType indexType, int index) {
    delegate().persistRecord(record, indexType, index);
  }

  @Override
  public byte[] encodeStringValueForInsert(KeyValueLeafPage page, byte[] value, int off, int len) {
    // Must forward explicitly: the interface default returns null ("store raw"), so a
    // decorator inheriting it would silently disable insert-time FSST encoding — no failure,
    // just the commit-time re-encode pass quietly coming back.
    return delegate().encodeStringValueForInsert(page, value, off, len);
  }

  @Override
  public void removeRecord(long recordKey, IndexType indexType, int index) {
    delegate().removeRecord(recordKey, indexType, index);
  }

  @Override
  public int createNameKey(String name, NodeKind kind) {
    return delegate().createNameKey(name, kind);
  }

  @Override
  public int keyForName(String name, NodeKind kind) {
    return delegate().keyForName(name, kind);
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
  public boolean stageUncommittedOverflowPage(final PageReference reference) {
    return delegate().stageUncommittedOverflowPage(reference);
  }

  @Override
  public UberPage commitWritePages(String commitMessage, Instant commitTimeStamp, boolean isIntermediateCommit) {
    return delegate().commitWritePages(commitMessage, commitTimeStamp, isIntermediateCommit);
  }

  @Override
  public void hardenCommit(UberPage uberPage, boolean isIntermediateCommit) {
    delegate().hardenCommit(uberPage, isIntermediateCommit);
  }

  @Override
  public void asyncFlush() {
    delegate().asyncFlush();
  }

  @Override
  public boolean isAsyncFlushLogBoundaryReached() {
    return delegate().isAsyncFlushLogBoundaryReached();
  }

  @Override
  public void recordAsyncFlushForegroundNanos(final long elapsedNanos) {
    delegate().recordAsyncFlushForegroundNanos(elapsedNanos);
  }

  @Override
  public void awaitPendingAsyncFlush() {
    delegate().awaitPendingAsyncFlush();
  }

  @Override
  public void markTransactionRollbackOnly(final Throwable cause) {
    delegate().markTransactionRollbackOnly(cause);
  }

  @Override
  public void assertTransactionWritable() {
    delegate().assertTransactionWritable();
  }

  @Override
  public <P extends Page> P prepareSecondaryIndexPage(final IndexType indexType) {
    return delegate().prepareSecondaryIndexPage(indexType);
  }

  @Override
  public @Nullable KeyValueLeafPage getModifiedPageForRead(final long recordPageKey, final IndexType indexType,
      final int index) {
    return delegate().getModifiedPageForRead(recordPageKey, indexType, index);
  }

  @Override
  public boolean isReadOnlyPageForRead(final KeyValueLeafPage page) {
    return delegate().isReadOnlyPageForRead(page);
  }

  @Override
  public @Nullable DataRecord getDetachedRecordForRead(final KeyValueLeafPage page, final long recordKey) {
    return delegate().getDetachedRecordForRead(page, recordKey);
  }

  @Override
  public void releasePageForRead(final @Nullable KeyValueLeafPage page) {
    delegate().releasePageForRead(page);
  }

  @Override
  protected abstract StorageEngineWriter delegate();
}
