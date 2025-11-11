package io.sirix.access.trx.node;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.api.*;
import io.sirix.io.Reader;
import io.sirix.page.UberPage;

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;

public interface InternalResourceSession<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends ResourceSession<R, W> {
  /**
   * Abort the write-transaction.
   */
  enum Abort {
    /**
     * Yes, abort.
     */
    YES,

    /**
     * No, don't abort.
     */
    NO
  }

  Reader createReader();

  Path getCommitFile();

  void assertAccess(int revision);

  PageTrx createPageTransaction(int trxID, int revision, int i, Abort no, boolean isBoundToNodeTrx);

  Lock getCommitLock();

  void setLastCommittedUberPage(UberPage lastUberPage);

  void closeWriteTransaction(int transactionID);

  void setNodePageWriteTransaction(int transactionID, PageTrx pageTrx);

  void closeNodePageWriteTransaction(int transactionID);

  void closeReadTransaction(int trxId);

  void closePageReadTransaction(Integer trxId);

  void closePageWriteTransaction(Integer transactionID);

  /**
   * Get the revision epoch tracker for MVCC-aware eviction.
   *
   * @return the revision epoch tracker
   */
  RevisionEpochTracker getRevisionEpochTracker();
}
