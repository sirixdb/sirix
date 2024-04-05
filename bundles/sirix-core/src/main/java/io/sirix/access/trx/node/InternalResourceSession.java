package io.sirix.access.trx.node;

import io.sirix.api.*;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.page.UberPage;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;

public interface InternalResourceSession<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends ResourceSession<R, W> {

  PageTrx createPageTransaction(long trxID,
      int revNumber, UberPage uberPage, boolean isBoundToNodeTrx,
      @NonNull TransactionIntentLog formerLog, boolean doAsyncCommit);

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

  Semaphore getRevisionRootPageLock();

  Path getCommitFile();

  void assertAccess(int revision);

  PageTrx createPageTransaction(long trxID, int revision, int i, Abort no, boolean isBoundToNodeTrx,
      TransactionIntentLog formerLog, boolean doAsyncCommit);

  Lock getCommitLock();

  void setLastCommittedUberPage(UberPage lastUberPage);

  void closeWriteTransaction(long transactionID);

  void setNodePageWriteTransaction(long transactionID, PageTrx pageTrx);

  void closeNodePageWriteTransaction(long transactionID);

  void closeReadTransaction(long trxId);

  void closePageReadTransaction(Long trxId);

  void closePageWriteTransaction(Long transactionID);
}
