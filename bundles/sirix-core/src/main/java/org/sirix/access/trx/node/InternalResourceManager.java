package org.sirix.access.trx.node;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageTrx;
import org.sirix.api.ResourceManager;
import org.sirix.page.UberPage;

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;

public interface InternalResourceManager<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends ResourceManager<R, W> {
  /**
   * Abort a write transaction.
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

  Path getCommitFile();

  void assertAccess(int revision);

  PageTrx createPageTransaction(long trxID, int revision, int i, Abort no, boolean isBoundToNodeTrx);

  Lock getCommitLock();

  void setLastCommittedUberPage(UberPage lastUberPage);

  void closeWriteTransaction(long transactionID);

  void setNodePageWriteTransaction(long transactionID, PageTrx pageTrx);

  void closeNodePageWriteTransaction(long transactionID);

  void closeReadTransaction(long trxId);

  void closePageReadTransaction(long trxId);

  void closePageWriteTransaction(long transactionID);
}
