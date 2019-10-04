package org.sirix.access.trx.node;

import org.sirix.api.*;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;

public interface InternalResourceManager<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends ResourceManager<R, W> {
  /** Abort a write transaction. */
  enum Abort {
    /** Yes, abort. */
    YES,

    /** No, don't abort. */
    NO
  }

  Path getCommitFile();

  void assertAccess(int revision);

  PageTrx<Long, Record, UnorderedKeyValuePage> createPageWriteTransaction(long trxID, int revision, int i, Abort no,
      boolean isBoundToNodeTrx);

  Lock getCommitLock();

  void setLastCommittedUberPage(UberPage lastUberPage);

  void closeWriteTransaction(long transactionID);

  void setNodePageWriteTransaction(long transactionID, PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx);

  void closeNodePageWriteTransaction(long transactionID);

  void closeReadTransaction(long trxId);

  void closePageReadTransaction(long trxId);

  void closePageWriteTransaction(long transactionID);
}
