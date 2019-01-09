package org.sirix.access.trx.node;

import java.nio.file.Path;
import java.util.concurrent.locks.Lock;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.ResourceManager;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;

public interface InternalResourceManager<R extends NodeReadTrx, W extends NodeWriteTrx> extends ResourceManager<R, W> {
  /** Abort a write transaction. */
  enum Abort {
    /** Yes, abort. */
    YES,

    /** No, don't abort. */
    NO
  }

  Path getCommitFile();

  void assertAccess(int revision);

  void closeNodePageWriteTransaction(long id);

  PageWriteTrx<Long, Record, UnorderedKeyValuePage> createPageWriteTransaction(long trxID, int revision, int i,
      Abort no);

  void setNodePageWriteTransaction(long id, PageWriteTrx<Long, Record, UnorderedKeyValuePage> trx);

  Lock getCommitLock();

  void setLastCommittedUberPage(UberPage lastUberPage);
}
