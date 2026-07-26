package io.sirix.access.trx.node;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineWriter;
import io.sirix.io.Reader;
import io.sirix.page.RevisionRootPage;
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

  StorageEngineWriter createPageTransaction(int trxID, int revision, int i, Abort no, boolean isBoundToNodeTrx);

  /**
   * Variant for pipelined async commits: bases the new page transaction on the given PENDING
   * (phase-1-complete, not yet hardened) uber page instead of {@code lastCommittedUberPage}, and
   * skips the crash-recovery truncate check (the commit marker legitimately exists while the
   * previous epoch hardens in the background). Pass {@code null} for the default behavior.
   */
  StorageEngineWriter createPageTransaction(int trxID, int revision, int i, Abort no, boolean isBoundToNodeTrx,
      UberPage pendingBaseUberPage);

  /**
   * Remove a node-bound page write transaction from the session's map WITHOUT closing it — used
   * by pipelined async commits, where the superseded page transaction is closed by the background
   * hardening thread after the beacon write.
   */
  void detachNodePageWriteTransaction(int transactionID);

  /**
   * Pipelined async commits: register / resolve / clear the PENDING revision root — the
   * phase-1-complete, canonical in-memory root of a revision whose hardening is still running in
   * the background. Readers of the pending revision (only the successor epoch can reach it)
   * resolve it from here, since neither the revisions-file record nor
   * {@code lastCommittedUberPage} exist for it yet. Depth-1 pipeline ⇒ at most one pending entry.
   */
  void putPendingRevisionRoot(int revision, RevisionRootPage rootPage);

  RevisionRootPage getPendingRevisionRoot(int revision);

  void clearPendingRevisionRoot(int revision);

  Lock getCommitLock();

  void setLastCommittedUberPage(UberPage lastUberPage);

  void closeWriteTransaction(int transactionID);

  void setNodePageWriteTransaction(int transactionID, StorageEngineWriter storageEngineWriter);

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
