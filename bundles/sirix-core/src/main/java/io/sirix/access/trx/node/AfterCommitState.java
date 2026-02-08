package io.sirix.access.trx.node;

public enum AfterCommitState {
    /**
     * Transaction stays open after commit, intermediate auto-commits create new revisions (sync).
     */
    KEEP_OPEN,

    /**
     * Transaction stays open after commit, intermediate auto-commits use async TIL rotation
     * without creating intermediate revisions. Only the final explicit commit creates a revision.
     * Optimized for bulk imports.
     */
    KEEP_OPEN_ASYNC,

    /**
     * Transaction is closed after commit.
     */
    CLOSE
}
