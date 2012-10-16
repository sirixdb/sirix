package org.sirix.api;

public interface PreCommitHook {
  void preCommit(final NodeReadTrx pRtx);
}
