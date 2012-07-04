package org.sirix.api;

public interface IPreCommitHook {
  void preCommit(final INodeReadTrx pRtx);
}
