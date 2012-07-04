package org.sirix.api;


public interface IPostCommitHook {

  void postCommit(final INodeReadTrx pRtx);
}
