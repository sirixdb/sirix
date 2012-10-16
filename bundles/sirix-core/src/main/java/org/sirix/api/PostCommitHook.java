package org.sirix.api;


public interface PostCommitHook {

  void postCommit(final NodeReadTrx pRtx);
}
