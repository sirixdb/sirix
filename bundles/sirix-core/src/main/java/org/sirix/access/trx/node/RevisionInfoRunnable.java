package org.sirix.access.trx.node;

import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.RevisionInfo;

import java.util.concurrent.Callable;

class RevisionInfoRunnable implements Callable<RevisionInfo> {

  private final ResourceManager<?, ?> resourceManager;

  private final int revision;

  RevisionInfoRunnable(final ResourceManager<?, ?> resourceManager, final int revision) {
    assert resourceManager != null;
    this.resourceManager = resourceManager;
    this.revision = revision;
  }

  @Override
  public RevisionInfo call() {
    try (final NodeReadOnlyTrx rtx = resourceManager.beginNodeReadOnlyTrx(revision)) {
      final CommitCredentials commitCredentials = rtx.getCommitCredentials();

      return new RevisionInfo(commitCredentials.getUser(), rtx.getRevisionNumber(), rtx.getRevisionTimestamp(),
          commitCredentials.getMessage());
    }
  }

}
