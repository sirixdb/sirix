package org.sirix.service.json;

import com.google.common.collect.ImmutableSet;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.JsonDiff;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.diff.*;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a JSON-diff serialization format.
 *
 * @author Johannes Lichtenberger
 */
public final class BasicJsonDiff implements DiffObserver, JsonDiff {

  private final List<DiffTuple> diffs;

  /**
   * Constructor.
   */
  public BasicJsonDiff() {
    this.diffs = new ArrayList<>();
  }

  /**
   * Diff two revisions.
   *
   * @param resourceManager   the resource manager to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  @Override
  public String generateDiff(JsonResourceManager resourceManager, int oldRevisionNumber, int newRevisionNumber) {
    return generateDiff(resourceManager, oldRevisionNumber, newRevisionNumber, 0, 0);
  }

  /**
   * Diff two revisions.
   *
   * @param resourceManager   the resource manager to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @param startNodeKey      the start node key
   * @param maxDepth          the maximum depth
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  @Override
  public String generateDiff(JsonResourceManager resourceManager, int oldRevisionNumber, int newRevisionNumber,
      long startNodeKey, long maxDepth) {
    diffs.clear();

    DiffFactory.invokeJsonDiff(new DiffFactory.Builder<>(resourceManager, newRevisionNumber, oldRevisionNumber,
        resourceManager.getResourceConfig().hashType == HashType.NONE
            ? DiffFactory.DiffOptimized.NO
            : DiffFactory.DiffOptimized.HASHED, ImmutableSet.of(this)).skipSubtrees(true)
                                                                      .newStartKey(startNodeKey)
                                                                      .oldStartKey(startNodeKey)
                                                                      .oldMaxDepth(maxDepth));

    return new JsonDiffSerializer(resourceManager, oldRevisionNumber, newRevisionNumber, diffs).serialize(true);
  }

  @Override
  public void diffListener(@Nonnull final DiffFactory.DiffType diffType, final long newNodeKey, final long oldNodeKey,
      @Nonnull final DiffDepth depth) {
    diffs.add(new DiffTuple(diffType, newNodeKey, oldNodeKey, depth));
  }

  @Override
  public void diffDone() {
  }
}
