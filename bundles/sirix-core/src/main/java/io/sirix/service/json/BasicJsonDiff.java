package io.sirix.service.json;

import com.google.common.collect.ImmutableSet;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.JsonDiff;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.*;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements a JSON-diff serialization format.
 *
 * @author Johannes Lichtenberger
 */
public final class BasicJsonDiff implements DiffObserver, JsonDiff {

  private final List<DiffTuple> diffs;
  private final String databaseName;

  /**
   * Constructor.
   *
   * @param databaseName The database name.
   */
  public BasicJsonDiff(final String databaseName) {
    this.databaseName = databaseName;
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
  public String generateDiff(JsonResourceSession resourceManager, int oldRevisionNumber, int newRevisionNumber) {
    return generateDiff(resourceManager, oldRevisionNumber, newRevisionNumber, 0, 0);
  }

  /**
   * Diff two revisions.
   *
   * @param session           the resource manager to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @param startNodeKey      the start node key
   * @param maxDepth          the maximum depth
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  @Override
  public String generateDiff(JsonResourceSession session, int oldRevisionNumber, int newRevisionNumber,
      long startNodeKey, long maxDepth) {
    diffs.clear();

    DiffFactory.invokeJsonDiff(new DiffFactory.Builder<>(session,
                                                         newRevisionNumber,
                                                         oldRevisionNumber,
                                                         session.getResourceConfig().hashType == HashType.NONE
                                                             ? DiffFactory.DiffOptimized.NO
                                                             : DiffFactory.DiffOptimized.HASHED,
                                                         ImmutableSet.of(this)).skipSubtrees(true)
                                                                               .newStartKey(startNodeKey)
                                                                               .oldStartKey(startNodeKey)
                                                                               .oldMaxDepth(maxDepth));

    return new JsonDiffSerializer(this.databaseName, session, oldRevisionNumber, newRevisionNumber, diffs).serialize(
        true);
  }

  @Override
  public void diffListener(final DiffFactory.@NonNull DiffType diffType, final long newNodeKey, final long oldNodeKey,
      @NonNull final DiffDepth depth) {
    diffs.add(new DiffTuple(diffType, newNodeKey, oldNodeKey, depth));
  }

  @Override
  public void diffDone() {
  }
}
