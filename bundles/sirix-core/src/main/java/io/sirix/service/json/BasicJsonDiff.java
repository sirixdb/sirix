package io.sirix.service.json;

import java.util.Set;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.JsonDiff;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.diff.DiffDepth;
import io.sirix.diff.DiffFactory;
import io.sirix.diff.DiffFactory.DiffOptimized;
import io.sirix.diff.DiffObserver;
import io.sirix.diff.DiffTuple;
import io.sirix.diff.JsonDiffSerializer;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements a JSON-diff serialization format.
 *
 * @author Johannes Lichtenberger
 */
public final class BasicJsonDiff implements DiffObserver, JsonDiff {

  private final List<DiffTuple> diffs;
  private final LongOpenHashSet insertedKeys = new LongOpenHashSet();
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
   * @param resourceSession the resource session to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  @Override
  public String generateDiff(JsonResourceSession resourceSession, int oldRevisionNumber, int newRevisionNumber) {
    return generateDiff(resourceSession, oldRevisionNumber, newRevisionNumber, 0, 0);
  }

  /**
   * Diff two revisions.
   *
   * @param session the resource session to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @param startNodeKey the start node key
   * @param maxDepth the maximum depth
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  @Override
  public String generateDiff(JsonResourceSession session, int oldRevisionNumber, int newRevisionNumber,
      long startNodeKey, long maxDepth) {
    return generateDiff(session, oldRevisionNumber, newRevisionNumber, startNodeKey, maxDepth, true);
  }

  /**
   * Diff two revisions with control over data inclusion.
   *
   * @param session the resource session to use
   * @param oldRevisionNumber the revision number of the older revision
   * @param newRevisionNumber the revision number of the newer revision
   * @param startNodeKey the start node key
   * @param maxDepth the maximum depth
   * @param includeData whether to include full subtree data for inserts (false for compact mode)
   * @return a JSON-String describing the differences encountered between the two revisions
   */
  public String generateDiff(JsonResourceSession session, int oldRevisionNumber, int newRevisionNumber,
      long startNodeKey, long maxDepth, boolean includeData) {
    final DiffOptimized optimized = session.getResourceConfig().hashType == HashType.NONE
        ? DiffOptimized.NO
        : DiffOptimized.HASHED;
    computeDiff(session, oldRevisionNumber, newRevisionNumber, startNodeKey, maxDepth, optimized);
    return new JsonDiffSerializer(this.databaseName, session, oldRevisionNumber, newRevisionNumber, diffs).serialize(
        includeData);
  }

  /**
   * Generate a compact diff for revision replay and update-operation fallback. Hash skips are
   * disabled and retained fragments are expanded to preserve node identity and descendant edits.
   * Public diff generation uses the separate optimized path.
   *
   * @param session the resource session to use
   * @param oldRevisionNumber the source revision before the changes
   * @param newRevisionNumber the source revision after the changes
   * @return the replay diff without serialized fragment data
   */
  public String generateDiffForReplay(final JsonResourceSession session, final int oldRevisionNumber,
      final int newRevisionNumber) {
    computeDiff(session, oldRevisionNumber, newRevisionNumber, 0, 0, DiffOptimized.NO);
    if (!diffs.isEmpty()) {
      expandRetainedFragments(session, oldRevisionNumber, newRevisionNumber);
    }
    return new JsonDiffSerializer(this.databaseName, session, oldRevisionNumber, newRevisionNumber, diffs).serialize(
        false);
  }

  private void computeDiff(final JsonResourceSession session, final int oldRevisionNumber, final int newRevisionNumber,
      final long startNodeKey, final long maxDepth, final DiffOptimized optimized) {
    diffs.clear();
    insertedKeys.clear();

    invokeDiff(session, oldRevisionNumber, newRevisionNumber, startNodeKey, maxDepth, optimized);
  }

  private void invokeDiff(final JsonResourceSession session, final int oldRevisionNumber, final int newRevisionNumber,
      final long startNodeKey, final long maxDepth, final DiffOptimized optimized) {
    DiffFactory.invokeJsonDiff(
        new DiffFactory.Builder<>(session, newRevisionNumber, oldRevisionNumber, optimized, Set.of(this)).skipSubtrees(
            true).newStartKey(startNodeKey).oldStartKey(startNodeKey).oldMaxDepth(maxDepth));
  }

  private void expandRetainedFragments(final JsonResourceSession session, final int oldRevisionNumber,
      final int newRevisionNumber) {
    try (final var previousRevision = session.beginNodeReadOnlyTrx(oldRevisionNumber);
        final var newRevision = session.beginNodeReadOnlyTrx(newRevisionNumber)) {
      final long previousMaxNodeKey = previousRevision.getMaxNodeKey();
      if (previousMaxNodeKey == 0) {
        return;
      }
      boolean hasRetainedKeys = false;
      for (int index = 0; index < diffs.size(); index++) {
        final DiffTuple tuple = diffs.get(index);
        if (tuple.getDiff() != DiffFactory.DiffType.INSERTED && tuple.getDiff() != DiffFactory.DiffType.REPLACEDNEW) {
          continue;
        }
        final long nodeKey = tuple.getNewNodeKey();
        if (tuple.getDiff() == DiffFactory.DiffType.REPLACEDNEW && (newRevision.moveTo(tuple.getOldNodeKey())
            || (nodeKey <= previousMaxNodeKey && previousRevision.moveTo(nodeKey)))) {
          normalizeReplacement(index, tuple, newRevision);
          continue;
        }
        newRevision.moveTo(nodeKey);
        if (nodeKey <= previousMaxNodeKey && previousRevision.moveTo(nodeKey)) {
          hasRetainedKeys = true;
          if (newRevision.hasFirstChild() || previousRevision.hasFirstChild()) {
            invokeDiff(session, oldRevisionNumber, newRevisionNumber, nodeKey, 0, DiffOptimized.NO);
          }
        } else {
          findFragmentRoots(newRevision, previousRevision, previousMaxNodeKey);
        }
      }
      if (hasRetainedKeys) {
        normalizeRetainedReplacements(newRevision);
      }
    }
  }

  private void normalizeRetainedReplacements(final JsonNodeReadOnlyTrx newRevision) {
    for (int index = 0; index < diffs.size(); index++) {
      final DiffTuple tuple = diffs.get(index);
      if (tuple.getDiff() == DiffFactory.DiffType.REPLACEDNEW) {
        normalizeReplacement(index, tuple, newRevision);
      }
    }
  }

  private void normalizeReplacement(final int index, final DiffTuple tuple, final JsonNodeReadOnlyTrx newRevision) {
    diffs.set(index, new DiffTuple(DiffFactory.DiffType.DELETED, 0, tuple.getOldNodeKey(), tuple.getDepth()));
    if (newRevision.moveTo(tuple.getOldNodeKey())) {
      diffListener(DiffFactory.DiffType.INSERTED, tuple.getOldNodeKey(), 0, tuple.getDepth());
    }
    diffListener(DiffFactory.DiffType.INSERTED, tuple.getNewNodeKey(), 0, tuple.getDepth());
  }

  private void findFragmentRoots(final JsonNodeReadOnlyTrx newRevision, final JsonNodeReadOnlyTrx previousRevision,
      final long previousMaxNodeKey) {
    final long rootKey = newRevision.getNodeKey();
    long greatestNewKey = rootKey;
    if (!newRevision.moveToFirstChild()) {
      return;
    }
    while (true) {
      final long nodeKey = newRevision.getNodeKey();
      if ((nodeKey <= previousMaxNodeKey && previousRevision.moveTo(nodeKey)) || nodeKey < greatestNewKey) {
        diffListener(DiffFactory.DiffType.INSERTED, nodeKey, 0, new DiffDepth(0, 0));
      } else if (newRevision.moveToFirstChild()) {
        greatestNewKey = Math.max(greatestNewKey, nodeKey);
        continue;
      }
      greatestNewKey = Math.max(greatestNewKey, nodeKey);
      while (!newRevision.hasRightSibling() && newRevision.getNodeKey() != rootKey) {
        newRevision.moveToParent();
      }
      if (newRevision.getNodeKey() == rootKey || !newRevision.moveToRightSibling()) {
        break;
      }
    }
  }

  @Override
  public void diffListener(final DiffFactory.DiffType diffType, final long newNodeKey, final long oldNodeKey,
      final DiffDepth depth) {
    if (diffType == DiffFactory.DiffType.SAME || diffType == DiffFactory.DiffType.SAMEHASH
        || diffType == DiffFactory.DiffType.REPLACEDOLD) {
      return;
    }
    if (diffType == DiffFactory.DiffType.INSERTED && !insertedKeys.add(newNodeKey)) {
      return;
    }
    diffs.add(new DiffTuple(diffType, newNodeKey, oldNodeKey, depth));
  }

  @Override
  public void diffDone() {}
}
