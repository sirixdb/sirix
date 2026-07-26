package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.ArrayDeque;
import java.util.Deque;

public class Paths {
  private Paths() {}

  public static boolean isPathNodeNotAQueryResult(final Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes,
      final PathSummaryReader pathSummary, final long pathNodeKey) {
    final var currentPathSegmentNames = new ArrayDeque<QueryPathSegment>();
    pathSegmentNamesToArrayIndexes.forEach(pathSegmentNameToArrayIndex -> {
      final var currentIndexes = new ArrayDeque<>(pathSegmentNameToArrayIndex.arrayIndexes());
      currentPathSegmentNames.addLast(
          new QueryPathSegment(pathSegmentNameToArrayIndex.pathSegmentName(), currentIndexes));
    });
    pathSummary.moveTo(pathNodeKey);
    var candidatePath = pathSummary.getPath();
    var queryPathSegment = currentPathSegmentNames.removeLast();
    String pathSegment = queryPathSegment.pathSegmentName();

    assert candidatePath != null;
    final var pathSteps = candidatePath.steps();

    final int lastIdx = pathSteps.size() - 1;
    for (int i = lastIdx; i >= 0; i--) {
      final var step = pathSteps.get(i);

      if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
        final Deque<Integer> indexesDeque = queryPathSegment.arrayIndexes();

        if (indexesDeque == null) {
          return true;
        } else if (indexesDeque.isEmpty()) {
          // OBJECT_NAMED_ARRAY anchors its pathNodeKey at the trailing
          // {@code []} ARRAY layer. A query that references the array by name only (no
          // array index) lands on this same path; permit the trailing {@code []} to be
          // consumed without an explicit index. Other CHILD_ARRAY steps still require a
          // matching index.
          if (i == lastIdx) {
            // trailing array layer of fused record — skip without index
            continue;
          }
          return true;
        } else {
          indexesDeque.removeLast();
        }
      } else if (step.getAxis() == Path.Axis.CHILD_OBJECT_FIELD && step.getValue().equals(new QNm(pathSegment))) {
        if (currentPathSegmentNames.isEmpty()) {
          pathSegment = null;
        } else {
          // iter#32 fusion: if this FIELD step is the path's LAST step and the query
          // segment carries a trailing array index, the indexed record IS the fused
          // OBJECT_NAMED_ARRAY (kindId 53) — its child element supplies the index. The
          // post-index walker (IndexExpr#PATH) handles the array indexing directly, so
          // unconsumed indexes here are fine. Drain them and proceed.
          final Deque<Integer> remainingIdx = queryPathSegment.arrayIndexes();
          if (!remainingIdx.isEmpty()) {
            if (i == lastIdx) {
              remainingIdx.clear();
            } else {
              return true;
            }
          }
          queryPathSegment = currentPathSegmentNames.removeLast();
          pathSegment = queryPathSegment.pathSegmentName();
        }
      } else {
        return true;
      }
    }

    return !currentPathSegmentNames.isEmpty();
  }
}
