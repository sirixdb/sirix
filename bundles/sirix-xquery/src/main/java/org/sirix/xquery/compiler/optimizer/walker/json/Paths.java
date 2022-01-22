package org.sirix.xquery.compiler.optimizer.walker.json;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.index.path.summary.PathSummaryReader;

import java.util.ArrayDeque;
import java.util.Deque;

public class Paths {
  private Paths() {
  }

  public static boolean isPathNodeNotAQueryResult(final Deque<QueryPathSegment> pathSegmentNamesToArrayIndexes,
      final PathSummaryReader pathSummary, final long pathNodeKey) {
    final var currentPathSegmentNames = new ArrayDeque<QueryPathSegment>();
    pathSegmentNamesToArrayIndexes.forEach(pathSegmentNameToArrayIndex -> {
      final var currentIndexes = new ArrayDeque<>(pathSegmentNameToArrayIndex.arrayIndexes());
      currentPathSegmentNames.addLast(new QueryPathSegment(pathSegmentNameToArrayIndex.pathSegmentName(), currentIndexes));
    });
    pathSummary.moveTo(pathNodeKey);
    var candidatePath = pathSummary.getPath();
    var queryPathSegment = currentPathSegmentNames.removeLast();
    String pathSegment = queryPathSegment.pathSegmentName();

    assert candidatePath != null;
    final var pathSteps = candidatePath.steps();

    for (int i = pathSteps.size() - 1; i >= 0; i--) {
      final var step = pathSteps.get(i);

      if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
        final Deque<Integer> indexesDeque = queryPathSegment.arrayIndexes();

        if (indexesDeque == null) {
          return true;
        } else if (indexesDeque.isEmpty()) {
          return true;
        } else {
          indexesDeque.removeLast();
        }
      } else if (step.getAxis() == Path.Axis.CHILD && step.getValue().equals(new QNm(pathSegment))) {
        if (currentPathSegmentNames.isEmpty()) {
          pathSegment = null;
        } else {
          if (!queryPathSegment.arrayIndexes().isEmpty()) {
            return true;
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
