package io.sirix.diff.algorithm.fmse;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.node.NodeKind;
import org.brackit.xquery.atomic.QNm;
import io.sirix.index.path.summary.PathSummaryReader;

/**
 * This functional class is used to compare leaf nodes. The comparison is done by comparing the
 * (characteristic) string for two nodes. If the strings are sufficient similar, the nodes are
 * considered to be equal.
 */
final class LeafNodeComparator implements NodeComparator<Long> {

  /**
   * Matching Criterion 1. For the "good matching problem", the following conditions must hold for
   * leafs x and y:
   * <ul>
   * <li>label(x) == label(y)</li>
   * <li>compare(value(x), value(y)) <= FMESF</li>
   * </ul>
   * where FMESF is in the range [0,1] and compare() computes the cost of updating a leaf node.
   */
  public static final double FMESF = 0.5;

  private enum Path {
    NO_PATH,

    MATCHES,

    PATH_LENGTH_IS_NOT_EQUAL,

    NO_MATCH_NO_LENGTH_EQUALS
  }

  private final QNm mId;

  private final XmlNodeReadOnlyTrx oldRtx;

  private final XmlNodeReadOnlyTrx newRtx;

  /** Path summary reader for the old revision. */
  private final PathSummaryReader oldPathSummary;

  /** Path summary reader for the new revision. */
  private final PathSummaryReader newPathSummary;

  private final FMSENodeComparisonUtils nodeComparisonUtils;

  public LeafNodeComparator(final QNm id, final XmlNodeReadOnlyTrx oldRtx, final XmlNodeReadOnlyTrx newRtx,
      final PathSummaryReader oldPathSummary, final PathSummaryReader newPathSummary,
      final FMSENodeComparisonUtils nodeComparisonUtils) {
    mId = id;
    this.oldRtx = oldRtx;
    this.newRtx = newRtx;
    this.oldPathSummary = oldPathSummary;
    this.newPathSummary = newPathSummary;
    this.nodeComparisonUtils = nodeComparisonUtils;
  }

  @Override
  public boolean isEqual(final Long firstNode, final Long secondNode) {
    assert firstNode != null;
    assert secondNode != null;

    // Old.
    oldRtx.moveTo(firstNode);

    // New.
    newRtx.moveTo(secondNode);

    assert oldRtx.getKind() == newRtx.getKind();
    double ratio = 0;

    if (oldRtx.getKind() == NodeKind.ATTRIBUTE || oldRtx.getKind() == NodeKind.NAMESPACE
        || oldRtx.getKind() == NodeKind.PROCESSING_INSTRUCTION) {
      if (oldRtx.getName().equals(newRtx.getName())) {
        ratio = 1;
        if (oldRtx.getKind() == NodeKind.ATTRIBUTE || oldRtx.getKind() == NodeKind.PROCESSING_INSTRUCTION) {
          ratio = nodeComparisonUtils.calculateRatio(oldRtx.getValue(), newRtx.getValue());

          if (ratio > FMESF) {
            final Path paths = checkPaths();

            ratio = adaptRatioByUsingThePathChecks(paths);
          }
        }
      }
    } else {
      if (nodeComparisonUtils.nodeValuesEqual(firstNode, secondNode, oldRtx, newRtx)) {
        ratio = 1;
      } else {
        ratio = nodeComparisonUtils.calculateRatio(nodeComparisonUtils.getNodeValue(firstNode, oldRtx),
                                                   nodeComparisonUtils.getNodeValue(secondNode, newRtx));
      }

      if (ratio > FMESF) {
        oldRtx.moveToParent();
        newRtx.moveToParent();

        ratio = nodeComparisonUtils.calculateRatio(nodeComparisonUtils.getNodeValue(oldRtx.getNodeKey(), oldRtx),
                                                   nodeComparisonUtils.getNodeValue(newRtx.getNodeKey(), newRtx));

        if (ratio > FMESF) {
          final var paths = checkPaths();

          ratio = adaptRatioByUsingThePathChecks(paths);
        }
      }
    }

    if (ratio > FMESF && mId != null
        && nodeComparisonUtils.checkIfAncestorIdsMatch(oldRtx.getNodeKey(), newRtx.getNodeKey(), mId))
      ratio = 1;

    // Old.
    oldRtx.moveTo(firstNode);

    // New.
    newRtx.moveTo(secondNode);

    return ratio > FMESF;
  }

  private double adaptRatioByUsingThePathChecks(final Path paths) {
    double ratio;
    if (paths != Path.PATH_LENGTH_IS_NOT_EQUAL && mId != null) {
      ratio = nodeComparisonUtils.checkIfAncestorIdsMatch(oldRtx.getNodeKey(), newRtx.getNodeKey(), mId)
          ? 1
          : 0;
    } else if (paths == Path.MATCHES) {
      ratio = 1;
    } else if (paths == Path.NO_PATH) {
      ratio = nodeComparisonUtils.checkAncestors(oldRtx.getNodeKey(), newRtx.getNodeKey())
          ? 1
          : 0;
    } else {
      ratio = 0;
    }
    return ratio;
  }

  private Path checkPaths() {
    if (oldRtx.getPathNodeKey() == 0 || newRtx.getPathNodeKey() == 0)
      return Path.NO_PATH;

    final var oldPathNode = newPathSummary.getPathNodeForPathNodeKey(newRtx.getPathNodeKey());
    newPathSummary.moveTo(oldPathNode.getNodeKey());
    var oldPath = newPathSummary.getPath();

    final var newPathNode = oldPathSummary.getPathNodeForPathNodeKey(oldRtx.getPathNodeKey());
    oldPathSummary.moveTo(newPathNode.getNodeKey());
    var newPath = oldPathSummary.getPath();

    //noinspection DataFlowIssue
    if (oldPath.getLength() != newPath.getLength())
      return Path.PATH_LENGTH_IS_NOT_EQUAL;
    else if (oldPath.matches(newPath))
      return Path.MATCHES;
    else
      return Path.NO_MATCH_NO_LENGTH_EQUALS;
  }
}
