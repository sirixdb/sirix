package org.sirix.diff.algorithm.fmse;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.Kind;

/**
 * This functional class is used to compare leaf nodes. The comparison is done by comparing the
 * (characteristic) string for two nodes. If the strings are sufficient similar, the nodes are
 * considered to be equal.
 */
final class LeafNodeEqualilty implements Comparator<Long> {

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

  private final XmlNodeReadOnlyTrx mOldRtx;

  private final XmlNodeReadOnlyTrx mNewRtx;

  /** Path summary reader for the old revision. */
  private final PathSummaryReader mOldPathSummary;

  /** Path summary reader for the new revision. */
  private final PathSummaryReader mNewPathSummary;

  private final FMSENodeComparisonUtils mNodeComparisonUtils;

  public LeafNodeEqualilty(final QNm id, final XmlNodeReadOnlyTrx oldRtx, final XmlNodeReadOnlyTrx newRtx,
      final PathSummaryReader oldPathSummary, final PathSummaryReader newPathSummary,
      final FMSENodeComparisonUtils nodeComparisonUtils) {
    mId = id;
    mOldRtx = oldRtx;
    mNewRtx = newRtx;
    mOldPathSummary = oldPathSummary;
    mNewPathSummary = newPathSummary;
    mNodeComparisonUtils = nodeComparisonUtils;
  }

  @Override
  public boolean isEqual(final Long firstNode, final Long secondNode) {
    assert firstNode != null;
    assert secondNode != null;

    // Old.
    mOldRtx.moveTo(firstNode);

    // New.
    mNewRtx.moveTo(secondNode);

    assert mOldRtx.getKind() == mNewRtx.getKind();
    double ratio = 0;

    if (mOldRtx.getKind() == Kind.ATTRIBUTE || mOldRtx.getKind() == Kind.NAMESPACE
        || mOldRtx.getKind() == Kind.PROCESSING_INSTRUCTION) {
      if (mOldRtx.getName().equals(mNewRtx.getName())) {
        ratio = 1;
        if (mOldRtx.getKind() == Kind.ATTRIBUTE || mOldRtx.getKind() == Kind.PROCESSING_INSTRUCTION) {
          ratio = mNodeComparisonUtils.calculateRatio(mOldRtx.getValue(), mNewRtx.getValue());

          if (ratio > FMESF) {
            final Path paths = checkPaths();

            ratio = adaptRatioByUsingThePathChecks(paths);
          }
        }
      }
    } else {
      if (mNodeComparisonUtils.nodeValuesEqual(firstNode, secondNode, mOldRtx, mNewRtx)) {
        ratio = 1;
      } else {
        ratio = mNodeComparisonUtils.calculateRatio(mNodeComparisonUtils.getNodeValue(firstNode, mOldRtx),
            mNodeComparisonUtils.getNodeValue(secondNode, mNewRtx));
      }

      if (ratio > FMESF) {
        mOldRtx.moveToParent();
        mNewRtx.moveToParent();

        ratio = mNodeComparisonUtils.calculateRatio(mNodeComparisonUtils.getNodeValue(mOldRtx.getNodeKey(), mOldRtx),
            mNodeComparisonUtils.getNodeValue(mNewRtx.getNodeKey(), mNewRtx));

        if (ratio > FMESF) {
          final var paths = checkPaths();

          ratio = adaptRatioByUsingThePathChecks(paths);
        }
      }
    }

    if (ratio > FMESF && mId != null
        && mNodeComparisonUtils.checkIfAncestorIdsMatch(mOldRtx.getNodeKey(), mNewRtx.getNodeKey(), mId))
      ratio = 1;

    // Old.
    mOldRtx.moveTo(firstNode);

    // New.
    mNewRtx.moveTo(secondNode);

    return ratio > FMESF;
  }

  private double adaptRatioByUsingThePathChecks(final Path paths) {
    double ratio;
    if (paths != Path.PATH_LENGTH_IS_NOT_EQUAL && mId != null) {
      ratio = mNodeComparisonUtils.checkIfAncestorIdsMatch(mOldRtx.getNodeKey(), mNewRtx.getNodeKey(), mId)
          ? 1
          : 0;
    } else if (paths == Path.MATCHES) {
      ratio = 1;
    } else if (paths == Path.NO_PATH) {
      ratio = mNodeComparisonUtils.checkAncestors(mOldRtx.getNodeKey(), mNewRtx.getNodeKey())
          ? 1
          : 0;
    } else {
      ratio = 0;
    }
    return ratio;
  }

  private Path checkPaths() {
    if (mOldRtx.getPathNodeKey() == 0 || mNewRtx.getPathNodeKey() == 0)
      return Path.NO_PATH;

    final var oldPathNode = mNewPathSummary.getPathNodeForPathNodeKey(mNewRtx.getPathNodeKey());
    final var oldPath = oldPathNode.getPath(mNewPathSummary);

    final var newPathNode = mOldPathSummary.getPathNodeForPathNodeKey(mOldRtx.getPathNodeKey());
    final var newPath = newPathNode.getPath(mOldPathSummary);

    if (oldPath.getLength() != newPath.getLength())
      return Path.PATH_LENGTH_IS_NOT_EQUAL;
    else if (oldPath.matches(newPath))
      return Path.MATCHES;
    else
      return Path.NO_MATCH_NO_LENGTH_EQUALS;
  }
}
