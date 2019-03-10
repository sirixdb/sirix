package org.sirix.diff.algorithm.fmse;

import java.util.Map;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;

/**
 * This functional class is used to compare inner nodes. FMES uses different comparison criteria for
 * leaf nodes and inner nodes. This class compares two nodes by calculating the number of common
 * children (i.e. children contained in the matching) in relation to the total number of children.
 */
final class InnerNodeEquality implements Comparator<Long> {

  /**
   * Matching Criterion 2. For the "good matching problem", the following conditions must hold inner
   * nodes x and y:
   * <ul>
   * <li>label(x) == label(y)</li>
   * <li>|common(x,y)| / max(|x|, |y|) > FMESTHRESHOLD</li>
   * </ul>
   * where FMESTHRESHOLD is in the range [0.5, 1] and common(x,y) computes the number of leafs that
   * can be matched between x and y.
   */
  private static final double FMESTHRESHOLD = 0.5;

  /** {@link Matching} reference. */
  private final Matching mMatching;

  private final XmlNodeReadOnlyTrx mOldRtx;

  private final XmlNodeReadOnlyTrx mNewRtx;

  private final QNm mIdName;

  private final FMSENodeComparisonUtils mNodeComparisonUtils;

  /**
   * Number of descendants in subtree of node on old revision.
   */
  private Map<Long, Long> mDescendantsOldRev;

  /**
   * Number of descendants in subtree of node on new revision.
   */
  private Map<Long, Long> mDescendantsNewRev;

  /**
   * Constructor.
   *
   * @param idName the name of an id-attribute, which has a unique value for a specific element node,
   *        might be {@code null}
   * @param matching {@link Matching} reference
   * @param wtx the transactional cursor on the old revision
   * @param rtx the transactional cursor on the new revision
   * @param nodeComparisonUtils comparison utils which might be used to compare nodes
   * @param descendantsOldRev number of descendants per node in old revision
   * @param descendantsNewRev number of descendants per node in new revision
   */
  public InnerNodeEquality(final QNm idName, final Matching matching, final XmlNodeReadOnlyTrx oldRtx,
      final XmlNodeReadOnlyTrx newRtx, final FMSENodeComparisonUtils nodeComparisonUtils,
      final Map<Long, Long> descendantsOldRev, final Map<Long, Long> descendantsNewRev) {
    assert matching != null;
    assert oldRtx != null;
    assert newRtx != null;
    assert nodeComparisonUtils != null;
    assert descendantsOldRev != null;
    assert descendantsNewRev != null;
    mIdName = idName;
    mMatching = matching;
    mOldRtx = oldRtx;
    mNewRtx = newRtx;
    mNodeComparisonUtils = nodeComparisonUtils;
    mDescendantsOldRev = descendantsOldRev;
    mDescendantsNewRev = descendantsNewRev;
  }

  @Override
  public boolean isEqual(final Long firstNode, final Long secondNode) {
    assert firstNode != null;
    assert secondNode != null;

    mOldRtx.moveTo(firstNode);
    mNewRtx.moveTo(secondNode);

    assert mOldRtx.getKind() == mNewRtx.getKind();

    boolean retVal = false;

    if (mIdName != null && mOldRtx.isElement() && mNewRtx.isElement()
        && mOldRtx.moveToAttributeByName(mIdName).hasMoved() && mNewRtx.moveToAttributeByName(mIdName).hasMoved()) {
      if (mNewRtx.getValue().equals(mOldRtx.getValue()))
        retVal = true;
      else
        retVal = false;
    } else if ((mOldRtx.hasFirstChild() || mOldRtx.hasAttributes() || mOldRtx.hasNamespaces())
        && (mNewRtx.hasFirstChild() || mNewRtx.hasAttributes() || mNewRtx.hasNamespaces())) {
      final long common = mMatching.containedDescendants(firstNode, secondNode);
      final long maxFamilySize = Math.max(mDescendantsOldRev.get(firstNode), mDescendantsNewRev.get(secondNode));
      if (common == 0 && maxFamilySize == 1) {
        retVal = mOldRtx.getName().equals(mNewRtx.getName());
      } else {
        retVal = ((double) common / (double) maxFamilySize) >= FMESTHRESHOLD;
      }
    } else {
      final QNm oldName = mOldRtx.getName();
      final QNm newName = mNewRtx.getName();
      if (oldName.getNamespaceURI().equals(newName.getNamespaceURI())
          && mNodeComparisonUtils.calculateRatio(oldName.getLocalName(), newName.getLocalName()) > 0.7) {
        retVal = mNodeComparisonUtils.checkAncestors(mOldRtx.getNodeKey(), mNewRtx.getNodeKey());
      }
    }

    return retVal;
  }
}
