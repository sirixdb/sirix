package io.sirix.diff.algorithm.fmse;

import java.util.Map;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.brackit.query.atomic.QNm;

/**
 * This functional class is used to compare inner nodes. FMES uses different
 * comparison criteria for leaf nodes and inner nodes. This class compares two
 * nodes by calculating the number of common children (i.e. children contained
 * in the matching) in relation to the total number of children.
 */
final class InnerNodeComparator implements NodeComparator<Long> {

	/**
	 * Matching Criterion 2. For the "good matching problem", the following
	 * conditions must hold inner nodes x and y:
	 * <ul>
	 * <li>label(x) == label(y)</li>
	 * <li>|common(x,y)| / max(|x|, |y|) > FMESTHRESHOLD</li>
	 * </ul>
	 * where FMESTHRESHOLD is in the range [0.5, 1] and common(x,y) computes the
	 * number of leafs that can be matched between x and y.
	 */
	private static final double FMESTHRESHOLD = 0.5;

	/** {@link Matching} reference. */
	private final Matching matching;

	private final XmlNodeReadOnlyTrx oldRtx;

	private final XmlNodeReadOnlyTrx newRtx;

	private final QNm idName;

	private final FMSENodeComparisonUtils nodeComparisonUtils;

	/**
	 * Number of descendants in subtree of node on old revision.
	 */
	private final Map<Long, Long> descendantsOldRev;

	/**
	 * Number of descendants in subtree of node on new revision.
	 */
	private final Map<Long, Long> descendantsNewRev;

	/**
	 * Constructor.
	 *
	 * @param idName
	 *            the name of an id-attribute, which has a unique value for a
	 *            specific element node, might be {@code null}
	 * @param matching
	 *            {@link Matching} reference
	 * @param oldRtx
	 *            the transactional cursor on the old revision
	 * @param newRtx
	 *            the transactional cursor on the new revision
	 * @param nodeComparisonUtils
	 *            comparison utils which might be used to compare nodes
	 * @param descendantsOldRev
	 *            number of descendants per node in old revision
	 * @param descendantsNewRev
	 *            number of descendants per node in new revision
	 */
	public InnerNodeComparator(final QNm idName, final Matching matching, final XmlNodeReadOnlyTrx oldRtx,
			final XmlNodeReadOnlyTrx newRtx, final FMSENodeComparisonUtils nodeComparisonUtils,
			final Map<Long, Long> descendantsOldRev, final Map<Long, Long> descendantsNewRev) {
		assert matching != null;
		assert oldRtx != null;
		assert newRtx != null;
		assert nodeComparisonUtils != null;
		assert descendantsOldRev != null;
		assert descendantsNewRev != null;
		this.idName = idName;
		this.matching = matching;
		this.oldRtx = oldRtx;
		this.newRtx = newRtx;
		this.nodeComparisonUtils = nodeComparisonUtils;
		this.descendantsOldRev = descendantsOldRev;
		this.descendantsNewRev = descendantsNewRev;
	}

	@Override
	public boolean isEqual(final Long firstNode, final Long secondNode) {
		assert firstNode != null;
		assert secondNode != null;

		oldRtx.moveTo(firstNode);
		newRtx.moveTo(secondNode);

		assert oldRtx.getKind() == newRtx.getKind();

		boolean retVal = false;

		if (idName != null && oldRtx.isElement() && newRtx.isElement() && oldRtx.moveToAttributeByName(idName)
				&& newRtx.moveToAttributeByName(idName)) {
			retVal = newRtx.getValue().equals(oldRtx.getValue());
		} else if ((oldRtx.hasFirstChild() || oldRtx.hasAttributes() || oldRtx.hasNamespaces())
				&& (newRtx.hasFirstChild() || newRtx.hasAttributes() || newRtx.hasNamespaces())) {
			final long common = matching.containedDescendants(firstNode, secondNode);
			final long maxFamilySize = Math.max(descendantsOldRev.get(firstNode), descendantsNewRev.get(secondNode));
			if (common == 0 && maxFamilySize == 1) {
				retVal = oldRtx.getName().equals(newRtx.getName());
			} else {
				retVal = ((double) common / (double) maxFamilySize) >= FMESTHRESHOLD;
			}
		} else {
			final QNm oldName = oldRtx.getName();
			final QNm newName = newRtx.getName();
			if (oldName.getNamespaceURI().equals(newName.getNamespaceURI())
					&& nodeComparisonUtils.calculateRatio(oldName.getLocalName(), newName.getLocalName()) > 0.7) {
				retVal = nodeComparisonUtils.checkAncestors(oldRtx.getNodeKey(), newRtx.getNodeKey());
			}
		}

		return retVal;
	}
}
