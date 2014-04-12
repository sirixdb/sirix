package org.sirix.gui.view.smallmultiple;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Comparator;
import java.util.List;

import org.sirix.gui.view.sunburst.SunburstItem;

/**
 * SunburstList container used in {@link SmallmultipleModel}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
final class SunburstListContainer implements Comparator<SunburstListContainer>,
		Comparable<SunburstListContainer> {
	/** Maximum depth. */
	final int mMaxDepth;

	/** Maximum depth of older revision. */
	final int mOldMaxDepth;

	/** {@link List} of {@link SunburstItem}s. */
	final List<SunburstItem> mItems;

	/** Revision of the item set. */
	final long mRevision;

	/**
	 * Constructor.
	 * 
	 * @param pMaxDepth
	 *          maximum depth
	 * @param pOldMaxDepth
	 *          maximum depth of older revision
	 * @param pItems
	 *          {@link List} of {@link SunburstItem}s
	 * @param pRevision
	 *          current revision
	 */
	public SunburstListContainer(final int pMaxDepth, final int pOldMaxDepth,
			final List<SunburstItem> pItems, final long pRevision) {
		checkArgument(pMaxDepth >= 0);
		checkArgument(pOldMaxDepth >= 0);
		checkArgument(pRevision >= 0);
		mMaxDepth = pMaxDepth;
		mOldMaxDepth = pOldMaxDepth;
		mItems = checkNotNull(pItems);
		mRevision = pRevision;
	}

	@Override
	public int compare(final SunburstListContainer pFirst,
			final SunburstListContainer pSecond) {
		final long firstRev = pFirst.mRevision;
		final long secondRev = pSecond.mRevision;
		return firstRev > secondRev ? 1 : firstRev == secondRev ? 0 : -1;
	}

	@Override
	public int compareTo(final SunburstListContainer pOther) {
		final long firstRev = mRevision;
		final long secondRev = pOther.mRevision;
		return firstRev > secondRev ? 1 : firstRev == secondRev ? 0 : -1;
	}
}
