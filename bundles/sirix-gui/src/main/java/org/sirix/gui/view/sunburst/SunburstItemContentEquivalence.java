package org.sirix.gui.view.sunburst;

import com.google.common.base.Equivalence;

/**
 * Content equivalence based on keys and on angles.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SunburstItemContentEquivalence extends Equivalence<SunburstItem> {
	/**
	 * Instance of {@link SunburstItemKeyEquivalence} to use as a
	 * delegate/composition.
	 */
	private final SunburstItemKeyEquivalence mDelegate = new SunburstItemKeyEquivalence();

	@Override
	public boolean doEquivalent(final SunburstItem pFirst,
			final SunburstItem pSecond) {
		boolean retVal = mDelegate.doEquivalent(pFirst, pSecond);
		retVal = retVal && pFirst.getAngleStart() == pSecond.getAngleStart()
				&& pFirst.getAngleEnd() == pSecond.getAngleEnd();
		return retVal;
	}

	@Override
	public int doHash(final SunburstItem pItem) {
		int result = mDelegate.doHash(pItem);
		result = 31 * result + (int) pItem.getAngleStart();
		result = 31 * result + (int) pItem.getAngleEnd();
		return result;
	}
}
