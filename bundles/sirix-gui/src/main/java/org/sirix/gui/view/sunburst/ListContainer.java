package org.sirix.gui.view.sunburst;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

public final class ListContainer {

	final int mMaxDepth;
	final List<SunburstItem> mItems;

	public ListContainer(final int pMaxDepth, final List<SunburstItem> pItems) {
		mMaxDepth = pMaxDepth;
		mItems = checkNotNull(pItems);
	}
}
