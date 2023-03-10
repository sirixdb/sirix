package org.sirix.gui.view.sunburst;

import static java.util.Objects.requireNonNull;

import java.util.List;


public final class ListContainer {

	final int mMaxDepth;
	final List<SunburstItem> mItems;

	public ListContainer(final int pMaxDepth, final List<SunburstItem> pItems) {
		mMaxDepth = pMaxDepth;
		mItems = requireNonNull(pItems);
	}
}
