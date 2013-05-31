package org.sirix.index;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.visitor.Visitor;
import org.sirix.axis.DescendantAxis;

public class IndexBuilder {

	private final NodeReadTrx mRtx;
	private final Set<Visitor> mBuilders;

	public IndexBuilder(final NodeReadTrx rtx,
			final Set<Visitor> builders) {
		mRtx = checkNotNull(rtx);
		mBuilders = checkNotNull(builders);
	}

	public void build() {
		final long nodeKey = mRtx.getNodeKey();
		mRtx.moveToDocumentRoot();

		for (@SuppressWarnings("unused")
		final long key : new DescendantAxis(mRtx)) {
			for (final @Nonnull
			Visitor builder : mBuilders) {
				mRtx.acceptVisitor(builder);
			}
		}
		mRtx.moveTo(nodeKey);
	}

}
