package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.keyvalue.NodeReferences;

import com.google.common.collect.AbstractIterator;

public final class PathFilterAxis extends AbstractIterator<NodeReferences> {

	private final Iterator<AVLNode<Long, NodeReferences>> mIter;
	private final PathFilter mFilter;

	public PathFilterAxis(final Iterator<AVLNode<Long, NodeReferences>> iter, final PathFilter filter) {
		mIter = checkNotNull(iter);
		mFilter = checkNotNull(filter);
	}

	@Override
	protected NodeReferences computeNext() {
		while (mIter.hasNext()) {
			final AVLNode<Long, NodeReferences> node = mIter.next();
			if (mFilter.filter(node)) {
				return node.getValue();
			}
		}
		return endOfData();
	}
}
