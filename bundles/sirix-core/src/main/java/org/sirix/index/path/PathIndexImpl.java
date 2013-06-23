package org.sirix.index.path;

import java.util.Iterator;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

import com.google.common.base.Optional;

public final class PathIndexImpl implements PathIndex<Long, NodeReferences> {

	@Override
	public PathIndexBuilder createBuilder(
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
		return new PathIndexBuilder(pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public PathIndexListener createListener(
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
		return new PathIndexListener(pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public Iterator<NodeReferences> openIndex(final PageReadTrx pageRtx, final Optional<Long> key,
			final IndexDef indexDef, final SearchMode mode, final PathFilter filter) {
		final AVLTreeReader<Long, NodeReferences> reader = AVLTreeReader
				.getInstance(pageRtx, indexDef.getType(), indexDef.getID());

		final Iterator<AVLNode<Long, NodeReferences>> iter = reader.new AVLNodeIterator(
				key, mode);

		return new PathFilterAxis(iter, filter);
	}

}
