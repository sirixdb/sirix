package org.sirix.index.cas;

import java.util.Iterator;

import org.brackit.xquery.atomic.Atomic;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexFilterAxis;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;

import com.google.common.collect.ImmutableSet;

public final class CASIndexImpl implements CASIndex<CASValue, NodeReferences> {

	@Override
	public CASIndexBuilder createBuilder(NodeReadTrx rtx,
			PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			PathSummaryReader pathSummaryReader, IndexDef indexDef) {
		return new CASIndexBuilder(rtx, pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public CASIndexListener createListener(
			PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			PathSummaryReader pathSummaryReader, IndexDef indexDef) {
		return new CASIndexListener(pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public Iterator<NodeReferences> openIndex(PageReadTrx pageReadTrx,
			IndexDef indexDef, SearchMode mode, CASFilterRange filter, Atomic low,
			Atomic high, boolean incLow, boolean incMax) {
		final AVLTreeReader<CASValue, NodeReferences> reader = AVLTreeReader
				.getInstance(pageReadTrx, indexDef.getType(), indexDef.getID());

		final Iterator<AVLNode<CASValue, NodeReferences>> iter = reader.new AVLNodeIterator(
				Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

		return null;
	}

	@Override
	public Iterator<NodeReferences> openIndex(PageReadTrx pageReadTrx,
			IndexDef indexDef, SearchMode mode, CASFilter filter, Atomic key,
			boolean incSelf) {
		final AVLTreeReader<CASValue, NodeReferences> reader = AVLTreeReader
				.getInstance(pageReadTrx, indexDef.getType(), indexDef.getID());

		final Iterator<AVLNode<CASValue, NodeReferences>> iter = reader.new AVLNodeIterator(
				Fixed.DOCUMENT_NODE_KEY.getStandardProperty());

		return new IndexFilterAxis<CASValue>(iter, ImmutableSet.of(filter));
	}
}
