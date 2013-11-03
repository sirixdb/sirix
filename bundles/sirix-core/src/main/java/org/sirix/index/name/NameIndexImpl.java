package org.sirix.index.name;

import java.util.Iterator;
import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.Filter;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexFilterAxis;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;

import com.google.common.collect.ImmutableSet;

public final class NameIndexImpl implements NameIndex<QNm, NodeReferences> {

	@Override
	public NameIndexBuilder createBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final IndexDef indexDef) {
		return new NameIndexBuilder(pageWriteTrx, indexDef);
	}

	@Override
	public NameIndexListener createListener(
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final IndexDef indexDef) {
		return new NameIndexListener(pageWriteTrx, indexDef);
	}

	@Override
	public Iterator<NodeReferences> openIndex(PageReadTrx pageRtx,
			IndexDef indexDef, NameFilter filter) {
		final AVLTreeReader<QNm, NodeReferences> reader = AVLTreeReader
				.getInstance(pageRtx, indexDef.getType(), indexDef.getID());

		final Iterator<AVLNode<QNm, NodeReferences>> iter = reader.new AVLNodeIterator(
				Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
		final Set<Filter> setFilter = filter == null ? ImmutableSet.<Filter> of()
				: ImmutableSet.<Filter> of(filter);

		return new IndexFilterAxis<QNm>(iter, setFilter);
	}
}
