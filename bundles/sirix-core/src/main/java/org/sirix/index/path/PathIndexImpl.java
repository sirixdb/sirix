package org.sirix.index.path;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.Filter;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexFilterAxis;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLNode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

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
	public Iterator<NodeReferences> openIndex(final PageReadTrx pageRtx, final IndexDef indexDef,
			final PathFilter filter) {
		final AVLTreeReader<Long, NodeReferences> reader =
				AVLTreeReader.getInstance(pageRtx, indexDef.getType(), indexDef.getID());

		if (filter.getPCRs().size() == 1) {
			final Optional<NodeReferences> optionalNodeReferences =
					reader.get(filter.getPCRs().iterator().next(), SearchMode.EQUAL);
			return Iterators.forArray(optionalNodeReferences.orElse(new NodeReferences()));
		} else {
			final Iterator<AVLNode<Long, NodeReferences>> iter =
					reader.new AVLNodeIterator(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
			final Set<Filter> setFilter = filter == null ? ImmutableSet.of() : ImmutableSet.of(filter);

			return new IndexFilterAxis<Long>(iter, setFilter);
		}
	}

}
