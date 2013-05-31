package org.sirix.index.path;

import java.util.Iterator;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

import com.google.common.base.Optional;

public final class PathIndexImpl<K extends Comparable<? super K>, V extends References>
		implements PathIndex<K, V> {

	@Override
	public PathIndexBuilder createBuilder(
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final PathSummaryReader pathSummaryReader,
			final IndexDef indexDef) {
		return new PathIndexBuilder(pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public PathIndexListener createListener(
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final PathSummaryReader pathSummaryReader,
			final IndexDef indexDef) {
		return new PathIndexListener(pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public Iterator<Optional<V>> openIndex(
			final PageReadTrx pageReadTrx, final K key,
			final IndexDef indexDef, final SearchMode mode) {
		final AVLTreeReader<K, V> reader = AVLTreeReader.getInstance(pageReadTrx,
				indexDef.getType(), indexDef.getID());
		return reader.new AVLIterator(key, mode);
	}

}
