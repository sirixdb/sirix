package org.sirix.index.cas;

import java.util.Iterator;

import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

public final class CASIndexImpl<K extends Comparable<? super K>, V extends References> implements CASIndex<K, V> {

	@Override
	public CASIndexBuilder createBuilder(NodeReadTrx rtx,
			@Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			@Nonnull PathSummaryReader pathSummaryReader, @Nonnull IndexDef indexDef) {
		return new CASIndexBuilder(rtx, pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public CASIndexListener createListener(
			@Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			@Nonnull PathSummaryReader pathSummaryReader, @Nonnull IndexDef indexDef) {
		return new CASIndexListener(pageWriteTrx, pathSummaryReader, indexDef);
	}

	@Override
	public Iterator<V> openIndex(PageReadTrx pageReadTrx,
			@Nonnull K key, @Nonnull IndexDef indexDef, @Nonnull SearchMode mode) {
		final AVLTreeReader<K, V> reader = AVLTreeReader.getInstance(pageReadTrx, indexDef.getType(), indexDef.getID());
		return reader.new AVLIterator(key, mode);
	}
}
