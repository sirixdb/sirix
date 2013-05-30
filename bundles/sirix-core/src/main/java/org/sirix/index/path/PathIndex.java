package org.sirix.index.path;

import java.util.Iterator;

import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.interfaces.References;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.page.UnorderedKeyValuePage;

import com.google.common.base.Optional;

public interface PathIndex<K extends Comparable<? super K>, V extends References> {
	PathIndexBuilder createBuilder(
			@Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			@Nonnull PathSummaryReader pathSummaryReader, @Nonnull IndexDef indexDef);

	PathIndexListener createListener(
			@Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			@Nonnull PathSummaryReader pathSummaryReader, @Nonnull IndexDef indexDef);

	Iterator<Optional<V>> openIndex(@Nonnull PageReadTrx pageReadTrx,
			@Nonnull K key, @Nonnull IndexDef indexDef, @Nonnull SearchMode mode);
}
