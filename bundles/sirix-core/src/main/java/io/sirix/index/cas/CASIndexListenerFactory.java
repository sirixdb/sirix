package io.sirix.index.cas;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.index.path.summary.PathSummaryReader;

public final class CASIndexListenerFactory {

	private final DatabaseType databaseType;

	public CASIndexListenerFactory(final DatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	public CASIndexListener create(final PageTrx pageTrx, final PathSummaryReader pathSummaryReader,
			final IndexDef indexDef) {
		final var pathSummary = requireNonNull(pathSummaryReader);
		final var avlTreeWriter = RBTreeWriter.<CASValue, NodeReferences>getInstance(this.databaseType, pageTrx,
				indexDef.getType(), indexDef.getID());
		final var type = requireNonNull(indexDef.getContentType());
		final var paths = requireNonNull(indexDef.getPaths());

		return new CASIndexListener(pathSummary, avlTreeWriter, paths, type);
	}
}
