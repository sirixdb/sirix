package io.sirix.index.path;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.index.IndexDef;
import io.sirix.api.PageTrx;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;

public final class PathIndexListenerFactory {

	private final DatabaseType databaseType;

	public PathIndexListenerFactory(final DatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	public PathIndexListener create(final PageTrx pageTrx, final PathSummaryReader pathSummaryReader,
			final IndexDef indexDef) {
		final var pathSummary = requireNonNull(pathSummaryReader);
		final var paths = requireNonNull(indexDef.getPaths());
		final var avlTreeWriter = RBTreeWriter.<Long, NodeReferences>getInstance(this.databaseType, pageTrx,
				indexDef.getType(), indexDef.getID());

		return new PathIndexListener(paths, pathSummary, avlTreeWriter);
	}
}
