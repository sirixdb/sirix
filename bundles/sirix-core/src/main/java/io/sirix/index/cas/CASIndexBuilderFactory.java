package io.sirix.index.cas;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.index.path.summary.PathSummaryReader;

public final class CASIndexBuilderFactory {

	private final DatabaseType databaseType;

	public CASIndexBuilderFactory(final DatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	public CASIndexBuilder create(final PageTrx pageTrx, final PathSummaryReader pathSummaryReader,
			final IndexDef indexDef) {
		final var rbTreeWriter = RBTreeWriter.<CASValue, NodeReferences>getInstance(this.databaseType, pageTrx,
				indexDef.getType(), indexDef.getID());
		final var pathSummary = requireNonNull(pathSummaryReader);
		final var paths = requireNonNull(indexDef.getPaths());
		final var type = requireNonNull(indexDef.getContentType());

		return new CASIndexBuilder(rbTreeWriter, pathSummary, paths, type);
	}
}
