package io.sirix.index.name;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.brackit.query.atomic.QNm;

public final class NameIndexListenerFactory {

	private final DatabaseType databaseType;

	public NameIndexListenerFactory(final DatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	public NameIndexListener create(final PageTrx pageWriteTrx, final IndexDef indexDefinition) {
		final var includes = requireNonNull(indexDefinition.getIncluded());
		final var excludes = requireNonNull(indexDefinition.getExcluded());
		assert indexDefinition.getType() == IndexType.NAME;
		final var avlTreeWriter = RBTreeWriter.<QNm, NodeReferences>getInstance(this.databaseType, pageWriteTrx,
				indexDefinition.getType(), indexDefinition.getID());

		return new NameIndexListener(includes, excludes, avlTreeWriter);
	}
}
