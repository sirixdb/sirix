package io.sirix.index.cas;

import io.sirix.access.trx.node.IndexController;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.AtomicUtil;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.Optional;
import java.util.Set;

public final class CASIndexListener {

	private final RBTreeWriter<CASValue, NodeReferences> indexWriter;
	private final PathSummaryReader pathSummaryReader;
	private final Set<Path<QNm>> paths;
	private final Type type;

	public CASIndexListener(final PathSummaryReader pathSummaryReader,
			final RBTreeWriter<CASValue, NodeReferences> indexWriter, final Set<Path<QNm>> paths, final Type type) {
		this.pathSummaryReader = pathSummaryReader;
		this.indexWriter = indexWriter;
		this.paths = paths;
		this.type = type;
	}

	public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey,
			final Str value) {
		var hasMoved = pathSummaryReader.moveTo(pathNodeKey);
		assert hasMoved;
		switch (type) {
			case INSERT -> {
				if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
					insert(node, pathNodeKey, value);
				}
			}
			case DELETE -> {
				if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
					indexWriter.remove(new CASValue(value, this.type, pathNodeKey), node.getNodeKey());
				}
			}
			default -> {
			}
		}
	}

	private void insert(final ImmutableNode node, final long pathNodeKey, final Str value) throws SirixIOException {
		boolean isOfType = false;
		try {
			AtomicUtil.toType(value, type);
			isOfType = true;
		} catch (final SirixRuntimeException ignored) {
		}

		if (isOfType) {
			final CASValue indexValue = new CASValue(value, type, pathNodeKey);
			final Optional<NodeReferences> textReferences = indexWriter.get(indexValue, SearchMode.EQUAL);
			if (textReferences.isPresent()) {
				setNodeReferences(node, new NodeReferences(textReferences.get().getNodeKeys()), indexValue);
			} else {
				setNodeReferences(node, new NodeReferences(), indexValue);
			}
		}
	}

	private void setNodeReferences(final ImmutableNode node, final NodeReferences references,
			final CASValue indexValue) {
		indexWriter.index(indexValue, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
	}
}
