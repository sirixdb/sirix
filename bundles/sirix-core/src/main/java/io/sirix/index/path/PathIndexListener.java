package io.sirix.index.path;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.SearchMode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Optional;
import java.util.Set;

public final class PathIndexListener {

	private final RBTreeWriter<Long, NodeReferences> indexWriter;
	private final PathSummaryReader pathSummaryReader;
	private final Set<Path<QNm>> paths;

	public PathIndexListener(final Set<Path<QNm>> paths, final PathSummaryReader pathSummaryReader,
			final RBTreeWriter<Long, NodeReferences> indexWriter) {
		this.indexWriter = indexWriter;
		this.pathSummaryReader = pathSummaryReader;
		this.paths = paths;
	}

	public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
		pathSummaryReader.moveTo(pathNodeKey);
		try {
			switch (type) {
				case INSERT :
					if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
						final Optional<NodeReferences> textReferences = indexWriter.get(pathNodeKey, SearchMode.EQUAL);
						if (textReferences.isPresent()) {
							setNodeReferences(node, textReferences.get(), pathNodeKey);
						} else {
							setNodeReferences(node, new NodeReferences(), pathNodeKey);
						}
					}
					break;
				case DELETE :
					if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
						indexWriter.remove(pathNodeKey, node.getNodeKey());
					}
					break;
				default :
			}
		} catch (final PathException e) {
			throw new SirixIOException(e);
		}
	}

	private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final long pathNodeKey)
			throws SirixIOException {
		indexWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
	}
}
