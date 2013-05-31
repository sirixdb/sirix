package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.access.IndexController.ChangeType;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;

import com.google.common.base.Optional;

final class PathIndexListener implements ChangeListener {

	private final AVLTreeWriter<Long, NodeReferences> mAVLTreeWriter;
	private final PathSummaryReader mPathSummaryReader;
	private final Set<Path<QNm>> mPaths;

	PathIndexListener(
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
		mAVLTreeWriter = AVLTreeWriter.getInstance(pageWriteTrx, indexDef.getType(),
				indexDef.getID());
		mPathSummaryReader = checkNotNull(pathSummaryReader);
		mPaths = checkNotNull(indexDef.getPaths());
	}

	@Override
	public void listen(final ChangeType type, final ImmutableNode node,
			final long pathNodeKey) throws SirixIOException {
		if (node instanceof NameNode) {
			mPathSummaryReader.moveTo(pathNodeKey);
			try {
				switch (type) {
				case INSERT:
					if (mPathSummaryReader.getPCRsForPaths(mPaths).contains(pathNodeKey)) {
						final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(
								pathNodeKey, SearchMode.EQUAL);
						if (textReferences.isPresent()) {
							setNodeReferences(node, textReferences.get(), pathNodeKey);
						} else {
							setNodeReferences(node, new NodeReferences(),
									pathNodeKey);
						}
					}
					break;
				case DELETE:
					if (mPathSummaryReader.getPCRsForPaths(mPaths).contains(pathNodeKey)) {
						mAVLTreeWriter.remove(pathNodeKey, node.getNodeKey());
					}
					break;
				default:
				}
			} catch (final PathException e) {
				throw new SirixIOException(e);
			}
		}
	}

	private void setNodeReferences(final ImmutableNode node,
			final NodeReferences references, final long pathNodeKey)
			throws SirixIOException {
		mAVLTreeWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()),
				MoveCursor.NO_MOVE);
	}
}
