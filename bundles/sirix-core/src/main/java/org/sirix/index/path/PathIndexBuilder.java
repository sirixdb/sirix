package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.access.AbstractVisitor;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.immutable.ImmutableAttribute;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

final class PathIndexBuilder extends AbstractVisitor {

	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(PathIndexBuilder.class));

	private final Set<Path<QNm>> mPaths;
	private final PathSummaryReader mPathSummaryReader;

	private final AVLTreeWriter<Long, NodeReferences> mAVLTreeWriter;

	PathIndexBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
		mPathSummaryReader = checkNotNull(pathSummaryReader);
		mPaths = checkNotNull(indexDef.getPaths());
		assert indexDef.getType() == IndexType.PATH;
		mAVLTreeWriter = AVLTreeWriter.getInstance(pageWriteTrx, indexDef.getType(),
				indexDef.getID());
	}

	@Override
	public VisitResult visit(ImmutableElement node) {
		return process(node);
	}

	@Override
	public VisitResult visit(ImmutableAttribute node) {
		return process(node);
	}

	private VisitResult process(final ImmutableNameNode node) {
		try {
			final long PCR = node.getPathNodeKey();
			if (mPathSummaryReader.getPCRsForPaths(mPaths).contains(PCR) || mPaths.isEmpty()) {
				final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(PCR,
						SearchMode.EQUAL);
				if (textReferences.isPresent()) {
					setNodeReferences(node, textReferences.get(), PCR);
				} else {
					setNodeReferences(node, new NodeReferences(), PCR);
				}
			}
		} catch (final PathException | SirixIOException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return VisitResultType.CONTINUE;

	}

	private void setNodeReferences(final ImmutableNode node,
			final NodeReferences references, final long pathNodeKey)
			throws SirixIOException {
		mAVLTreeWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()),
				MoveCursor.NO_MOVE);
	}

}
