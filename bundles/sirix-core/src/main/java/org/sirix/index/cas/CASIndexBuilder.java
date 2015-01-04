package org.sirix.index.cas;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.AbstractVisitor;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.AtomicUtil;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.Kind;
import org.sirix.node.immutable.ImmutableAttribute;
import org.sirix.node.immutable.ImmutableText;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Builds a content-and-structure (CAS) index.
 * 
 * @author Johannes Lichtenberger
 *
 */
final class CASIndexBuilder extends AbstractVisitor {

	private static final LogWrapper LOGGER = new LogWrapper(
			LoggerFactory.getLogger(CASIndexBuilder.class));

	private final NodeReadTrx mRtx;
	private final Set<Path<QNm>> mPaths;
	private final PathSummaryReader mPathSummaryReader;
	private final AVLTreeWriter<CASValue, NodeReferences> mAVLTreeWriter;
	private final Type mType;

	CASIndexBuilder(final NodeReadTrx rtx,
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final PathSummaryReader pathSummaryReader, final IndexDef indexDefinition) {
		mRtx = checkNotNull(rtx);
		mPathSummaryReader = checkNotNull(pathSummaryReader);
		mPaths = checkNotNull(indexDefinition.getPaths());
		mAVLTreeWriter = AVLTreeWriter.getInstance(pageWriteTrx,
				indexDefinition.getType(), indexDefinition.getID());
		mType = checkNotNull(indexDefinition.getContentType());
	}

	@Override
	public VisitResult visit(ImmutableText node) {
		return process(node);
	}

	@Override
	public VisitResult visit(ImmutableAttribute node) {
		return process(node);
	}

	private VisitResult process(final ImmutableNode node) {
		try {
			if (node.getKind() == Kind.TEXT) {
				mRtx.moveTo(node.getParentKey());
			}
			final long PCR = mRtx.isDocumentRoot() ? 0 : mRtx.getNameNode()
					.getPathNodeKey();
			if (mPaths.isEmpty()
					|| mPathSummaryReader.getPCRsForPaths(mPaths).contains(PCR)) {
				final Str strValue = new Str(((ImmutableValueNode) node).getValue());

				boolean isOfType = false;
				try {
					if (mType != Type.STR)
						AtomicUtil.toType(strValue, mType);
					isOfType = true;
				} catch (final SirixRuntimeException e) {
				}

				if (isOfType) {
					final CASValue value = new CASValue(strValue, mType, PCR);
					final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(
							value, SearchMode.EQUAL);
					if (textReferences.isPresent()) {
						setNodeReferences(node, textReferences.get(), value);
					} else {
						setNodeReferences(node, new NodeReferences(), value);
					}
				}
			}
			mRtx.moveTo(node.getNodeKey());
		} catch (final PathException | SirixIOException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return VisitResultType.CONTINUE;
	}

	private void setNodeReferences(final ImmutableNode node,
			final NodeReferences references, final CASValue value)
			throws SirixIOException {
		mAVLTreeWriter.index(value, references.addNodeKey(node.getNodeKey()),
				MoveCursor.NO_MOVE);
	}

}
