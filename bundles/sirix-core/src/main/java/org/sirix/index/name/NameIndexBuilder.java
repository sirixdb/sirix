package org.sirix.index.name;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import java.util.Set;

import org.brackit.xquery.atomic.QNm;
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
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

final class NameIndexBuilder extends AbstractVisitor {

	private static final LogWrapper LOGGER =
			new LogWrapper(LoggerFactory.getLogger(NameIndexBuilder.class));

	private final Set<QNm> mIncludes;
	private final Set<QNm> mExcludes;
	private final AVLTreeWriter<QNm, NodeReferences> mAVLTreeWriter;

	public NameIndexBuilder(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final IndexDef indexDefinition) {
		mIncludes = checkNotNull(indexDefinition.getIncluded());
		mExcludes = checkNotNull(indexDefinition.getExcluded());
		assert indexDefinition.getType() == IndexType.NAME;
		mAVLTreeWriter =
				AVLTreeWriter.getInstance(pageWriteTrx, indexDefinition.getType(), indexDefinition.getID());
	}

	@Override
	public VisitResult visit(final ImmutableElement node) {
		final QNm name = node.getName();
		final boolean included = (mIncludes.isEmpty() || mIncludes.contains(name));
		final boolean excluded = (!mExcludes.isEmpty() && mExcludes.contains(name));

		if (!included || excluded) {
			return VisitResultType.CONTINUE;
		}

		final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(name, SearchMode.EQUAL);

		try {
			if (textReferences.isPresent()) {
				setNodeReferences(node, textReferences.get(), name);
			} else {
				setNodeReferences(node, new NodeReferences(), name);
			}
		} catch (final SirixIOException e) {
			LOGGER.error(e.getMessage(), e);
		}

		return VisitResultType.CONTINUE;
	}

	private void setNodeReferences(final ImmutableNode node, final NodeReferences references,
			final QNm name) throws SirixIOException {
		mAVLTreeWriter.index(name, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
	}

}
