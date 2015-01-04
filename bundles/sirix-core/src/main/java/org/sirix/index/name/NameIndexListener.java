package org.sirix.index.name;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.IndexController.ChangeType;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;

final class NameIndexListener implements ChangeListener {

	private final Set<QNm> mIncludes;
	private final Set<QNm> mExcludes;
	private final AVLTreeWriter<QNm, NodeReferences> mAVLTreeWriter;

	public NameIndexListener(
			final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final IndexDef indexDefinition) {
		mIncludes = checkNotNull(indexDefinition.getIncluded());
		mExcludes = checkNotNull(indexDefinition.getExcluded());
		assert indexDefinition.getType() == IndexType.NAME;
		mAVLTreeWriter = AVLTreeWriter.getInstance(pageWriteTrx,
				indexDefinition.getType(), indexDefinition.getID());
	}

	@Override
	public void listen(ChangeType type, @Nonnull ImmutableNode node,
			long pathNodeKey) throws SirixIOException {
		if (node instanceof NameNode) {
			final NameNode nameNode = (NameNode) node;
			final QNm name = nameNode.getName();
			final boolean included = (mIncludes.isEmpty() || mIncludes.contains(name));
			final boolean excluded = (!mExcludes.isEmpty() && mExcludes
					.contains(name));

			if (!included || excluded) {
				return;
			}

			switch (type) {
			case INSERT:
				final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(
						name, SearchMode.EQUAL);
				if (textReferences.isPresent()) {
					setNodeReferences(node, textReferences.get(), name);
				} else {
					setNodeReferences(node, new NodeReferences(), name);
				}
				break;
			case DELETE:
				mAVLTreeWriter.remove(name, node.getNodeKey());
				break;
			default:
			}
		}
	}

	private void setNodeReferences(final ImmutableNode node,
			final NodeReferences references, final QNm name) throws SirixIOException {
		mAVLTreeWriter.index(name, references.addNodeKey(node.getNodeKey()),
				MoveCursor.NO_MOVE);
	}

}
