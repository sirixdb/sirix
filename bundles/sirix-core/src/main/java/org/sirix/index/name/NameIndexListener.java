package org.sirix.index.name;

import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Optional;
import java.util.Set;

public final class NameIndexListener {

  private final Set<QNm> mIncludes;
  private final Set<QNm> mExcludes;
  private final RBTreeWriter<QNm, NodeReferences> mAVLTreeWriter;

  public NameIndexListener(final Set<QNm> includes, final Set<QNm> excludes,
      final RBTreeWriter<QNm, NodeReferences> avlTreeWriter) {
    mIncludes = includes;
    mExcludes = excludes;
    mAVLTreeWriter = avlTreeWriter;
  }

  public void listen(ChangeType type, @NonNull ImmutableNode node, QNm name) {
    final boolean included = (mIncludes.isEmpty() || mIncludes.contains(name));
    final boolean excluded = (!mExcludes.isEmpty() && mExcludes.contains(name));

    if (!included || excluded) {
      return;
    }

    switch (type) {
      case INSERT:
        final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(name, SearchMode.EQUAL);
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

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final QNm name) {
    mAVLTreeWriter.index(name, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }

}
