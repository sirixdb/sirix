package org.sirix.index.name;

import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.immutable.ImmutableNode;

public final class NameIndexListener {

  private final Set<QNm> mIncludes;
  private final Set<QNm> mExcludes;
  private final AVLTreeWriter<QNm, NodeReferences> mAVLTreeWriter;

  public NameIndexListener(final Set<QNm> includes, final Set<QNm> excludes,
      final AVLTreeWriter<QNm, NodeReferences> avlTreeWriter) {
    mIncludes = includes;
    mExcludes = excludes;
    mAVLTreeWriter = avlTreeWriter;
  }

  public void listen(ChangeType type, @Nonnull ImmutableNode node, QNm name) {
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
