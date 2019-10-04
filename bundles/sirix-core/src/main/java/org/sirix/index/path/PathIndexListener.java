package org.sirix.index.path;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.exception.SirixIOException;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Optional;
import java.util.Set;

public final class PathIndexListener {

  private final AVLTreeWriter<Long, NodeReferences> mAVLTreeWriter;
  private final PathSummaryReader mPathSummaryReader;
  private final Set<Path<QNm>> mPaths;

  public PathIndexListener(final Set<Path<QNm>> paths, final PathSummaryReader pathSummaryReader,
      final AVLTreeWriter<Long, NodeReferences> avlTreeWriter) {
    mAVLTreeWriter = avlTreeWriter;
    mPathSummaryReader = pathSummaryReader;
    mPaths = paths;
  }

  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    mPathSummaryReader.moveTo(pathNodeKey);
    try {
      switch (type) {
        case INSERT:
          if (mPathSummaryReader.getPCRsForPaths(mPaths, false).contains(pathNodeKey)) {
            final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(pathNodeKey, SearchMode.EQUAL);
            if (textReferences.isPresent()) {
              setNodeReferences(node, textReferences.get(), pathNodeKey);
            } else {
              setNodeReferences(node, new NodeReferences(), pathNodeKey);
            }
          }
          break;
        case DELETE:
          if (mPathSummaryReader.getPCRsForPaths(mPaths, false).contains(pathNodeKey)) {
            mAVLTreeWriter.remove(pathNodeKey, node.getNodeKey());
          }
          break;
        default:
      }
    } catch (final PathException e) {
      throw new SirixIOException(e);
    }
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final long pathNodeKey)
      throws SirixIOException {
    mAVLTreeWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
