package org.sirix.index.path;

import java.util.Optional;
import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.exception.SirixIOException;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.immutable.ImmutableNode;

public final class PathIndexListener {

  private final RBTreeWriter<Long, NodeReferences> avlTreeWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;

  public PathIndexListener(final Set<Path<QNm>> paths, final PathSummaryReader pathSummaryReader,
      final RBTreeWriter<Long, NodeReferences> avlTreeWriter) {
    this.avlTreeWriter = avlTreeWriter;
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
  }

  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    pathSummaryReader.moveTo(pathNodeKey);
    try {
      switch (type) {
        case INSERT:
          if (pathSummaryReader.getPCRsForPaths(paths, false).contains(pathNodeKey)) {
            final Optional<NodeReferences> textReferences = avlTreeWriter.get(pathNodeKey, SearchMode.EQUAL);
            if (textReferences.isPresent()) {
              setNodeReferences(node, textReferences.get(), pathNodeKey);
            } else {
              setNodeReferences(node, new NodeReferences(), pathNodeKey);
            }
          }
          break;
        case DELETE:
          if (pathSummaryReader.getPCRsForPaths(paths, false).contains(pathNodeKey)) {
            avlTreeWriter.remove(pathNodeKey, node.getNodeKey());
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
    avlTreeWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
