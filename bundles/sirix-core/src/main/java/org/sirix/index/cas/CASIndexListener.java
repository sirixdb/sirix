package org.sirix.index.cas;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.AtomicUtil;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Optional;
import java.util.Set;

public final class CASIndexListener {

  private final RBTreeWriter<CASValue, NodeReferences> redBlackTreeWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private final Type type;

  public CASIndexListener(final PathSummaryReader pathSummaryReader,
      final RBTreeWriter<CASValue, NodeReferences> redBlackTreeWriter, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.redBlackTreeWriter = redBlackTreeWriter;
    this.paths = paths;
    this.type = type;
  }

  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey, final Str value) {
    assert pathSummaryReader.moveTo(pathNodeKey);
    switch (type) {
      case INSERT:
        if (pathSummaryReader.getPCRsForPaths(paths, false).contains(pathNodeKey)) {
          insert(node, pathNodeKey, value);
        }
        break;
      case DELETE:
        if (pathSummaryReader.getPCRsForPaths(paths, false).contains(pathNodeKey)) {
          redBlackTreeWriter.remove(new CASValue(value, this.type, pathNodeKey), node.getNodeKey());
        }
        break;
      default:
    }
  }

  private void insert(final ImmutableNode node, final long pathNodeKey, final Str value) throws SirixIOException {
    boolean isOfType = false;
    try {
      AtomicUtil.toType(value, type);
      isOfType = true;
    } catch (final SirixRuntimeException e) {
    }

    if (isOfType) {
      final CASValue indexValue = new CASValue(value, type, pathNodeKey);
      final Optional<NodeReferences> textReferences = redBlackTreeWriter.get(indexValue, SearchMode.EQUAL);
      if (textReferences.isPresent()) {
        setNodeReferences(node, new NodeReferences(textReferences.get().getNodeKeys()), indexValue);
      } else {
        setNodeReferences(node, new NodeReferences(), indexValue);
      }
    }
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final CASValue indexValue) {
    redBlackTreeWriter.index(indexValue, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
