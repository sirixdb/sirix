package org.sirix.index.cas;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.AtomicUtil;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Optional;
import java.util.Set;

public final class CASIndexListener {

  private final AVLTreeWriter<CASValue, NodeReferences> mAVLTreeWriter;
  private final PathSummaryReader mPathSummaryReader;
  private final Set<Path<QNm>> mPaths;
  private final Type mType;

  public CASIndexListener(final PathSummaryReader pathSummaryReader,
      final AVLTreeWriter<CASValue, NodeReferences> avlTreeWriter, final Set<Path<QNm>> paths, final Type type) {
    mPathSummaryReader = pathSummaryReader;
    mAVLTreeWriter = avlTreeWriter;
    mPaths = paths;
    mType = type;
  }

  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey, final Str value) {
    mPathSummaryReader.moveTo(pathNodeKey);
    try {
      switch (type) {
        case INSERT:
          if (mPathSummaryReader.getPCRsForPaths(mPaths, false).contains(pathNodeKey)) {
            insert(node, pathNodeKey, value);
          }
          break;
        case DELETE:
          if (mPathSummaryReader.getPCRsForPaths(mPaths, false).contains(pathNodeKey)) {
            mAVLTreeWriter.remove(new CASValue(value, mType, pathNodeKey), node.getNodeKey());
          }
          break;
        default:
      }
    } catch (final PathException e) {
      throw new SirixIOException(e);
    }
  }

  private void insert(final ImmutableNode node, final long pathNodeKey, final Str value) throws SirixIOException {
    boolean isOfType = false;
    try {
      AtomicUtil.toType(value, mType);
      isOfType = true;
    } catch (final SirixRuntimeException e) {
    }

    if (isOfType) {
      final CASValue indexValue = new CASValue(value, mType, pathNodeKey);
      final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(indexValue, SearchMode.EQUAL);
      if (textReferences.isPresent()) {
        setNodeReferences(node, textReferences.get(), indexValue);
      } else {
        setNodeReferences(node, new NodeReferences(), indexValue);
      }
    }
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final CASValue indexValue) {
    mAVLTreeWriter.index(indexValue, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
