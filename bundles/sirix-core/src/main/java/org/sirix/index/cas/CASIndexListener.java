package org.sirix.index.cas;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Optional;
import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.xdm.Type;
import org.sirix.access.trx.node.xdm.IndexController.ChangeType;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.index.AtomicUtil;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexDef;
import org.sirix.index.SearchMode;
import org.sirix.index.avltree.AVLTreeReader.MoveCursor;
import org.sirix.index.avltree.AVLTreeWriter;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;

public final class CASIndexListener implements ChangeListener {

  private final AVLTreeWriter<CASValue, NodeReferences> mAVLTreeWriter;
  private final PathSummaryReader mPathSummaryReader;
  private final Set<Path<QNm>> mPaths;
  private final Type mType;

  public CASIndexListener(final PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    mAVLTreeWriter = AVLTreeWriter.getInstance(pageWriteTrx, indexDef.getType(), indexDef.getID());
    mPathSummaryReader = checkNotNull(pathSummaryReader);
    mPaths = checkNotNull(indexDef.getPaths());
    mType = checkNotNull(indexDef.getContentType());
  }

  @Override
  public void listen(final ChangeType type, final ImmutableNode node, final long pathNodeKey)
      throws SirixIOException {
    if (node instanceof ValueNode) {
      final ValueNode valueNode = ((ValueNode) node);
      mPathSummaryReader.moveTo(pathNodeKey);
      try {
        switch (type) {
          case INSERT:
            if (mPathSummaryReader.getPCRsForPaths(mPaths, false).contains(pathNodeKey)) {
              insert(valueNode, pathNodeKey);
            }
            break;
          case DELETE:
            if (mPathSummaryReader.getPCRsForPaths(mPaths, false).contains(pathNodeKey)) {
              mAVLTreeWriter.remove(
                  new CASValue(new Str(valueNode.getValue()), mType, pathNodeKey),
                  node.getNodeKey());
            }
            break;
          default:
        }
      } catch (final PathException e) {
        throw new SirixIOException(e);
      }
    }
  }

  private void insert(final ValueNode node, final long pathNodeKey) throws SirixIOException {
    final Str strValue = new Str(node.getValue());

    boolean isOfType = false;
    try {
      AtomicUtil.toType(strValue, mType);
      isOfType = true;
    } catch (final SirixRuntimeException e) {
    }

    if (isOfType) {
      final CASValue indexValue = new CASValue(strValue, mType, pathNodeKey);
      final Optional<NodeReferences> textReferences =
          mAVLTreeWriter.get(indexValue, SearchMode.EQUAL);
      if (textReferences.isPresent()) {
        setNodeReferences(node, textReferences.get(), indexValue);
      } else {
        setNodeReferences(node, new NodeReferences(), indexValue);
      }
    }
  }

  private void setNodeReferences(final Node node, final NodeReferences references,
      final CASValue indexValue) throws SirixIOException {
    mAVLTreeWriter.index(indexValue, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
