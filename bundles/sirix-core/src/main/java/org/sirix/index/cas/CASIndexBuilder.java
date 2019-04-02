package org.sirix.index.cas;

import java.util.Optional;
import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.brackit.xquery.xdm.Type;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
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
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

public final class CASIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CASIndexBuilder.class));

  private final AVLTreeWriter<CASValue, NodeReferences> mAVLTreeWriter;

  private final PathSummaryReader mPathSummaryReader;

  private final Set<Path<QNm>> mPaths;

  private final Type mType;

  public CASIndexBuilder(final AVLTreeWriter<CASValue, NodeReferences> avlTreeWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths, final Type type) {
    mPathSummaryReader = pathSummaryReader;
    mPaths = paths;
    mAVLTreeWriter = avlTreeWriter;
    mType = type;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      if (mPaths.isEmpty() || mPathSummaryReader.getPCRsForPaths(mPaths, true).contains(pathNodeKey)) {
        final Str strValue = new Str(((ImmutableValueNode) node).getValue());

        boolean isOfType = false;
        try {
          if (mType != Type.STR)
            AtomicUtil.toType(strValue, mType);
          isOfType = true;
        } catch (final SirixRuntimeException e) {
        }

        if (isOfType) {
          final CASValue value = new CASValue(strValue, mType, pathNodeKey);
          final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(value, SearchMode.EQUAL);
          if (textReferences.isPresent()) {
            setNodeReferences(node, textReferences.get(), value);
          } else {
            setNodeReferences(node, new NodeReferences(), value);
          }
        }
      }
    } catch (final PathException | SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final CASValue value)
      throws SirixIOException {
    mAVLTreeWriter.index(value, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
