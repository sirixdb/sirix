package org.sirix.index.cas;

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
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.CASValue;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.node.immutable.json.ImmutableBooleanNode;
import org.sirix.node.immutable.json.ImmutableNumberNode;
import org.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import org.sirix.node.immutable.json.ImmutableObjectNumberNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

public final class CASIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CASIndexBuilder.class));

  private final RBTreeWriter<CASValue, NodeReferences> rbTreeWriter;

  private final PathSummaryReader pathSummaryReader;

  private final Set<Path<QNm>> paths;

  private final Type type;

  public CASIndexBuilder(final RBTreeWriter<CASValue, NodeReferences> rbTreeWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.rbTreeWriter = rbTreeWriter;
    this.type = type;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      if (paths.isEmpty() || pathSummaryReader.getPCRsForPaths(paths, true).contains(pathNodeKey)) {
        final Str strValue;

        if (node instanceof ImmutableValueNode) {
          strValue = new Str(((ImmutableValueNode) node).getValue());
        } else if (node instanceof ImmutableObjectNumberNode) {
          strValue = new Str(String.valueOf(((ImmutableObjectNumberNode) node).getValue()));
        } else if (node instanceof ImmutableNumberNode) {
          strValue = new Str(String.valueOf(((ImmutableNumberNode) node).getValue()));
        } else if (node instanceof ImmutableObjectBooleanNode) {
          strValue = new Str(String.valueOf(((ImmutableObjectBooleanNode) node).getValue()));
        } else if (node instanceof ImmutableBooleanNode) {
          strValue = new Str(String.valueOf(((ImmutableBooleanNode) node).getValue()));
        } else {
          throw new IllegalStateException("Value not supported.");
        }

        boolean isOfType = false;
        try {
          if (type != Type.STR)
            AtomicUtil.toType(strValue, type);
          isOfType = true;
        } catch (final SirixRuntimeException ignored) {
        }

        if (isOfType) {
          final CASValue value = new CASValue(strValue, type, pathNodeKey);
          final Optional<NodeReferences> textReferences = rbTreeWriter.get(value, SearchMode.EQUAL);
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
    rbTreeWriter.index(value, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
