package io.sirix.index.cas;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.AtomicUtil;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import io.sirix.node.immutable.json.ImmutableObjectNumberNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import io.sirix.utils.LogWrapper;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.index.path.summary.PathSummaryReader;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

public final class CASIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CASIndexBuilder.class));

  private final RBTreeWriter<CASValue, NodeReferences> indexWriter;

  private final PathSummaryReader pathSummaryReader;

  private final Set<Path<QNm>> paths;

  private final Type type;

  public CASIndexBuilder(final RBTreeWriter<CASValue, NodeReferences> indexWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.indexWriter = indexWriter;
    this.type = type;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      if (paths.isEmpty() || pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
        final Str strValue = switch (node) {
          case ImmutableValueNode immutableValueNode -> new Str(immutableValueNode.getValue());
          case ImmutableObjectNumberNode immutableObjectNumberNode ->
              new Str(String.valueOf(immutableObjectNumberNode.getValue()));
          case ImmutableNumberNode immutableNumberNode -> new Str(String.valueOf(immutableNumberNode.getValue()));
          case ImmutableObjectBooleanNode immutableObjectBooleanNode ->
              new Str(String.valueOf(immutableObjectBooleanNode.getValue()));
          case ImmutableBooleanNode immutableBooleanNode -> new Str(String.valueOf(immutableBooleanNode.getValue()));
          case null, default -> throw new IllegalStateException("Value not supported.");
        };

        boolean isOfType = false;
        try {
          if (type != Type.STR)
            AtomicUtil.toType(strValue, type);
          isOfType = true;
        } catch (final SirixRuntimeException ignored) {
        }

        if (isOfType) {
          final CASValue value = new CASValue(strValue, type, pathNodeKey);
          final Optional<NodeReferences> textReferences = indexWriter.get(value, SearchMode.EQUAL);
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
    indexWriter.index(value, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
