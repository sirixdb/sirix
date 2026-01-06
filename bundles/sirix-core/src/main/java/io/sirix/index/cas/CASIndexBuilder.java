package io.sirix.index.cas;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.AtomicUtil;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTIndexWriter;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Builder for CAS indexes.
 * 
 * <p>Supports both traditional RBTree and high-performance HOT index backends.</p>
 */
public final class CASIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CASIndexBuilder.class));

  private final @Nullable RBTreeWriter<CASValue, NodeReferences> rbTreeWriter;
  private final @Nullable HOTIndexWriter<CASValue> hotWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private final Type type;
  private final boolean useHOT;

  /**
   * Constructor with RBTree writer (legacy path).
   */
  public CASIndexBuilder(final RBTreeWriter<CASValue, NodeReferences> indexWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.rbTreeWriter = indexWriter;
    this.hotWriter = null;
    this.type = type;
    this.useHOT = false;
  }

  /**
   * Constructor with HOT writer (high-performance path).
   */
  public CASIndexBuilder(final HOTIndexWriter<CASValue> hotWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.type = type;
    this.useHOT = true;
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
          if (useHOT) {
            processHOT(node, value);
          } else {
            processRBTree(node, value);
          }
        }
      }
    } catch (final PathException | SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }

  private void processRBTree(final ImmutableNode node, final CASValue value) throws SirixIOException {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(value, SearchMode.EQUAL);
    if (textReferences.isPresent()) {
      setNodeReferencesRBTree(node, textReferences.get(), value);
    } else {
      setNodeReferencesRBTree(node, new NodeReferences(), value);
    }
  }

  private void processHOT(final ImmutableNode node, final CASValue value) throws SirixIOException {
    assert hotWriter != null;
    NodeReferences existingRefs = hotWriter.get(value, SearchMode.EQUAL);
    if (existingRefs != null) {
      setNodeReferencesHOT(node, existingRefs, value);
    } else {
      setNodeReferencesHOT(node, new NodeReferences(), value);
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references, final CASValue value)
      throws SirixIOException {
    assert rbTreeWriter != null;
    rbTreeWriter.index(value, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }

  private void setNodeReferencesHOT(final ImmutableNode node, final NodeReferences references, final CASValue value)
      throws SirixIOException {
    assert hotWriter != null;
    hotWriter.index(value, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
