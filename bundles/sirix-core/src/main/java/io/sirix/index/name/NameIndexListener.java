package io.sirix.index.name;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import io.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Optional;
import java.util.Set;

/**
 * Listener for NAME index changes.
 * 
 * <p>Supports both traditional RBTree and high-performance HOT index backends.</p>
 */
public final class NameIndexListener {

  private final Set<QNm> includes;
  private final Set<QNm> excludes;
  private final @Nullable RBTreeWriter<QNm, NodeReferences> rbTreeWriter;
  private final @Nullable HOTIndexWriter<QNm> hotWriter;
  private final boolean useHOT;

  /**
   * Constructor with RBTree writer (legacy path).
   */
  public NameIndexListener(final Set<QNm> includes, final Set<QNm> excludes,
      final RBTreeWriter<QNm, NodeReferences> indexTreeWriter) {
    this.includes = includes;
    this.excludes = excludes;
    this.rbTreeWriter = indexTreeWriter;
    this.hotWriter = null;
    this.useHOT = false;
  }

  /**
   * Constructor with HOT writer (high-performance path).
   */
  public NameIndexListener(final Set<QNm> includes, final Set<QNm> excludes,
      final HOTIndexWriter<QNm> hotWriter) {
    this.includes = includes;
    this.excludes = excludes;
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.useHOT = true;
  }

  public void listen(IndexController.ChangeType type, @NonNull ImmutableNode node, QNm name) {
    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    if (!included || excluded) {
      return;
    }

    switch (type) {
      case INSERT -> {
        if (useHOT) {
          handleInsertHOT(node, name);
        } else {
          handleInsertRBTree(node, name);
        }
      }
      case DELETE -> {
        if (useHOT) {
          assert hotWriter != null;
          hotWriter.remove(name, node.getNodeKey());
        } else {
          assert rbTreeWriter != null;
          rbTreeWriter.remove(name, node.getNodeKey());
        }
      }
      default -> {
      }
    }
  }

  private void handleInsertRBTree(@NonNull ImmutableNode node, QNm name) {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(name, SearchMode.EQUAL);
    if (textReferences.isPresent()) {
      setNodeReferencesRBTree(node, textReferences.get(), name);
    } else {
      setNodeReferencesRBTree(node, new NodeReferences(), name);
    }
  }

  private void handleInsertHOT(@NonNull ImmutableNode node, QNm name) {
    assert hotWriter != null;
    NodeReferences existingRefs = hotWriter.get(name, SearchMode.EQUAL);
    if (existingRefs != null) {
      setNodeReferencesHOT(node, existingRefs, name);
    } else {
      setNodeReferencesHOT(node, new NodeReferences(), name);
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references, 
      final QNm name) {
    assert rbTreeWriter != null;
    rbTreeWriter.index(name, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }

  private void setNodeReferencesHOT(final ImmutableNode node, final NodeReferences references,
      final QNm name) {
    assert hotWriter != null;
    hotWriter.index(name, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
