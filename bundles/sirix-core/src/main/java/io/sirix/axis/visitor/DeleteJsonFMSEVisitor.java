package io.sirix.axis.visitor;

import io.sirix.access.trx.node.json.AbstractJsonNodeVisitor;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.diff.algorithm.fmse.json.JsonMatching;
import io.sirix.exception.SirixException;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.immutable.json.ImmutableObjectNullNode;
import io.sirix.node.immutable.json.ImmutableObjectNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectStringNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.utils.LogWrapper;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Visitor for the second step of the JSON FMSE algorithm: deletes unmatched nodes. No text node
 * merging is needed for JSON (unlike XML).
 */
public class DeleteJsonFMSEVisitor extends AbstractJsonNodeVisitor {

  private static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(DeleteJsonFMSEVisitor.class));

  private final JsonMatching matching;
  private final JsonNodeTrx wtx;
  private final long startKey;

  public DeleteJsonFMSEVisitor(final JsonNodeTrx wtx, final JsonMatching matching,
      @NonNegative final long startKey) {
    this.wtx = requireNonNull(wtx);
    this.matching = requireNonNull(matching);
    checkArgument(startKey >= 0, "start key must be >= 0!");
    this.startKey = startKey;
  }

  // ==================== Inner node visitors ====================

  @Override
  public VisitResult visit(final ImmutableObjectNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableArrayNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableObjectKeyNode node) {
    return deleteIfUnmatched(node);
  }

  // ==================== Leaf node visitors ====================

  @Override
  public VisitResult visit(final ImmutableStringNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableNumberNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableBooleanNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableNullNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableObjectStringNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableObjectNumberNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableObjectBooleanNode node) {
    return deleteIfUnmatched(node);
  }

  @Override
  public VisitResult visit(final ImmutableObjectNullNode node) {
    return deleteIfUnmatched(node);
  }

  // ==================== Internal deletion logic ====================

  private VisitResult deleteIfUnmatched(final ImmutableNode node) {
    final long partnerKey = matching.partner(node.getNodeKey());
    if (partnerKey == JsonMatching.NO_PARTNER) {
      VisitResult retVal = delete(node);
      if (node.getNodeKey() == startKey) {
        retVal = VisitResultType.TERMINATE;
      }
      return retVal;
    }
    return VisitResultType.CONTINUE;
  }

  /**
   * Deletes the node and adapts cursor position for the VisitorDescendantAxis.
   */
  private VisitResult delete(final ImmutableNode node) {
    try {
      wtx.moveTo(node.getNodeKey());
      final long nodeKey = wtx.getNodeKey();

      // Case: Has no right and no left sibl. but the parent has a right sibl.
      final boolean movedToParent = wtx.moveToParent();
      assert movedToParent;
      final long parentNodeKey = wtx.getNodeKey();
      if (wtx.getChildCount() == 1 && wtx.hasRightSibling()) {
        wtx.moveTo(nodeKey);
        wtx.remove();
        assert wtx.getNodeKey() == parentNodeKey;
        return LocalVisitResult.SKIPSUBTREEPOPSTACK;
      }
      wtx.moveTo(nodeKey);

      // Case: Has left sibl. but no right sibl.
      if (!wtx.hasRightSibling() && wtx.hasLeftSibling()) {
        final long leftSiblKey = wtx.getLeftSiblingKey();
        wtx.remove();
        assert wtx.getNodeKey() == leftSiblKey;
        return VisitResultType.SKIPSUBTREE;
      }

      // Case: Has right sibl. and left sibl.
      if (wtx.hasRightSibling() && wtx.hasLeftSibling()) {
        wtx.remove();
        final boolean moved = wtx.moveToLeftSibling();
        assert moved;
        return VisitResultType.SKIPSUBTREE;
      }

      // Case: Has right sibl. but no left sibl.
      if (wtx.hasRightSibling() && !wtx.hasLeftSibling()) {
        final long rightSiblKey = wtx.getRightSiblingKey();
        wtx.remove();
        wtx.moveToParent();
        assert wtx.getFirstChildKey() == rightSiblKey;
        return VisitResultType.CONTINUE;
      }

      // Case: Has no right and no left sibl.
      final long parentKey = wtx.getParentKey();
      wtx.remove();
      assert wtx.getNodeKey() == parentKey;
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }
}
