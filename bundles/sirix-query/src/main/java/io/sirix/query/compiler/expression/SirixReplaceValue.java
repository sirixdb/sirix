/*
 * [New BSD License]
 * Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Brackit Project Team nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.query.compiler.expression;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.expr.ConstructedNodeBuilder;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Kind;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.update.ReplaceValue;
import io.brackit.query.update.op.OpType;
import io.brackit.query.update.op.UpdateOp;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.node.XmlDBNode;
import io.sirix.utils.ToStringHelper;
import io.sirix.utils.XMLToken;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Replaces Sirix element content through the writer cursor so child removal cannot navigate a stale
 * read snapshot. Retains the original target for Brackit's pending-update bookkeeping; other node
 * kinds use Brackit's replace-value implementation.
 */
public final class SirixReplaceValue extends ConstructedNodeBuilder implements Expr {
  private final Expr sourceExpr;

  private final Expr targetExpr;

  public SirixReplaceValue(final Expr sourceExpr, final Expr targetExpr) {
    this.sourceExpr = requireNonNull(sourceExpr);
    this.targetExpr = requireNonNull(targetExpr);
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    return evaluateToItem(ctx, tuple);
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
    final Sequence target = targetExpr.evaluate(ctx, tuple);
    final Item targetItem;
    if (target == null) {
      throw new QueryException(ErrorCode.ERR_UPDATE_INSERT_TARGET_IS_EMPTY_SEQUENCE);
    } else if (target instanceof Item item) {
      targetItem = item;
    } else {
      try (final Iter iterator = target.iterate()) {
        targetItem = iterator.next();
        if (targetItem == null) {
          throw new QueryException(ErrorCode.ERR_UPDATE_INSERT_TARGET_IS_EMPTY_SEQUENCE);
        }
        if (iterator.next() != null) {
          throw new QueryException(ErrorCode.ERR_UPDATE_REPLACE_TARGET_NOT_A_EATCP_NODE);
        }
      }
    }
    if (targetItem instanceof XmlDBNode node && node.getKind() == Kind.ELEMENT) {
      final String text = buildTextContent(sourceExpr.evaluate(ctx, tuple));
      final int invalidCodePoint = text == null
          ? -1
          : XMLToken.firstInvalidXmlChar(text);
      if (invalidCodePoint != -1) {
        throw new DocumentException("Replacement value contains an invalid XML character: U+%04X", invalidCodePoint);
      }
      ctx.addPendingUpdate(new ReplaceContent(node, text, node.getTrx(), node.getNodeKey()));
      return null;
    }
    // Preserve Brackit's validation and value-update behavior for other targets. An Item is itself
    // an Expr, so the already evaluated target is reused without evaluating targetExpr again.
    return new ReplaceValue(sourceExpr, targetItem).evaluateToItem(ctx, tuple);
  }

  @Override
  public boolean isUpdating() {
    return true;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }

  private record ReplaceContent(XmlDBNode target, String value, XmlNodeReadOnlyTrx reader,
      long key) implements UpdateOp {
    @Override
    public XmlDBNode getTarget() {
      return target;
    }

    @Override
    public OpType getType() {
      return OpType.REPLACE_ELEMENT_CONTENT;
    }

    @Override
    public String toString() {
      return ToStringHelper.of(this).add("target", target).toString();
    }

    @Override
    public void apply() {
      final XmlResourceSession resource = reader.getResourceSession();
      final XmlNodeTrx writer;
      final Optional<XmlNodeTrx> runningWriter = resource.getNodeTrx();
      if (runningWriter.isPresent()) {
        writer = runningWriter.orElseThrow();
      } else {
        writer = resource.beginNodeTrx();
        if (reader.getRevisionNumber() < resource.getMostRecentRevisionNumber()) {
          writer.revertTo(reader.getRevisionNumber());
        }
      }
      if (!writer.moveTo(key)) {
        return;
      }
      while (writer.hasFirstChild()) {
        writer.moveToFirstChild();
        writer.remove();
        writer.moveTo(key);
      }
      if (value != null && !value.isEmpty()) {
        writer.insertTextAsFirstChild(value);
      }
    }
  }

}
