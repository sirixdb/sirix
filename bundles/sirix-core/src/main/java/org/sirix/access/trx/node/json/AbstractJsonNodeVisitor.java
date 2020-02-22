/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access.trx.node.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.immutable.json.ImmutableArrayNode;
import org.sirix.node.immutable.json.ImmutableBooleanNode;
import org.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import org.sirix.node.immutable.json.ImmutableNullNode;
import org.sirix.node.immutable.json.ImmutableNumberNode;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.immutable.json.ImmutableObjectNode;
import org.sirix.node.immutable.json.ImmutableStringNode;

/**
 * <h1>AbstractVisitor</h1>
 *
 * <p>
 * Inspired by the dom4j approach {@code AbsVisitor} is an abstract base class which is useful for
 * implementing inheritance or when using anonymous inner classes to create simple
 * {@link XmlNodeVisitor} implementations.
 * </p>
 *
 * <h2>Usage Examples:</h2>
 *
 * <code><pre>
 * final Visitor visitor = new NamespaceChangeVisitor(session);
 * for (final long nodeKey : new DescendantAxis.Builder(rtx).includeSelf().build()) {
 *      rtx.acceptVisitor(visitor);
 * }
 * </pre></code>
 *
 * <code><pre>
 * // MyVisitor extends AbstractVisitor.
 * final Visitor visitor = new MyVisitor(rtx) {
 *   public void visit(final ImmutableObjectKey node) {
 *     rtx.moveTo(node.getKey());
 *     LOGGER.info("Object key name: " + rtx.getName());
 *   }
 * };
 *
 * for (final long nodeKey : new DescendantAxis(rtx);) {
 *   rtx.acceptVisitor(visitor);
 * }
 * </pre></code>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractJsonNodeVisitor implements JsonNodeVisitor {
  @Override
  public VisitResult visit(ImmutableArrayNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableBooleanNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableJsonDocumentRootNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableNullNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableObjectKeyNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableNumberNode node) {
    return VisitResultType.CONTINUE;
  };

  @Override
  public VisitResult visit(ImmutableObjectNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(ImmutableStringNode node) {
    return VisitResultType.CONTINUE;
  }
}
