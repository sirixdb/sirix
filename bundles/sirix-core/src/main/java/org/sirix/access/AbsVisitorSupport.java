/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.immutable.ImmutableAttribute;
import org.sirix.node.immutable.ImmutableComment;
import org.sirix.node.immutable.ImmutableDocument;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.immutable.ImmutableNamespace;
import org.sirix.node.immutable.ImmutablePI;
import org.sirix.node.immutable.ImmutableText;

/**
 * <h1>AbsVisitorSupport</h1>
 * 
 * <p>
 * Inspired by the dom4j approach {@code VisitorSupport} is an abstract base class which is useful for
 * implementation inheritence or when using anonymous inner classes to create simple {@link IVisitor}
 * implementations.
 * </p>
 * 
 * <h2>Usage Examples:</h2>
 * 
 * <code><pre>
 * final IVisitor visitor = new NamespaceChangeVisitor(session);
 * for (final IAxis axis = new DescendantAxis.Builder(rtx).includeSelf().build(); axis.hasNext();) {
 *      axis.next();
 *      rtx.acceptVisitor(visitor);
 * }
 * </pre></code>
 * 
 * <code><pre>
 * final IVisitor visitor = new AbsVisitorSupport(rtx) {
 *      public void visit(final ImmutableElement pNode) {
 *              rtx.moveTo(pNode.getKey());
 *              LOGGER.info(
 *                      "Element name: " + mRtx.getQName().getLocalName()
 *              );
 *      }     
 * };
 * 
 * for (final IAxis axis = new DescendantAxis(rtx); axis.hasNext();) {
 *      axis.next();
 *      rtx.acceptVisitor(visitor);
 * }
 * </pre></code>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsVisitorSupport implements IVisitor {
  @Override
  public IVisitResult visit(@Nonnull final ImmutablePI pNode) {
    return EVisitResult.CONTINUE;
  }
  
  @Override
  public IVisitResult visit(@Nonnull final ImmutableComment pNode) {
    return EVisitResult.CONTINUE;
  }
  
  @Override
  public IVisitResult visit(@Nonnull final ImmutableElement pNode) {
    return EVisitResult.CONTINUE;
  }

  @Override
  public IVisitResult visit(@Nonnull final ImmutableText pNode) {
    return EVisitResult.CONTINUE;
  }

  @Override
  public IVisitResult visit(@Nonnull final ImmutableDocument pNode) {
    return EVisitResult.CONTINUE;
  }

  @Override
  public IVisitResult visit(@Nonnull final ImmutableAttribute pNode) {
    return EVisitResult.CONTINUE;
  }

  @Override
  public IVisitResult visit(@Nonnull final ImmutableNamespace pNode) {
    return EVisitResult.CONTINUE;
  }
}
