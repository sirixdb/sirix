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

package org.treetank.diff;

import org.treetank.api.INodeReadTrx;
import org.treetank.diff.DiffFactory.Builder;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ElementNode;

/**
 * Structural diff, thus no attributes and namespace nodes are taken into account.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
final class StructuralDiff extends AbsDiff {

  /**
   * Constructor.
   * 
   * @param pBuilder
   *          {@link Builder} reference
   * @throws AbsTTException
   */
  public StructuralDiff(final Builder pBuilder) throws AbsTTException {
    super(pBuilder);
  }

  @Override
  boolean checkNodes(final INodeReadTrx pNewRtx, final INodeReadTrx pOldRtx) {
    boolean found = false;
    if (pNewRtx.getNode().getNodeKey() == pOldRtx.getNode().getNodeKey()
      && pNewRtx.getNode().getKind() == pOldRtx.getNode().getKind()) {
      switch (pNewRtx.getNode().getKind()) {
      case ELEMENT_KIND:
        final ElementNode newNode = (ElementNode)pNewRtx.getNode();
        final ElementNode oldNode = (ElementNode)pOldRtx.getNode();
        if (newNode.getNameKey() == oldNode.getNameKey()) {
          found = true;
        }
        break;
      case TEXT_KIND:
        if (pNewRtx.getValueOfCurrentNode().equals(pOldRtx.getValueOfCurrentNode())) {
          found = true;
        }
        break;
      default:
        // Do nothing.
      }
    }
    return found;
  }
}
