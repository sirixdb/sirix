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

package org.sirix.diff;

import javax.annotation.Nonnull;

import org.sirix.api.INodeReadTrx;
import org.sirix.diff.DiffFactory.Builder;
import org.sirix.exception.AbsTTException;
import org.sirix.node.ElementNode;
import org.sirix.node.interfaces.INode;

/**
 * Full diff including attributes and namespaces. Note that this class is thread safe.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
final class FullDiff extends AbsDiff {

  /**
   * Constructor.
   * 
   * @param pBuilder
   *          {@link Builder} reference
   * @throws AbsTTException
   *           if anything goes wrong while setting up sirix transactions
   */
  FullDiff(@Nonnull final Builder pBuilder) throws AbsTTException {
    super(pBuilder);
  }

  @Override
  boolean checkNodes(@Nonnull final INodeReadTrx pFirstRtx,
    @Nonnull final INodeReadTrx pSecondRtx) {
    assert pFirstRtx != null;
    assert pSecondRtx != null;

    boolean found = false;
    INode firstNode = pFirstRtx.getNode();
    INode secondNode = pSecondRtx.getNode();
    if (firstNode.getNodeKey() == secondNode.getNodeKey()
      && firstNode.getKind() == secondNode.getKind()) {
      switch (firstNode.getKind()) {
      case ELEMENT:
        final ElementNode firstElement = (ElementNode)pFirstRtx.getNode();
        final ElementNode secondElement = (ElementNode)pSecondRtx.getNode();

        if (firstElement.getNameKey() == secondElement.getNameKey()) {
          if (((ElementNode)pFirstRtx.getNode()).getNamespaceCount() == 0
            && ((ElementNode)pFirstRtx.getNode()).getAttributeCount() == 0
            && ((ElementNode)pSecondRtx.getNode()).getAttributeCount() == 0
            && ((ElementNode)pSecondRtx.getNode()).getNamespaceCount() == 0) {
            found = true;
          } else if (firstElement.getAttributeKeys().equals(
            secondElement.getAttributeKeys())
            && firstElement.getNamespaceKeys().equals(
              secondElement.getNamespaceKeys())) {
            found = true;
          }
        }
        break;
      case TEXT:
        found =
          pFirstRtx.getValueOfCurrentNode().equals(
            pSecondRtx.getValueOfCurrentNode());
        break;
      default:
        throw new IllegalStateException(
          "Other node types currently not supported!");
      }
    }

    return found;
  }
}
