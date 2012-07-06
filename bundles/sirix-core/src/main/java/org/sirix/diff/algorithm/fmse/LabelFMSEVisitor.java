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
package org.sirix.diff.algorithm.fmse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sirix.access.AbsVisitorSupport;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.exception.AbsTTException;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.TextNode;

/**
 * Label visitor. Treats empty-elements as internal nodes.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class LabelFMSEVisitor extends AbsVisitorSupport {

  /** {@link INodeReadTrx} implementation. */
  private final INodeReadTrx mRtx;

  /** For each node type: list of inner nodes. */
  private final Map<EKind, List<Long>> mLabels;

  /** For each node type: list of leaf nodes. */
  private final Map<EKind, List<Long>> mLeafLabels;

  /**
   * Constructor.
   * 
   * @param pSession
   *          {@link ISession} implementation
   * @throws AbsTTException
   *           if setting up sirix fails
   */
  public LabelFMSEVisitor(final INodeReadTrx pReadTransaction) throws AbsTTException {
    mRtx = pReadTransaction;
    mLabels = new HashMap<>();
    mLeafLabels = new HashMap<>();
  }

  @Override
  public EVisitResult visit(final ElementNode pNode) {
    final long nodeKey = pNode.getNodeKey();
    mRtx.moveTo(nodeKey);
    for (int i = 0; i < pNode.getAttributeCount(); i++) {
      mRtx.moveToAttribute(i);
      addLeafLabel();
      mRtx.moveTo(nodeKey);
    }
    for (int i = 0; i < pNode.getNamespaceCount(); i++) {
      mRtx.moveToNamespace(i);
      addLeafLabel();
      mRtx.moveTo(nodeKey);
    }
    if (!mLabels.containsKey(pNode.getKind())) {
      mLabels.put(pNode.getKind(), new ArrayList<Long>());
    }
    mLabels.get(pNode.getKind()).add(pNode.getNodeKey());
    return EVisitResult.CONTINUE;
  }

  @Override
  public EVisitResult visit(final TextNode pNode) {
    mRtx.moveTo(pNode.getNodeKey());
    addLeafLabel();
    return EVisitResult.CONTINUE;
  }

  /**
   * Add leaf node label.
   */
  private void addLeafLabel() {
    final EKind nodeKind = mRtx.getNode().getKind();
    if (!mLeafLabels.containsKey(nodeKind)) {
      mLeafLabels.put(nodeKind, new ArrayList<Long>());
    }
    mLeafLabels.get(nodeKind).add(mRtx.getNode().getNodeKey());
  }

  /**
   * Get labels.
   * 
   * @return the Labels
   */
  public Map<EKind, List<Long>> getLabels() {
    return mLabels;
  }

  /**
   * Get leaf labels.
   * 
   * @return the leaf labels
   */
  public Map<EKind, List<Long>> getLeafLabels() {
    return mLeafLabels;
  }
}
