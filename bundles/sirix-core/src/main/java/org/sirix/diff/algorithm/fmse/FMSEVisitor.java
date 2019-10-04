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
package org.sirix.diff.algorithm.fmse;

import org.sirix.access.trx.node.xml.AbstractXdmNodeVisitor;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.xdm.ImmutableComment;
import org.sirix.node.immutable.xdm.ImmutableElement;
import org.sirix.node.immutable.xdm.ImmutablePI;
import org.sirix.node.immutable.xdm.ImmutableText;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Initialize data structures.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class FMSEVisitor extends AbstractXdmNodeVisitor {

  /** {@link XmlNodeReadOnlyTrx} reference. */
  private final XmlNodeReadOnlyTrx mRtx;

  /** Determines if nodes are in order. */
  private final Map<Long, Boolean> mInOrder;

  /** Descendant count per node. */
  private final Map<Long, Long> mDescendants;

  /**
   * Constructor.
   *
   * @param readTransaction the transaction cursor
   * @param inOrder {@link Map} reference to track ordered nodes
   * @param descendants {@link Map} reference to track descendants per node
   * @throws SirixException if setting up sirix fails
   * @throws NullPointerException if one of the arguments is {@code null}
   */
  public FMSEVisitor(final XmlNodeReadOnlyTrx readTransaction, final Map<Long, Boolean> inOrder,
      final Map<Long, Long> descendants) {
    mRtx = checkNotNull(readTransaction);
    mInOrder = checkNotNull(inOrder);
    mDescendants = checkNotNull(descendants);
  }

  @Override
  public VisitResultType visit(final ImmutableElement node) {
    final long nodeKey = node.getNodeKey();
    mRtx.moveTo(nodeKey);
    for (int i = 0, attCount = mRtx.getAttributeCount(); i < attCount; i++) {
      mRtx.moveToAttribute(i);
      fillStructuralDataStructures();
      mRtx.moveTo(nodeKey);
    }
    for (int i = 0, nspCount = mRtx.getNamespaceCount(); i < nspCount; i++) {
      mRtx.moveToNamespace(i);
      fillStructuralDataStructures();
      mRtx.moveTo(nodeKey);
    }
    countDescendants();
    return VisitResultType.CONTINUE;
  }

  /**
   * Fill data structures.
   */
  private void fillStructuralDataStructures() {
    mInOrder.put(mRtx.getNodeKey(), true);
    mDescendants.put(mRtx.getNodeKey(), 1L);
  }

  /**
   * Count descendants of node (including self).
   */
  private void countDescendants() {
    long descendants = 0;
    final long nodeKey = mRtx.getNodeKey();
    descendants += mRtx.getNamespaceCount();
    descendants += mRtx.getAttributeCount();
    if (mRtx.hasFirstChild()) {
      mRtx.moveToFirstChild();
      do {
        descendants += mDescendants.get(mRtx.getNodeKey());
        if (mRtx.getKind() == NodeKind.ELEMENT) {
          descendants += 1;
        }
      } while (mRtx.hasRightSibling() && mRtx.moveToRightSibling().hasMoved());
    }
    mRtx.moveTo(nodeKey);
    mInOrder.put(mRtx.getNodeKey(), false);
    mDescendants.put(mRtx.getNodeKey(), descendants);
  }

  @Override
  public VisitResultType visit(final ImmutableText node) {
    return visiLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutableComment node) {
    return visiLeafNode(node);
  }

  @Override
  public VisitResultType visit(final ImmutablePI node) {
    return visiLeafNode(node);
  }

  /**
   * Visit a leaf node.
   *
   * @param pNode the node to visit
   * @return {@link VisitResultType} value to continue normally
   */
  private VisitResultType visiLeafNode(final ImmutableNode pNode) {
    final long nodeKey = pNode.getNodeKey();
    mRtx.moveTo(nodeKey);
    mInOrder.put(mRtx.getNodeKey(), false);
    mDescendants.put(mRtx.getNodeKey(), 1L);
    return VisitResultType.CONTINUE;
  }
}
