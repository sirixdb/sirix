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

import org.sirix.access.trx.node.xml.AbstractXmlNodeVisitor;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.xml.ImmutableComment;
import org.sirix.node.immutable.xml.ImmutableElement;
import org.sirix.node.immutable.xml.ImmutablePI;
import org.sirix.node.immutable.xml.ImmutableText;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Initialize data structures.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class FMSEVisitor extends AbstractXmlNodeVisitor {

  /** {@link XmlNodeReadOnlyTrx} reference. */
  private final XmlNodeReadOnlyTrx rtx;

  /** Determines if nodes are in order. */
  private final Map<Long, Boolean> inOrder;

  /** Descendant count per node. */
  private final Map<Long, Long> descendants;

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
    rtx = checkNotNull(readTransaction);
    this.inOrder = checkNotNull(inOrder);
    this.descendants = checkNotNull(descendants);
  }

  @Override
  public VisitResultType visit(final ImmutableElement node) {
    final long nodeKey = node.getNodeKey();
    rtx.moveTo(nodeKey);
    for (int i = 0, attCount = rtx.getAttributeCount(); i < attCount; i++) {
      rtx.moveToAttribute(i);
      fillStructuralDataStructures();
      rtx.moveTo(nodeKey);
    }
    for (int i = 0, nspCount = rtx.getNamespaceCount(); i < nspCount; i++) {
      rtx.moveToNamespace(i);
      fillStructuralDataStructures();
      rtx.moveTo(nodeKey);
    }
    countDescendants();
    return VisitResultType.CONTINUE;
  }

  /**
   * Fill data structures.
   */
  private void fillStructuralDataStructures() {
    inOrder.put(rtx.getNodeKey(), true);
    descendants.put(rtx.getNodeKey(), 1L);
  }

  /**
   * Count descendants of node (including self).
   */
  private void countDescendants() {
    long descendants = 0;
    final long nodeKey = rtx.getNodeKey();
    descendants += rtx.getNamespaceCount();
    descendants += rtx.getAttributeCount();
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      do {
        descendants += this.descendants.get(rtx.getNodeKey());
        if (rtx.getKind() == NodeKind.ELEMENT) {
          descendants += 1;
        }
      } while (rtx.hasRightSibling() && rtx.moveToRightSibling());
    }
    rtx.moveTo(nodeKey);
    inOrder.put(rtx.getNodeKey(), false);
    this.descendants.put(rtx.getNodeKey(), descendants);
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
   * @param node the node to visit
   * @return {@link VisitResultType} value to continue normally
   */
  private VisitResultType visiLeafNode(final ImmutableNode node) {
    final long nodeKey = node.getNodeKey();
    rtx.moveTo(nodeKey);
    inOrder.put(rtx.getNodeKey(), false);
    descendants.put(rtx.getNodeKey(), 1L);
    return VisitResultType.CONTINUE;
  }
}
