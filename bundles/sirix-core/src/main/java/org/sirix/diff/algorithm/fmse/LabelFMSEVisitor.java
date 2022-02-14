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
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.xml.ImmutableElement;
import org.sirix.node.immutable.xml.ImmutableText;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Label visitor. Treats empty-elements as internal nodes.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class LabelFMSEVisitor extends AbstractXmlNodeVisitor {

  /** {@link XmlNodeReadOnlyTrx} implementation. */
  private final XmlNodeReadOnlyTrx rtx;

  /** For each node type: list of inner nodes. */
  private final Map<NodeKind, List<Long>> labels;

  /** For each node type: list of leaf nodes. */
  private final Map<NodeKind, List<Long>> leafLabels;

  /**
   * Constructor.
   *
   * @param readTrx a read only transaction
   */
  public LabelFMSEVisitor(final XmlNodeReadOnlyTrx readTrx) {
    rtx = checkNotNull(readTrx);
    labels = new HashMap<>();
    leafLabels = new HashMap<>();
  }

  @Override
  public VisitResultType visit(final ImmutableElement node) {
    final long nodeKey = node.getNodeKey();
    rtx.moveTo(nodeKey);
    for (int i = 0, nspCount = rtx.getNamespaceCount(); i < nspCount; i++) {
      rtx.moveToNamespace(i);
      addLeafLabel();
      rtx.moveTo(nodeKey);
    }
    for (int i = 0, attCount = rtx.getAttributeCount(); i < attCount; i++) {
      rtx.moveToAttribute(i);
      addLeafLabel();
      rtx.moveTo(nodeKey);
    }
    if (!labels.containsKey(node.getKind())) {
      labels.put(node.getKind(), new ArrayList<Long>());
    }
    labels.get(node.getKind()).add(node.getNodeKey());
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResultType visit(final ImmutableText node) {
    rtx.moveTo(node.getNodeKey());
    addLeafLabel();
    return VisitResultType.CONTINUE;
  }

  /**
   * Add leaf node label.
   */
  private void addLeafLabel() {
    final NodeKind nodeKind = rtx.getKind();
    if (!leafLabels.containsKey(nodeKind)) {
      leafLabels.put(nodeKind, new ArrayList<>());
    }
    leafLabels.get(nodeKind).add(rtx.getNodeKey());
  }

  /**
   * Get labels.
   *
   * @return the Labels
   */
  public Map<NodeKind, List<Long>> getLabels() {
    return labels;
  }

  /**
   * Get leaf labels.
   *
   * @return the leaf labels
   */
  public Map<NodeKind, List<Long>> getLeafLabels() {
    return leafLabels;
  }
}
