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

import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.node.NodeKind;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Keeps track of nodes in a matching.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class Matching {

  /** Forward matching. */
  private final Map<Long, Long> mapping;

  /** Backward machting. */
  private final Map<Long, Long> reverseMapping;

  /**
   * Tracks the (grand-)parent-child relation of nodes. We use this to speed up the calculation of the
   * number of nodes in the subtree of two nodes that are in the matching.
   */
  private final ConnectionMap<Long> isInSubtree;

  /** {@link XmlNodeReadOnlyTrx} reference on old revision. */
  private final XmlNodeReadOnlyTrx rtxOld;

  /** {@link XmlNodeReadOnlyTrx} reference on new revision. */
  private final XmlNodeReadOnlyTrx rtxNew;

  /**
   * Creates a new matching.
   *
   * @param rtxOld {@link XmlNodeReadOnlyTrx} reference on old revision
   * @param rtxNew {@link XmlNodeReadOnlyTrx} reference on new revision.
   */
  public Matching(final XmlNodeReadOnlyTrx rtxOld, final XmlNodeReadOnlyTrx rtxNew) {
    mapping = new HashMap<>();
    reverseMapping = new HashMap<>();
    isInSubtree = new ConnectionMap<>();
    this.rtxOld = checkNotNull(rtxOld);
    this.rtxNew = checkNotNull(rtxNew);
  }

  /**
   * Copy constructor. Creates a new matching with the same state as the matching pMatch.
   *
   * @param match the original {@link Matching} reference
   */
  public Matching(final Matching match) {
    mapping = new HashMap<>(match.mapping);
    reverseMapping = new HashMap<>(match.reverseMapping);
    isInSubtree = new ConnectionMap<>(match.isInSubtree);
    rtxOld = match.rtxOld;
    rtxNew = match.rtxNew;
  }

  /**
   * Adds the matching x -&gt; y.
   *
   * @param nodeX source node (in old revision)
   * @param nodeY partner of nodeX (in new revision)
   */
  public void add(final @NonNegative long nodeX, final @NonNegative long nodeY) {
    rtxOld.moveTo(nodeX);
    rtxNew.moveTo(nodeY);
    if (rtxOld.getKind() != rtxNew.getKind()) {
      throw new AssertionError();
    }
    mapping.put(nodeX, nodeY);
    reverseMapping.put(nodeY, nodeX);
    updateSubtreeMap(nodeX, rtxOld);
    updateSubtreeMap(nodeY, rtxNew);
  }

  /**
   * Remove matching.
   *
   * @param nodeX source node for which to remove the connection
   */
  public boolean remove(final @NonNegative long nodeX) {
    reverseMapping.remove(mapping.get(nodeX));
    return mapping.remove(nodeX) != null;
  }

  /**
   * For each anchestor of n: n is in it's subtree.
   *
   * @param key key of node in subtree
   * @param rtx {@link XmlNodeReadOnlyTrx} reference
   */
  private void updateSubtreeMap(final @NonNegative long key, final XmlNodeReadOnlyTrx rtx) {
    assert key >= 0;
    assert rtx != null;

    isInSubtree.set(key, key, true);
    rtx.moveTo(key);
    if (rtx.hasParent()) {
      while (rtx.hasParent()) {
        rtx.moveToParent();
        isInSubtree.set(rtx.getNodeKey(), key, true);
      }
      rtx.moveTo(key);
    }
  }

  /**
   * Checks if the matching contains the pair (x, y).
   *
   * @param nodeX source node
   * @param nodeY partner of x
   * @return true iff add(x, y) was invoked first
   */
  public boolean contains(final @NonNegative long nodeX, final @NonNegative long nodeY) {
    return mapping.get(nodeX) != null && mapping.get(nodeX).equals(nodeY);
  }

  /**
   * Counts the number of descendant nodes in the subtrees of x and y that are also in the matching.
   *
   * @param nodeX first subtree root node
   * @param nodeY second subtree root node
   * @return number of descendant which have been matched
   */
  public long containedDescendants(final @NonNegative long nodeX, final @NonNegative long nodeY) {
    long retVal = 0;

    rtxOld.moveTo(nodeX);
    for (final Axis axis = new DescendantAxis(rtxOld, IncludeSelf.YES); axis.hasNext();) {
      axis.nextLong();
      retVal += isInSubtree.get(nodeY, partner(rtxOld.getNodeKey()))
          ? 1
          : 0;
      if (rtxOld.getKind() == NodeKind.ELEMENT) {
        for (int i = 0, nspCount = rtxOld.getNamespaceCount(); i < nspCount; i++) {
          rtxOld.moveToNamespace(i);
          retVal += isInSubtree.get(nodeY, partner(axis.asXmlNodeReadTrx().getNodeKey()))
              ? 1
              : 0;
          rtxOld.moveToParent();
        }
        for (int i = 0, attCount = rtxOld.getAttributeCount(); i < attCount; i++) {
          rtxOld.moveToAttribute(i);
          retVal += isInSubtree.get(nodeY, partner(axis.asXmlNodeReadTrx().getNodeKey()))
              ? 1
              : 0;
          rtxOld.moveToParent();
        }
      }
    }

    return retVal;
  }

  /**
   * Returns the partner node of {@code pNode} according to mapping.
   *
   * @param node node for which a partner has to be found
   * @return the {@code nodeKey} of the other node or {@code null}
   */
  public Long partner(final @NonNegative long node) {
    return mapping.get(node);
  }

  /**
   * Returns the node for which "node" is the partner (normally used for retrieving partners of nodes
   * in new revision).
   *
   * @param node node for which a reverse partner has to be found
   * @return x iff add(x, node) was called before
   */
  public Long reversePartner(final @NonNegative long node) {
    return reverseMapping.get(node);
  }

  /** Reset internal datastructures. */
  public void reset() {
    mapping.clear();
    reverseMapping.clear();
    isInSubtree.reset();
  }
}
