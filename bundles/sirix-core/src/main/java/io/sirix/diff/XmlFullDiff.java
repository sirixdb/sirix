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

package io.sirix.diff;

import java.util.List;
import java.util.Objects;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixException;
import io.sirix.node.NodeKind;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * Full diff including attributes and namespaces. Note that this class is thread safe.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
final class XmlFullDiff extends AbstractDiff<XmlNodeReadOnlyTrx, XmlNodeTrx> {

  /**
   * Constructor.
   *
   * @param builder {@link DiffFactory.Builder} reference
   * @throws SirixException if anything goes wrong while setting up sirix transactions
   */
  XmlFullDiff(final DiffFactory.Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder) throws SirixException {
    super(builder);
  }

  @Override
  NodeKind documentNode() {
    return NodeKind.XML_DOCUMENT;
  }

  @Override
  boolean checkNodes(final XmlNodeReadOnlyTrx newRtx, final XmlNodeReadOnlyTrx oldRtx) {
    boolean found = false;
    if (newRtx.getNodeKey() == oldRtx.getNodeKey() && newRtx.getParentKey() == oldRtx.getParentKey()
        && newRtx.getKind() == oldRtx.getKind()) {
      switch (newRtx.getKind()) {
        case ELEMENT:
          if (checkNamesForEquality(newRtx, oldRtx) && newRtx.getAttributeKeys().equals(oldRtx.getAttributeKeys())
              && newRtx.getNamespaceKeys().equals(oldRtx.getNamespaceKeys())) {
            found = true;

            final long newNodeKey = newRtx.getNodeKey();
            final long oldNodeKey = oldRtx.getNodeKey();

            for (final long nsp : newRtx.getNamespaceKeys()) {
              newRtx.moveTo(nsp);
              oldRtx.moveTo(nsp);

              if (!checkNamesForEquality(newRtx, oldRtx)) {
                found = false;
                break;
              }
            }
            newRtx.moveTo(newNodeKey);
            oldRtx.moveTo(oldNodeKey);

            if (found) {
              for (final long attr : newRtx.getAttributeKeys()) {
                newRtx.moveTo(attr);
                oldRtx.moveTo(attr);

                if (!(checkNamesForEquality(newRtx, oldRtx) && Objects.equals(newRtx.getValue(), oldRtx.getValue()))) {
                  found = false;
                  break;
                }
              }

              newRtx.moveTo(newNodeKey);
              oldRtx.moveTo(oldNodeKey);
            }
          }
          break;
        case PROCESSING_INSTRUCTION:
          found = newRtx.getValue().equals(oldRtx.getValue()) && checkNamesForEquality(newRtx, oldRtx);
          break;
        case TEXT:
        case COMMENT:
          found = newRtx.getValue().equals(oldRtx.getValue());
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Other node types currently not supported!");
      }
    }

    return found;
  }

  @Override
  void emitNonStructuralDiff(final XmlNodeReadOnlyTrx newRtx, final XmlNodeReadOnlyTrx oldRtx, final DiffDepth depth,
      final DiffFactory.DiffType diff) {
    if (newRtx.isElement() || oldRtx.isElement()) {
      if (diff == DiffFactory.DiffType.UPDATED) {
        // Emit diffing using LongOpenHashSet for O(1) set operations instead of O(n*m) ArrayList.removeAll.
        final long newNodeKey = newRtx.getNodeKey();
        final long oldNodeKey = oldRtx.getNodeKey();

        final List<Long> newNsKeys = newRtx.getNamespaceKeys();
        final List<Long> oldNsKeys = oldRtx.getNamespaceKeys();
        final LongOpenHashSet oldNsSet = new LongOpenHashSet(oldNsKeys);
        final LongOpenHashSet newNsSet = new LongOpenHashSet(newNsKeys);

        // Inserted namespaces: in new but not in old
        for (final long nsp : newNsKeys) {
          if (!oldNsSet.contains(nsp)) {
            newRtx.moveTo(nsp);
            fireDiff(DiffFactory.DiffType.INSERTED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
          }
        }
        newRtx.moveTo(newNodeKey);

        // Removed namespaces: in old but not in new
        for (final long nsp : oldNsKeys) {
          if (!newNsSet.contains(nsp)) {
            oldRtx.moveTo(nsp);
            fireDiff(DiffFactory.DiffType.DELETED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
          }
        }
        oldRtx.moveTo(oldNodeKey);

        final List<Long> newAttrKeys = newRtx.getAttributeKeys();
        final List<Long> oldAttrKeys = oldRtx.getAttributeKeys();
        final LongOpenHashSet oldAttrSet = new LongOpenHashSet(oldAttrKeys);
        final LongOpenHashSet newAttrSet = new LongOpenHashSet(newAttrKeys);

        // Inserted attributes: in new but not in old
        for (final long attribute : newAttrKeys) {
          if (!oldAttrSet.contains(attribute)) {
            newRtx.moveTo(attribute);
            fireDiff(DiffFactory.DiffType.INSERTED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
          }
        }
        newRtx.moveTo(newNodeKey);

        // Removed attributes: in old but not in new
        for (final long attribute : oldAttrKeys) {
          if (!newAttrSet.contains(attribute)) {
            oldRtx.moveTo(attribute);
            fireDiff(DiffFactory.DiffType.DELETED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
          }
        }
        oldRtx.moveTo(oldNodeKey);

        // Emit same: namespaces in both old and new
        for (final long nsp : newNsKeys) {
          if (oldNsSet.contains(nsp)) {
            newRtx.moveTo(nsp);
            oldRtx.moveTo(nsp);
            if (newRtx.getName().equals(oldRtx.getName())) {
              fireDiff(DiffFactory.DiffType.SAME, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
            } else {
              fireDiff(DiffFactory.DiffType.UPDATED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
            }
          }
        }
        newRtx.moveTo(newNodeKey);
        oldRtx.moveTo(oldNodeKey);

        // Emit same: attributes in both old and new
        for (final long attr : newAttrKeys) {
          if (oldAttrSet.contains(attr)) {
            newRtx.moveTo(attr);
            oldRtx.moveTo(attr);
            if (checkNamesForEquality(newRtx, oldRtx) && Objects.equals(newRtx.getValue(), oldRtx.getValue())) {
              fireDiff(DiffFactory.DiffType.SAME, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
            } else {
              fireDiff(DiffFactory.DiffType.UPDATED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
            }
          }
        }

        // Move back to original element nodes.
        newRtx.moveTo(newNodeKey);
        oldRtx.moveTo(oldNodeKey);
      } else if (diff == DiffFactory.DiffType.SAME) {
        for (int i = 0, nspCount = newRtx.getNamespaceCount(); i < nspCount; i++) {
          newRtx.moveToNamespace(i);
          final long newNodeKey = newRtx.getNodeKey();
          oldRtx.moveTo(newNodeKey);

          fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          newRtx.moveToParent();
          oldRtx.moveToParent();
        }

        for (int i = 0, attCount = newRtx.getAttributeCount(); i < attCount; i++) {
          newRtx.moveToAttribute(i);
          final long newNodeKey = newRtx.getNodeKey();
          oldRtx.moveTo(newNodeKey);

          fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          newRtx.moveToParent();
          oldRtx.moveToParent();
        }
      } else if (diff == DiffFactory.DiffType.DELETED) {
        for (int i = 0, nspCount = oldRtx.getNamespaceCount(); i < nspCount; i++) {
          oldRtx.moveToNamespace(i);

          fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          oldRtx.moveToParent();
        }

        for (int i = 0, attCount = oldRtx.getAttributeCount(); i < attCount; i++) {
          oldRtx.moveToAttribute(i);

          fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          oldRtx.moveToParent();
        }
      } else if (diff == DiffFactory.DiffType.INSERTED) {
        for (int i = 0, nspCount = newRtx.getNamespaceCount(); i < nspCount; i++) {
          newRtx.moveToNamespace(i);

          fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          newRtx.moveToParent();
        }

        for (int i = 0, attCount = newRtx.getAttributeCount(); i < attCount; i++) {
          newRtx.moveToAttribute(i);

          fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          newRtx.moveToParent();
        }
      }
    }
  }

  @Override
  boolean checkNodeNamesOrValues(final XmlNodeReadOnlyTrx newRtx, final XmlNodeReadOnlyTrx oldRtx) {
    if (newRtx.getKind() != oldRtx.getKind()) {
      return false;
    }
    return switch (newRtx.getKind()) {
      case ELEMENT, PROCESSING_INSTRUCTION -> checkNamesForEquality(newRtx, oldRtx);
      case TEXT, COMMENT -> newRtx.getValue().equals(oldRtx.getValue());
      default -> false;
    };
  }

  private boolean checkNamesForEquality(final XmlNodeReadOnlyTrx newRtx, final XmlNodeReadOnlyTrx oldRtx) {
    return newRtx.getURIKey() == oldRtx.getURIKey() && newRtx.getLocalNameKey() == oldRtx.getLocalNameKey()
        && newRtx.getPrefixKey() == oldRtx.getPrefixKey();
  }
}
