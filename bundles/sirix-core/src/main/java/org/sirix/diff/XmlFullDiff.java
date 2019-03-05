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

package org.sirix.diff;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.diff.DiffFactory.Builder;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.exception.SirixException;

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
   * @param builder {@link Builder} reference
   * @throws SirixException if anything goes wrong while setting up sirix transactions
   */
  XmlFullDiff(final Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder) throws SirixException {
    super(builder);
  }

  @Override
  boolean checkNodes(final XmlNodeReadOnlyTrx newRtx, final XmlNodeReadOnlyTrx oldRtx) {
    boolean found = false;
    if (newRtx.getNodeKey() == oldRtx.getNodeKey() && newRtx.getParentKey() == oldRtx.getParentKey()
        && newRtx.getKind() == oldRtx.getKind()) {
      switch (newRtx.getKind()) {
        case ELEMENT:
          if (newRtx.getPrefixKey() == oldRtx.getPrefixKey() && newRtx.getLocalNameKey() == oldRtx.getLocalNameKey()
              && newRtx.getAttributeKeys().equals(oldRtx.getAttributeKeys())
              && newRtx.getNamespaceKeys().equals(oldRtx.getNamespaceKeys())) {
            found = true;

            final long newNodeKey = newRtx.getNodeKey();
            final long oldNodeKey = oldRtx.getNodeKey();

            for (final long nsp : newRtx.getNamespaceKeys()) {
              newRtx.moveTo(nsp);
              oldRtx.moveTo(nsp);

              if (!(newRtx.getPrefixKey() == oldRtx.getPrefixKey()
                  && Objects.equals(newRtx.getNamespaceURI(), oldRtx.getNamespaceURI()))) {
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

                if (!(newRtx.getPrefixKey() == oldRtx.getPrefixKey()
                    && Objects.equals(newRtx.getNamespaceURI(), oldRtx.getNamespaceURI())
                    && newRtx.getLocalNameKey() == oldRtx.getLocalNameKey()
                    && Objects.equals(newRtx.getValue(), oldRtx.getValue()))) {
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
          found = newRtx.getValue().equals(oldRtx.getValue()) && newRtx.getName().equals(oldRtx.getName());
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
      final DiffType diff) {
    if (newRtx.isElement() || oldRtx.isElement()) {
      if (diff == DiffType.SAME) {
        for (int i = 0, nspCount = newRtx.getNamespaceCount(); i < nspCount; i++) {
          final long newNodeKey = newRtx.moveToNamespace(i).getCursor().getNodeKey();
          oldRtx.moveTo(newNodeKey);

          fireDiff(DiffType.SAME, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          newRtx.moveToParent();
          oldRtx.moveToParent();
        }

        for (int i = 0, attCount = newRtx.getAttributeCount(); i < attCount; i++) {
          final long newNodeKey = newRtx.moveToAttribute(i).getCursor().getNodeKey();
          oldRtx.moveTo(newNodeKey);

          fireDiff(DiffType.SAME, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);

          newRtx.moveToParent();
          oldRtx.moveToParent();
        }
      } else if (diff != DiffType.SAMEHASH) {
        // Emit diffing.
        final long newNodeKey = newRtx.getNodeKey();
        final long oldNodeKey = oldRtx.getNodeKey();

        final List<Long> insertedNamespaces = new ArrayList<>(newRtx.getNamespaceKeys());
        insertedNamespaces.removeAll(oldRtx.getNamespaceKeys());

        for (final long nsp : insertedNamespaces) {
          newRtx.moveTo(nsp);
          fireDiff(DiffType.INSERTED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        }
        newRtx.moveTo(newNodeKey);

        final List<Long> removedNamespaces = new ArrayList<>(oldRtx.getNamespaceKeys());
        removedNamespaces.removeAll(newRtx.getNamespaceKeys());

        for (final long nsp : removedNamespaces) {
          oldRtx.moveTo(nsp);
          fireDiff(DiffType.DELETED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        }
        oldRtx.moveTo(oldNodeKey);

        final List<Long> insertedAttributes = new ArrayList<>(newRtx.getAttributeKeys());
        insertedAttributes.removeAll(oldRtx.getAttributeKeys());

        for (final long attribute : insertedAttributes) {
          newRtx.moveTo(attribute);
          fireDiff(DiffType.INSERTED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        }
        newRtx.moveTo(newNodeKey);

        final List<Long> removedAttributes = new ArrayList<>(oldRtx.getAttributeKeys());
        removedAttributes.removeAll(newRtx.getAttributeKeys());

        for (final long attribute : removedAttributes) {
          oldRtx.moveTo(attribute);
          fireDiff(DiffType.DELETED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        }
        oldRtx.moveTo(oldNodeKey);

        // Emit same.
        final List<Long> sameNamespaces = new ArrayList<>(newRtx.getNamespaceKeys());
        sameNamespaces.retainAll(oldRtx.getNamespaceKeys());

        for (final long nsp : sameNamespaces) {
          newRtx.moveTo(nsp);
          oldRtx.moveTo(nsp);

          if (newRtx.getPrefixKey() == oldRtx.getPrefixKey()
              && Objects.equals(newRtx.getNamespaceURI(), oldRtx.getNamespaceURI()))
            fireDiff(DiffType.SAME, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
          else
            fireDiff(DiffType.UPDATED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        }
        newRtx.moveTo(newNodeKey);
        oldRtx.moveTo(oldNodeKey);

        final List<Long> sameAttributes = new ArrayList<>(newRtx.getAttributeKeys());
        sameAttributes.retainAll(oldRtx.getAttributeKeys());

        for (final long attr : sameAttributes) {
          newRtx.moveTo(attr);
          oldRtx.moveTo(attr);

          if (newRtx.getPrefixKey() == oldRtx.getPrefixKey()
              && Objects.equals(newRtx.getNamespaceURI(), oldRtx.getNamespaceURI())
              && newRtx.getLocalNameKey() == oldRtx.getLocalNameKey()
              && Objects.equals(newRtx.getValue(), oldRtx.getValue()))
            fireDiff(DiffType.SAME, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
          else
            fireDiff(DiffType.UPDATED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        }

        // Move back to original element nodes.
        newRtx.moveTo(newNodeKey);
        oldRtx.moveTo(oldNodeKey);
      }
    }
  }
}
