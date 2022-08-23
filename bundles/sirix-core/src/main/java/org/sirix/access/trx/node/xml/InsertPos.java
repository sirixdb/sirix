/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.access.trx.node.xml;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.index.IndexType;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.xml.TextNode;
import org.sirix.settings.Fixed;

/**
 * Determines the position of the insertion of nodes and appropriate methods for movement and the
 * copy of whole subtrees.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public enum InsertPos {
  /**
   * Insert as first child.
   */
  ASFIRSTCHILD {
    @Override
    void processMove(final StructNode fromNode, final StructNode toNode, final XmlNodeTrx wtx) {
      assert fromNode != null;
      assert toNode != null;
      assert wtx != null;

      // Adapt childCount of parent where the subtree has to be inserted.
      StructNode newParent = wtx.getPageWtx().prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
      if (fromNode.getParentKey() != toNode.getNodeKey()) {
        newParent.incrementChildCount();
      }

      if (toNode.hasFirstChild()) {
        wtx.moveTo(toNode.getFirstChildKey());

        if (wtx.getKind() == NodeKind.TEXT && fromNode.getKind() == NodeKind.TEXT) {
          final StringBuilder builder = new StringBuilder(wtx.getValue());

          // Adapt right sibling key of moved node.
          wtx.moveTo(wtx.getRightSiblingKey());
          final TextNode moved =
              wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
          moved.setRightSiblingKey(wtx.getNodeKey());

          // Merge text nodes.
          wtx.moveTo(moved.getNodeKey());
          builder.insert(0, wtx.getValue());
          wtx.setValue(builder.toString());

          // Remove first child.
          wtx.moveTo(toNode.getFirstChildKey());
          wtx.remove();

          // Adapt left sibling key of former right sibling of first child.
          wtx.moveTo(moved.getRightSiblingKey());
          final StructNode rightSibling =
              wtx.getPageWtx().prepareRecordForModification(wtx.getNodeKey(), IndexType.DOCUMENT, -1);
          rightSibling.setLeftSiblingKey(fromNode.getNodeKey());
        } else {
          // Adapt left sibling key of former first child.
          final StructNode oldFirstChild =
              wtx.getPageWtx().prepareRecordForModification(toNode.getFirstChildKey(), IndexType.DOCUMENT, -1);
          oldFirstChild.setLeftSiblingKey(fromNode.getNodeKey());

          // Adapt right sibling key of moved node.
          final StructNode moved =
              wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
          moved.setRightSiblingKey(oldFirstChild.getNodeKey());
        }
      } else {
        // Adapt right sibling key of moved node.
        final StructNode moved =
            wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
        moved.setRightSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
      }

      // Adapt first child key of parent where the subtree has to be inserted.
      newParent = wtx.getPageWtx().prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
      newParent.setFirstChildKey(fromNode.getNodeKey());

      // Adapt left sibling key and parent key of moved node.
      final StructNode moved =
          wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
      moved.setLeftSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
      moved.setParentKey(toNode.getNodeKey());
    }

    @Override
    void insertNode(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
      assert wtx != null;
      assert rtx != null;
      assert wtx.getKind() == NodeKind.ELEMENT || wtx.getKind() == NodeKind.XML_DOCUMENT;
      switch (rtx.getKind()) {
        case ELEMENT:
          wtx.insertElementAsFirstChild(rtx.getName());
          break;
        case TEXT:
          assert wtx.getKind() == NodeKind.ELEMENT;
          wtx.insertTextAsFirstChild(rtx.getValue());
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node type not known!");
      }

    }
  },
  /**
   * Insert as right sibling.
   */
  ASRIGHTSIBLING {
    @Override
    void processMove(final StructNode fromNode, final StructNode toNode, final XmlNodeTrx wtx) {
      assert fromNode != null;
      assert toNode != null;
      assert wtx != null;

      // Increment child count of parent node if moved node was not a child
      // before.
      if (fromNode.getParentKey() != toNode.getParentKey()) {
        final StructNode parentNode =
            wtx.getPageWtx().prepareRecordForModification(toNode.getParentKey(), IndexType.DOCUMENT, -1);
        parentNode.incrementChildCount();
      }

      final boolean hasMoved = wtx.moveTo(toNode.getRightSiblingKey());

      if (fromNode.getKind() == NodeKind.TEXT && toNode.getKind() == NodeKind.TEXT) {
        // Merge text: FROM and TO are of TEXT_KIND.
        wtx.moveTo(toNode.getNodeKey());
        final StringBuilder builder = new StringBuilder(wtx.getValue());

        // Adapt left sibling key of former right sibling of first child.
        if (toNode.hasRightSibling()) {
          final StructNode rightSibling =
              wtx.getPageWtx().prepareRecordForModification(wtx.getRightSiblingKey(), IndexType.DOCUMENT, -1);
          rightSibling.setLeftSiblingKey(fromNode.getNodeKey());
        }

        // Adapt sibling keys of moved node.
        final TextNode movedNode =
            wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
        movedNode.setRightSiblingKey(toNode.getRightSiblingKey());
        // Adapt left sibling key of moved node.
        movedNode.setLeftSiblingKey(wtx.getLeftSiblingKey());

        // Merge text nodes.
        wtx.moveTo(movedNode.getNodeKey());
        builder.append(wtx.getValue());
        wtx.setValue(builder.toString());

        final StructNode insertAnchor =
            wtx.getPageWtx().prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(fromNode.getNodeKey());

        // Remove first child.
        wtx.moveTo(toNode.getNodeKey());
        wtx.remove();
      } else if (hasMoved && fromNode.getKind() == NodeKind.TEXT && wtx.getKind() == NodeKind.TEXT) {
        // Merge text: RIGHT and FROM are of TEXT_KIND.
        final StringBuilder builder = new StringBuilder(wtx.getValue());

        // Adapt left sibling key of former right sibling of first child.
        final StructNode rightSibling =
            wtx.getPageWtx().prepareRecordForModification(wtx.getNodeKey(), IndexType.DOCUMENT, -1);
        rightSibling.setLeftSiblingKey(fromNode.getNodeKey());

        // Adapt sibling keys of moved node.
        final TextNode movedNode =
            wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
        movedNode.setRightSiblingKey(rightSibling.getNodeKey());
        movedNode.setLeftSiblingKey(toNode.getNodeKey());

        // Merge text nodes.
        wtx.moveTo(movedNode.getNodeKey());
        builder.insert(0, wtx.getValue());
        wtx.setValue(builder.toString());

        // Remove right sibling.
        wtx.moveTo(toNode.getRightSiblingKey());
        wtx.remove();

        final StructNode insertAnchor =
            wtx.getPageWtx().prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(fromNode.getNodeKey());
      } else {
        // No text merging involved.
        final StructNode insertAnchor =
            wtx.getPageWtx().prepareRecordForModification(toNode.getNodeKey(), IndexType.DOCUMENT, -1);
        final long rightSiblKey = insertAnchor.getRightSiblingKey();
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(fromNode.getNodeKey());

        if (rightSiblKey > -1) {
          // Adapt left sibling key of former right sibling.
          final StructNode oldRightSibling =
              wtx.getPageWtx().prepareRecordForModification(rightSiblKey, IndexType.DOCUMENT, -1);
          oldRightSibling.setLeftSiblingKey(fromNode.getNodeKey());
        }
        // Adapt right- and left-sibling key of moved node.
        final StructNode movedNode =
            wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
        movedNode.setRightSiblingKey(rightSiblKey);
        movedNode.setLeftSiblingKey(insertAnchor.getNodeKey());
      }

      // Adapt parent key of moved node.
      final StructNode movedNode =
          wtx.getPageWtx().prepareRecordForModification(fromNode.getNodeKey(), IndexType.DOCUMENT, -1);
      movedNode.setParentKey(toNode.getParentKey());
    }

    @Override
    void insertNode(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
      assert wtx != null;
      assert rtx != null;
      assert wtx.getKind() == NodeKind.ELEMENT || wtx.getKind() == NodeKind.TEXT;
      switch (rtx.getKind()) {
        case ELEMENT:
          wtx.insertElementAsRightSibling(rtx.getName());
          break;
        case TEXT:
          wtx.insertTextAsRightSibling(rtx.getValue());
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node type not known!");
      }
    }
  },
  /**
   * Insert as a non structural node.
   */
  ASNONSTRUCTURAL {
    @Override
    void processMove(final StructNode fromNode, final StructNode toNode, final XmlNodeTrx wtx) {
      // Not allowed.
      throw new AssertionError("May never be invoked!");
    }

    @Override
    void insertNode(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
      assert wtx != null;
      assert rtx != null;
      assert wtx.getKind() == NodeKind.ELEMENT;
      switch (rtx.getKind()) {
        case NAMESPACE:
          final QNm name = rtx.getName();
          wtx.insertNamespace(new QNm(name.getNamespaceURI(), name.getLocalName(), ""));
          wtx.moveToParent();
          break;
        case ATTRIBUTE:
          wtx.insertAttribute(rtx.getName(), rtx.getValue());
          wtx.moveToParent();
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Only namespace- and attribute-nodes are permitted!");
      }
    }
  },

  ASLEFTSIBLING {
    @Override
    void processMove(final StructNode fromNode, final StructNode toNode, final XmlNodeTrx wtx) {
      throw new UnsupportedOperationException();
    }

    @Override
    void insertNode(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) {
      assert wtx != null;
      assert rtx != null;
      assert wtx.getKind() == NodeKind.ELEMENT || wtx.getKind() == NodeKind.TEXT;
      switch (rtx.getKind()) {
        case ELEMENT:
          wtx.insertElementAsLeftSibling(rtx.getName());
          break;
        case TEXT:
          wtx.insertTextAsLeftSibling(rtx.getValue());
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node type not known!");
      }
    }
  };

  /**
   * Process movement of a subtree.
   *
   * @param fromNode root of subtree to move
   * @param toNode   determines where the subtree has to be inserted
   * @param wtx      write-transaction which implements the {@link XmlNodeTrx} interface
   * @throws SirixException if an I/O error occurs
   */
  abstract void processMove(final StructNode fromNode, final StructNode toNode, final XmlNodeTrx wtx)
      throws SirixException;

  /**
   * Insert a node (copy operation).
   *
   * @param rtx read-transaction which implements the {@link XmlNodeReadOnlyTrx} interface
   * @param wtx write-transaction which implements the {@link XmlNodeTrx} interface
   * @throws SirixException if insertion of node fails
   */
  abstract void insertNode(final XmlNodeTrx wtx, final XmlNodeReadOnlyTrx rtx) throws SirixException;
}
