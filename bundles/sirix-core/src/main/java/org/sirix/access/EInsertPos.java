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
package org.sirix.access;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.EKind;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.page.EPage;
import org.sirix.settings.EFixed;

/**
 * Determines the position of the insertion of nodes and appropriate methods for movement and the copy of
 * whole subtrees.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
enum EInsertPos {
  /** Insert as first child. */
  ASFIRSTCHILD {
    @Override
    void processMove(final @Nonnull IStructNode pFromNode,
      final @Nonnull IStructNode pToNode, final @Nonnull NodeWriteTrx pWtx)
      throws SirixException {
      assert pFromNode != null;
      assert pToNode != null;
      assert pWtx != null;

      // Adapt childCount of parent where the subtree has to be inserted.
      IStructNode newParent =
        (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
          pToNode.getNodeKey(), EPage.NODEPAGE);
      if (pFromNode.getParentKey() != pToNode.getNodeKey()) {
        newParent.incrementChildCount();
      }
      pWtx.getPageTransaction().finishNodeModification(newParent,
        EPage.NODEPAGE);

      if (pToNode.hasFirstChild()) {
        pWtx.moveTo(pToNode.getFirstChildKey());

        if (pWtx.getNode().getKind() == EKind.TEXT
          && pFromNode.getKind() == EKind.TEXT) {
          final StringBuilder builder =
            new StringBuilder(pWtx.getValueOfCurrentNode());

          // Adapt right sibling key of moved node.
          pWtx.moveTo(((TextNode)pWtx.getNode()).getRightSiblingKey());
          final TextNode moved =
            (TextNode)pWtx.getPageTransaction().prepareNodeForModification(
              pFromNode.getNodeKey(), EPage.NODEPAGE);
          moved.setRightSiblingKey(pWtx.getNode().getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(moved,
            EPage.NODEPAGE);

          // Merge text nodes.
          pWtx.moveTo(moved.getNodeKey());
          builder.insert(0, pWtx.getValueOfCurrentNode());
          pWtx.setValue(builder.toString());

          // Remove first child.
          pWtx.moveTo(pToNode.getFirstChildKey());
          pWtx.remove();

          // Adapt left sibling key of former right sibling of first child.
          pWtx.moveTo(moved.getRightSiblingKey());
          final IStructNode rightSibling =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
              pWtx.getNode().getNodeKey(), EPage.NODEPAGE);
          rightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(rightSibling,
            EPage.NODEPAGE);
        } else {
          // Adapt left sibling key of former first child.
          final IStructNode oldFirstChild =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
              pToNode.getFirstChildKey(), EPage.NODEPAGE);
          oldFirstChild.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(oldFirstChild,
            EPage.NODEPAGE);

          // Adapt right sibling key of moved node.
          final IStructNode moved =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
              pFromNode.getNodeKey(), EPage.NODEPAGE);
          moved.setRightSiblingKey(oldFirstChild.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(moved,
            EPage.NODEPAGE);
        }
      } else {
        // Adapt right sibling key of moved node.
        final IStructNode moved =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
            pFromNode.getNodeKey(), EPage.NODEPAGE);
        moved.setRightSiblingKey(EFixed.NULL_NODE_KEY.getStandardProperty());
        pWtx.getPageTransaction().finishNodeModification(moved, EPage.NODEPAGE);
      }

      // Adapt first child key of parent where the subtree has to be inserted.
      newParent =
        (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
          pToNode.getNodeKey(), EPage.NODEPAGE);
      newParent.setFirstChildKey(pFromNode.getNodeKey());
      pWtx.getPageTransaction().finishNodeModification(newParent,
        EPage.NODEPAGE);

      // Adapt left sibling key and parent key of moved node.
      final IStructNode moved =
        (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
          pFromNode.getNodeKey(), EPage.NODEPAGE);
      moved.setLeftSiblingKey(EFixed.NULL_NODE_KEY.getStandardProperty());
      moved.setParentKey(pToNode.getNodeKey());
      pWtx.getPageTransaction().finishNodeModification(moved, EPage.NODEPAGE);
    }

    @Override
    void insertNode(final @Nonnull INodeWriteTrx pWtx,
      final @Nonnull INodeReadTrx pRtx) throws SirixException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == EKind.ELEMENT
        || pWtx.getNode().getKind() == EKind.DOCUMENT_ROOT;
      switch (pRtx.getNode().getKind()) {
      case ELEMENT:
        pWtx.insertElementAsFirstChild(pRtx.getQNameOfCurrentNode());
        break;
      case TEXT:
        assert pWtx.getStructuralNode().getKind() == EKind.ELEMENT;
        pWtx.insertTextAsFirstChild(pRtx.getValueOfCurrentNode());
        break;
      default:
        throw new IllegalStateException("Node type not known!");
      }

    }
  },
  /** Insert as right sibling. */
  ASRIGHTSIBLING {
    @Override
    void processMove(final @Nonnull IStructNode pFromNode,
      final @Nonnull IStructNode pToNode, final @Nonnull NodeWriteTrx pWtx)
      throws SirixException {
      assert pFromNode != null;
      assert pToNode != null;
      assert pWtx != null;

      // Increment child count of parent node if moved node was not a child before.
      if (pFromNode.getParentKey() != pToNode.getParentKey()) {
        final IStructNode parentNode =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
            pToNode.getParentKey(), EPage.NODEPAGE);
        parentNode.incrementChildCount();
        pWtx.getPageTransaction().finishNodeModification(parentNode,
          EPage.NODEPAGE);
      }

      final boolean hasMoved = pWtx.moveTo(pToNode.getRightSiblingKey());

      if (pFromNode.getKind() == EKind.TEXT && pToNode.getKind() == EKind.TEXT) {
        // Merge text: FROM and TO are of TEXT_KIND.
        pWtx.moveTo(pToNode.getNodeKey());
        final StringBuilder builder =
          new StringBuilder(pWtx.getValueOfCurrentNode());

        // Adapt left sibling key of former right sibling of first child.
        if (pToNode.hasRightSibling()) {
          final IStructNode rightSibling =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
              ((TextNode)pWtx.getNode()).getRightSiblingKey(), EPage.NODEPAGE);
          rightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(rightSibling,
            EPage.NODEPAGE);
        }

        // Adapt sibling keys of moved node.
        final TextNode movedNode =
          (TextNode)pWtx.getPageTransaction().prepareNodeForModification(
            pFromNode.getNodeKey(), EPage.NODEPAGE);
        movedNode.setRightSiblingKey(pToNode.getRightSiblingKey());
        // Adapt left sibling key of moved node.
        movedNode.setLeftSiblingKey(((TextNode)pWtx.getNode())
          .getLeftSiblingKey());
        pWtx.getPageTransaction().finishNodeModification(movedNode,
          EPage.NODEPAGE);

        // Merge text nodes.
        pWtx.moveTo(movedNode.getNodeKey());
        builder.append(pWtx.getValueOfCurrentNode());
        pWtx.setValue(builder.toString());

        final IStructNode insertAnchor =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
            pToNode.getNodeKey(), EPage.NODEPAGE);
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(insertAnchor,
          EPage.NODEPAGE);

        // Remove first child.
        pWtx.moveTo(pToNode.getNodeKey());
        pWtx.remove();
      } else if (hasMoved && pFromNode.getKind() == EKind.TEXT
        && pWtx.getNode().getKind() == EKind.TEXT) {
        // Merge text: RIGHT and FROM are of TEXT_KIND.
        final StringBuilder builder =
          new StringBuilder(pWtx.getValueOfCurrentNode());

        // Adapt left sibling key of former right sibling of first child.
        final IStructNode rightSibling =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
            pWtx.getNode().getNodeKey(), EPage.NODEPAGE);
        rightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(rightSibling,
          EPage.NODEPAGE);

        // Adapt sibling keys of moved node.
        final TextNode movedNode =
          (TextNode)pWtx.getPageTransaction().prepareNodeForModification(
            pFromNode.getNodeKey(), EPage.NODEPAGE);
        movedNode.setRightSiblingKey(rightSibling.getNodeKey());
        movedNode.setLeftSiblingKey(pToNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(movedNode,
          EPage.NODEPAGE);

        // Merge text nodes.
        pWtx.moveTo(movedNode.getNodeKey());
        builder.insert(0, pWtx.getValueOfCurrentNode());
        pWtx.setValue(builder.toString());

        // Remove right sibling.
        pWtx.moveTo(pToNode.getRightSiblingKey());
        pWtx.remove();

        final IStructNode insertAnchor =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
            pToNode.getNodeKey(), EPage.NODEPAGE);
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(insertAnchor,
          EPage.NODEPAGE);
      } else {
        // No text merging involved.
        final IStructNode insertAnchor =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
            pToNode.getNodeKey(), EPage.NODEPAGE);
        final long rightSiblKey = insertAnchor.getRightSiblingKey();
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(insertAnchor,
          EPage.NODEPAGE);

        if (rightSiblKey > -1) {
          // Adapt left sibling key of former right sibling.
          final IStructNode oldRightSibling =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
              rightSiblKey, EPage.NODEPAGE);
          oldRightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(oldRightSibling,
            EPage.NODEPAGE);
        }
        // Adapt right- and left-sibling key of moved node.
        final IStructNode movedNode =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
            pFromNode.getNodeKey(), EPage.NODEPAGE);
        movedNode.setRightSiblingKey(rightSiblKey);
        movedNode.setLeftSiblingKey(insertAnchor.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(movedNode,
          EPage.NODEPAGE);
      }

      // Adapt parent key of moved node.
      final IStructNode movedNode =
        (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
          pFromNode.getNodeKey(), EPage.NODEPAGE);
      movedNode.setParentKey(pToNode.getParentKey());
      pWtx.getPageTransaction().finishNodeModification(movedNode,
        EPage.NODEPAGE);
    }

    @Override
    void insertNode(final @Nonnull INodeWriteTrx pWtx,
      final @Nonnull INodeReadTrx pRtx) throws SirixException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == EKind.ELEMENT
        || pWtx.getNode().getKind() == EKind.TEXT;
      switch (pRtx.getNode().getKind()) {
      case ELEMENT:
        pWtx.insertElementAsRightSibling(pRtx.getQNameOfCurrentNode());
        break;
      case TEXT:
        pWtx.insertTextAsRightSibling(pRtx.getValueOfCurrentNode());
        break;
      default:
        throw new IllegalStateException("Node type not known!");
      }
    }
  },
  /** Insert as a non structural node. */
  ASNONSTRUCTURAL {
    @Override
    void processMove(final @Nonnull IStructNode pFromNode,
      final @Nonnull IStructNode pToNode, final @Nonnull NodeWriteTrx pWtx)
      throws SirixException {
      // Not allowed.
      throw new AssertionError("May never be invoked!");
    }

    @Override
    void insertNode(final @Nonnull INodeWriteTrx pWtx,
      final @Nonnull INodeReadTrx pRtx) throws SirixException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == EKind.ELEMENT;
      switch (pRtx.getNode().getKind()) {
      case NAMESPACE:
        final QName name = pRtx.getQNameOfCurrentNode();
        pWtx.insertNamespace(new QName(name.getNamespaceURI(), "", name
          .getLocalPart()));
        pWtx.moveToParent();
        break;
      case ATTRIBUTE:
        pWtx.insertAttribute(pRtx.getQNameOfCurrentNode(), pRtx
          .getValueOfCurrentNode());
        pWtx.moveToParent();
        break;
      default:
        throw new IllegalStateException(
          "Only namespace- and attribute-nodes are permitted!");
      }
    }
  },

  ASLEFTSIBLING {
    @Override
    void processMove(final @Nonnull IStructNode pFromNode,
      final @Nonnull IStructNode pToNode, final @Nonnull NodeWriteTrx pWtx)
      throws SirixException {
      throw new UnsupportedOperationException();
    }

    @Override
    void insertNode(@Nonnull final INodeWriteTrx pWtx,
      @Nonnull final INodeReadTrx pRtx) throws SirixException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == EKind.ELEMENT
        || pWtx.getNode().getKind() == EKind.TEXT;
      switch (pRtx.getNode().getKind()) {
      case ELEMENT:
        pWtx.insertElementAsLeftSibling(pRtx.getQNameOfCurrentNode());
        break;
      case TEXT:
        pWtx.insertTextAsLeftSibling(pRtx.getValueOfCurrentNode());
        break;
      default:
        throw new IllegalStateException("Node type not known!");
      }
    }
  };

  /**
   * Process movement of a subtree.
   * 
   * @param pFromNode
   *          root of subtree to move
   * @param pToNode
   *          determines where the subtree has to be inserted
   * @param pWtx
   *          write-transaction which implements the {@link INodeWriteTrx} interface
   * @throws SirixException
   *           if an I/O error occurs
   */
  abstract void processMove(@Nonnull final IStructNode pFromNode,
    @Nonnull final IStructNode pToNode, @Nonnull final NodeWriteTrx pWtx)
    throws SirixException;

  /**
   * Insert a node (copy operation).
   * 
   * @param pRtx
   *          read-transaction which implements the {@link INodeReadTrx} interface
   * @param pWtx
   *          write-transaction which implements the {@link INodeWriteTrx} interface
   * @throws SirixException
   *           if insertion of node fails
   */
  abstract void insertNode(@Nonnull final INodeWriteTrx pWtx,
    @Nonnull final INodeReadTrx pRtx) throws SirixException;
}
