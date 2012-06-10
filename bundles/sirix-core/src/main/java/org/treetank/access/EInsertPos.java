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
package org.treetank.access;

import javax.xml.namespace.QName;

import org.treetank.api.INodeReadTrx;
import org.treetank.api.INodeWriteTrx;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ENode;
import org.treetank.node.TextNode;
import org.treetank.node.interfaces.IStructNode;
import org.treetank.settings.EFixed;

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
    void processMove(final IStructNode pFromNode, final IStructNode pToNode, final NodeWriteTrx pWtx)
      throws AbsTTException {
      assert pFromNode != null;
      assert pToNode != null;
      assert pWtx != null;

      // Adapt childCount of parent where the subtree has to be inserted.
      IStructNode newParent =
        (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pToNode.getNodeKey());
      if (pFromNode.getParentKey() != pToNode.getNodeKey()) {
        newParent.incrementChildCount();
      }
      pWtx.getPageTransaction().finishNodeModification(newParent);

      if (pToNode.hasFirstChild()) {
        pWtx.moveTo(pToNode.getFirstChildKey());

        if (pWtx.getNode().getKind() == ENode.TEXT_KIND && pFromNode.getKind() == ENode.TEXT_KIND) {
          final StringBuilder builder = new StringBuilder(pWtx.getValueOfCurrentNode());

          // Adapt right sibling key of moved node.
          pWtx.moveTo(((TextNode)pWtx.getNode()).getRightSiblingKey());
          final TextNode moved =
            (TextNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
          moved.setRightSiblingKey(pWtx.getNode().getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(moved);

          // Merge text nodes.
          pWtx.moveTo(moved.getNodeKey());
          builder.insert(0, pWtx.getValueOfCurrentNode() + " ");
          pWtx.setValue(builder.toString());

          // Remove first child.
          pWtx.moveTo(pToNode.getFirstChildKey());
          pWtx.remove();

          // Adapt left sibling key of former right sibling of first child.
          pWtx.moveTo(moved.getRightSiblingKey());
          final IStructNode rightSibling =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pWtx.getNode().getNodeKey());
          rightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(rightSibling);
        } else {
          // Adapt left sibling key of former first child.
          final IStructNode oldFirstChild =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pToNode.getFirstChildKey());
          oldFirstChild.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(oldFirstChild);

          // Adapt right sibling key of moved node.
          final IStructNode moved =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
          moved.setRightSiblingKey(oldFirstChild.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(moved);
        }
      } else {
        // Adapt right sibling key of moved node.
        final IStructNode moved =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
        moved.setRightSiblingKey(EFixed.NULL_NODE_KEY.getStandardProperty());
        pWtx.getPageTransaction().finishNodeModification(moved);
      }

      // Adapt first child key of parent where the subtree has to be inserted.
      newParent = (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pToNode.getNodeKey());
      newParent.setFirstChildKey(pFromNode.getNodeKey());
      pWtx.getPageTransaction().finishNodeModification(newParent);

      // Adapt left sibling key and parent key of moved node.
      final IStructNode moved =
        (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
      moved.setLeftSiblingKey(EFixed.NULL_NODE_KEY.getStandardProperty());
      moved.setParentKey(pToNode.getNodeKey());
      pWtx.getPageTransaction().finishNodeModification(moved);
    }

    @Override
    void insertNode(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) throws AbsTTException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == ENode.ELEMENT_KIND || pWtx.getNode().getKind() == ENode.ROOT_KIND;
      switch (pRtx.getNode().getKind()) {
      case ELEMENT_KIND:
        pWtx.insertElementAsFirstChild(pRtx.getQNameOfCurrentNode());
        break;
      case TEXT_KIND:
        assert pWtx.getStructuralNode().getKind() == ENode.ELEMENT_KIND;
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
    void processMove(final IStructNode pFromNode, final IStructNode pToNode, final NodeWriteTrx pWtx)
      throws AbsTTException {
      assert pFromNode != null;
      assert pToNode != null;
      assert pWtx != null;

      // Increment child count of parent node if moved node was not a child before.
      if (pFromNode.getParentKey() != pToNode.getParentKey()) {
        final IStructNode parentNode =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pToNode.getParentKey());
        parentNode.incrementChildCount();
        pWtx.getPageTransaction().finishNodeModification(parentNode);
      }

      final boolean hasMoved = pWtx.moveTo(pToNode.getRightSiblingKey());

      if (pFromNode.getKind() == ENode.TEXT_KIND && pToNode.getKind() == ENode.TEXT_KIND) {
        // Merge text: FROM and TO are of TEXT_KIND.
        pWtx.moveTo(pToNode.getNodeKey());
        final StringBuilder builder = new StringBuilder(pWtx.getValueOfCurrentNode()).append(" ");

        // Adapt left sibling key of former right sibling of first child.
        if (pToNode.hasRightSibling()) {
          final IStructNode rightSibling =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(
              ((TextNode)pWtx.getNode()).getRightSiblingKey());
          rightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(rightSibling);
        }

        // Adapt sibling keys of moved node.
        final TextNode movedNode =
          (TextNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
        movedNode.setRightSiblingKey(pToNode.getRightSiblingKey());
        // Adapt left sibling key of moved node.
        movedNode.setLeftSiblingKey(((TextNode)pWtx.getNode()).getLeftSiblingKey());
        pWtx.getPageTransaction().finishNodeModification(movedNode);

        // Merge text nodes.
        pWtx.moveTo(movedNode.getNodeKey());
        builder.append(pWtx.getValueOfCurrentNode());
        pWtx.setValue(builder.toString());

        final IStructNode insertAnchor =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pToNode.getNodeKey());
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(insertAnchor);

        // Remove first child.
        pWtx.moveTo(pToNode.getNodeKey());
        pWtx.remove();
      } else if (hasMoved && pFromNode.getKind() == ENode.TEXT_KIND
        && pWtx.getNode().getKind() == ENode.TEXT_KIND) {
        // Merge text: RIGHT and FROM are of TEXT_KIND.
        final StringBuilder builder = new StringBuilder(pWtx.getValueOfCurrentNode());

        // Adapt left sibling key of former right sibling of first child.
        final IStructNode rightSibling =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pWtx.getNode().getNodeKey());
        rightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(rightSibling);

        // Adapt sibling keys of moved node.
        final TextNode movedNode =
          (TextNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
        movedNode.setRightSiblingKey(rightSibling.getNodeKey());
        movedNode.setLeftSiblingKey(pToNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(movedNode);

        // Merge text nodes.
        pWtx.moveTo(movedNode.getNodeKey());
        builder.insert(0, pWtx.getValueOfCurrentNode() + " ");
        pWtx.setValue(builder.toString());

        // Remove right sibling.
        pWtx.moveTo(pToNode.getRightSiblingKey());
        pWtx.remove();

        final IStructNode insertAnchor =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pToNode.getNodeKey());
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(insertAnchor);
      } else {
        // No text merging involved.
        final IStructNode insertAnchor =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pToNode.getNodeKey());
        final long rightSiblKey = insertAnchor.getRightSiblingKey();
        // Adapt right sibling key of node where the subtree has to be inserted.
        insertAnchor.setRightSiblingKey(pFromNode.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(insertAnchor);

        if (rightSiblKey > -1) {
          // Adapt left sibling key of former right sibling.
          final IStructNode oldRightSibling =
            (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(rightSiblKey);
          oldRightSibling.setLeftSiblingKey(pFromNode.getNodeKey());
          pWtx.getPageTransaction().finishNodeModification(oldRightSibling);
        }
        // Adapt right- and left-sibling key of moved node.
        final IStructNode movedNode =
          (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
        movedNode.setRightSiblingKey(rightSiblKey);
        movedNode.setLeftSiblingKey(insertAnchor.getNodeKey());
        pWtx.getPageTransaction().finishNodeModification(movedNode);
      }

      // Adapt parent key of moved node.
      final IStructNode movedNode =
        (IStructNode)pWtx.getPageTransaction().prepareNodeForModification(pFromNode.getNodeKey());
      movedNode.setParentKey(pToNode.getParentKey());
      pWtx.getPageTransaction().finishNodeModification(movedNode);
    }

    @Override
    void insertNode(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) throws AbsTTException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == ENode.ELEMENT_KIND || pWtx.getNode().getKind() == ENode.TEXT_KIND;
      switch (pRtx.getNode().getKind()) {
      case ELEMENT_KIND:
        pWtx.insertElementAsRightSibling(pRtx.getQNameOfCurrentNode());
        break;
      case TEXT_KIND:
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
    void processMove(final IStructNode pFromNode, final IStructNode pToNode, final NodeWriteTrx pWtx)
      throws AbsTTException {
      // Not allowed.
      throw new AssertionError("May never be invoked!");
    }

    @Override
    void insertNode(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) throws AbsTTException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == ENode.ELEMENT_KIND;
      switch (pRtx.getNode().getKind()) {
      case NAMESPACE_KIND:
        final QName name = pRtx.getQNameOfCurrentNode();
        pWtx.insertNamespace(new QName(name.getNamespaceURI(), "", name.getLocalPart()));
        pWtx.moveToParent();
        break;
      case ATTRIBUTE_KIND:
        pWtx.insertAttribute(pRtx.getQNameOfCurrentNode(), pRtx.getValueOfCurrentNode());
        pWtx.moveToParent();
        break;
      default:
        throw new IllegalStateException("Only namespace- and attribute-nodes are permitted!");
      }
    }
  }, 
  
  ASLEFTSIBLING {
    @Override
    void processMove(final IStructNode pFromNode, final IStructNode pToNode, final NodeWriteTrx pWtx)
      throws AbsTTException {
      throw new UnsupportedOperationException();
    }

    @Override
    void insertNode(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) throws AbsTTException {
      assert pWtx != null;
      assert pRtx != null;
      assert pWtx.getNode().getKind() == ENode.ELEMENT_KIND || pWtx.getNode().getKind() == ENode.TEXT_KIND;
      switch (pRtx.getNode().getKind()) {
      case ELEMENT_KIND:
        pWtx.insertElementAsLeftSibling(pRtx.getQNameOfCurrentNode());
        break;
      case TEXT_KIND:
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
   * @throws AbsTTException
   *           if an I/O error occurs
   */
  abstract void processMove(final IStructNode pFromNode, final IStructNode pToNode, final NodeWriteTrx pWtx)
    throws AbsTTException;

  /**
   * Insert a node (copy operation).
   * 
   * @param pRtx
   *          read-transaction which implements the {@link INodeReadTrx} interface
   * @param pWtx
   *          write-transaction which implements the {@link INodeWriteTrx} interface
   * @throws AbsTTException
   *           if insertion of node fails
   */
  abstract void insertNode(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) throws AbsTTException;
}
