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

package org.treetank.gui.view.tree;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Rectangle;

import javax.swing.JTree;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

/**
 * <h1>Tree</h1>
 * 
 * <p>
 * Provides highlighting functionality to highlight subtrees of selected nodes.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz.
 * 
 */
public final class Tree extends JTree {

  /** Generated UID. */
  private static final long serialVersionUID = -4157303763028056619L;

  /** Color used to highlight selected subtrees. */
  public static final Color HIGHLIGHT_COLOR = new Color(255, 255, 204);

  /** Highlight path. */
  private TreePath mSelectionPath;

  /**
   * Default Constructor.
   */
  public Tree() {
  }

  /**
   * Constructor with TreeModel peter.
   * 
   * @param model
   *          TreeModel to use.
   */
  public Tree(final TreeModel model) {
    super(model);
  }

  /**
   * Set the selection path.
   * 
   * @param pSelectionPath
   *          Selection path.
   */
  @Override
  public void setSelectionPath(final TreePath pSelectionPath) {
    mSelectionPath = pSelectionPath;
    getSelectionModel().setSelectionPath(pSelectionPath);
    treeDidChange();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TreePath getSelectionPath() {
    return mSelectionPath;
  }

  @Override
  protected void paintComponent(final Graphics pGraphics) {
    // Paint background ourself.
    pGraphics.setColor(getBackground());
    pGraphics.fillRect(0, 0, getWidth(), getHeight());

    // Paint the highlight if any.
    pGraphics.setColor(HIGHLIGHT_COLOR);
    final int fromRow = getRowForPath(mSelectionPath);

    if (fromRow != -1) {
      int toRow = fromRow;
      while (toRow < getRowCount()) {
        final TreePath path = getPathForRow(toRow);
        if (mSelectionPath.isDescendant(path)) {
          toRow++;
        } else {
          break;
        }
      }

      // Paint a rectangle.
      final Rectangle fromBounds = getRowBounds(fromRow);
      final Rectangle toBounds = getRowBounds(toRow - 1);
      pGraphics.fillRect(0, fromBounds.y, getWidth(), toBounds.y - fromBounds.y + toBounds.height);
    }

    setOpaque(false); // trick not to paint background
    super.paintComponent(pGraphics);
    setOpaque(true);
  }
}
