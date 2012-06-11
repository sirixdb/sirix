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

package org.treetank.gui.view.sunburst;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;

import org.treetank.api.INodeWriteTrx;
import org.treetank.exception.AbsTTException;
import org.treetank.gui.view.VisualItemAxis;
import org.treetank.gui.view.model.AbsModel;
import org.treetank.gui.view.sunburst.SunburstView.Embedded;
import org.treetank.service.xml.shredder.EInsert;

import processing.core.PApplet;
import com.google.common.base.Optional;
import controlP5.ControlGroup;

/**
 * Menu options.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
enum EMenu {
  /** Insert XML fragment as first child. */
  INSERT_FRAGMENT_AS_FIRST_CHILD {
    @Override
    void createMenuItem(final AbsModel<?, ?> pModel, final JPopupMenu pMenu, final INodeWriteTrx pWtx,
      final ControlGroup<?> pCtrl) {
      // Create and add a menu item
      final JMenuItem item = new JMenuItem("insert as first child");
      item.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(final ActionEvent pEvent) {
          pCtrl.setVisible(true);
          pCtrl.open();
          pModel.setInsert(EInsert.ASFIRSTCHILD);
        }
      });
      pMenu.add(item);

    }
  },

  /** Insert XML fragment as right sibling. */
  INSERT_FRAGMENT_AS_RIGHT_SIBLING {
    @Override
    void createMenuItem(final AbsModel<?, ?> pModel, final JPopupMenu pMenu, final INodeWriteTrx pWtx,
      final ControlGroup<?> pCtrl) {
      // Create and add a menu item
      final JMenuItem item = new JMenuItem("insert as right sibling");
      item.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(final ActionEvent pEvent) {
          pCtrl.setVisible(true);
          pCtrl.open();
          pModel.setInsert(EInsert.ASRIGHTSIBLING);
        }
      });
      pMenu.add(item);
    }
  },

  /** Delete node. */
  DELETE {
    @Override
    void createMenuItem(final AbsModel<?, ?> pModel, final JPopupMenu pMenu, final INodeWriteTrx pWtx,
      final ControlGroup<?> pCtrl) {
      // Create and add a menu item
      final JMenuItem item = new JMenuItem("delete node");
      item.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(final ActionEvent pEvent) {
          delete(pModel.getParent(), pWtx);
        }
      });
      pMenu.add(item);
    }

    /**
     * Delete the current node, and it's subtree.
     * 
     * @p pParent {@link PApplet} instance
     * @p p {@link INodeWriteTrx} instance
     */
    private void delete(final PApplet pParent, final INodeWriteTrx pWtx) {
      try {
        pWtx.remove();
        pWtx.commit();
        pWtx.close();
        ((Embedded)pParent).refresh(Optional.<VisualItemAxis> absent());
      } catch (final AbsTTException e) {
        JOptionPane.showMessageDialog(pParent, "Failed to delete node: " + e.getMessage());
      }
    }
  };

  /**
   * Create a menu item.
   * 
   * @p pModel
   *    the model
   * @p pMenu {@link JPopupMenu} reference
   * @p pWtx {@link INodeWriteTrx} reference to delete a subtree
   * @p pCtrl {@link ControlGroup} to add XML fragments
   */
  abstract void createMenuItem(final AbsModel<?, ?> pModel, final JPopupMenu pMenu, final INodeWriteTrx pWtx,
    final ControlGroup<?> pCtrl);
}
