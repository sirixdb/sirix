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

package org.sirix.gui.view.tree;

import com.google.common.base.Optional;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ComponentListener;
import java.awt.event.ContainerListener;
import java.awt.event.FocusListener;

import javax.swing.JComponent;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.TreeSelectionModel;

import org.sirix.gui.GUI;
import org.sirix.gui.GUIProp;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.VisualItemAxis;
import org.sirix.gui.view.IView;
import org.sirix.gui.view.IVisualItem;
import org.sirix.gui.view.ViewNotifier;
import org.sirix.node.interfaces.INode;

/**
 * <h1>TreeView</h1>
 * 
 * <p>
 * Tree view on a sirix storage.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class TreeView extends JScrollPane implements IView {

  /**
   * SerialUID.
   */
  private static final long serialVersionUID = 5191158290313970043L;

  /** Name of the view. */
  private static final String NAME = "TreeView";

  /** Row height. */
  private static final int ROW_HEIGHT = 20;

  /** {@link TreeView} instance. */
  private static TreeView mView;

  /** A {@link JTree} instance. */
  private final JTree mTree;

  /** {@link ViewNotifier}, which notifies views of changes. */
  private final ViewNotifier mNotifier;

  /** Main {@link GUI} window. */
  private final GUI mGUI;

  /**
   * Private Constructor, called from singleton factory method.
   * 
   * @param paramNotifier
   *          {@link ViewNotifier} to notify views of changes etc.pp.
   */
  private TreeView(final ViewNotifier paramNotifier) {
    mNotifier = paramNotifier;
    mGUI = paramNotifier.getGUI();

    // Add view to notifier.
    mNotifier.add(this);

    // Build tree view.
    mTree = new Tree(null);
    mTree.setBackground(Color.WHITE);

    /*
     * Performance tweak to use FixedLayoutManager and only invoke
     * getChild(..) for nodes inside view "bounding box". Avoids caching but
     * therefore more rendering calls.
     */
    mTree.setRowHeight(ROW_HEIGHT);
    mTree.setLargeModel(true);

    // Selection Model.
    mTree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);

    // Add the tree to the scroll pane.
    setViewportView(mTree);
    setBackground(Color.WHITE);
  }

  /**
   * Singleton factory.
   * 
   * @param paramNotifier
   *          {@link ViewNotifier} to notify views of changes etc.pp.
   * @return {@link TreeView} instance.
   */
  public static synchronized TreeView getInstance(final ViewNotifier paramNotifier) {
    if (mView == null) {
      mView = new TreeView(paramNotifier);
    }

    return mView;
  }

  /**
   * Not supported.
   * 
   * @see Object#clone()
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  @Override
  public boolean isVisible() {
    return GUIProp.EShowViews.SHOWTREE.getValue();
  }

  @Override
  public String name() {
    return NAME;
  };

  @Override
  public JComponent component() {
    return this;
  }

  @Override
  public void refreshUpdate(final Optional<VisualItemAxis> pAxis) {
    // Use our sirix model and renderer.
    dispose();
    final ReadDB db = mGUI.getReadDB();
    mTree.setModel(new TreeModel(db));
    mTree.setCellRenderer(new TreeCellRenderer(db));

    if (mTree.getTreeSelectionListeners().length == 0) {
      // Listen for when the selection changes.
      mTree.addTreeSelectionListener(new TreeSelectionListener() {
        @Override
        public void valueChanged(final TreeSelectionEvent paramE) {
          if (paramE.getNewLeadSelectionPath() != null
            && paramE.getNewLeadSelectionPath() != paramE.getOldLeadSelectionPath()) {
            /*
             * Returns the last path element of the selection. This
             * method is useful only when the selection model allows
             * a single selection.
             */
            final INode node = (INode)paramE.getNewLeadSelectionPath().getLastPathComponent();
            db.setKey(node.getNodeKey());
            mNotifier.update(mView, Optional.<VisualItemAxis> absent());
          }
        }
      });
    }
  }

  @Override
  public void dispose() {
    mTree.clearSelection();
    mTree.setModel(null);
    mTree.setCellRenderer(null);
    for (final FocusListener listener : mTree.getFocusListeners()) {
      mTree.removeFocusListener(listener);
    }
    for (final ComponentListener listener : mTree.getComponentListeners()) {
      mTree.removeComponentListener(listener);
    }
    for (final TreeSelectionListener listener : mTree.getTreeSelectionListeners()) {
      mTree.removeTreeSelectionListener(listener);
    }
    for (final ContainerListener listener : mTree.getContainerListeners()) {
      mTree.removeContainerListener(listener);
    }
  }

  @Override
  public void refreshInit() {
    refreshUpdate(Optional.<VisualItemAxis> absent());
  }

  @Override
  public Dimension getPreferredSize() {
    assert mGUI != null;
    final Dimension mainFrame = mGUI.getSize();
    return new Dimension(mainFrame.width - 16, mainFrame.height - mGUI.getJMenuBar().getHeight() - 37);
  }

  @Override
  public void hover(final IVisualItem pItem) {
  }

  @Override
  public void resize() {
  }
}
