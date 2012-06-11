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

package org.treetank.gui.view;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Window;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;

import org.treetank.gui.GUI;

/**
 * Container for all views.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class ViewContainer extends JPanel {

  /**
   * SerialUID.
   */
  private static final long serialVersionUID = -9151769742021251809L;

  /** Container singleton instance. */
  private static ViewContainer mContainer;

  /** Main {@link GUI} reference. */
  private final GUI mGUI;

  /** All implementations of the {@link IView} instance. */
  private final List<IView> mViews;

  /**
   * Private constructor.
   * 
   * @param paramGUI
   *          the main {@link GUI} reference
   * @param paramView
   *          first {@link IView} implementation
   * @param paramViews
   *          {@link IView}s to layout
   */
  private ViewContainer(final GUI paramGUI, final IView paramView, final IView... paramViews) {
    mGUI = paramGUI;
    mViews = new LinkedList<IView>();
    mViews.add(paramView);
    mViews.addAll(Arrays.asList(paramViews));
    setLayout(new BorderLayout());
  }

  /**
   * Create a {@link ViewContainer} singleton instance.
   * 
   * paramView and paramViews parameters because at least one view must be specified (see Effective Java).
   * 
   * @param paramGUI
   *          the main {@link GUI} reference
   * @param paramView
   *          {@link IView} implementation
   * @param paramViews
   *          {@link IView} implementations to layout
   * @return {@link ViewContainer} singleton instance
   */
  public synchronized static ViewContainer getInstance(final GUI paramGUI, final IView paramView,
    final IView... paramViews) {
    if (mContainer == null) {
      mContainer = new ViewContainer(paramGUI, paramView, paramViews);
    }

    return mContainer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void revalidate() {
    super.revalidate();
  }

  /** Layout the views. */
  public void layoutViews() {
    removeAll();

    final List<IView> views = visibleViews();

    for (final IView view : views) {
      if (mGUI.getReadDB() != null) {
        view.refreshInit();
      }
    }

    JComponent tmpView = null;
    int width = 0;
    int i = 0;
    JSplitPane pane;
    for (final IView view : views) {
      if (views.size() == 1) {
        add(view.component(), BorderLayout.PAGE_START);
        tmpView = view.component();
        break;
      } else if (i % 2 != 0 && width < mGUI.getSize().width) {
        assert tmpView != null;
        pane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        if (tmpView instanceof IView) {
          tmpView = ((IView)tmpView).component();
        }
        setupPane(pane, tmpView, view.component());
        add(pane);
        tmpView = pane;
      } else if (i > 0) {
        assert tmpView != null;
        pane = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        setupPane(pane, tmpView, view.component());
        add(pane);
        tmpView = view.component();
      } else {
        tmpView = view.component();
      }

      i++;
      width += (mGUI.getWidth() / 2f);
    }

    super.revalidate();
    super.repaint();
  }

  /**
   * Setup of the {@link JSplitPane} reference.
   * 
   * @param paramPane
   *          {@link JSplitPane} reference
   * @param paramTmpView
   *          {@link JComponent} reference to the left
   * @param paramView
   *          {@link JComponent} reference to the right
   */
  private void
    setupPane(final JSplitPane paramPane, final JComponent paramTmpView, final JComponent paramView) {
    assert paramPane != null;
    assert paramTmpView != null;
    assert paramView != null;
    paramPane.setSize(new Dimension(mGUI.getWidth(), mGUI.getHeight()));
    paramPane.setAlignmentX(mGUI.getWidth() / 2f);
    paramPane.setAlignmentY(mGUI.getHeight() / 2f);
    paramPane.setDividerLocation(0.5);
    paramPane.setContinuousLayout(true);
    paramPane.setLeftComponent(paramTmpView);
    paramPane.setRightComponent(paramView);
    paramPane.addPropertyChangeListener(JSplitPane.DIVIDER_LOCATION_PROPERTY, new Listener(paramPane));
    final Container parent = paramPane.getParent();
    if (parent instanceof JComponent) {
      ((JComponent)parent).revalidate();
    }
    final Window window = SwingUtilities.getWindowAncestor(this);
    if (window != null) {
      window.validate();
    }
  }

  /**
   * Get list of visible views.
   * 
   * @return list of visible views
   */
  private List<IView> visibleViews() {
    final List<IView> views = new LinkedList<IView>();

    for (final IView view : mViews) {
      if (view.isVisible()) {
        views.add(view);
      }
    }

    return views;
  }

  /** Listener for JSplitPane events to resize AWT (Processing) views. */
  private static class Listener implements PropertyChangeListener {
    /** {@link JSplitPane} reference. */
    private JSplitPane mPane;

    /**
     * Constructor.
     * 
     * @param paramPane
     *          {@link JSplitPane} reference
     */
    Listener(final JSplitPane paramPane) {
      assert paramPane != null;
      mPane = paramPane;
    }

    /** {@inheritDoc} */
    @Override
    public void propertyChange(final PropertyChangeEvent paramEvent) {
      if (JSplitPane.DIVIDER_LOCATION_PROPERTY.equals(paramEvent.getPropertyName())) {
        for (final Component component : mPane.getComponents()) {
          if (component instanceof IView) {
            ((IView)component).resize();
          }
        }
      }
    }
  }
}
