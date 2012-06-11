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

import static com.google.common.base.Preconditions.checkNotNull;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.MouseEvent;

import javax.swing.JComponent;
import javax.swing.JMenuBar;

import org.treetank.gui.view.controls.IControl;
import processing.core.PApplet;

/**
 * Processing view base class used for composition.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class ProcessingEmbeddedView {

  /** {@link IProcessingGUI} reference. */
  private final IProcessingGUI mProcessingGUI;

  /** {@link ViewNotifier} reference. */
  private final ViewNotifier mNotifier;

  /** {@link IControl} implementation. */
  private final IControl mControl;

  /** {@link IView} implementation. */
  private final IView mView;

  /** Processing view. */
  private final PApplet mApplet;

  /** {@link ProcessingEmbeddedView} reference. */
  private static ProcessingEmbeddedView mEmbeddedView;

  /**
   * Constructor.
   * 
   * @param paramProcessingGUI
   *          {@link IProcessingGUI} implementation
   * @param paramControl
   *          {@link IControl} implementation
   * @param paramViewNotifier
   *          {@link ViewNotifier} reference
   */
  private ProcessingEmbeddedView(final PApplet pApplet, final IView paramView,
    final IProcessingGUI paramProcessingGUI, final IControl paramControl, final ViewNotifier paramViewNotifier) {
    mApplet = pApplet;
    mView = paramView;
    mProcessingGUI = paramProcessingGUI;
    mControl = paramControl;
    mNotifier = paramViewNotifier;
  }

  /**
   * Get an instance, usually the same instance until the view resets everything.
   * 
   * @param paramView
   *          the implementing {@link IView} instance, which embeds the processing view
   * @param paramProcessingGUI
   *          {@link IProcessingGUI} implementation
   * @param paramControl
   *          {@link IControl} implementation
   * @param paramViewNotifier
   *          {@link ViewNotifier} reference
   * @return {@link ProcessingEmbeddedView} singleton
   */
  public static synchronized ProcessingEmbeddedView
    getInstance(final PApplet pApplet, final IView paramView, final IProcessingGUI paramProcessingGUI,
      final IControl paramControl, final ViewNotifier paramViewNotifier) {
    checkNotNull(pApplet);
    checkNotNull(paramView);
    checkNotNull(paramProcessingGUI);
    checkNotNull(paramControl);
    checkNotNull(paramViewNotifier);
    if (mEmbeddedView == null) {
      mEmbeddedView =
        new ProcessingEmbeddedView(pApplet, paramView, paramProcessingGUI, paramControl, paramViewNotifier);
    }
    return mEmbeddedView;
  }

  /** Reset the view. */
  public void resetEmbedded() {
    mEmbeddedView = null;
  }

  /** {@inheritDoc} */
  public void draw() {
    if (mProcessingGUI != null) {
      mProcessingGUI.draw();
      handleHLWeight();
    }
  }

  /** {@inheritDoc} */
  public void mouseEntered(final MouseEvent paramEvent) {
    if (mControl != null) {
      mControl.mouseEntered(paramEvent);
      handleHLWeight();
    }
  }

  /** {@inheritDoc} */
  public void mouseExited(final MouseEvent paramEvent) {
    if (mControl != null) {
      mControl.mouseExited(paramEvent);
      handleHLWeight();
    }
  }

  /** {@inheritDoc} */
  public void keyReleased() {
    if (mControl != null) {
      mControl.keyReleased();
      handleHLWeight();
    }
  }

  /** {@inheritDoc} */
  public void mousePressed(final MouseEvent paramEvent) {
    if (mControl != null) {
      mControl.mousePressed(paramEvent);
      handleHLWeight();
    }
  }

  //
  // /** Refresh. Thus Treetank storage has been updated to a new revision. */
  // public void refresh() {
  // mNotifier.update(mView, Opti);
  // }

  /** Update the GUI. */
  public void updateGUI() {
    if (mView != null) {
      final Dimension dim = mView.component().getSize();
      mApplet.resize(dim.width - 2, dim.height);
    }
    if (mProcessingGUI != null) {
      mProcessingGUI.update();
      mProcessingGUI.relocate();
    }
  }

  /**
   * Handle mix of heavyweight ({@link PApplet}) and leightweight ( {@link JMenuBar}) components.
   */
  public void handleHLWeight() {
    final Container parent = mView.component().getParent();
    if (parent instanceof JComponent) {
      ((JComponent)parent).revalidate();
    }
    // final Window window = SwingUtilities.getWindowAncestor(this);
    // if (window != null) {
    // window.validate();
    // }
  }

  /**
   * Provide hovering mechanisms for the current {@link IVisualItem} implementation.
   * 
   * @param paramItem
   *          {@link IVisualItem} implementation
   */
  public void hover(final IVisualItem paramItem) {
    mView.hover(paramItem);
  }
}
