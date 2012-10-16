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
package org.sirix.gui.view;

import static com.google.common.base.Preconditions.checkNotNull;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.MouseEvent;

import javax.swing.JComponent;
import javax.swing.JMenuBar;

import org.sirix.gui.view.controls.Control;
import org.sirix.gui.view.sunburst.AbsSunburstGUI.EResetZoomer;

import processing.core.PApplet;

/**
 * Processing view base class used for composition.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class ProcessingEmbeddedView {

  /** {@link ProcessingGUI} reference. */
  private final ProcessingGUI mProcessingGUI;

  /** {@link Control} implementation. */
  private final Control mControl;

  /** {@link View} implementation. */
  private final View mView;

  /** Processing view. */
  private final PApplet mApplet;

  /** {@link ProcessingEmbeddedView} reference. */
  private static ProcessingEmbeddedView mEmbeddedView;

  /**
   * Constructor.
   * 
   * @param pProcessingGUI
   *          {@link ProcessingGUI} implementation
   * @param pControl
   *          {@link Control} implementation
   * @param pViewNotifier
   *          {@link ViewNotifier} reference
   */
  private ProcessingEmbeddedView(final PApplet pApplet, final View pView,
    final ProcessingGUI pProcessingGUI, final Control pControl, final ViewNotifier pViewNotifier) {
    mApplet = pApplet;
    mView = pView;
    mProcessingGUI = pProcessingGUI;
    mControl = pControl;
  }

  /**
   * Get an instance, usually the same instance until the view resets everything.
   * 
   * @param pView
   *          the implementing {@link View} instance, which embeds the processing view
   * @param pProcessingGUI
   *          {@link ProcessingGUI} implementation
   * @param pControl
   *          {@link Control} implementation
   * @param pViewNotifier
   *          {@link ViewNotifier} reference
   * @return {@link ProcessingEmbeddedView} singleton
   */
  public static synchronized ProcessingEmbeddedView
    getInstance(final PApplet pApplet, final View pView, final ProcessingGUI pProcessingGUI,
      final Control pControl, final ViewNotifier pViewNotifier) {
    checkNotNull(pApplet);
    checkNotNull(pView);
    checkNotNull(pProcessingGUI);
    checkNotNull(pControl);
    checkNotNull(pViewNotifier);
    if (mEmbeddedView == null) {
      mEmbeddedView =
        new ProcessingEmbeddedView(pApplet, pView, pProcessingGUI, pControl, pViewNotifier);
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
  public void mouseEntered(final MouseEvent pEvent) {
    if (mControl != null) {
      mControl.mouseEntered(pEvent);
      handleHLWeight();
    }
  }

  /** {@inheritDoc} */
  public void mouseExited(final MouseEvent pEvent) {
    if (mControl != null) {
      mControl.mouseExited(pEvent);
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
  public void mousePressed(final MouseEvent pEvent) {
    if (mControl != null) {
      mControl.mousePressed(pEvent);
      handleHLWeight();
    }
  }

  /** Update the GUI. */
  public void updateGUI() {
    if (mView != null) {
      final Dimension dim = mView.component().getSize();
      mApplet.resize(dim.width - 2, dim.height);
    }
    if (mProcessingGUI != null) {
      mProcessingGUI.update(EResetZoomer.YES);
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
//		final Window window = SwingUtilities.getWindowAncestor(this);
//		if (window != null) {
//			window.validate();
//		}
  }

  /**
   * Provide hovering mechanisms for the current {@link VisualItem} implementation.
   * 
   * @param pItem
   *          {@link VisualItem} implementation
   */
  public void hover(final VisualItem pItem) {
    mView.hover(pItem);
  }
}
