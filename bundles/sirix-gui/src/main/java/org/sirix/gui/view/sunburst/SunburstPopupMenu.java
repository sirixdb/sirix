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

package org.sirix.gui.view.sunburst;

import javax.swing.JPopupMenu;

import org.sirix.api.INodeWriteTrx;
import org.sirix.gui.view.model.AbsModel;
import org.sirix.gui.view.model.interfaces.IModel;

import controlP5.ControlGroup;

/**
 * Sunburst PopupMenu to insert and delete nodes.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstPopupMenu extends JPopupMenu {

  /**
   * SerialversionUID.
   */
  private static final long serialVersionUID = -3346902332490335444L;

  /** Model which implements {@link IModel}. */
  private final AbsModel<?, ?> mModel;

  /** sirix {@link INodeWriteTrx}. */
  private static INodeWriteTrx mWtx;

  /** Textarea for XML fragment input. */
  private final ControlGroup<?> mCtrl;

  /** Instance of this class. */
  private static SunburstPopupMenu mSunburstPopupMenu;

  /**
   * Private constructor.
   * 
   * @param paramModel
   *          model which implements {@link IModel}
   * @param paramWtx
   *          sirix {@link INodeWriteTrx}
   * @param paramCtrl
   *          control group for XML input
   */
  private SunburstPopupMenu(final AbsModel<?, ?> paramModel, final INodeWriteTrx paramWtx,
    final ControlGroup<?> paramCtrl) {
    mModel = paramModel;
    mWtx = paramWtx;
    mCtrl = paramCtrl;

    switch (mWtx.getNode().getKind()) {
    case ELEMENT_KIND:
      createMenu();
      break;
    case TEXT_KIND:
      EMenu.INSERT_FRAGMENT_AS_RIGHT_SIBLING.createMenuItem(mModel, this, mWtx, mCtrl);
      EMenu.DELETE.createMenuItem(mModel, this, mWtx, mCtrl);
      break;
    }
  }

  /**
   * Singleton factory.
   * 
   * @param paramModel
   *          the {@link AbsModel} model
   * @param paramGUI
   *          {@link SunburstGUI} instance
   * @param paramWtx
   *          sirix {@link INodeWriteTrx}
   * @param paramCtrl
   *          control group for XML input
   * @return singleton {@link SunburstPopupMenu} instance
   */
  public static synchronized SunburstPopupMenu getInstance(final AbsModel<?, ?> paramModel,
    final INodeWriteTrx paramWtx, final ControlGroup<?> paramCtrl) {
    if (mSunburstPopupMenu == null || !paramWtx.equals(mWtx)) {
      mSunburstPopupMenu = new SunburstPopupMenu(paramModel, paramWtx, paramCtrl);
    }
    return mSunburstPopupMenu;
  }

  /**
   * Create all menu items.
   */
  private void createMenu() {
    for (EMenu menu : EMenu.values()) {
      // Create and add a menu item
      menu.createMenuItem(mModel, this, mWtx, mCtrl);
    }
  }
}
