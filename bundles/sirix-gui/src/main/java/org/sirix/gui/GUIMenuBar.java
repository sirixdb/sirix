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

package org.sirix.gui;

import javax.swing.JComponent;
import javax.swing.JMenu;
import javax.swing.JMenuBar;

import static org.sirix.gui.GUIConstants.MENUBAR;
import static org.sirix.gui.GUIConstants.MENUITEMS;

/**
 * This is the menu bar of the main window.
 * The menu structure is defined in {@link GUIConstants#MENUBAR} and {@link GUIConstants#MENUITEMS}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz.
 */
@SuppressWarnings("serial")
public final class GUIMenuBar extends JMenuBar {

  /**
   * Constructor.
   * 
   * @param paramGUI
   *          Main {@link GUI} frame.
   */
  public GUIMenuBar(final GUI paramGUI) {
    // Loop through all menu entries
    for (int i = 0; i < MENUBAR.length; i++) {
      final JMenu menu = new JMenu(MENUBAR[i]);
      menu.setMnemonic((int)MENUBAR[i].charAt(0));

      for (int j = 0; j < MENUITEMS[i].length; j++) {
        final IGUICommand cmd = MENUITEMS[i][j];
        final JComponent item = cmd.type().construct(paramGUI, cmd);
        menu.add(item);
      }

      add(menu);
    }
  }
}
