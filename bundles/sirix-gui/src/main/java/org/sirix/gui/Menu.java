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

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JCheckBoxMenuItem;
import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.JSeparator;

/**
 * Determines and creates the appropriate menu item type.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 */
enum Menu {
  /** Menu item. */
  MENU {
    @Override
    JComponent construct(final GUI paramGUI, final IGUICommand paramCommand) {
      assert paramGUI != null;
      assert paramCommand != null;
      final JMenuItem item = new JMenuItem(paramCommand.desc());
      setupItem(item, paramGUI, paramCommand);
      return item;
    }
  },

  /** Separator item. */
  SEPARATOR {
    @Override
    JComponent construct(final GUI paramGUI, final IGUICommand paramCommand) {
      return new JSeparator();
    }
  },

  /** Checkbox item. */
  CHECKBOXITEM {
    @Override
    JComponent construct(final GUI paramGUI, final IGUICommand paramCommand) {
      assert paramGUI != null;
      assert paramCommand != null;
      final JCheckBoxMenuItem item = new JCheckBoxMenuItem(paramCommand.desc());
      setupItem(item, paramGUI, paramCommand);
      item.setSelected(paramCommand.selected());
      return item;
    }
  },

  /** Radio button item. */
  RADIOBUTTONITEM {
    @Override
    JComponent construct(final GUI paramGUI, final IGUICommand paramCommand) {
      assert paramGUI != null;
      assert paramCommand != null;
      final JRadioButtonMenuItem item = new JRadioButtonMenuItem(paramCommand.desc());
      setupItem(item, paramGUI, paramCommand);
      return item;
    }
  };

  /**
   * Construct menu item.
   * 
   * @param paramGUI
   *          reference to main GUI frame
   * @param paramCommand
   *          the {@link IGUICommand}
   * @return component reference
   * @throws AssertionError
   *           if {@code paramGUI} or {@code paramCommand} is {@code null}
   */
  abstract JComponent construct(final GUI paramGUI, final IGUICommand paramCommand);

  /**
   * Setup a menu item.
   * 
   * @param paramItem
   *          the item to set up
   * @param paramGUI
   *          reference to main GUI frame
   * @param paramCommand
   *          the menu command
   */
  void setupItem(final JMenuItem paramItem, final GUI paramGUI, final IGUICommand paramCommand) {
    assert paramItem != null;
    assert paramGUI != null;
    assert paramCommand != null;
    paramItem.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(final ActionEvent paramEvent) {
        paramCommand.execute(paramGUI);
      }
    });

    paramItem.setMnemonic(paramCommand.desc().charAt(0));
  }
}
