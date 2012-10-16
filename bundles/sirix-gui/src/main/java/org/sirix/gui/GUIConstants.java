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

import java.awt.Color;

import static org.sirix.gui.GUICommands.*;

/**
 * <h1>GUIConstants</h1>
 * 
 * <p>
 * Some constants which are used all over the GUI packages.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz.
 * 
 */
public final class GUIConstants {

  // COLORS ===================================================================

  /** Document root color. */
  public static final Color DOC_COLOR = new Color(128, 0, 0);

  /** Element color. */
  public static final Color ELEMENT_COLOR = new Color(0, 0, 128);

  /** Attribute color. */
  public static final Color ATTRIBUTE_COLOR = new Color(0, 128, 0);

  /** Namespace color. */
  public static final Color NAMESPACE_COLOR = new Color(128, 128, 128);

  /** Text color. */
  public static final Color TEXT_COLOR = Color.BLACK;

  /** Hover color. */
  public static final Color HOVER_COLOR = new Color(77, 77, 77);

  // OTHER ===================================================================

  /** Newline string representation. */
  public static final String NEWLINE = System.getProperty("line.separator");

  // MENU =====================================================================

  /** Menu file. */
  static final String MENUFILE = "File";

  /** Menu views. */
  static final String MENUVIEWS = "Views";

  // MENUBARS =================================================================

  /** Top menu entries. */
  static final String[] MENUBAR = {
    MENUFILE, MENUVIEWS
  };

  /** Two-dimensional Menu entries, containing the menu item commands. */
  static final IGUICommand[][] MENUITEMS = {
    {
      OPEN, SHREDDER, SHREDDER_UPDATE, SERIALIZE, SEPARATOR, QUIT
    }, {
      TREE, TEXT, SMALLMULTIPLES, SUNBURST
    }
  };

  /**
   * Private constructor.
   */
  private GUIConstants() {
    // No instance allowed.
    throw new AssertionError("No instance allowed!");
  }
}
