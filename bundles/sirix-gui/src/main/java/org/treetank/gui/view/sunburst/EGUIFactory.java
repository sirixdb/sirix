/**
 * 
 */
package org.treetank.gui.view.sunburst;

import org.treetank.gui.ReadDB;
import org.treetank.gui.view.smallmultiple.SmallmultipleGUI;
import org.treetank.gui.view.sunburst.control.AbsSunburstControl;

import processing.core.PApplet;

/**
 * GUI Factory.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum EGUIFactory {
  /** Sunburst GUI. */
  SUNBURSTGUI {
    /** {@inheritDoc} */
    @Override
    public AbsSunburstGUI getInstance(final PApplet paramApplet, final AbsSunburstControl paramControl,
      final ReadDB paramReadDB) {
      checkParams(paramApplet, paramControl, paramReadDB);
      return SunburstGUI.getInstance(paramApplet, paramControl, paramReadDB);
    }
  },

  /** Small multiples GUI. */
  SMALLMULTIPLESGUI {
    /** {@inheritDoc} */
    @Override
    public AbsSunburstGUI getInstance(final PApplet paramApplet, final AbsSunburstControl paramControl,
      final ReadDB paramReadDB) {
      checkParams(paramApplet, paramControl, paramReadDB);
      return SmallmultipleGUI.getInstance(paramApplet, paramControl, paramReadDB);
    }
  };

  /**
   * Get an instance of a GUI which extends {@link AbsSunburstGUI}. The classes itself have to implement
   * a singleton mechanism if it's necessary.
   * 
   * @param paramApplet
   *          parent processing applet
   * @param paramControl
   *          associated controller
   * @param paramReadDB
   *          read database
   * @return instance of a GUI which extends {@link AbsSunburstGUI}
   */
  public abstract AbsSunburstGUI getInstance(final PApplet paramApplet,
    final AbsSunburstControl paramControl, final ReadDB paramReadDB);

  /**
   * Check parameters.
   * 
   * @param paramApplet
   *          parent processing applet
   * @param paramControl
   *          associated controller
   * @param paramReadDB
   *          read database
   */
  private static void checkParams(PApplet paramApplet, AbsSunburstControl paramControl, ReadDB paramReadDB) {
    if (paramApplet == null || paramControl == null || paramReadDB == null) {
      throw new IllegalArgumentException("Non of the parameters can be null!");
    }
  }
}
