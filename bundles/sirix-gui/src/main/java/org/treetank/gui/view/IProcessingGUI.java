/**
 * 
 */
package org.treetank.gui.view;

import processing.core.PApplet;

/**
 * Interface for processing GUIs.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface IProcessingGUI {
  /**
   * Processing.org draw method.
   */
  void draw();

  /**
   * Update the GUI.
   */
  void update();

  /**
   * Relocate ControlP5 stuff after the frame which includes the view has been resized.
   */
  void relocate();

  /**
   * Get the Processing {@link PApplet} instance.
   */
  PApplet getApplet();

  /** Reset the GUI */
  void resetGUI();
}
