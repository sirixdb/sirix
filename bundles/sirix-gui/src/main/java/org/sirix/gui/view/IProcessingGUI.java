/**
 * 
 */
package org.sirix.gui.view;

import javax.annotation.Nonnull;

import org.sirix.gui.view.sunburst.AbsSunburstGUI.EResetZoomer;

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
	 * 
	 * @param pReset
	 *          determines if zoomer must be resetted or not
	 */
	void update(final @Nonnull EResetZoomer pReset);

	/**
	 * Relocate ControlP5 stuff after the frame which includes the view has been
	 * resized.
	 */
	void relocate();

	/**
	 * Get the Processing {@link PApplet} instance.
	 */
	PApplet getApplet();

	/** Reset the GUI */
	void resetGUI();
}
