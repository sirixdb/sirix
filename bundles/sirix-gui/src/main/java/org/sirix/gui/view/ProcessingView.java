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

import java.awt.event.KeyListener;
import java.awt.event.MouseListener;

import org.sirix.gui.ProgressGlassPane;
import org.sirix.gui.view.controls.Control;
import org.sirix.gui.view.model.interfaces.Model;

import processing.core.PApplet;

/**
 * Interface which all processing views have to implement.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface ProcessingView extends MouseListener, KeyListener {

	/** @see */
	void setup();

	void draw();

	/**
	 * Get glassPane.
	 * 
	 * @return the glassPane
	 */
	ProgressGlassPane getGlassPane();

	/**
	 * Get Swing view associated with the processing view.
	 * 
	 * @return {@link View} reference
	 */
	View getView();

	/**
	 * Get controller.
	 * 
	 * @return reference of an {@link Control} implementation
	 */
	Control getController();

	/**
	 * Set visibility.
	 * 
	 * @param paramFlag
	 *          if true, view is visible, if false view is not visible
	 */
	void setVisible(boolean paramFlag);

	/**
	 * Get GUI.
	 * 
	 * @return GUI which implements the {@link ProcessingGUI} interface
	 */
	ProcessingGUI getGUI();

	/**
	 * Get embedded view.
	 * 
	 * @return {@link ProcessingEmbeddedView} instance
	 */
	ProcessingEmbeddedView getEmbeddedView();

	void setEmbeddedView(final ProcessingEmbeddedView pEmbeddedView);

	void dispose();

	void stop();

	void init();

	PApplet getApplet();

	boolean isFocused();

	boolean isDone();

	void update();

	boolean isShowing();

	void refreshUpdate();

	void noLoop();

	Model<?, ?> getModel();
}
