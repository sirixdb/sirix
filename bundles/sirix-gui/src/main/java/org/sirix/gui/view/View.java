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

import javax.swing.JComponent;

import com.google.common.base.Optional;

/**
 * <h1>IView</h1>
 * 
 * <p>
 * Interface every view has to implement.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface View {
	/**
	 * Get name of the view.
	 * 
	 * @return name of the view
	 */
	String name();

	/**
	 * Called when the data reference has changed.
	 */
	void refreshInit();

	/**
	 * Called when updates have been done regarding the data structure.
	 * 
	 * @param pAxis
	 *          optional diff axis
	 */
	void refreshUpdate(final Optional<VisualItemAxis> pAxis);

	/**
	 * {@link VisualItem} is hovered.
	 * 
	 * @param paramItem
	 *          {@link VisualItem} instance
	 */
	void hover(final VisualItem paramItem);

	/**
	 * Returns if this view is currently visible.
	 * 
	 * @return result of check
	 */
	boolean isVisible();

	/**
	 * Called when frame is going to dispose.
	 */
	void dispose();

	/**
	 * Get the component.
	 * 
	 * @return the component
	 */
	JComponent component();

	/** Resize view. */
	void resize();
}
