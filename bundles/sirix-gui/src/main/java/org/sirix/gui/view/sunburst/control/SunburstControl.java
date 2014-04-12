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
package org.sirix.gui.view.sunburst.control;

import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.gicentre.utils.move.ZoomPanListener;
import org.sirix.gui.view.controls.Control;
import org.sirix.gui.view.sunburst.SunburstItem;

import controlP5.ControlListener;

/**
 * Interface for SunburstControllers which adds specific methods to
 * {@link Control}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface SunburstControl extends Control, ControlListener,
		ZoomPanListener {
	/**
	 * Method to process event for submit-button.
	 * 
	 * @param pValue
	 *          change value
	 * @throws XMLStreamException
	 *           if the XML fragment isn't well formed
	 */
	void submit(int pValue) throws XMLStreamException;

	/**
	 * Method to process event for commit-button.
	 * 
	 * @param pValue
	 *          change value
	 * @throws XMLStreamException
	 *           if the XML fragment isn't well formed
	 */
	void commit(int pValue) throws XMLStreamException;

	/**
	 * Method to process event for cancel-button.
	 * 
	 * @param pValue
	 *          change value
	 */
	void cancel(int pValue);

	/**
	 * Set items, which usually invokes the same method in the model.
	 * 
	 * @param pItems
	 *          {@link List} of {@link SunburstItems}
	 * @throws NullPointerException
	 *           if {@code pItems} is {@code null}
	 */
	void setItems(List<SunburstItem> pItems);

	/**
	 * Set depth max of outer ring.
	 * 
	 * @param depthMax
	 *          max depth of outer ring
	 */
	void setNewMaxDepth(int depthMax);

	/**
	 * Set depth max of inner ring.
	 * 
	 * @param depthMax
	 *          max depth of inner ring
	 */
	void setOldMaxDepth(int depthMax);
}
