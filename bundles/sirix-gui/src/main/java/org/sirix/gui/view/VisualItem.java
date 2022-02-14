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

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.gui.view.sunburst.Draw;
import org.sirix.gui.view.sunburst.EGreyState;
import org.sirix.gui.view.sunburst.SunburstItem.EColorNode;
import org.sirix.gui.view.sunburst.XPathState;

import processing.core.PGraphics;

/**
 * Interface for a visual item.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface VisualItem extends Comparable<VisualItem> {
	/**
	 * Update an item.
	 * 
	 * @param pDraw
	 *          drawing mode
	 * @param pMappingMode
	 *          determines how to normalize
	 * @param pGraphics
	 *          the {@link PGraphics} instance to write to
	 */
	void update(Draw pDraw, int pMappingMode, @NonNull PGraphics pGraphics);

	/**
	 * Item hovered.
	 * 
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 */
	void hover(PGraphics pGraphic);

	/**
	 * Get node key.
	 * 
	 * @return node key
	 */
	long getKey();

	/**
	 * Set XPath state.
	 * 
	 * @param pState
	 *          {@link XPathState} value
	 */
	void setXPathState(XPathState pState);

	/**
	 * Set grey state.
	 * 
	 * @param pState
	 *          {@link EGreyState} value
	 */
	void setGreyState(EGreyState pState);

	/**
	 * Get grey state.
	 * 
	 * @return grey state
	 */
	EGreyState getGreyState();

	/**
	 * Get type of diff.
	 * 
	 * @return type of diff
	 */
	DiffType getDiff();

	/**
	 * Set if node should be colored or not.
	 * 
	 * @param pColorNode
	 *          enum to determine if node should be colored or not
	 */
	void setColorNode(EColorNode pColorNode);

	/**
	 * Set the revision the item belongs to.
	 * 
	 * @param pRevision
	 *          the revision to set
	 */
	void setRevision(@NonNegative long pRevision);

	/** Get the revision the item belongs to. */
	long getRevision();

	/** Get text value of the item. */
	String getText();

	/**
	 * Set the minimum value.
	 * 
	 * @param pMinimum
	 *          value
	 */
	void setMinimum(@NonNegative float pMinimum);

	/**
	 * Set the maximum value.
	 * 
	 * @param pMaximum
	 *          value
	 */
	void setMaximum(@NonNegative float pMaximum);

	/**
	 * Item depth.
	 * 
	 * @return the depth of the item
	 */
	int getDepth();

	/**
	 * Get value.
	 * 
	 * @return value
	 */
	float getValue();

	/**
	 * Get index of parent item.
	 * 
	 * @return index of the parent item
	 */
	int getIndexToParent();

	/**
	 * Get minimum value.
	 * 
	 * @return minimum value
	 */
	float getMinimum();

	/**
	 * Get maximum value.
	 * 
	 * @return maximum value
	 */
	float getMaximum();

	/**
	 * Get nodeKey of node in older revision which has been compared.
	 * 
	 * @return {@code nodeKey} of node in old revision
	 */
	long getOldKey();

	/**
	 * Get modification count.
	 * 
	 * @return modification count
	 */
	int getModificationCount();

	/**
	 * Get maximum depth of unchanged items.
	 * 
	 * @return maximum depth of unchanged items
	 */
	int getOriginalDepth();
}
