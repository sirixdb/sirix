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

package org.sirix.gui.view.sunburst;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import org.sirix.gui.view.sunburst.SunburstItem.EStructType;

/**
 * 
 * <p>
 * Relations between a node and it's children. Container class used to simplify
 * the {@link SunburstItem.Builder} constructor.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class NodeRelations {
	/** Depth in the tree. */
	final int mDepth;

	/** Determines the structural kind of the node. */
	final EStructType mStructKind;

	/** Current node value. */
	final float mValue;

	/** Global minimum value. */
	final float mMinValue;

	/** Global maximum value. */
	final float mMaxValue;

	/** Index to the parent item. */
	final int mIndexToParent;

	/** Determines if one must be subtracted. */
	transient boolean mSubtract;

	/** Original depth. */
	final int mOrigDepth;

	/**
	 * Set all fields.
	 * 
	 * @param pDepth
	 *          depth in the tree
	 * @param pStructKind
	 *          determines the structural kind of the node
	 * @param pValue
	 *          value of the node
	 * @param pMinValue
	 *          global minimum value
	 * @param pMaxValue
	 *          global maximum value
	 * @param pIndexToParent
	 *          index to the parent item
	 * @return new {@link NodeRelations} instance
	 */
	public NodeRelations(final int pOrigDepth, final int pDepth,
			final EStructType pStructKind, final float pValue, final float pMinValue,
			final float pMaxValue, final int pIndexToParent) {
		checkArgument(pOrigDepth >= 0, "pOrigDepth must be >= 0!");
		checkArgument(pDepth >= 0, "pDepth must be >= 0!");
		requireNonNull(pStructKind);
		checkArgument(pValue >= 0, "pValue must be >= 0!");
		checkArgument(pMinValue >= 0, "pMinValue must be >= 0!");
		checkArgument(pMaxValue >= 0, "pMaxValue must be >= 0!");
		checkArgument(pIndexToParent >= -1, "pIndexToParent must be >= 0!");
		mOrigDepth = pOrigDepth;
		mDepth = pDepth;
		mStructKind = pStructKind;
		mValue = pValue;
		mMinValue = pMinValue;
		mMaxValue = pMaxValue;
		mIndexToParent = pIndexToParent;
	}

	/**
	 * Set subtract.
	 * 
	 * @p pSubtract determines if one must be subtracted
	 * @return this relation
	 */
	public NodeRelations setSubtract(final boolean pSubtract) {
		mSubtract = pSubtract;
		return this;
	}
}
