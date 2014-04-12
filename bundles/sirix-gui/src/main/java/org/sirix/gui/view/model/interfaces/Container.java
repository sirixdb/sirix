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
package org.sirix.gui.view.model.interfaces;

import javax.annotation.Nonnegative;

import org.sirix.gui.view.sunburst.Pruning;

/**
 * Container used as parameters for models.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <T>
 *          the concrete instance
 */
public interface Container<T> {

	/**
	 * Set start key in old revision.
	 * 
	 * @param pKey
	 *          node key to start from
	 * @return instance
	 */
	T setOldStartKey(@Nonnegative long pKey);

	/**
	 * Set start key in new revision.
	 * 
	 * @param pKey
	 *          node key to start from
	 * @return instance
	 */
	T setNewStartKey(@Nonnegative long pKey);

	/**
	 * Determines if tree should be pruned or not.
	 * 
	 * @param pPruning
	 *          {@link Pruning} enum which determines if tree should be pruned or
	 *          not
	 * @return instance
	 */
	T setPruning(Pruning pPruning);
}
