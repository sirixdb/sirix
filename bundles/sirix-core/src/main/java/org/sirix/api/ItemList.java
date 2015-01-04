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

package org.sirix.api;

import java.util.Optional;

import org.sirix.node.interfaces.Node;

/**
 * <h1>ItemList</h1>
 * <p>
 * Data structure to store <a
 * href="http://www.w3.org/TR/xpath-datamodel/#dt-item"> XDM items</a>.
 * </p>
 * <p>
 * This structure is used to store atomic values that are needed for the
 * evaluation of a query. They can be results of a query expression or be
 * specified directly in the query e.g. as literals perform an arithmetic
 * operation or a comparison.
 * </p>
 * <p>
 * Since these items have to be distinguishable from nodes their key will be a
 * negative long value (node key is always a positive long value). This value is
 * retrieved by negate their index in the internal data structure.
 * </p>
 */
public interface ItemList<T extends Node> {

	/**
	 * Adds an item to the item list and assigns a unique item key to the item and
	 * return it. The item key is the negatived index of the item in the item
	 * list. The key is negatived to make it distinguishable from a node.
	 * 
	 * @param pNode
	 *          the item to add
	 * @return the item key
	 */
	int addItem(final T pNode);

	/**
	 * Returns the item at a given index in the item list. If the given index is
	 * the item key, it has to be negated before.
	 * 
	 * @param pKey
	 *          key of the item, that should be returned
	 * @return item at the given index
	 */
	Optional<T> getItem(final long pKey);

	/**
	 * Determines how many items are in the list.
	 * 
	 * @return list size
	 */
	int size();
}
