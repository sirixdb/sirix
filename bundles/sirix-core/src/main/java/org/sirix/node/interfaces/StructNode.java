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

package org.sirix.node.interfaces;

/**
 * Class to denote that an {@link Node} has structural attributes. That means
 * that a class can have pointers to neighbors as well as a first child pointer
 * and various other structural measures as the {@code childCount} and
 * {@code descendantCount}.
 */
public interface StructNode extends Node {

	/**
	 * Declares, whether the item has a first child.
	 * 
	 * @return true, if item has a first child, otherwise false
	 */
	boolean hasFirstChild();

	/**
	 * Declares, whether the item has a left sibling.
	 * 
	 * @return true, if item has a left sibling, otherwise false
	 */
	boolean hasLeftSibling();

	/**
	 * Declares, whether the item has a right sibling.
	 * 
	 * @return true, if item has a right sibling, otherwise false
	 */
	boolean hasRightSibling();

	/**
	 * Get the number of children of the node.
	 * 
	 * @return node's number of children
	 */
	long getChildCount();

	/**
	 * Get the number of descendants of the node.
	 * 
	 * @return node's number of descendants
	 */
	long getDescendantCount();

	/**
	 * Gets key of the context item's first child.
	 * 
	 * @return first child's key
	 */
	long getFirstChildKey();

	/**
	 * Gets key of the context item's left sibling.
	 * 
	 * @return left sibling key
	 */
	long getLeftSiblingKey();

	/**
	 * Gets key of the context item's right sibling.
	 * 
	 * @return right sibling key
	 */
	long getRightSiblingKey();

	/**
	 * Setting the right sibling key to this node.
	 * 
	 * @param pNodeKey
	 *          the new key to be set.
	 */
	void setRightSiblingKey(long pNodeKey);

	/**
	 * Setting the left sibling key to this node.
	 * 
	 * @param pNodeKey
	 *          the new key to be set.
	 */
	void setLeftSiblingKey(long pNodeKey);

	/**
	 * Setting the first child sibling key to this node.
	 * 
	 * @param pNodeKey
	 *          the new key to be set.
	 */
	void setFirstChildKey(long pNodeKey);

	/**
	 * Decrementing the child count.
	 * 
	 */
	void decrementChildCount();

	/**
	 * Incrementing the child count.
	 */
	void incrementChildCount();

	/**
	 * Decrementing the descendant count.
	 */
	void decrementDescendantCount();

	/**
	 * Incrementing the descendant count.
	 */
	void incrementDescendantCount();

	/**
	 * Set the descendant count.
	 * 
	 * @param pDescendantCount
	 *          new descendant count
	 */
	void setDescendantCount(long pDescendantCount);
}
