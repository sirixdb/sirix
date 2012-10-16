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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;

/**
 * <h1>INode</h1>
 * 
 * <p>
 * Common interface for all node kinds.
 * </p>
 */
public interface Node extends NodeBase {

  /**
   * Gets value type of the item.
   * 
   * @return value type
   */
  int getTypeKey();

  /**
   * Setting the type key.
   * 
   * @param typeKey
   *          the type to set
   */
  void setTypeKey(int typeKey);
  
  /**
   * Determines if {@code pOther} is the same item.
   * 
   * @param other
   *          the other node
   * @return {@code true}, if it is the same item, {@code false} otherwise
   */
  boolean isSameItem(@Nullable Node other);
  
  /**
   * Accept a visitor and use double dispatching to invoke the visitor method.
   * 
   * @param visitor
   *          implementation of the {@link Visitor} interface
   * @return the result of a visit
   */
  VisitResult acceptVisitor(@Nonnull Visitor visitor);
  
  /**
   * Setting the actual hash of the structure. The hash of one node should
   * have the entire integrity of the related subtree.
   * 
   * @param pHash
   *          hash to be set for this node
   * 
   */
  void setHash(long hash);

  /**
   * Getting the persistent stored hash.
   * 
   * @return the hash of this node
   */
  long getHash();

  /**
   * Setting the parent key.
   * 
   * @param pNodeKey
   *          the parent to be set
   */
  void setParentKey(long nodeKey);
  
  /**
   * Gets key of the context item's parent.
   * 
   * @return parent key
   */
  long getParentKey();

  /**
   * Declares, whether the item has a parent.
   * 
   * @return {@code true}, if item has a parent, {@code false} otherwise
   */
  boolean hasParent();
}
