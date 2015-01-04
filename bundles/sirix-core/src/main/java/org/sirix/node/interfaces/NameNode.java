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

import org.sirix.node.interfaces.immutable.ImmutableNameNode;

/**
 * Mutable node with a name.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public interface NameNode extends ImmutableNameNode {
	/**
	 * Gets key of prefix.
	 * 
	 * @return key of prefix of the qualified name
	 */
	int getPrefixKey();

	/**
	 * Gets key of local name.
	 * 
	 * @return key of local name of the qualified name
	 */
	int getLocalNameKey();

	/**
	 * Gets key of the URI.
	 * 
	 * @return URI key
	 */
	int getURIKey();

	/**
	 * Get a path node key.
	 * 
	 * @return path node key
	 */
	long getPathNodeKey();

	/**
	 * Setting the prefix key.
	 * 
	 * @param nameKey
	 *          the prefix key to be set
	 */
	void setPrefixKey(int prefixKey);

	/**
	 * Setting the local name key.
	 * 
	 * @param localNameKey
	 *          the local name key to be set
	 */
	void setLocalNameKey(int localNameKey);

	/**
	 * Setting the uri key.
	 * 
	 * @param uriKey
	 *          the urikey to be set
	 */
	void setURIKey(int uriKey);

	/**
	 * Set a path node key.
	 * 
	 * @param nodeKey
	 */
	void setPathNodeKey(long nodeKey);
}
