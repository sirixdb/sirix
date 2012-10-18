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

package org.sirix.diff;

import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.diff.DiffFactory.Builder;
import org.sirix.exception.SirixException;

/**
 * Full diff including attributes and namespaces. Note that this class is thread
 * safe.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
final class FullDiff extends AbstractDiff {

	/**
	 * Constructor.
	 * 
	 * @param builder
	 *          {@link Builder} reference
	 * @throws SirixException
	 *           if anything goes wrong while setting up sirix transactions
	 */
	FullDiff(final @Nonnull Builder builder) throws SirixException {
		super(builder);
	}

	@Override
	boolean checkNodes(final @Nonnull NodeReadTrx firstRtx,
			final @Nonnull NodeReadTrx secondRtx) {
		assert firstRtx != null;
		assert secondRtx != null;

		boolean found = false;
		if (firstRtx.getNodeKey() == secondRtx.getNodeKey()
				&& firstRtx.getKind() == secondRtx.getKind()) {
			switch (firstRtx.getKind()) {
			case ELEMENT:
				if (firstRtx.getNameKey() == secondRtx.getNameKey()) {
					if (firstRtx.getNamespaceCount() == 0
							&& firstRtx.getAttributeCount() == 0
							&& firstRtx.getAttributeCount() == 0
							&& firstRtx.getNamespaceCount() == 0) {
						found = true;
					} else if (firstRtx.getAttributeKeys().equals(
							secondRtx.getAttributeKeys())
							&& secondRtx.getNamespaceKeys().equals(
									secondRtx.getNamespaceKeys())) {
						found = true;
					}
				}
				break;
			case PROCESSING:
				found = firstRtx.getValue().equals(secondRtx.getValue())
						&& firstRtx.getName().equals(secondRtx.getName());
				break;
			case TEXT:
			case COMMENT:
				found = firstRtx.getValue().equals(secondRtx.getValue());
				break;
			default:
				throw new IllegalStateException(
						"Other node types currently not supported!");
			}
		}

		return found;
	}
}
