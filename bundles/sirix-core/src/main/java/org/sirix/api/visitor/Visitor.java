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

package org.sirix.api.visitor;

import javax.annotation.Nonnull;

import org.sirix.node.immutable.ImmutableAttribute;
import org.sirix.node.immutable.ImmutableComment;
import org.sirix.node.immutable.ImmutableDocument;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.immutable.ImmutableNamespace;
import org.sirix.node.immutable.ImmutablePI;
import org.sirix.node.immutable.ImmutableText;

/**
 * Interface which must be implemented from visitors to implement functionality
 * based on the visitor pattern.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface Visitor {
	/**
	 * Do something when visiting a {@link ImmutablePI}.
	 * 
	 * @param node
	 *          the {@link ImmutablePI}
	 */
	VisitResult visit(@Nonnull final ImmutablePI node);

	/**
	 * Do something when visiting a {@link ImmutableComment}.
	 * 
	 * @param node
	 *          the {@link ImmutableComment}
	 */
	VisitResult visit(@Nonnull final ImmutableComment node);

	/**
	 * Do something when visiting an {@link ImmutableElement}.
	 * 
	 * @param node
	 *          the {@link ImmutableElement}
	 */
	VisitResult visit(@Nonnull final ImmutableElement node);

	/**
	 * Do something when visiting a {@link ImmutableAttribute}.
	 * 
	 * @param node
	 *          the {@link ImmutableAttribute}
	 */
	VisitResult visit(@Nonnull final ImmutableAttribute node);

	/**
	 * ImmutableDocumentRoot Do something when visiting a
	 * {@link ImmutableNamespace}.
	 * 
	 * @param node
	 *          the {@link ImmutableNamespace}
	 */
	VisitResult visit(@Nonnull final ImmutableNamespace node);

	/**
	 * Do something when visiting a {@link ImmutableText}.
	 * 
	 * @param node
	 *          the {@link ImmutableText}
	 */
	VisitResult visit(@Nonnull final ImmutableText node);

	/**
	 * Do something when visiting the {@link ImmutableDocument}.
	 * 
	 * @param node
	 *          the {@link ImmutableDocument}
	 */
	VisitResult visit(@Nonnull final ImmutableDocument node);
}
