/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.xpath;

import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.types.Type;

/**
 * <h1>SingleType</h1>
 * <p>
 * A single type defines a type the a single item can have. It consists of an atomic type and a
 * optional interrogation that, when present indicates that the item can also be the empty sequence.
 * </p>
 */
public class SingleType {

	private Type mAtomicType;

	private final boolean mhasInterogation;

	/**
	 * Constructor.
	 * 
	 * @param atomic string representation of the atomic value
	 * @param mIntero true, if interrogation sign is present
	 * @throws SirixXPathException
	 */
	public SingleType(final String atomic, final boolean mIntero) throws SirixXPathException {

		// get atomic type
		mAtomicType = null; // TODO. = null is not good style
		for (Type type : Type.values()) {
			if (type.getStringRepr().equals(atomic)) {
				mAtomicType = type;
				break;
			}
		}

		if (mAtomicType == null) {
			throw EXPathError.XPST0051.getEncapsulatedException();
		}

		mhasInterogation = mIntero;
	}

	/**
	 * Gets the atomic type.
	 * 
	 * @return atomic type.
	 */
	public Type getAtomic() {

		return mAtomicType;
	}

	/**
	 * Specifies, whether interrogation sign is present and therefore the empty sequence is valid too.
	 * 
	 * @return true, if interrogation sign is present.
	 */
	public boolean hasInterogation() {

		return mhasInterogation;
	}

}
