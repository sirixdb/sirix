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

import org.sirix.api.Filter;

/**
 * <h1>SequenceType</h1>
 * <p>
 * A sequence type defines a type a the items in a sequnce can have. It consists of either an
 * empty-sequence-test, or an ItemType(kind test, item() or atomic value) and an optional wildcard
 * (*, ?, +)
 * </p>
 */
public class SequenceType {

	private final boolean mIsEmptySequence;

	private final Filter mFilter;

	private final boolean mHasWildcard;

	private final char mWildcard;

	/**
	 * Constructor with no arguments means, the sequence type is the empty sequence.
	 */
	public SequenceType() {

		mIsEmptySequence = true;
		mHasWildcard = false;
		mWildcard = ' ';
		mFilter = null;
	}

	/**
	 * Constructor. Sequence type is an ItemType.
	 * 
	 * @param mFilter item type filter
	 */
	public SequenceType(final Filter mFilter) {

		mIsEmptySequence = false;
		this.mFilter = mFilter;
		mHasWildcard = false;
		mWildcard = ' ';
	}

	/**
	 * Constructor. Sequence type is an ItemType with an wildcard.
	 * <li>'ItemType ?' means the sequence has zero or one items that are of the ItemType</li>
	 * <li>'ItemType +' means the sequence one or more items that are of the ItemType</li>
	 * <li>'ItemType *' means the sequence has zero or more items that are of the ItemType</li>
	 * 
	 * @param filter item type filter
	 * @param mWildcard either '*', '?' or '+'
	 */
	public SequenceType(final Filter filter, final char mWildcard) {

		mIsEmptySequence = false;
		this.mFilter = filter;
		mHasWildcard = true;
		this.mWildcard = mWildcard;
	}

	/**
	 * 
	 * @return true, if sequence is the empty sequence
	 */
	public boolean isEmptySequence() {

		return mIsEmptySequence;
	}

	/**
	 * @return the ItemType test
	 */
	public Filter getFilter() {

		return mFilter;
	}

	/**
	 * @return true, if a wildcard is present
	 */
	public boolean hasWildcard() {

		return mHasWildcard;
	}

	/**
	 * Returns the wildcard's char representation.
	 * 
	 * @return wildcard sign
	 */
	public char getWildcard() {

		return mWildcard;
	}

}
