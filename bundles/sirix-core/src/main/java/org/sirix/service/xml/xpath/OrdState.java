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

/**
 * Enum represents different states of the result order that help to specify, if the result query
 * will be in document order or not.
 */
public enum OrdState {

	/** State of the HiddersMichiels - automaton. */
	LIN {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdChild() {
			mOrdRank++;
			return this;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdParent() {
			if (mOrdRank > 0) {
				mOrdRank--;
			}
			return this;
		}
	},

	/** State of the HiddersMichiels - automaton. */
	MAX1 {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdChild() {
			return OrdState.GENSIB;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdDesc() {
			return OrdState.SIB;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdFollPre() {
			return OrdState.SIB;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdFollPreSib() {
			return OrdState.GENSIB;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdAncestor() {
			return OrdState.LIN;
		}
	},

	/** State of the HiddersMichiels - automaton. */
	GENSIB {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdChild() {
			return this;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdParent() {
			return OrdState.GEN;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdDesc() {
			return OrdState.SIB;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdFollPreSib() {
			mOrdRank++;
			return OrdState.GEN;

		}
	},

	/** State of the HiddersMichiels - automaton. */
	GEN {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdChild() {
			mOrdRank++;
			return this;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdParent() {
			if (mOrdRank > 0) {
				mOrdRank--;
			}
			return this;
		}
	},

	/** State of the HiddersMichiels - automaton. */
	SIB {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdChild() {
			mOrdRank++;
			return this;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public OrdState updateOrdParent() {
			if (mOrdRank > 0) {
				mOrdRank--;
				return this;
			} else {
				return OrdState.UNORD;
			}
		}
	},

	/** State of the HiddersMichiels - automaton. */
	UNORD;

	/**
	 * If mOrderRank is 0, the result sequence will be in document order. If it is greater than 0 it
	 * is not any more, but it can retain the ordered property, if a certain sequence of axis follows.
	 * For more details see [Hidders, J., Michiels, P., "Avoiding Unnecessary Ordering Operations in
	 * XPath", 2003]
	 */
	static int mOrdRank;

	/**
	 * Changes the state according to a child step.
	 * 
	 * @return the updated order state
	 */
	public OrdState updateOrdChild() {
		mOrdRank++;
		return this;
	}

	/**
	 * Changes the state according to a union step.
	 * 
	 * @return the updated order state
	 */
	public OrdState updateOrdUnion() {
		return OrdState.UNORD;
	}

	/**
	 * Changes the state according to a parent step.
	 * 
	 * @return the updated order state
	 */
	public OrdState updateOrdParent() {
		return this;
	}

	/**
	 * Changes the state according to a descendant/ descendant-or.self step.
	 * 
	 * @return the updated order state
	 */
	public OrdState updateOrdDesc() {
		return OrdState.UNORD;
	}

	/**
	 * Changes the state according to a following/preceding step.
	 * 
	 * @return the updated order state
	 */
	public OrdState updateOrdFollPre() {
		return OrdState.UNORD;
	}

	/**
	 * Changes the state according to a following-sibling/preceding-sibling step.
	 * 
	 * @return the updated order state
	 */
	public OrdState updateOrdFollPreSib() {
		if (mOrdRank == 0) {
			mOrdRank++;
		}
		return this;
	}

	/**
	 * Changes the state according to a ancestor step.
	 * 
	 * @return the updated order state
	 */
	public OrdState updateOrdAncestor() {
		return OrdState.UNORD;
	}

	/**
	 * Initializes the order state.
	 */
	public void init() {
		mOrdRank = 0;

	}
}
