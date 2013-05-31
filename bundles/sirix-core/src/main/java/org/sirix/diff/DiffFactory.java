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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import javax.annotation.Nonnegative;

import org.sirix.access.HashKind;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;

/**
 * Factory method for public access.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class DiffFactory {

	/**
	 * Possible kinds of differences between two nodes.
	 */
	public enum DiffType {
		/** Nodes are the same. */
		SAME,

		/**
		 * Nodes are the same (including subtrees), internally used for
		 * optimizations.
		 */
		SAMEHASH,

		/** Node has been inserted. */
		INSERTED,

		/** Node has been deleted. */
		DELETED,

		/** Node has been updated. */
		UPDATED,

		/** Node has been replaced. */
		REPLACED,

		/** Node has been replaced. */
		REPLACEDNEW,

		/** Node has been replaced. */
		REPLACEDOLD,

		/** Node has been moved from. */
		MOVEDFROM,

		/** Node has been moved to. */
		MOVEDTO
	}

	/**
	 * Determines if an optimized diff calculation should be done, which is
	 * faster.
	 */
	public enum DiffOptimized {
		/** Normal diff. */
		NO,

		/** Optimized diff. */
		HASHED
	}

	/** Determines the kind of diff algorithm to invoke. */
	private enum DiffAlgorithm {
		/** Full diff. */
		FULL {
			@Override
			void invoke(final Builder builder) throws SirixException {
				new FullDiff(builder).diffMovement();
			}
		},

		/**
		 * Structural diff (doesn't recognize differences in namespace and attribute
		 * nodes.
		 */
		STRUCTURAL {
			@Override
			void invoke(final Builder builder) throws SirixException {
				new StructuralDiff(builder).diffMovement();
			}
		};

		/**
		 * Invoke diff.
		 * 
		 * @param pBuilder
		 *          {@link Builder} reference
		 * @throws SirixException
		 *           if anything while diffing goes wrong related to sirix
		 */
		abstract void invoke(final Builder pBuilder) throws SirixException;
	}

	/**
	 * Create a new {@link Builder} instance.
	 * 
	 * @param session
	 *          the {@link Session} to use
	 * @param newRev
	 *          new revision to compare
	 * @param oldRev
	 *          old revision to compare
	 * @param diffKind
	 *          kind of diff (optimized or not)
	 * @param observers
	 *          {@link Set} of observers
	 * @return new {@link Builder} instance
	 */
	public static Builder builder(final Session session,
			final @Nonnegative int newRev, final @Nonnegative int oldRev,
			final DiffOptimized diffKind,
			final Set<DiffObserver> observers) {
		return new Builder(session, newRev, oldRev, diffKind, observers);
	}

	/** Builder to simplify static methods. */
	public static final class Builder {

		/** {@link Session} reference. */
		final Session mSession;

		/** Start key of new revision. */
		transient long mNewStartKey;

		/** Start key of old revision. */
		transient long mOldStartKey;

		/** New revision. */
		final int mNewRev;

		/** Old revision. */
		final int mOldRev;

		/** Depth of "root" node in new revision. */
		transient int mNewDepth;

		/** Depth of "root" node in old revision. */
		transient int mOldDepth;

		/** Diff kind. */
		final DiffOptimized mKind;

		/** {@link Set} of {@link DiffObserver}s. */
		final Set<DiffObserver> mObservers;

		/** Kind of diff to invoke. */
		transient DiffAlgorithm mDiffKind;

		/** Kind of hash. */
		transient HashKind mHashKind = HashKind.ROLLING;

		/** Set if the GUI is used. */
		transient boolean mIsGUI = true;

		/**
		 * Constructor.
		 * 
		 * @param session
		 *          the {@link Session} to use
		 * @param newRev
		 *          new revision to compare
		 * @param oldRev
		 *          old revision to compare
		 * @param diffKind
		 *          kind of diff (optimized or not)
		 * @param observers
		 *          {@link Set} of observers
		 */
		public Builder(final Session session,
				final @Nonnegative int newRev, final @Nonnegative int oldRev,
				final DiffOptimized diffKind,
				final Set<DiffObserver> observers) {
			mSession = checkNotNull(session);
			checkArgument(newRev >= 0, "paramNewRev must be >= 0!");
			mNewRev = newRev;
			checkArgument(oldRev >= 0, "paramOldRev must be >= 0!");
			mOldRev = oldRev;
			mKind = checkNotNull(diffKind);
			mObservers = checkNotNull(observers);
		}

		/**
		 * Set to true if the algorithm is used by the GUI, otherwise false.
		 * 
		 * @param isGUI
		 *          determines if the algorithm is used by the GUI or not
		 * @return this builder
		 */
		public Builder setIsGUI(final boolean isGUI) {
			mIsGUI = isGUI;
			return this;
		}

		/**
		 * Set start node key in old revision.
		 * 
		 * @param oldKey
		 *          start node key in old revision
		 * @return this builder
		 */
		public Builder setOldStartKey(final @Nonnegative long oldKey) {
			checkArgument(oldKey >= 0, "oldKey must be >= 0!");
			mOldStartKey = oldKey;
			return this;
		}

		/**
		 * Set start node key in new revision.
		 * 
		 * @param pNewKey
		 *          start node key in new revision
		 * @return this builder
		 */
		public Builder setNewStartKey(final @Nonnegative long newKey) {
			checkArgument(newKey >= 0, "newKey must be >= 0!");
			mNewStartKey = newKey;
			return this;
		}

		/**
		 * Set new depth.
		 * 
		 * @param newDepth
		 *          depth of "root" node in new revision
		 * @return this builder
		 */
		public Builder setNewDepth(final @Nonnegative int newDepth) {
			checkArgument(newDepth >= 0, "newDepth must be >= 0!");
			mNewDepth = newDepth;
			return this;
		}

		/**
		 * Set old depth.
		 * 
		 * @param oldDepth
		 *          depth of "root" node in old revision
		 * @return this builder
		 */
		public Builder setOldDepth(final int oldDepth) {
			checkArgument(oldDepth >= 0, "oldDepth must be >= 0!");
			mOldDepth = oldDepth;
			return this;
		}

		/**
		 * Set kind of diff-algorithm.
		 * 
		 * @param pDiffAlgorithm
		 *          {@link DiffAlgorithm} instance
		 * 
		 * @return this builder
		 */
		public Builder setDiffAlgorithm(final DiffAlgorithm pDiffAlgorithm) {
			mDiffKind = checkNotNull(pDiffAlgorithm);
			return this;
		}

		/**
		 * Set kind of hash. <strong>Must be the same as used for the database
		 * creation</strong>.
		 * 
		 * @param kind
		 *          {@link HashKind} instance
		 * @return this builder
		 */
		public Builder setHashKind(final HashKind kind) {
			mHashKind = checkNotNull(kind);
			return this;
		}
	}

	/**
	 * Private constructor.
	 */
	private DiffFactory() {
		// No instantiation allowed.
		throw new AssertionError("No instantiation allowed!");
	}

	/**
	 * Do a full diff.
	 * 
	 * @param builder
	 *          {@link Builder} reference
	 * @throws SirixException
	 */
	public static synchronized void invokeFullDiff(final Builder builder)
			throws SirixException {
		DiffAlgorithm.FULL.invoke(builder);
	}

	/**
	 * Do a structural diff.
	 * 
	 * @param builder
	 *          {@link Builder} reference
	 * @throws SirixException
	 */
	public static synchronized void invokeStructuralDiff(
			final Builder builder) throws SirixException {
		DiffAlgorithm.STRUCTURAL.invoke(builder);
	}
}
