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
import javax.annotation.Nonnull;

import org.sirix.access.EHashKind;
import org.sirix.api.IDatabase;
import org.sirix.api.ISession;
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
  public enum EDiff {
    /** Nodes are the same. */
    SAME,

    /** Nodes are the same (including subtrees), internally used for optimizations. */
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
   * Determines if an optimized diff calculation should be done, which is faster.
   */
  public enum EDiffOptimized {
    /** Normal diff. */
    NO,

    /** Optimized diff. */
    HASHED
  }

  /** Determines the kind of diff to invoke. */
  private enum EDiffKind {
    /** Full diff. */
    FULL {
      @Override
      void invoke(final Builder pBuilder) throws SirixException {
        new FullDiff(pBuilder).diffMovement();
      }
    },

    /** Structural diff (doesn't recognize differences in namespace and attribute nodes. */
    STRUCTURAL {
      @Override
      void invoke(final Builder pBuilder) throws SirixException {
        new StructuralDiff(pBuilder).diffMovement();
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

  /** Builder to simplify static methods. */
  public static final class Builder {

    /** {@link ISession} reference. */
    final ISession mSession;

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
    final EDiffOptimized mKind;

    /** {@link Set} of {@link IDiffObserver}s. */
    final Set<IDiffObserver> mObservers;

    /** Kind of diff to invoke. */
    transient EDiffKind mDiffKind;

    /** Kind of hash. */
    transient EHashKind mHashKind = EHashKind.Rolling;

    /** Set if the GUI is used. */
    transient boolean mIsGUI = true;

    /**
     * Constructor.
     * 
     * @param pDb
     *          {@link IDatabase} instance
     * @param pNewRev
     *          new revision to compare
     * @param pOldRev
     *          old revision to compare
     * @param pDiffKind
     *          kind of diff (optimized or not)
     * @param pObservers
     *          {@link Set} of observers
     */
    public Builder(final @Nonnull ISession pSession, final @Nonnegative int pNewRev, final @Nonnegative int pOldRev,
      final @Nonnull EDiffOptimized pDiffKind, final @Nonnull Set<IDiffObserver> pObservers) {
      mSession = checkNotNull(pSession);
      checkArgument(pNewRev >= 0, "paramNewRev must be >= 0!");
      mNewRev = pNewRev;
      checkArgument(pOldRev >= 0, "paramOldRev must be >= 0!");
      mOldRev = pOldRev;
      mKind = checkNotNull(pDiffKind);
      mObservers = checkNotNull(pObservers);
    }

    /**
     * Set to true if the algorithm is used by the GUI, otherwise false.
     * 
     * @param pIsGUI
     *          determines if the algorithm is used by the GUI or not
     * @return this builder
     */
    public Builder setIsGUI(final boolean pIsGUI) {
      mIsGUI = pIsGUI;
      return this;
    }

    /**
     * Set start node key in old revision.
     * 
     * @param pOldKey
     *          start node key in old revision
     * @return this builder
     */
    public Builder setOldStartKey(final long pOldKey) {
      checkArgument(pOldKey >= 0, "paramOldKey must be >= 0!");
      mOldStartKey = pOldKey;
      return this;
    }

    /**
     * Set start node key in new revision.
     * 
     * @param pNewKey
     *          start node key in new revision
     * @return this builder
     */
    public Builder setNewStartKey(final long pNewKey) {
      checkArgument(pNewKey >= 0, "paramNewKey must be >= 0!");
      mNewStartKey = pNewKey;
      return this;
    }

    /**
     * Set new depth.
     * 
     * @param pNewDepth
     *          depth of "root" node in new revision
     * @return this builder
     */
    public Builder setNewDepth(final int pNewDepth) {
      checkArgument(pNewDepth >= 0, "paramNewDepth must be >= 0!");
      mNewDepth = pNewDepth;
      return this;
    }

    /**
     * Set old depth.
     * 
     * @param pOldDepth
     *          depth of "root" node in old revision
     * @return this builder
     */
    public Builder setOldDepth(final int pOldDepth) {
      checkArgument(pOldDepth >= 0, "paramOldDepth must be >= 0!");
      mOldDepth = pOldDepth;
      return this;
    }

    /**
     * Set kind of diff.
     * 
     * @param pDiffKind
     *          {@link EDiffKind} instance
     * 
     * @return this builder
     */
    public Builder setDiffKind(final EDiffKind pDiffKind) {
      mDiffKind = checkNotNull(pDiffKind);
      return this;
    }

    /**
     * Set kind of hash. <strong>Must be the same as used for the database creation</strong>.
     * 
     * @param pDiffKind
     *          {@link EHashKind} instance
     * 
     * @return this builder
     */
    public Builder setHashKind(final EHashKind pHashKind) {
      mHashKind = checkNotNull(pHashKind);
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
   * @param pBuilder
   *          {@link Builder} reference
   * @throws SirixException
   */
  public static synchronized void invokeFullDiff(final Builder pBuilder) throws SirixException {
    EDiffKind.FULL.invoke(pBuilder);
  }

  /**
   * Do a structural diff.
   * 
   * @param pBuilder
   *          {@link Builder} reference
   * @throws SirixException
   */
  public static synchronized void invokeStructuralDiff(final Builder pBuilder) throws SirixException {
    EDiffKind.STRUCTURAL.invoke(pBuilder);
  }
}
