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

package org.sirix.diff;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Set;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceSession;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
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
     * Nodes are the same (including subtrees), internally used for optimizations.
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
   * Determines if an optimized diff calculation should be done, which is faster.
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
    XML_FULL {
      @Override
      <R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> void invoke(
          final Builder<R, W> builder) {
        @SuppressWarnings("unchecked")
        final Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> xmlDiffBuilder =
            (Builder<XmlNodeReadOnlyTrx, XmlNodeTrx>) builder;
        new XmlFullDiff(xmlDiffBuilder).diffMovement();
      }
    },

    /**
     * Structural diff (doesn't recognize differences in namespace and attribute nodes.
     */
    XML_STRUCTURAL {
      @Override
      <R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> void invoke(
          final Builder<R, W> builder) {
        @SuppressWarnings("unchecked")
        final Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> xmlDiffBuilder =
            (Builder<XmlNodeReadOnlyTrx, XmlNodeTrx>) builder;
        new XmlStructuralDiff(xmlDiffBuilder).diffMovement();
      }
    },

    /**
     * JSON diff.
     */
    JSON {
      @Override
      <R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> void invoke(
          final Builder<R, W> builder) {
        @SuppressWarnings("unchecked")
        final Builder<JsonNodeReadOnlyTrx, JsonNodeTrx> jsonDiffBuilder =
            (Builder<JsonNodeReadOnlyTrx, JsonNodeTrx>) builder;
        new JsonDiff(jsonDiffBuilder).diffMovement();
      }
    };

    /**
     * Invoke diff.
     *
     * @param builder {@link Builder} reference
     * @throws SirixException if anything while diffing goes wrong related to sirix
     */
    abstract <R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> void invoke(
        final Builder<R, W> builder);
  }

  /**
   * Create a new {@link Builder} instance.
   *
   * @param resourceManager the {@link ResourceSession} to use
   * @param newRev new revision to compare
   * @param oldRev old revision to compare
   * @param diffKind kind of diff (optimized or not)
   * @param observers {@link Set} of observers
   * @return new {@link Builder} instance
   */
  public static Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder(final XmlResourceSession resourceManager,
      final @NonNegative int newRev, final @NonNegative int oldRev, final DiffOptimized diffKind,
      final Set<DiffObserver> observers) {
    return new Builder<>(resourceManager, newRev, oldRev, diffKind, observers);
  }

  /** Builder to simplify static methods. */
  public static final class Builder<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> {

    /** {@link ResourceSession} reference. */
    final ResourceSession<R, W> resMgr;

    /** Start key of new revision. */
    transient long newStartKey;

    /** Start key of old revision. */
    transient long oldStartKey;

    /** New revision. */
    final int newRev;

    /** Old revision. */
    final int oldRev;

    /** Depth of "root" node in new revision. */
    transient int newDepth;

    /** Depth of "root" node in old revision. */
    transient int oldDepth;

    /** Diff kind. */
    final DiffOptimized kind;

    /** {@link Set} of {@link DiffObserver}s. */
    final Set<DiffObserver> observers;

    /** Kind of diff to invoke. */
    transient DiffAlgorithm mDiffKind;

    /** Kind of hash. */
    transient HashType hashKind = HashType.ROLLING;

    /** Set if the GUI is used. */
    transient boolean isGUI = true;

    /** Determines if subtrees are skipped after detecting an insert/delete... */
    transient boolean skipSubtrees = false;

    /** The maximum depth. */
      transient long oldMaxDepth;

    /**
     * Constructor.
     *
     * @param resMgr the {@link ResourceSession} to use
     * @param newRev new revision to compare
     * @param oldRev old revision to compare
     * @param diffKind kind of diff (optimized or not)
     * @param observers {@link Set} of observers
     */
    public Builder(final ResourceSession<R, W> resMgr, final @NonNegative int newRev, final @NonNegative int oldRev,
        final DiffOptimized diffKind, final Set<DiffObserver> observers) {
      this.resMgr = checkNotNull(resMgr);
      checkArgument(newRev >= 0, "paramNewRev must be >= 0!");
      this.newRev = newRev;
      checkArgument(oldRev >= 0, "paramOldRev must be >= 0!");
      this.oldRev = oldRev;
      kind = checkNotNull(diffKind);
      this.observers = checkNotNull(observers);
    }

    /**
     * Set to true if the algorithm is used by the GUI, otherwise false.
     *
     * @param isGUI determines if the algorithm is used by the GUI or not
     * @return this builder
     */
    public Builder<R, W> isGUI(final boolean isGUI) {
      this.isGUI = isGUI;
      return this;
    }

    /**
     * Set start node key in old revision.
     *
     * @param oldKey start node key in old revision
     * @return this builder
     */
    public Builder<R, W> oldStartKey(final @NonNegative long oldKey) {
      checkArgument(oldKey >= 0, "oldKey must be >= 0!");
      oldStartKey = oldKey;
      return this;
    }

    /**
     * Set old max depth.
     *
     * @param oldMaxDepth maximum depth of traversal
     * @return this builder
     */
    public Builder<R, W> oldMaxDepth(final @NonNegative long oldMaxDepth) {
      checkArgument(oldMaxDepth >= 0, "oldMaxDepth must be >= 0!");
      this.oldMaxDepth = oldMaxDepth;
      return this;
    }

    /**
     * Set start node key in new revision.
     *
     * @param newKey start node key in new revision
     * @return this builder
     */
    public Builder<R, W> newStartKey(final @NonNegative long newKey) {
      checkArgument(newKey >= 0, "newKey must be >= 0!");
      newStartKey = newKey;
      return this;
    }

    /**
     * Set new depth.
     *
     * @param newDepth depth of "root" node in new revision
     * @return this builder
     */
    public Builder<R, W> newDepth(final @NonNegative int newDepth) {
      checkArgument(newDepth >= 0, "newDepth must be >= 0!");
      this.newDepth = newDepth;
      return this;
    }

    /**
     * Set old depth.
     *
     * @param oldDepth depth of "root" node in old revision
     * @return this builder
     */
    public Builder<R, W> oldDepth(final int oldDepth) {
      checkArgument(oldDepth >= 0, "oldDepth must be >= 0!");
      this.oldDepth = oldDepth;
      return this;
    }

    /**
     * Set kind of diff-algorithm.
     *
     * @param diffAlgorithm {@link DiffAlgorithm} instance
     *
     * @return this builder
     */
    public Builder<R, W> diffAlgorithm(final DiffAlgorithm diffAlgorithm) {
      mDiffKind = checkNotNull(diffAlgorithm);
      return this;
    }

    /**
     * Set kind of hash. <strong>Must be the same as used for the database creation</strong>.
     *
     * @param kind {@link HashType} instance
     * @return this builder
     */
    public Builder<R, W> hashKind(final HashType kind) {
      hashKind = checkNotNull(kind);
      return this;
    }

    /**
     * Set if subtrees after detecting an insert/delete/replace should be skipped..
     *
     * @param skipSubtrees {@code true}, if subtrees should be skipped, {@code false} if not
     * @return this builder
     */
    public Builder<R, W> skipSubtrees(final boolean skipSubtrees) {
      this.skipSubtrees = skipSubtrees;
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
   * Do a full JSON diff.
   *
   * @param builder {@link Builder} reference
   */
  public static synchronized void invokeJsonDiff(final Builder<JsonNodeReadOnlyTrx, JsonNodeTrx> builder) {
    DiffAlgorithm.JSON.invoke(builder);
  }

  /**
   * Do a full diff.
   *
   * @param builder {@link Builder} reference
   */
  public static synchronized void invokeFullXmlDiff(final Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder) {
    DiffAlgorithm.XML_FULL.invoke(builder);
  }

  /**
   * Do a structural diff.
   *
   * @param builder {@link Builder} reference
   */
  public static synchronized void invokeStructuralXmlDiff(final Builder<XmlNodeReadOnlyTrx, XmlNodeTrx> builder) {
    DiffAlgorithm.XML_STRUCTURAL.invoke(builder);
  }
}
