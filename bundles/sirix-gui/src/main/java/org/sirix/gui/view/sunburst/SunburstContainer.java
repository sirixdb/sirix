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

package org.sirix.gui.view.sunburst;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.diff.DiffFactory.EDiff;
import org.sirix.gui.view.model.interfaces.Container;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.smallmultiple.ECompare;
import org.sirix.gui.view.sunburst.SunburstItem;

/**
 * Contains settings used for updating the model.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstContainer implements Container<SunburstContainer> {

	/** Index of currently clicked {@link SunburstItem}. */
	private transient int mHitTestIndex;

	/** Old revision. */
	private transient int mOldRevision = -1;

	/** Revision to compare. */
	private transient int mRevision;

	/** Max depth in the tree. */
	private transient int mDepth;

	/** Modification weight. */
	private transient float mModWeight;

	/** Node key to start from in old revision. */
	private transient long mOldStartKey;

	/** Node key to start from in new revision. */
	private transient long mNewStartKey;

	/** Determines if pruning should be enabled or not. */
	private transient Pruning mPruning = Pruning.NO;

	/** GUI which extends {@link AbsSunburstGUI}. */
	private transient AbsSunburstGUI mGUI;

	/**
	 * Lock, such that the GUI doesn't receive notifications from many Models at
	 * the same time.
	 */
	private transient Semaphore mLock = new Semaphore(1);

	/** Determines how to compare trees. */
	private transient ECompare mCompare = ECompare.SINGLEINCREMENTAL;

	/** {@link Model} implementation. */
	private final Model<SunburstContainer, SunburstItem> mModel;

	/** {@link EDiff} of root node of subtree to compare. */
	private transient EDiff mDiff = EDiff.SAME;

	/** Shared {@link CountDownLatch} instance. */
	private transient CountDownLatch mLatch;

	/** Determines if move detection should be used. */
	private transient boolean mMoveDetection;

	/**
	 * Constructor.
	 * 
	 * @param paramGUI
	 *          GUI which extends {@link AbsSunburstGUI}
	 * @param paramModel
	 *          {@link Model} implementation
	 */
	public SunburstContainer(final AbsSunburstGUI paramGUI,
			final Model<SunburstContainer, SunburstItem> paramModel) {
		mGUI = checkNotNull(paramGUI);
		mModel = checkNotNull(paramModel);
	}

	/**
	 * Set lock.
	 * 
	 * @param paramLock
	 *          shared semaphore
	 * @return instance of this class
	 */
	public SunburstContainer setLock(final Semaphore paramLock) {
		mLock = checkNotNull(paramLock);
		return this;
	}

	/**
	 * Set latch.
	 * 
	 * @param paramLock
	 *          shared latch
	 * @return instance of this class
	 */
	public SunburstContainer setLatch(final CountDownLatch paramLatch) {
		mLatch = checkNotNull(paramLatch);
		return this;
	}

	/**
	 * Set the GUI.
	 * 
	 * @param paramGUI
	 *          GUI which extends {@link AbsSunburstGUI}
	 * @return instance of this class
	 * */
	public SunburstContainer setGUI(final AbsSunburstGUI paramGUI) {
		mGUI = checkNotNull(paramGUI);
		return this;
	}

	/**
	 * Get GUI.
	 * 
	 * @return GUI which extends {@link AbsSunburstGUI}
	 */
	public AbsSunburstGUI getGUI() {
		return mGUI;
	}

	/** {@inheritDoc} */
	@Override
	public SunburstContainer setOldStartKey(@Nonnegative final long paramKey) {
		checkArgument(paramKey >= 0, "paramKey must be >= 0!");
		mOldStartKey = paramKey;
		return this;
	}

	/** {@inheritDoc} */
	@Override
	public SunburstContainer setNewStartKey(@Nonnegative final long paramKey) {
		checkArgument(paramKey >= 0, "paramKey must be >= 0!");
		mNewStartKey = paramKey;
		return this;
	}

	/**
	 * Get start key of new revision.
	 * 
	 * @return the key
	 */
	public long getNewStartKey() {
		return mNewStartKey;
	}

	/**
	 * Get start key of old revision.
	 * 
	 * @return the key
	 */
	public long getOldStartKey() {
		return mOldStartKey;
	}

	/**
	 * Get latch.
	 * 
	 * @return the latch instance
	 */
	public CountDownLatch getLatch() {
		return mLatch;
	}

	/**
	 * Set revision to compare.
	 * 
	 * @param paramRevision
	 *          the Revision to set
	 * @return instance of this class
	 */
	public SunburstContainer setRevision(@Nonnegative final int paramRevision) {
		assert paramRevision > 0;
		mRevision = paramRevision;
		return this;
	}

	/**
	 * Set old revision.
	 * 
	 * @param paramRevision
	 *          the old revision to set
	 * @return instance of this class
	 */
	public SunburstContainer setOldRevision(final @Nonnegative int paramRevision) {
		checkArgument(paramRevision >= 0, "paramRevision must be >= 0!");
		mOldRevision = paramRevision;
		return this;
	}

	/**
	 * Get the revision to compare.
	 * 
	 * @return the revision
	 */
	public int getRevision() {
		return mRevision;
	}

	/**
	 * Set modification weight.
	 * 
	 * @param paramModWeight
	 *          the modWeight to set
	 * @return instance of this class
	 */
	public SunburstContainer setModWeight(@Nonnegative final float paramModWeight) {
		checkArgument(paramModWeight >= 0, "paramModWeight must be >= 0!");
		mModWeight = paramModWeight;
		return this;
	}

	/**
	 * Set all remaining member variables.
	 * 
	 * @param paramRevision
	 *          revision to compare
	 * @param paramDepth
	 *          Depth in the tree
	 * @param paramModificationWeight
	 *          weighting of modifications
	 * @return instance of this class
	 */
	public SunburstContainer setAll(@Nonnegative final int paramRevision,
			@Nonnegative final int paramDepth, final float paramModificationWeight) {
		setRevision(paramRevision);
		setDepth(paramDepth);
		setModWeight(paramModificationWeight);
		return this;
	}

	/**
	 * Set depth.
	 * 
	 * @param paramDepth
	 *          the depth to set
	 * @return instance of this class
	 */
	public SunburstContainer setDepth(@Nonnegative final int paramDepth) {
		checkArgument(paramDepth >= 0, "paramDepth must be >= 0!");
		mDepth = paramDepth;
		return this;
	}

	@Override
	public SunburstContainer setPruning(@Nonnull final Pruning pPruning) {
		mPruning = checkNotNull(pPruning);
		return this;
	}

	/**
	 * Get pruning.
	 * 
	 * @return pruning method
	 */
	public @Nonnull
	Pruning getPruning() {
		return mPruning;
	}

	/**
	 * Get the depth.
	 * 
	 * @return the depth
	 */
	public int getDepth() {
		return mDepth;
	}

	/**
	 * Get the diff.
	 * 
	 * @return the diif
	 */
	public EDiff getDiff() {
		return mDiff;
	}

	/**
	 * Get modification weight.
	 * 
	 * @return the modification weight
	 */
	public float getModWeight() {
		return mModWeight;
	}

	/**
	 * Set hit test index.
	 * 
	 * @param paramHitTestIndex
	 *          the hitTestIndex to set
	 * @return instance of this class
	 */
	public SunburstContainer setHitTestIndex(
			@Nonnegative final int paramHitTestIndex) {
		assert paramHitTestIndex > -1;
		mHitTestIndex = paramHitTestIndex;
		return this;
	}

	/**
	 * Set move detection.
	 * 
	 * @param pMoveDetection
	 *          determines if move detection should be used
	 * @return instance of this class
	 */
	public SunburstContainer setMoveDetection(final boolean pMoveDetection) {
		mMoveDetection = pMoveDetection;
		return this;
	}

	/**
	 * Get hit test index.
	 * 
	 * @return the hitTestIndex
	 */
	public int getHitTestIndex() {
		return mHitTestIndex;
	}

	/**
	 * Get lock.
	 * 
	 * @return semaphore initialized to one
	 */
	public Semaphore getLock() {
		return mLock;
	}

	/**
	 * Set compare method.
	 * 
	 * @param paramCompare
	 *          determines method to compare trees
	 * @return instance of this class
	 */
	public SunburstContainer setCompare(@Nonnull final ECompare paramCompare) {
		mCompare = checkNotNull(paramCompare);
		return this;
	}

	/**
	 * Set diff of root node.
	 * 
	 * @param paramDiff
	 *          determines diff of root node to compare
	 * @return instance of this class
	 */
	public SunburstContainer setDiff(@Nonnull final EDiff paramDiff) {
		mDiff = checkNotNull(paramDiff);
		return this;
	}

	/**
	 * Get compare method.
	 * 
	 * @return the compare method
	 */
	public ECompare getCompare() {
		return mCompare;
	}

	/**
	 * Get old revision.
	 * 
	 * @return old revision
	 */
	public int getOldRevision() {
		return mOldRevision;
	}

	/**
	 * Get model.
	 * 
	 * @return {@link Model} implementation
	 */
	public Model<SunburstContainer, SunburstItem> getModel() {
		return mModel;
	}

	/**
	 * Get move detection.
	 * 
	 * @return {@code true} if move detection is enabled, {@code false} otherwise
	 */
	public boolean getMoveDetection() {
		return mMoveDetection;
	}
}
