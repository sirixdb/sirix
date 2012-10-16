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
package org.sirix.gui.view.model;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.NodeReadTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.diff.DiffDepth;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.diff.DiffFactory.DiffOptimized;
import org.sirix.diff.DiffTuple;
import org.sirix.diff.DiffObserver;
import org.sirix.diff.algorithm.fmse.Levenshtein;
import org.sirix.exception.SirixException;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.DiffDatabase;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.model.interfaces.TraverseModel;
import org.sirix.gui.view.smallmultiple.ECompare;
import org.sirix.gui.view.sunburst.AbsSunburstGUI;
import org.sirix.gui.view.sunburst.Pruning;
import org.sirix.gui.view.sunburst.Item;
import org.sirix.gui.view.sunburst.NodeRelations;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.SunburstItem.EStructType;
import org.sirix.gui.view.sunburst.axis.AbsSunburstAxis;
import org.sirix.gui.view.sunburst.axis.DiffSunburstAxis;
import org.sirix.gui.view.sunburst.model.Modification;
import org.sirix.gui.view.sunburst.model.Modifications;
import org.sirix.node.Kind;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import processing.core.PApplet;
import processing.core.PConstants;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.MoreExecutors;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.TransactionRunner;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.DatabaseException;

/**
 * Traverse and compare trees.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class TraverseCompareTree extends AbsTraverseModel implements
		Callable<Void>, DiffObserver, TraverseModel {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(TraverseCompareTree.class));

	/**
	 * Shared {@link ExecutorService} instance using the same thread for
	 * execution.
	 */
	private static final ExecutorService SAME_THREAD_EXECUTOR = MoreExecutors
			.sameThreadExecutor();

	/** Shared {@link ForkJoinPool} instance. */
	private static final ForkJoinPool FORK_JOIN_POOL = new ForkJoinPool();

	/** {@link Levenshtein} instance. */
	private final Levenshtein mLevenshtein = new Levenshtein();

	/** Diff threshold, determining when a database has to be used. */
	private static final int DIFF_THRESHOLD = 100_000_000;

	/** Timeout for {@link CountDownLatch}. */
	private static final long TIMEOUT_S = 6000; // 100mins

	/** Number of available processors. */
	private static final int PROCESSORS = Runtime.getRuntime()
			.availableProcessors();

	/** Thread pool. */
	private static final ExecutorService POOL = PROCESSORS > 3 ? Executors
			.newFixedThreadPool(PROCESSORS) : SAME_THREAD_EXECUTOR;

	/** Locking changes which are fired. */
	private final Semaphore mLock;

	/**
	 * {@link CountDownLatch} to wait until {@link List} of {@link DiffType}s has been
	 * created.
	 */
	private final CountDownLatch mStart;

	/** New revision to compare. */
	private final int mNewRevision;

	/** Old revision to compare. */
	private final int mOldRevision;

	/** Key in new revision from which to start traversal. */
	private final long mNewStartKey;

	/** Key in old revision from which to start traversal. */
	private final long mOldStartKey;

	/** {@link Model} implementation. */
	private final Model<?, ?> mModel;

	/** {@link List} of {@link SunburstItem}s. */
	private final List<SunburstItem> mItems;

	/** Maximum depth in the tree. */
	private int mDepthMax;

	/** {@link ReadDb} instance. */
	private final ReadDB mDb;

	/** Maximum descendant count in tree. */
	private int mMaxDescendantCount;

	/** Maximum descendant count {@link Future}. */
	private Future<Modification> mMaxDescendantCountFuture;

	/** Parent processing frame. */
	private final PApplet mParent;

	/** {@link NodeReadTrx} instance. */
	private NodeReadTrx mOldRtx;

	/** Weighting of modifications. */
	private float mModWeight;

	/** Maximum depth in new revision. */
	private int mNewDepthMax;

	/** {@link NodeReadTrx} on the revision to compare. */
	private NodeReadTrx mNewRtx;

	/** {@link Map} of {@link DiffTuple}s. */
	private Map<Integer, DiffTuple> mDiffs;

	/** Queue for diff-tuples used by a . */
	private BlockingQueue<DiffTuple> mDiffQueue;

	/** Start depth in the tree. */
	private int mDepth;

	/** Determines if tree should be pruned or not. */
	private Pruning mPrune;

	/** Determines if current item is pruned or not. */
	private boolean mIsPruned;

	/** {@link DiffSunburstAxis} instance. */
	private AbsSunburstAxis mAxis;

	/** GUI which extends {@link AbsSunburstGUI}. */
	private final AbsSunburstGUI mGUI;

	/** Determines how to compare the two trees. */
	private final ECompare mCompare;

	/** Database to handle diffs. */
	private final DiffDatabase mDiffDatabase;

	/** {@link TransactionRunner} instance. */
	private final TransactionRunner mRunner;

	/** Counts diff entries. */
	private int mEntries;

	/** Determines if diffs have been done. */
	private volatile boolean mDone;

	/** {@link BlockingQueue} for the modifications of each node. */
	private final BlockingQueue<Future<Modification>> mModificationQueue;

	/** Datastructure to capture DELETED nodes. */
	private final Map<Long, Integer> mOldKeys;

	/** Datastructure to capture INSERTED nodes. */
	private final Map<Long, Integer> mNewKeys;

	/** The observer. */
	private final DiffObserver mObserver;

	/** Determines if UPDATES have been fired. */
	private boolean mHasUpdatedNodes;

	/** Determines if last node has been {@code UPDATED}. */
	private boolean mLastNodeUpdated;

	/** Count root node modifications. */
	private int mModifications;

	/** Determines if move detection is enabled or disabled. */
	private final boolean mMoveDetection;

	/**
	 * Constructor.
	 * 
	 * @param pContainer
	 *          {@link SunburstContainer} reference
	 */
	public TraverseCompareTree(@Nonnull final SunburstContainer pContainer) {
		checkNotNull(pContainer);
		checkArgument(pContainer.getRevision() >= 0);
		checkArgument(pContainer.getOldStartKey() >= 0);
		checkArgument(pContainer.getNewStartKey() >= 0);
		checkArgument(pContainer.getDepth() >= 0);
		checkArgument(pContainer.getModWeight() >= 0);
		checkArgument(pContainer.getRevision() > pContainer.getOldRevision(),
				"paramNewRevision must be greater than the currently opened revision!");
		checkNotNull(pContainer.getPruning());
		checkNotNull(pContainer.getGUI());
		checkNotNull(pContainer.getModel());

		LOGWRAPPER.debug("new revision: " + pContainer.getRevision());
		LOGWRAPPER.debug("old revision: " + pContainer.getOldRevision());

		mObserver = this;
		mDiffDatabase = new DiffDatabase(new File("target"));
		mRunner = new TransactionRunner(mDiffDatabase.getEnvironment());
		mOldKeys = new HashMap<>();
		mNewKeys = new HashMap<>();
		mModel = pContainer.getModel();
		addPropertyChangeListener(mModel);
		mDb = mModel.getDb();
		mOldRevision = pContainer.getOldRevision() != -1 ? pContainer
				.getOldRevision() : mDb.getRevisionNumber();

		try {
			mNewRtx = mDb.getSession().beginNodeReadTrx(pContainer.getRevision());
			mOldRtx = mModel.getDb().getSession().beginNodeReadTrx(mOldRevision);
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}

		mGUI = pContainer.getGUI();
		mNewRevision = pContainer.getRevision();
		mModWeight = pContainer.getModWeight();
		mDiffs = new LinkedHashMap<>(1500);
		mStart = new CountDownLatch(2);
		mItems = new ArrayList<>();
		mParent = ((AbsModel<?, ?>) mModel).getParent();
		mDepth = pContainer.getDepth();
		mOldRtx.moveTo(pContainer.getNewStartKey());
		if (mOldRtx.getKind() == Kind.DOCUMENT_ROOT) {
			mOldRtx.moveToFirstChild();
		}
		mOldStartKey = pContainer.getNewStartKey() == 0 ? mOldRtx.getNodeKey()
				: pContainer.getNewStartKey();
		mNewRtx.moveTo(pContainer.getNewStartKey());
		if (mNewRtx.getKind() == Kind.DOCUMENT_ROOT) {
			mNewRtx.moveToFirstChild();
		}
		mNewStartKey = pContainer.getNewStartKey() == 0 ? mNewRtx.getNodeKey()
				: pContainer.getNewStartKey();
		mPrune = pContainer.getPruning();
		mLock = pContainer.getLock();
		mCompare = pContainer.getCompare();
		mDiffQueue = new LinkedBlockingQueue<>();
		mModificationQueue = new LinkedBlockingQueue<>();
		mHasUpdatedNodes = (mPrune == Pruning.ITEMSIZE ? true : false);
		mLastNodeUpdated = false;
		mMoveDetection = pContainer.getMoveDetection();
	}

	@Override
	public Void call() {
		final long startTime = System.nanoTime();
		LOGWRAPPER.debug("Build sunburst items.");

		try {
			firePropertyChange("progress", null, 0);

			// Invoke diff.
			LOGWRAPPER.debug("CountDownLatch: " + mStart.getCount());

			POOL.submit(new Callable<Void>() {
				@Override
				public Void call() throws SirixException {
					DiffOptimized optimized = DiffOptimized.NO;
					if (mPrune == Pruning.DIFF
							|| mPrune == Pruning.DIFF_WITHOUT_SAMEHASHES) {
						optimized = DiffOptimized.HASHED;
					}
					DiffFactory.invokeStructuralDiff(new DiffFactory.Builder(mDb
							.getSession(), mNewRevision, mOldRtx.getRevisionNumber(),
							optimized, ImmutableSet.of(mObserver)).setNewDepth(mDepth)
							.setOldDepth(mDepth).setNewStartKey(mNewStartKey)
							.setOldStartKey(mOldStartKey));
					return null;
				}
			});

			if (PROCESSORS == 2) {
				final ExecutorService pool = Executors.newSingleThreadExecutor();
				mDepthMax = pool.submit(new Callable<Integer>() {
					@Override
					public Integer call() {
						// Maximum depth in old revision.
						return getDepthMax();
					}
				}).get();
				pool.shutdown();
			} else {
				mDepthMax = POOL.submit(new Callable<Integer>() {
					@Override
					public Integer call() {
						// Maximum depth in old revision.
						return getDepthMax();
					}
				}).get();
			}

			// Wait for diff list to complete.
			final boolean done = mStart.await(TIMEOUT_S, TimeUnit.SECONDS);
			if (!done) {
				LOGWRAPPER.error("Diff failed - Timeout occured after " + TIMEOUT_S
						+ " seconds!");
			}

			final int size = mDiffs.size();
			if (mEntries > DIFF_THRESHOLD) {
				final int mapSize = mDiffDatabase.getMap().size();
				LOGWRAPPER.debug("mapSize: " + mapSize);
			}

			LOGWRAPPER.debug("size: " + size);
			int i = 0;
			if (mMoveDetection) {
				detectMoves();
			}

			i = 0;
			final Map<Integer, DiffTuple> diffs = mEntries > DIFF_THRESHOLD ? mDiffDatabase
					.getMap() : mDiffs;
			firePropertyChange("diffs", null, diffs);

			for (mAxis = new DiffSunburstAxis(IncludeSelf.YES, this, mNewRtx,
					mOldRtx, diffs, mDepthMax, mDepth, mPrune); mAxis.hasNext(); i++) {
				mAxis.next();
				if (mCompare == ECompare.SINGLEINCREMENTAL) {
					final int progress = (int) ((i / (float) size) * 100);
					firePropertyChange("progress", null, progress);
				}
			}
		} catch (final InterruptedException | ExecutionException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}

		try {
			mOldRtx.close();
			mNewRtx.close();
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		LOGWRAPPER.info(mItems.size() + " SunburstItems created!");
		LOGWRAPPER.debug("oldMaxDepth: " + mDepthMax);

		mLock.acquireUninterruptibly();

		// Order of property changes is significant.
		firePropertyChange("oldRev", null, mOldRevision);
		firePropertyChange("newRev", null, mNewRevision);
		firePropertyChange("oldmaxdepth", null, mDepthMax);
		firePropertyChange("maxDepth", null, mNewDepthMax);
		firePropertyChange("items", null, mItems);
		firePropertyChange("updated", null, mHasUpdatedNodes);
		firePropertyChange("revision", null, mNewRevision);
		firePropertyChange("done", null, true);
		firePropertyChange("progress", null, 100);

		LOGWRAPPER.debug("Property changes sent!");
		// Lock is released in the controller.

		// mDiffDatabase.close();

		final long endTime = System.nanoTime();

		System.out.println((endTime - startTime) * 1e-6 / 1000);

		return null;
	}

	/** Detect moves. */
	private void detectMoves() {
		// Only do move detection if the diffs don't have to be saved in a
		// berkeleydb database.
		if (mDiffs.size() <= DIFF_THRESHOLD) {
			for (final DiffTuple diffCont : mDiffs.values()) {
				final Integer newIndex = mNewKeys.get(diffCont.getOldNodeKey());
				if (newIndex != null
						&& (diffCont.getDiff() == DiffType.DELETED || diffCont.getDiff() == DiffType.MOVEDFROM)) {
					LOGWRAPPER.debug("new node key: "
							+ mDiffs.get(newIndex).getNewNodeKey());
					mDiffs.get(newIndex).setDiff(DiffType.MOVEDTO);
				}
				final Integer oldIndex = mOldKeys.get(diffCont.getNewNodeKey());
				if (oldIndex != null
						&& (diffCont.getDiff() == DiffType.INSERTED || diffCont.getDiff() == DiffType.MOVEDTO)) {
					mDiffs.get(oldIndex).setDiff(DiffType.MOVEDFROM)
							.setIndex(mNewKeys.get(diffCont.getNewNodeKey()));
				}
			}
		}
	}

	@Override
	public void diffListener(@Nonnull final DiffType pDiff,
			@Nonnull final long pNewNodeKey, @Nonnull final long pOldNodeKey,
			@Nonnull final DiffDepth pDepth) {
		LOGWRAPPER.debug("kind of diff: " + pDiff);

		if (mPrune != Pruning.DIFF_WITHOUT_SAMEHASHES
				|| (mPrune == Pruning.DIFF_WITHOUT_SAMEHASHES && pDiff != DiffType.SAMEHASH)
				|| mEntries == 0) {
			final DiffTuple diffCont = new DiffTuple(pDiff, pNewNodeKey, pOldNodeKey,
					pDepth);
			final DiffType diff = diffCont.getDiff();
			if (!mHasUpdatedNodes && mLastNodeUpdated) {
				// Has at least one diff, thus it's safe.
				final DiffTuple oldCont = mDiffs.get(mEntries - 1);
				final int oldDepth = getDepth(oldCont);
				final int newDepth = getDepth(diffCont);
				if (newDepth > oldDepth) {
					mHasUpdatedNodes = true;
				}
			}
			try {
				mDiffQueue.put(diffCont);
			} catch (final InterruptedException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
			mDiffs.put(mEntries, diffCont);
			switch (diff) {
			case INSERTED:
				mNewKeys.put(pNewNodeKey, mEntries);
				mModifications++;
				break;
			case DELETED:
				mOldKeys.put(pOldNodeKey, mEntries);
				mModifications++;
				break;
			case UPDATED:
				mLastNodeUpdated = true;
				mModifications++;
				break;
			case REPLACEDNEW:
			case REPLACEDOLD:
				mModifications++;
				break;
			default:
				// Do nothing.
			}
			mEntries++;
			if (mEntries % DIFF_THRESHOLD == 0) {
				try {
					// Works even if transactions are not enabled.
					mRunner.run(new PopulateDatabase(mDiffDatabase, mDiffs, mDiffDatabase
							.getMap().size()));
				} catch (final Exception e) {
					LOGWRAPPER.error(e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * Get depth of a node from a {@link DiffTuple} instance.
	 * 
	 * @param pDiffCont
	 *          {@link DiffTuple} instance
	 * @return the {@code depth} of the node
	 */
	private int getDepth(@Nonnull final DiffTuple pDiffCont) {
		int depth;
		final DiffType diff = pDiffCont.getDiff();
		if (diff == DiffType.DELETED || diff == DiffType.MOVEDFROM
				|| diff == DiffType.REPLACEDOLD) {
			depth = pDiffCont.getDepth().getOldDepth();
		} else {
			depth = pDiffCont.getDepth().getNewDepth();
		}
		return depth;
	}

	@Override
	public void diffDone() {
		if (mDiffDatabase.getMap().get(1) != null) {
			try {
				mRunner.run(new PopulateDatabase(mDiffDatabase, mDiffs, mDiffDatabase
						.getMap().size()));
			} catch (final Exception e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}
		mDone = true;
		mStart.countDown();
	}

	/** Populate the diff database. */
	private static class PopulateDatabase implements TransactionWorker {

		/** {@link StoredMap} reference. */
		private Map<Integer, DiffTuple> mMap;

		/** {@link List} of {@link DiffTuple}s. */
		private Map<Integer, DiffTuple> mValue;

		/** Old size of database. */
		private int mOldSize;

		/**
		 * Constructor.
		 * 
		 * @param pDatabase
		 *          {@link DiffDatabase} reference
		 * @param pValue
		 *          {@link Map} of {@link DiffTuple}s
		 * @param pOldSize
		 *          size of the database map
		 */
		public PopulateDatabase(@Nonnull final DiffDatabase pDatabase,
				@Nonnull final Map<Integer, DiffTuple> pValue, final int pOldSize) {
			checkNotNull(pDatabase);
			checkNotNull(pValue);
			checkArgument(pOldSize >= 0, "pOldSize must be >= 0!");
			mMap = pDatabase.getMap();
			mValue = pValue;
			mOldSize = pOldSize;
		}

		@Override
		public void doWork() throws DatabaseException {
			final int size = mMap.size();
			for (int i = size, j = mOldSize; j < mValue.size(); i++, j++) {
				mMap.put(i, mValue.get(j));
			}
		}
	}

	/**
	 * Get the maximum depth in the tree of the nodes which haven't changed.
	 * 
	 * @return {@code maximum depth} of unchanged nodes
	 */
	private int getDepthMax() {
		int depthMax = 0;
		for (boolean isNotEmpty = !mDiffQueue.isEmpty(); ((isNotEmpty = !mDiffQueue
				.isEmpty()) == true) || !mDone;) {
			if (isNotEmpty) {
				final DiffTuple tuple = mDiffQueue.peek();
				final DiffType diff = tuple.getDiff();
				if (diff == DiffType.SAME || diff == DiffType.SAMEHASH) {
					// Set depth max.
					depthMax = Math.max(mDiffQueue.poll().getDepth().getOldDepth()
							- mDepth, depthMax);
				} else {
					mDiffQueue.poll();
				}
			}
		}
		mStart.countDown();
		return depthMax;
	}

	@Override
	public BlockingQueue<Future<Modification>> getModificationQueue() {
		return mModificationQueue;
	}

	@Override
	public float createSunburstItem(@Nonnull final Item pItem,
			@Nonnegative final int pDepth, @Nonnegative final int pIndex) {
		checkNotNull(pItem);
		checkArgument(pDepth >= 0, "pDepth must be positive!");
		checkArgument(pIndex >= 0, "pIndex must be >= 0!");

		// Initialize variables.
		final float angle = pItem.mAngle;
		final float parExtension = pItem.mExtension;
		final int indexToParent = pItem.mIndexToParent;
		final int descendantCount = pItem.mDescendantCount;
		final int parentDescCount = pItem.mParentDescendantCount;
		final int modificationCount = pItem.mModificationCount;
		long parentModificationCount = pItem.mParentModificationCount;
		final boolean subtract = pItem.mSubtract;
		final DiffTuple diffCont = pItem.mDiff;
		final int origDepth = pItem.mOrigDepth;
		final int nextDepth = pItem.mNextDepth;
		final int depth = pDepth;

		// Calculate extension.
		float extension = 2 * PConstants.PI;
		if (indexToParent > -1) {
			if (mItems.get(indexToParent).getSubtract()) {
				parentModificationCount -= FACTOR;
			}
			extension = (1f - mModWeight)
					* (parExtension * (float) descendantCount / ((float) parentDescCount - 1f))
					+ mModWeight
					// -1 because we add the descendant-or-self count to the
					// modificationCount/parentModificationCount.
					* (parExtension * (float) modificationCount / ((float) parentModificationCount - 1f));
		}

		LOGWRAPPER.debug("ITEM: " + pIndex);
		LOGWRAPPER.debug("modificationCount: " + modificationCount);
		LOGWRAPPER.debug("parentModificationCount: " + parentModificationCount);
		LOGWRAPPER.debug("descendantCount: " + descendantCount);
		LOGWRAPPER.debug("parentDescCount: " + parentDescCount);
		LOGWRAPPER.debug("indexToParent: " + indexToParent);
		LOGWRAPPER.debug("extension: " + extension);
		LOGWRAPPER.debug("depth: " + depth);
		LOGWRAPPER.debug("next Depth: " + nextDepth);
		LOGWRAPPER.debug("angle: " + angle);

		if (mPrune == Pruning.ITEMSIZE
				&& extension < TraverseModel.ANGLE_TO_PRUNE
				&& modificationCount <= descendantCount) {
			nodePruned();
		} else {
			// Add a sunburst item.
			if (mPrune == Pruning.DIFF && diffCont.getDiff() == DiffType.SAMEHASH) {
				mIsPruned = true;
			} else {
				mIsPruned = false;
			}

			final NodeReadTrx rtx = (diffCont.getDiff() == DiffType.DELETED
					|| diffCont.getDiff() == DiffType.MOVEDFROM || diffCont.getDiff() == DiffType.REPLACEDOLD) ? mOldRtx
					: mNewRtx;

			final EStructType structKind = nextDepth > depth ? EStructType.ISINNERNODE
					: EStructType.ISLEAFNODE;

			// Set node relations.
			String text = "";
			NodeRelations relations = null;
			final DiffType currDiff = diffCont.getDiff();
			if (rtx.getKind() == Kind.TEXT) {
				if (currDiff == DiffType.DELETED || currDiff == DiffType.MOVEDFROM
						|| currDiff == DiffType.REPLACEDOLD) {
					text = mOldRtx.getValue();
				} else {
					text = mNewRtx.getValue();
				}
				if (currDiff == DiffType.UPDATED
						|| ((currDiff == DiffType.REPLACEDNEW || currDiff == DiffType.REPLACEDOLD) && mOldRtx
								.getKind() == mNewRtx.getKind())) {
					final String oldValue = mOldRtx.getValue();
					final String newValue = mNewRtx.getValue();
					float similarity = 1;
					// try {
					// Integer.parseInt(oldValue);
					// Integer.parseInt(newValue);
					//
					// // TODO: Implement similarity measure on numerical data (easy!).
					// } catch (final NumberFormatException e) {
					similarity = mLevenshtein.getSimilarity(oldValue, newValue);
					// }
					relations = new NodeRelations(origDepth, depth, structKind,
							similarity, 0, 1, indexToParent).setSubtract(subtract);
				} else if (currDiff == DiffType.SAME || currDiff == DiffType.SAMEHASH) {
					relations = new NodeRelations(origDepth, depth, structKind, 1, 0, 1,
							indexToParent).setSubtract(subtract);
				} else {
					relations = new NodeRelations(origDepth, depth, structKind, 0, 0, 1,
							indexToParent).setSubtract(subtract);
				}
			} else {
				if (mMaxDescendantCount == 0) {
					if (mPrune == Pruning.NO) {
						try {
							mMaxDescendantCount = mMaxDescendantCountFuture.get()
									.getDescendants();
						} catch (final InterruptedException | ExecutionException e) {
							LOGWRAPPER.error(e.getMessage(), e);
						}
					} else {
						mMaxDescendantCount = mAxis.getDescendantCount();
					}
				}
				relations = new NodeRelations(origDepth, depth, structKind,
						descendantCount, 1, mMaxDescendantCount, indexToParent)
						.setSubtract(subtract);
			}

			// Build item.
			final SunburstItem.Builder builder = new SunburstItem.Builder(mParent,
					angle, extension, relations, mDb, mGUI).setNodeKey(rtx.getNodeKey())
					.setKind(rtx.getKind()).setDiff(diffCont.getDiff());

			if (modificationCount > descendantCount) {
				final int diffCounts = (modificationCount - descendantCount)
						/ TraverseModel.FACTOR;
				LOGWRAPPER.debug("modCount: " + diffCounts);
				builder.setModifications(diffCounts);
			}

			if (text.isEmpty()) {
				QName name = null;
				if (diffCont.getDiff() == DiffType.DELETED
						|| diffCont.getDiff() == DiffType.MOVEDFROM
						|| diffCont.getDiff() == DiffType.REPLACEDOLD) {
					name = mOldRtx.getName();
					builder.setAttributes(fillAttributes(mOldRtx));
					builder.setNamespaces(fillNamespaces(mOldRtx));
					builder.setOldKey(mOldRtx.getNodeKey());

					LOGWRAPPER.debug("name: " + name.getLocalPart());
					builder.setOldQName(name);
				} else {
					name = mNewRtx.getName();
					builder.setAttributes(fillAttributes(mNewRtx));
					builder.setNamespaces(fillNamespaces(mNewRtx));

					LOGWRAPPER.debug("name: " + name.getLocalPart());
					builder.setQName(name);
				}
			} else {
				LOGWRAPPER.debug("text: " + text);

				if (currDiff == DiffType.DELETED || currDiff == DiffType.MOVEDFROM
						|| currDiff == DiffType.REPLACEDOLD) {
					builder.setOldText(text);
					builder.setOldKey(mOldRtx.getNodeKey());
				} else {
					builder.setText(text);
				}
			}
			updated(diffCont.getDiff(), builder);

			final SunburstItem item = builder.build();
			if (item.getDiff() == DiffType.MOVEDFROM) {
				LOGWRAPPER.debug("movedToIndex: " + diffCont.getIndex());
				item.setIndexMovedTo(diffCont.getIndex() - mAxis.getPrunedNodes());
			}

			mItems.add(item);

			// Set depth max.
			mNewDepthMax = Math.max(depth, mNewDepthMax);
		}

		return extension;
	}

	/** Subtree of current item is going to be pruned. */
	private void nodePruned() {
		mIsPruned = true;
		mAxis.decrementIndex();
	}

	/**
	 * Add old node text or {@link QName} to the {@link SunburstItem.Builder}.
	 * 
	 * @param pDiff
	 *          determines if it's value is {@code EDiff.UPDATED},
	 *          {@code EDiff.REPLACEDOLD} or {@code EDiff.REPLACEDNEW}
	 * @param pBuilder
	 *          {@link SunburstItem.Builder} reference
	 */
	private void updated(@Nonnull final DiffType pDiff,
			@Nonnull final SunburstItem.Builder pBuilder) {
		assert pBuilder != null;
		if (pDiff == DiffType.UPDATED) {
			if (mOldRtx.getKind() == Kind.TEXT) {
				pBuilder.setOldText(mOldRtx.getValue());
			} else {
				pBuilder.setOldQName(mOldRtx.getName());
			}
		}
	}

	@Override
	public boolean getIsPruned() {
		return mIsPruned;
	}

	@Override
	public void descendants(@Nonnull final Optional<NodeReadTrx> pRtx)
			throws InterruptedException, ExecutionException {
		try {
			final ExecutorService executor = Executors.newSingleThreadExecutor();
			executor.submit(new GetDescendants());
			executor.shutdown();
		} catch (final Exception e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	/**
	 * Callable to get {@code descendant-or-self}-count as well as the
	 * {@code modification}-count of each node.
	 */
	private final class GetDescendants implements Callable<Void> {

		@Override
		public Void call() throws Exception {
			final int depthThreshold = 4;

			final Map<Integer, DiffTuple> diffs = mEntries > DIFF_THRESHOLD ? mDiffDatabase
					.getMap() : mDiffs;
			DiffTuple diff = diffs.get(0);
			final int rootDepth = (diff.getDiff() == DiffType.DELETED
					|| diff.getDiff() == DiffType.MOVEDFROM || diff.getDiff() == DiffType.REPLACEDOLD) ? diff
					.getDepth().getOldDepth() : diff.getDepth().getNewDepth();
			final boolean subtract = (diff.getDiff() != DiffType.SAME && diff.getDiff() != DiffType.SAMEHASH) ? true
					: false;
			boolean first = true;
			if (diffs.size() == 1) {
				final Future<Modification> modifications = SAME_THREAD_EXECUTOR
						.submit(Callables.returning(new Modification(TraverseModel.FACTOR
								* mModifications, diffs.size(), subtract)));
				mMaxDescendantCountFuture = modifications;
				mModificationQueue.put(modifications);
			} else {
				assert diffs.size() > 1;
				diff = diffs.get(1);
				int currDepth = (diff.getDiff() == DiffType.DELETED
						|| diff.getDiff() == DiffType.MOVEDFROM || diff.getDiff() == DiffType.REPLACEDOLD) ? diff
						.getDepth().getOldDepth() : diff.getDepth().getNewDepth();
				for (int index = 0; index < mDiffs.size() && currDepth > rootDepth; index++) {
					Future<Modification> modifications = null;
					if (first) {
						first = false;
						modifications = SAME_THREAD_EXECUTOR.submit(Callables
								.returning(new Modification(TraverseModel.FACTOR
										* mModifications, diffs.size(), subtract)));
						mMaxDescendantCountFuture = modifications;
					} else {
						if (currDepth > depthThreshold || PROCESSORS < 3) {
							modifications = SAME_THREAD_EXECUTOR.submit(Modifications
									.getInstance(index, diffs));
						} else {
							final ForkJoinTask<Modification> mods = Modifications
									.getInstance(index, diffs);
							modifications = FORK_JOIN_POOL.submit(mods);
						}
					}
					assert modifications != null;
					mModificationQueue.put(modifications);
					if (index + 1 < diffs.size()) {
						final DiffTuple currDiffCont = diffs.get(index + 1);
						if (currDiffCont.getDiff() == DiffType.DELETED
								|| currDiffCont.getDiff() == DiffType.MOVEDFROM
								|| currDiffCont.getDiff() == DiffType.REPLACEDOLD) {
							currDepth = currDiffCont.getDepth().getOldDepth();
						} else {
							currDepth = currDiffCont.getDepth().getNewDepth();
						}
					}
				}
			}

			return null;
		}
	}
}
