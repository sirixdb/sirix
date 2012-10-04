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

package org.sirix.gui.view.sunburst.model;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Callables;
import controlP5.ControlGroup;

import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Syntax;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.ISession;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.model.AbsModel;
import org.sirix.gui.view.model.AbsTraverseModel;
import org.sirix.gui.view.model.interfaces.IChangeModel;
import org.sirix.gui.view.model.interfaces.IContainer;
import org.sirix.gui.view.sunburst.AbsSunburstGUI;
import org.sirix.gui.view.sunburst.EPruning;
import org.sirix.gui.view.sunburst.Item;
import org.sirix.gui.view.sunburst.NodeRelations;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstGUI;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.SunburstItem.EStructType;
import org.sirix.gui.view.sunburst.SunburstPopupMenu;
import org.sirix.gui.view.sunburst.axis.SunburstDescendantAxis;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.service.xml.shredder.EShredderCommit;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.settings.EFixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;
import processing.core.PApplet;
import processing.core.PConstants;

/**
 * <h1>SunburstModel</h1>
 * 
 * <p>
 * The model, which interacts with sirix.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstModel extends
		AbsModel<SunburstContainer, SunburstItem> implements IChangeModel {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(SunburstModel.class));

	/** {@link INodeWriteTrx} instance. */
	private INodeWriteTrx mWtx;

	/**
	 * Constructor.
	 * 
	 * @param pApplet
	 *          the processing {@link PApplet} core library
	 * @param pDb
	 *          {@link ReadDB} reference
	 */
	public SunburstModel(@Nonnull final PApplet pApplet, @Nonnull final ReadDB pDb) {
		super(pApplet, pDb);
	}

	@Override
	public void update(@Nonnull final IContainer<SunburstContainer> pContainer) {
		mLastItems.push(new ArrayList<>(mItems));
		mLastDepths.push(mLastMaxDepth);
		traverseTree(pContainer);
	}

	@Override
	public void traverseTree(
			@Nonnull final IContainer<SunburstContainer> pContainer) {
		final SunburstContainer container = (SunburstContainer) checkNotNull(pContainer);
		checkArgument(container.getNewStartKey() >= 0);
		checkArgument(container.getOldStartKey() >= 0);
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			executor.submit(new TraverseTree(container.getNewStartKey(), container
					.getPruning(), container.getGUI(), this));
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
		shutdown(executor);
	}

	/** Traverse a tree (single revision). */
	private static final class TraverseTree extends AbsTraverseModel implements
			Callable<Void> {

		/** Key from which to start traversal. */
		private transient long mKey;

		/** {@link SunburstModel} instance. */
		private final SunburstModel mModel;

		/** {@link INodeReadTrx} instance. */
		private final INodeReadTrx mRtx;

		/** {@link List} of {@link SunburstItem}s. */
		private final List<SunburstItem> mItems;

		/** Maximum depth in the tree. */
		private transient int mDepthMax;

		/** Minimum text length. */
		private transient int mMinTextLength;

		/** Maximum text length. */
		private transient int mMaxTextLength;

		/** {@link ReadDb} instance. */
		private final ReadDB mDb;

		/** Maximum descendant count in tree. */
		private transient int mMaxDescendantCount;

		/** Parent processing frame. */
		private transient PApplet mParent;

		/** Depth in the tree. */
		private transient int mDepth;

		/** Determines if tree should be pruned or not. */
		private transient EPruning mPruning;

		/** Determines if current item has been pruned or not. */
		private transient boolean mPruned;

		/** GUI which extends the {@link SunburstGUI}. */
		private final AbsSunburstGUI mGUI;

		/**
		 * Constructor.
		 * 
		 * @param pKey
		 *          key from which to start traversal
		 * @param pPruning
		 *          pruning of nodes
		 * @param pModel
		 *          the {@link SunburstModel}
		 * @param pGUI
		 *          GUI which extends the {@link SunburstGUI}
		 */
		private TraverseTree(@Nonnegative final long pKey,
				@Nonnull final EPruning pPruning, @Nonnull final AbsSunburstGUI pGUI,
				@Nonnull final SunburstModel pModel) throws SirixException {
			assert pKey >= 0;
			assert pModel != null;
			assert pGUI != null;
			mModel = pModel;
			addPropertyChangeListener(mModel);
			mPruning = pPruning;
			mDb = mModel.getDb();
			mRtx = mModel.getDb().getSession()
					.beginNodeReadTrx(mModel.getDb().getRevisionNumber());
			mMaxDescendantCount = (int) mRtx.getDescendantCount();
			boolean moved = pKey == EFixed.DOCUMENT_NODE_KEY.getStandardProperty() ? mRtx
					.moveToFirstChild().hasMoved() : mRtx.moveTo(pKey).hasMoved();
			assert moved;
			mKey = mRtx.getNodeKey();
			mParent = mModel.getParent();
			mItems = new LinkedList<>();
			mGUI = pGUI;
		}

		@Override
		public Void call() {
			LOGWRAPPER.debug("Build sunburst items.");

			firePropertyChange("progress", null, 0);

			// Get min and max textLength.
			if (mPruning == EPruning.NO) {
				getMinMaxTextLength();
			}

			try {
				// Iterate over nodes and perform appropriate stack actions internally.
				int i = 0;
				for (final SunburstDescendantAxis axis = new SunburstDescendantAxis(
						mRtx, EIncludeSelf.YES, this, mPruning); axis.hasNext(); i++) {
					axis.next();
					final int progress = (int) ((float) i / (float) mMaxDescendantCount * (float) 100);
					if (progress > 0 && progress < 100) {
						firePropertyChange("progress", null, progress);
					}
				}

				LOGWRAPPER.debug("Built " + mItems.size() + " SunburstItems!");
				mRtx.close();
			} catch (final SirixException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}

			// Fire property changes.
			firePropertyChange("maxDepth", null, mDepthMax);
			firePropertyChange("items", null, mItems);
			firePropertyChange("done", null, true);
			firePropertyChange("progress", null, 100);

			return null;
		}

		@Override
		public BlockingQueue<Future<Modification>> getModificationQueue() {
			return null;
		}

		@Override
		public float createSunburstItem(@Nonnull final Item pItem,
				@Nonnegative final int pDepth, @Nonnegative final int pIndex) {
			checkArgument(pDepth >= 0, "must be positive: %s", pDepth);
			checkArgument(pIndex >= 0, "must be >= 0: %s", pIndex);

			// Initialize variables.
			final float angle = pItem.mAngle;
			final float extension = pItem.mExtension;
			final int indexToParent = pItem.mIndexToParent;
			final int descendantCount = pItem.mDescendantCount;
			final int parDescendantCount = pItem.mParentDescendantCount;
			final int depth = pDepth;

			// Add a sunburst item.
			final EStructType structKind = mRtx.hasFirstChild() ? EStructType.ISINNERNODE
					: EStructType.ISLEAFNODE;

			// Calculate extension.
			float childExtension = 2 * PConstants.PI;
			if (indexToParent > -1) {
				childExtension = extension * (float) descendantCount
						/ ((float) parDescendantCount - 1f);
			}
			LOGWRAPPER.debug("ITEM: " + pIndex);
			LOGWRAPPER.debug("descendantCount: " + descendantCount);
			LOGWRAPPER.debug("parentDescCount: " + parDescendantCount);
			LOGWRAPPER.debug("indexToParent: " + indexToParent);
			LOGWRAPPER.debug("extension: " + childExtension);
			LOGWRAPPER.debug("depth: " + depth);
			LOGWRAPPER.debug("angle: " + angle);

			// Set node relations.
			String text = null;
			NodeRelations relations = null;
			if (mRtx.getKind() == EKind.TEXT) {
				relations = new NodeRelations(depth, depth, structKind, mRtx.getValue()
						.length(), mMinTextLength, mMaxTextLength, indexToParent);
				text = mRtx.getValue();
				// LOGWRAPPER.debug("text: " + text);
			} else {
				relations = new NodeRelations(depth, depth, structKind,
						descendantCount, 0, mMaxDescendantCount, indexToParent);
			}

			// Build item.
			final SunburstItem.Builder builder = new SunburstItem.Builder(mParent,
					angle, childExtension, relations, mDb, mGUI).setNodeKey(
					mRtx.getNodeKey()).setKind(mRtx.getKind());
			if (text != null) {
				builder.setText(text).build();
			} else {
				// LOGWRAPPER.debug("QName: " + mRtx.getQNameOfCurrentNode());
				builder.setQName(mRtx.getQName()).build();
				builder.setAttributes(fillAttributes(mRtx));
				builder.setNamespaces(fillNamespaces(mRtx));
			}
			final SunburstItem item = builder.build();
			mItems.add(item);

			firePropertyChange("items", null, mItems);
			firePropertyChange("item", null, item);

			// Set depth max.
			mDepthMax = Math.max(depth, mDepthMax);

			return childExtension;
		}

		@Override
		public boolean getIsPruned() {
			return mPruned;
		}

		/**
		 * Get minimum and maximum global text length.
		 */
		void getMinMaxTextLength() {
			mMinTextLength = Integer.MAX_VALUE;
			mMaxTextLength = Integer.MIN_VALUE;
			for (final IAxis axis = new DescendantAxis(mRtx, EIncludeSelf.YES); axis
					.hasNext();) {
				axis.next();
				if (axis.getTrx().getKind() == EKind.TEXT) {
					final int length = axis.getTrx().getValue().length();
					if (length < mMinTextLength) {
						mMinTextLength = length;
					}

					if (length > mMaxTextLength) {
						mMaxTextLength = length;
					}
				}
			}
			if (mMinTextLength == Integer.MAX_VALUE) {
				mMinTextLength = 0;
			}
			if (mMaxTextLength == Integer.MIN_VALUE) {
				mMaxTextLength = 0;
			}

			LOGWRAPPER.debug("MINIMUM text length: " + mMinTextLength);
			LOGWRAPPER.debug("MAXIMUM text length: " + mMaxTextLength);
		}

		@Override
		public void descendants(@Nonnull final Optional<INodeReadTrx> pRtx)
				throws InterruptedException, ExecutionException {
			checkNotNull(pRtx);

			try {
				final ExecutorService executor = Executors.newSingleThreadExecutor();
				executor.submit(new GetDescendants(pRtx.get()));
				mModel.shutdown(executor);
			} catch (final SirixException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}

		/**
		 * Callable to get descendant-or-self count of each node.
		 */
		private final class GetDescendants implements Callable<Void> {

			/** {@link INodeReadTrx} implementation. */
			private final INodeReadTrx mRtx;

			/**
			 * Get descendants.
			 * 
			 * @param pRtx
			 *          {@link INodeReadTrx} implementation
			 * @throws SirixException
			 *           if traversing a sirix resource fails
			 */
			GetDescendants(final INodeReadTrx pRtx) throws SirixException {
				mRtx = mDb.getSession().beginNodeReadTrx(mDb.getRevisionNumber());
				mRtx.moveTo(pRtx.getNodeKey());
			}

			/** {@inheritDoc} */
			@Override
			public Void call() throws SirixException, ExecutionException,
					InterruptedException {
				final ExecutorService executor = Executors.newFixedThreadPool(Runtime
						.getRuntime().availableProcessors());
				switch (mPruning) {
				case DEPTH:
					mDepth = 0;
					boolean first = true;

					if (mRtx.getKind() == EKind.DOCUMENT_ROOT) {
						mRtx.moveToFirstChild();
					}
					final long key = mRtx.getNodeKey();

					if (mRtx.hasFirstChild()) {
						boolean moved = false;
						while (first || mRtx.getNodeKey() != key) {
							if (mRtx.hasFirstChild()) {
								if (first) {
									first = false;
									final Future<Integer> descs = countDescendants(mRtx, executor);
									mMaxDescendantCount = descs.get();
									LOGWRAPPER.debug("DESCS: " + mMaxDescendantCount);
									firePropertyChange("maxDescendantCount", null,
											mMaxDescendantCount);
									firePropertyChange("descendants", null, descs);
									mRtx.moveToFirstChild();
									mDepth++;
								} else {
									if (mDepth >= DEPTH_TO_PRUNE) {
										while (!mRtx.hasRightSibling() && mRtx.getNodeKey() != key) {
											mRtx.moveToParent();
											mDepth--;
										}
										mRtx.moveToRightSibling();
									} else {
										if (!moved) {
											firePropertyChange("descendants", null,
													countDescendants(mRtx, executor));
										}
										mRtx.moveToFirstChild();
										mDepth++;
									}
									moved = false;
								}
							} else {
								boolean movedToNextFollowing = false;
								while (!mRtx.hasRightSibling() && mRtx.getNodeKey() != key) {
									if (!moved && !movedToNextFollowing
											&& mDepth < DEPTH_TO_PRUNE) {
										firePropertyChange("descendants", null,
												countDescendants(mRtx, executor));
									}
									mRtx.moveToParent();
									mDepth--;
									movedToNextFollowing = true;
								}
								if (mRtx.getNodeKey() != key) {
									if (movedToNextFollowing) {
										mRtx.moveToRightSibling();
										moved = true;
										if (mDepth < DEPTH_TO_PRUNE) {
											firePropertyChange("descendants", null,
													countDescendants(mRtx, executor));
										}
									} else {
										if (mDepth < DEPTH_TO_PRUNE && !moved) {
											firePropertyChange("descendants", null,
													countDescendants(mRtx, executor));
										}
										moved = false;
										mRtx.moveToRightSibling();
									}
								}
							}
						}

						mRtx.moveTo(mKey);
					} else {
						final Future<Integer> future = countDescendants(mRtx, executor);
						firePropertyChange("maxDescendantCount", null, future.get());
						firePropertyChange("descendants", null, future);
					}
					break;
				case NO:
					// Get descendants for every node and save it to a list.
					boolean firstNode = true;
					for (final IAxis axis = new DescendantAxis(mRtx, EIncludeSelf.YES); axis
							.hasNext(); axis.next()) {
						if (axis.getTrx().getKind() != EKind.DOCUMENT_ROOT) {
							// try {
							final Future<Integer> futureSubmitted = executor.submit(Callables
									.returning((int) mRtx.getDescendantCount() + 1));// */new
																																		// Descendants(mDb.getSession(),
																																		// mRtx
							// .getRevisionNumber(), mRtx.getItem().getKey()));
							if (firstNode) {
								firstNode = false;
								mMaxDescendantCount = futureSubmitted.get();
								firePropertyChange("maxDescendantCount", null,
										mMaxDescendantCount);
							}
							firePropertyChange("descendants", null, futureSubmitted);
							// } catch (TTIOException e) {
							// LOGWRAPPER.error(e.getMessage(), e);
							// }
						}
					}
					break;
				}
				firePropertyChange("descendants", null,
						executor.submit(Callables.returning(DESCENDANTS_DONE)));
				mModel.shutdown(executor);
				mRtx.close();
				return null;
			}
		}

		/**
		 * Count descendants.
		 * 
		 * @param pRtx
		 *          {@link INodeReadTrx} instance
		 */
		Future<Integer> countDescendants(final INodeReadTrx pRtx,
				final ExecutorService pExecutor) throws SirixException {
			assert pRtx != null;
			assert pExecutor != null;

			try {
				return pExecutor.submit(new PrunedDescendants(mDb.getSession(), pRtx
						.getRevisionNumber(), pRtx.getNodeKey(), mDepth));
			} catch (final SirixException e) {
				LOGWRAPPER.error(e.getMessage(), e);
				return null;
			}
		}

		/** Counts descendants but pruned after a specified level. */
		private final static class PrunedDescendants implements Callable<Integer> {

			/** sirix {@link INodeReadTrx}. */
			private final INodeReadTrx mRtx;

			/** Current depth in the tree. */
			private transient int mDepth;

			/**
			 * Constructor.
			 * 
			 * @param pRtx
			 *          {@link INodeReadTrx} over which to iterate
			 */
			PrunedDescendants(final ISession pSession, final int pRevision,
					final long pNodeKey, final int pDepth) throws SirixException {
				assert pSession != null;
				assert !pSession.isClosed();
				assert pRevision >= 0;
				assert pNodeKey >= 0;
				assert pDepth >= 0 && pDepth <= DEPTH_TO_PRUNE;
				mRtx = pSession.beginNodeReadTrx(pRevision);
				mRtx.moveTo(pNodeKey);
				mDepth = pDepth;
			}

			@Override
			public Integer call() throws Exception {
				assert mDepth < DEPTH_TO_PRUNE;
				if (mDepth + 1 >= DEPTH_TO_PRUNE) {
					mRtx.close();
					return 1;
				}
				int retVal = 1;
				final long key = mRtx.getNodeKey();
				boolean first = true;
				while (first || mRtx.getNodeKey() != key) {
					first = false;
					if (mRtx.hasFirstChild()) {
						mDepth++;
						if (mDepth < DEPTH_TO_PRUNE) {
							mRtx.moveToFirstChild();
							retVal++;
						} else {
							mDepth--;
							retVal += nextFollowingNode(mRtx, key);
						}
					} else {
						retVal += nextFollowingNode(mRtx, key);
					}
				}
				mRtx.close();
				return retVal;
			}

			/**
			 * Move transaction to the next following node.
			 * 
			 * @param pRtx
			 *          {@link INodeReadTrx} implementation
			 * @param pKey
			 *          root key
			 */
			private int nextFollowingNode(final INodeReadTrx pRtx, final long pKey) {
				checkNotNull(pRtx);
				int retVal = 0;
				while (!pRtx.hasRightSibling() && pRtx.getNodeKey() != pKey) {
					pRtx.moveToParent();
					mDepth--;
				}
				if (pRtx.getNodeKey() != pKey) {
					pRtx.moveToRightSibling();
					if (mDepth < DEPTH_TO_PRUNE) {
						retVal++;
					}
				}
				return retVal;
			}
		}
	}

	/**
	 * Shredder XML fragment input.
	 * 
	 * @param pFragment
	 *          XML fragment to shredder (might be text as well)
	 * @throws SirixException
	 *           if shredding in sirix fails
	 * @throws XMLStreamException
	 *           if parser can't parse the XML fragment
	 */
	@Override
	public void addXMLFragment(final String pFragment) throws SirixException,
			XMLStreamException {
		if (!pFragment.isEmpty()) {
			try {
				// Very simple heuristic to determine if it's character input or an XML
				// fragment.
				if (pFragment.startsWith("<")) {
					// Annotation in this context questionable since it can't be checked
					// at compile time!
					@Syntax("XML")
					final String xml = pFragment;
					final XMLEventReader reader = XMLInputFactory.newInstance()
							.createXMLEventReader(new ByteArrayInputStream(xml.getBytes()));
					final ExecutorService service = Executors.newSingleThreadExecutor();
					service.submit(new XMLShredder.Builder(mWtx, reader, mInsert).build());
					service.shutdown();
					service.awaitTermination(60, TimeUnit.SECONDS);
				} else {
					switch (mInsert) {
					case ASFIRSTCHILD:
						mWtx.insertTextAsFirstChild(pFragment);
						break;
					case ASRIGHTSIBLING:
						mWtx.insertTextAsRightSibling(pFragment);
					default:
						mWtx.insertTextAsLeftSibling(pFragment);
					}
				}
			} catch (final InterruptedException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			}
		}
	}

	/**
	 * Commit changes.
	 * 
	 * @throws SirixException
	 *           if commiting or closeing transaction fails
	 */
	@Override
	public void commit() throws SirixException {
		mWtx.commit();
		mWtx.close();
	}

	/**
	 * Create a popup menu for modifying nodes.
	 * 
	 * @param pEvent
	 *          the current {@link MouseEvent}
	 * @param pCtrl
	 *          {@link ControlGroup} to insert XML fragment
	 * @param pHitTestIndex
	 *          the index of the {@link SunburstItem} which is currently hovered
	 * @throws SirixException
	 */
	public void popupMenu(final MouseEvent pEvent, final ControlGroup<?> pCtrl,
			final int pHitTestIndex) throws SirixException {
		if (mWtx == null || mWtx.isClosed()) {
			mWtx = getDb().getSession().beginNodeWriteTrx();
			mWtx.revertTo(getDb().getRevisionNumber());
		}
		mWtx.moveTo(((SunburstItem) getItem(pHitTestIndex)).getKey());
		final SunburstPopupMenu menu = SunburstPopupMenu.getInstance(this, mWtx,
				pCtrl);
		menu.show(pEvent.getComponent(), pEvent.getX(), pEvent.getY());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void propertyChange(final PropertyChangeEvent pEvent) {
		switch (pEvent.getPropertyName().toLowerCase()) {
		case "maxdepth":
			mLastMaxDepth = (Integer) pEvent.getNewValue();
			firePropertyChange("maxDepth", null, mLastMaxDepth);
			break;
		case "done":
			firePropertyChange("done", null, true);
			break;
		case "items":
			mItems = (List<SunburstItem>) pEvent.getNewValue();
			firePropertyChange("items", null, mItems);
			break;
		case "item":
			firePropertyChange("item", null, pEvent.getNewValue());
			break;
		case "progress":
			firePropertyChange("progress", null, pEvent.getNewValue());
			break;
		}
	}
}
