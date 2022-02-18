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

import java.awt.event.MouseEvent;
import java.util.List;

import org.checkerframework.checker.index.qual.NonNegative;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.sirix.exception.SirixException;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.ViewNotifier;
import org.sirix.gui.view.ViewUtilities;
import org.sirix.gui.view.VisualItemAxis;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.sunburst.AbstractSunburstGUI.EResetZoomer;
import org.sirix.gui.view.sunburst.SunburstView.Embedded;
import org.sirix.gui.view.sunburst.control.AbstractSunburstControl;
import org.sirix.gui.view.sunburst.model.SunburstCompareModel;
import org.sirix.gui.view.sunburst.model.SunburstModel;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import processing.core.PApplet;
import processing.core.PConstants;

import com.google.common.base.Optional;

import controlP5.ControlEvent;
import controlP5.ControllerGroup;
import controlP5.Toggle;

/**
 * Controller for the {@link SunburstView}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstControl extends AbstractSunburstControl {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(SunburstControl.class));

	/** {@link SunburstControl} instance. */
	private static SunburstControl mControl;

	/** {@link SunburstGUI} instance. */
	private final SunburstGUI mSunburstGUI;

	private volatile int mHitTestIndex;

	private volatile boolean mRefresh;

	/**
	 * Constructor.
	 * 
	 * @param pParent
	 *          parent processing {@link PApplet}
	 * @param pModel
	 *          an {@link Model} implementation
	 * @param pDb
	 *          {@link ReadDB} instance
	 */
	private SunburstControl(final Embedded pParent,
			final Model<SunburstContainer, SunburstItem> pModel, final ReadDB pDb) {
		super(pParent, pModel, pDb);
		assert pParent != null;
		mSunburstGUI = (SunburstGUI) mGUI;
		final SunburstContainer container = new SunburstContainer(mSunburstGUI,
				mModel).setNewStartKey(mDb.getNodeKey());
		if (mSunburstGUI.mUsePruning) {
			if (mSunburstGUI.mUseDiffView == ViewType.DIFF
					&& mSunburstGUI.mUseDiffView.getValue()) {
				container.setPruning(Pruning.DIFF);
			} else {
				container.setPruning(Pruning.DEPTH);
			}
		} else {
			container.setPruning(Pruning.NO);
		}
		mModel.traverseTree(container);
	}

	/**
	 * Get instance.
	 * 
	 * @param pParent
	 *          parent processing {@link PApplet}
	 * @param pModel
	 *          an {@link Model} implementation
	 * @param pDb
	 *          {@link ReadDB} instance
	 * @return {@link SunburstControl} instance
	 */
	public static synchronized SunburstControl getInstance(
			final Embedded pParent,
			final Model<SunburstContainer, SunburstItem> pModel, final ReadDB pDb) {
		if (mControl == null) {
			mControl = new SunburstControl(checkNotNull(pParent),
					checkNotNull(pModel), checkNotNull(pDb));
		}
		return mControl;
	}

	@Override
	protected AbstractSunburstGUI getGUIInstance() {
		return SunburstGUI.getInstance((Embedded) mParent, this, mDb);
	}

	@Override
	public void controlEvent(final ControlEvent pControlEvent) {
		super.controlEvent(pControlEvent);
		if (pControlEvent.isGroup()) {
			if (pControlEvent.getGroup().getName().equals("Compare revision")) {
				mSunburstGUI.mParent.noLoop();
				mSunburstGUI.mSelectedRev = (int) pControlEvent.getGroup().getValue();
				final int selectedRev = mSunburstGUI.mSelectedRev;
				mModel = new SunburstCompareModel(mSunburstGUI.mParent,
						mSunburstGUI.mDb);
				mModel.addPropertyChangeListener(mSunburstGUI);
				final SunburstContainer container = new SunburstContainer(mSunburstGUI,
						mModel);
				if (mSunburstGUI.mUsePruning) {
					container.setPruning(Pruning.DIFF);
				} else {
					container.setPruning(Pruning.NO);
				}
				container.setMoveDetection(mSunburstGUI.mUseMoveDetection);
				mModel.traverseTree(container.setRevision(selectedRev).setModWeight(
						mSunburstGUI.getModificationWeight()));
				mSunburstGUI.mUseDiffView = ViewType.DIFF;
				mSunburstGUI.mUseDiffView.setValue(true);
				final SunburstView view = (SunburstView) ((Embedded) mSunburstGUI.mParent)
						.getView();
				final ViewNotifier notifier = view.getNotifier();
				mDb.setCompareRevisionNumber(selectedRev);
				notifier.init(view);
			}
		} else if (pControlEvent.isController()) {
			if (pControlEvent.getController() instanceof Toggle) {
				final Toggle toggle = (Toggle) pControlEvent.getController();
				final boolean state = toggle.getState();
				switch (toggle.getId()) {
				case 3:
					mSunburstGUI.setUseArc(state);
					break;
				case 4:
					mSunburstGUI.setFisheye(state);
					break;
				case 5:
					if (state) {
						mRefresh = true;
					}
					mSunburstGUI.setUsePruning(state);
					break;
				case 6:
					mSunburstGUI.setUseAttribute(state);
					break;
				case 7:
					mSunburstGUI.setUseMoveDetection(state);
					break;
				}
			}
		}
	}

	/**
	 * Is getting called from processings keyRealeased-method and implements it.
	 * 
	 * @see processing.core.PApplet#keyReleased()
	 */
	@Override
	public void keyReleased() {
		if (!mSunburstGUI.mXPathField.isActive() && !mSunburstGUI.mCtrl.isOpen()) {
			switch (mSunburstGUI.mParent.key) {
			case 'r':
			case 'R':
				mSunburstGUI.mParent.frameRate(35);
				mSunburstGUI.mRad = 0;
				mSunburstGUI.resetZoom();
				mSunburstGUI.mZoomPanReset = true;
				break;
			case 's':
			case 'S':
				// Save PNG.
				mSunburstGUI.mParent.saveFrame(ViewUtilities.SAVEPATH
						+ ViewUtilities.timestamp() + "_##.png");
				break;
			case 'p':
			case 'P':
				// Save PDF.
				mSunburstGUI.mParent.frameRate(60);
				mSunburstGUI.setSavePDF(true);
				PApplet.println("\n" + "saving to pdf – starting");
				mSunburstGUI.mParent.beginRecord(PConstants.PDF, ViewUtilities.SAVEPATH
						+ ViewUtilities.timestamp() + ".pdf");
				mSunburstGUI.mParent.textMode(PConstants.SHAPE);
				break;
			case 'q':
				// mSunburstGUI.setViewKind(EView.NODIFF);
				((Embedded) mSunburstGUI.mParent).refreshUpdate();
				break;
			case '\b':
				// Backspace.
				mModel.undo();
				mSunburstGUI.undo();
				final SunburstView view = (SunburstView) ((Embedded) mSunburstGUI.mParent)
						.getView();
				final ViewNotifier notifier = view.getNotifier();
				notifier.getGUI().getReadDB().setKey(mModel.getItem(0).getKey());
				notifyChanges(mModel.subList(0, mModel.getItemsSize()));
				break;
			case '1':
				mSunburstGUI.setMappingMode(1);
				break;
			case '2':
				mSunburstGUI.setMappingMode(2);
				break;
			case '3':
				mSunburstGUI.setMappingMode(3);
				break;
			case 'o':
			case 'O':
				if (mSunburstGUI.mUseDiffView == ViewType.NODIFF) {
					mSunburstGUI.mUseDiffView.setValue(true);
					mSunburstGUI.mRevisions = mSunburstGUI.getControlP5()
							.addDropdownList("Compare revision",
									mSunburstGUI.mParent.width - 250, 100, 100, 120);
					assert mSunburstGUI.mDb != null;
					try {
						for (long i = mSunburstGUI.mDb.getRevisionNumber() + 1, newestRev = mSunburstGUI.mDb
								.getSession().beginNodeReadTrx().getRevisionNumber(); i <= newestRev; i++) {
							mSunburstGUI.mRevisions.addItem("Revision " + i, (int) i);
						}
					} catch (final SirixException e) {
						LOGWRAPPER.error(e.getMessage(), e);
						JOptionPane.showMessageDialog(mSunburstGUI.mParent, this,
								"Failed to open read transaction: " + e.getMessage(),
								JOptionPane.ERROR_MESSAGE);
					}
				}
				break;
			default:
				// Do nothing.
			}

			switch (mSunburstGUI.mParent.key) {
			case '1':
			case '2':
			case '3':
				mSunburstGUI.update(EResetZoomer.YES);
				break;
			case 'm':
			case 'M':
				mSunburstGUI.setShowGUI(mSunburstGUI.getControlP5().getGroup("menu")
						.isOpen());
				mSunburstGUI.setShowGUI(!mSunburstGUI.isShowGUI());
				break;
			default:
				// No action.
			}

			final ControllerGroup<?> menu = mSunburstGUI.getControlP5().getGroup(
					"menu");
			if (mSunburstGUI.isShowGUI()) {
				menu.open();
				mSunburstGUI.getApplet().frameRate(5);
			} else {
				menu.close();
				mSunburstGUI.getApplet().frameRate(40);
			}

			if (mSunburstGUI.mParent.keyCode == PConstants.RIGHT) {
				mSunburstGUI.mRad += 5;
				mSunburstGUI.mRadChanged = true;
			} else if (mSunburstGUI.mParent.keyCode == PConstants.LEFT) {
				mSunburstGUI.mRad -= 5;
				mSunburstGUI.mRadChanged = true;
			}
		}
	}

	/**
	 * Implements processing mousePressed.
	 * 
	 * @param pEvent
	 *          The {@link MouseEvent}.
	 * 
	 * @see processing.core.PApplet#mousePressed
	 */
	@Override
	public void mousePressed(final MouseEvent pEvent) {
		mSunburstGUI.getControlP5().controlWindow.mouseEvent(pEvent);
		mSunburstGUI.zoomMouseEvent(pEvent);

		mSunburstGUI.setShowGUI(mSunburstGUI.getControlP5().getGroup("menu")
				.isOpen());

		if (!mSunburstGUI.isShowGUI() && mSunburstGUI.mDone) {
			boolean doMouseOver = true;
			if (mSunburstGUI.mRevisions != null && mSunburstGUI.mRevisions.isOpen()) {
				doMouseOver = false;
			}

			if (doMouseOver) {
				// Mouse rollover.
				if (!mSunburstGUI.mParent.keyPressed) {
					if (mSunburstGUI.mHitTestIndex != -1) {
						synchronized (mSunburstGUI) {
							mHitTestIndex = mSunburstGUI.mHitTestIndex;
						}
						mSunburstGUI.pushImg();

						// Bug in processing's mousbotton, thus used SwingUtilities.
						if (SwingUtilities.isLeftMouseButton(pEvent)
								&& !mSunburstGUI.mCtrl.isOpen()) {
							final SunburstContainer container = new SunburstContainer(
									mSunburstGUI, mModel);
							if (mSunburstGUI.mUsePruning) {
								if (mSunburstGUI.mUseDiffView == ViewType.DIFF
										&& mSunburstGUI.mUseDiffView.getValue()) {
									container.setPruning(Pruning.DIFF);
								} else {
									container.setPruning(Pruning.DEPTH);
								}
							} else {
								container.setPruning(Pruning.NO);
							}

							container.setMoveDetection(mSunburstGUI.mUseMoveDetection);

							final SunburstView view = (SunburstView) ((Embedded) mSunburstGUI.mParent)
									.getView();
							final ViewNotifier notifier = view.getNotifier();
							final SunburstItem item = mModel.getItem(mHitTestIndex);
							notifier.getGUI().getReadDB().setKey(item.getKey());

							if (mSunburstGUI.mUseDiffView == ViewType.DIFF) {
								LOGWRAPPER.debug("old rev: " + container.getOldRevision());
								mModel.update(container
										.setAll(mSunburstGUI.mSelectedRev, item.getDepth(),
												mSunburstGUI.getModificationWeight())
										.setNewStartKey(item.getKey()).setDiff(item.getDiff()));
							} else {
								mModel.update(container.setNewStartKey(item.getKey()));
							}
							refreshed(container, mHitTestIndex);
						} else if (SwingUtilities.isRightMouseButton(pEvent)) {
							if (mSunburstGUI.mUseDiffView == ViewType.NODIFF) {
								try {
									((SunburstModel) mModel).popupMenu(pEvent,
											mSunburstGUI.mCtrl, mHitTestIndex);
								} catch (final SirixException e) {
									LOGWRAPPER.error(e.getMessage(), e);
									JOptionPane.showMessageDialog(mSunburstGUI.mParent, this,
											"Failed to commit change: " + e.getMessage(),
											JOptionPane.ERROR_MESSAGE);
								}
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Determines if a refresh must be done, that is if items can be upscaled or
	 * not.
	 * 
	 * @param pContainer
	 *          {@link SunburstContainer} reference with settings
	 * @param pIndex
	 *          index of the new root item
	 */
	private void refreshed(final SunburstContainer pContainer,
			@NonNegative final int pIndex) {
		assert pContainer != null;
		assert pIndex >= 0;
		if (mRefresh || pContainer.getPruning() == Pruning.ITEMSIZE
				|| pContainer.getPruning() == Pruning.DEPTH) {
			mRefresh = false;
			mModel.traverseTree(pContainer);
		} else {
			mSunburstGUI.transformRoot(pIndex);
		}
	}

	@Override
	public void setItems(final List<SunburstItem> pItems) {
		super.setItems(pItems);
		notifyChanges(pItems);
	}

	@Override
	public void setOldMaxDepth(final int pOldDepthMax) {
		checkArgument(pOldDepthMax >= 0, "pOldMaxDepth must be >= 0!");
		mModel.setOldDepthMax(pOldDepthMax);
	}

	@Override
	public void setNewMaxDepth(final int pNewDepthMax) {
		checkArgument(pNewDepthMax >= 0, "pOldMaxDepth must be >= 0!");
		mModel.setNewDepthMax(pNewDepthMax);
	}

	/**
	 * Notify other views of changes.
	 * 
	 * @param pItems
	 *          the new item list
	 */
	private void notifyChanges(final List<SunburstItem> pItems) {
		assert pItems != null;
		final SunburstView view = (SunburstView) ((Embedded) mSunburstGUI.mParent)
				.getView();
		final ViewNotifier notifier = view.getNotifier();
		if (mModel instanceof SunburstModel) {
			notifier.update(view, Optional.<VisualItemAxis> absent());
		} else if (mModel instanceof SunburstCompareModel) {
			notifier.update(view, Optional.of(new VisualItemAxis(pItems)));
		}
	}

	/**
	 * XPath expression.
	 * 
	 * @param pXPath
	 *          the XPath expression
	 */
	public void xpath(final String pXPath) {
		mModel.evaluateXPath(pXPath);
	}

	/**
	 * Method to process event for cancel-button.
	 * 
	 * @param pValue
	 *          change value
	 */
	@Override
	public void cancel(final int pValue) {
		mSunburstGUI.mTextArea.clear();
		mSunburstGUI.mCtrl.setVisible(false);
	}

	/**
	 * Method to process event for submit-button.
	 * 
	 * @param pValue
	 *          change value
	 * @throws XMLStreamException
	 *           if the XML fragment isn't well formed
	 */
	@Override
	public void submit(final int pValue) throws XMLStreamException {
		try {
			assert mModel instanceof SunburstModel;
			mSunburstGUI.mCtrl.setVisible(false);
			mSunburstGUI.mCtrl.setOpen(false);
			((SunburstModel) mModel).addXMLFragment(mSunburstGUI.mTextArea.getText());
			mSunburstGUI.mTextArea.clear();
		} catch (final FactoryConfigurationError | SirixException e) {
			JOptionPane.showMessageDialog(mSunburstGUI.mParent,
					"Failed to commit change: " + e.getMessage());
		}
	}

	/**
	 * Method to process event for commit-button.
	 * 
	 * @param pValue
	 *          change value
	 * @throws XMLStreamException
	 *           if the XML fragment isn't well formed
	 */
	@Override
	public void commit(@NonNegative final int pValue) throws XMLStreamException {
		try {
			assert mModel instanceof SunburstModel;
			mSunburstGUI.mCtrl.setVisible(false);
			mSunburstGUI.mCtrl.setOpen(false);
			((SunburstModel) mModel).addXMLFragment(mSunburstGUI.mTextArea.getText());
			((SunburstModel) mModel).commit();
			mSunburstGUI.mTextArea.clear();
		} catch (final FactoryConfigurationError | SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
			JOptionPane.showMessageDialog(mSunburstGUI.mParent,
					"Failed to commit change: " + e.getMessage());
		}
		((Embedded) mSunburstGUI.mParent).refresh(Optional
				.<VisualItemAxis> absent());
	}

	/**
	 * Refresh storage after an update.
	 * 
	 * @param pDB
	 *          {@link ReadDB} instance
	 */
	public void refreshUpdate(final ReadDB pDB) {
		mDb = checkNotNull(pDB);
		mSunburstGUI.mDone = false;
		mSunburstGUI.mUseDiffView = ViewType.NODIFF;
		mModel = new SunburstModel(mSunburstGUI.mParent, mSunburstGUI.mDb);
		final SunburstContainer container = new SunburstContainer(mSunburstGUI,
				mModel).setNewStartKey(mDb.getNodeKey());
		if (mSunburstGUI.mUsePruning) {
			container.setPruning(Pruning.DEPTH);
		} else {
			container.setPruning(Pruning.NO);
		}
		container.setOldRevision(mDb.getRevisionNumber());
		if (mSunburstGUI.mSelectedRev > 0) {
			container.setRevision(mSunburstGUI.mSelectedRev);
		}
		if (mHitTestIndex > 0) {
			final SunburstItem item = mModel.getItem(mHitTestIndex);
			container.setDepth(item.getDepth())
					.setModWeight(mSunburstGUI.getModificationWeight())
					.setNewStartKey(item.getKey());
		} else {
			container.setModWeight(mSunburstGUI.getModificationWeight());
		}
		mModel.updateDb(mDb, container);
		mSunburstGUI.updateDb(mDb);
	}

	@Override
	public void resetControl() {
		// Reset on purpose (ignore FindBugs).
		mControl = null;
	}
}
