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

import static com.google.common.base.Preconditions.checkNotNull;

import java.beans.PropertyChangeEvent;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.xml.stream.XMLStreamException;

import org.sirix.gui.ReadDB;
import org.sirix.gui.view.ViewUtilities;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.sunburst.SunburstView.Embedded;
import org.sirix.gui.view.sunburst.control.SunburstControl;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import processing.core.PApplet;
import processing.core.PConstants;
import processing.core.PImage;
import processing.core.PVector;
import controlP5.Button;
import controlP5.ControlGroup;
import controlP5.DropdownList;
import controlP5.Textfield;
import controlP5.Toggle;

/**
 *
 * <p>
 * Internal Sunburst view GUI.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SunburstGUI extends AbstractSunburstGUI {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(SunburstGUI.class));

	/** Single threaded {@link ExecutorService}. */
	private static final ExecutorService EXECUTOR_SERVICE = Executors
			.newSingleThreadExecutor();

	/** Amount of effect in the fisheye transformation. */
	private static final float EFFECT_AMOUNT = 0.9f;

	/** The GUI of the Sunburst view. */
	private static volatile SunburstGUI mGUI;

	/** Determines if zooming or panning is resetted. */
	transient boolean mZoomPanReset;

	/** Determines if fisheye should be used. */
	transient boolean mFisheye;

	/**
	 * {@link DropdownList} of available revisions, which are newer than the
	 * currently opened revision.
	 */
	transient DropdownList mRevisions;

	/** {@link ControlGroup} to encapsulate the components to insert XML fragments. */
	transient ControlGroup<?> mCtrl;

	/** {@link Textfield} to insert an XML fragment. */
	transient Textfield mTextArea;

	/** Determines if it is currently zooming or panning or has been in the past. */
	private transient boolean mIsZoomingPanning;

	/** Determines if GUI has been initialized. */
	transient boolean mInitialized;

	/** Determines if visualization has been rotated. */
	transient boolean mRadChanged;

	/**
	 * Private constructor.
	 * 
	 * @param pApplet
	 *          parent processing applet
	 * @param pReadDB
	 *          read database
	 */
	private SunburstGUI(final PApplet pApplet, final SunburstControl pControl,
			final ReadDB pReadDB) {
		super(pApplet, pControl, pReadDB);
		mDb = pReadDB;
	}

	/**
	 * Factory method (Singleton). Note that it's always called from the animation
	 * thread, thus it doesn't need to be synchronized.
	 * 
	 * @param pParentApplet
	 *          parent processing applet
	 * @param pControl
	 *          associated controller
	 * @param pReadDB
	 *          read database
	 * @return a {@link SunburstGUI} singleton
	 */
	public static SunburstGUI getInstance(final PApplet pParentApplet,
			final SunburstControl pControl, final ReadDB pReadDB) {
		if (mGUI == null) {
			synchronized (SunburstGUI.class) {
				if (mGUI == null) {
					assert pParentApplet instanceof Embedded;
					mGUI = new SunburstGUI(pParentApplet, pControl, pReadDB);
				}
			}
		}
		return mGUI;
	}

	@Override
	public void resetGUI() {
		mGUI = null;
	}

	@Override
	protected void setup() {
		mOldSelectedRev = mDb.getRevisionNumber();
		final Toggle toggleArc = getControlP5().addToggle("mUseArc", mUseArc,
				LEFT + 0, TOP + mPosY + 60, 15, 15);
		toggleArc.setCaptionLabel("Arc / Rect");
		mToggles.add(toggleArc);
		final Toggle toggleFisheye = getControlP5().addToggle("mFisheye", mFisheye,
				LEFT + 0, TOP + mPosY + 80, 15, 15);
		toggleFisheye.setCaptionLabel("Fisheye lense");
		mToggles.add(toggleFisheye);
		final Toggle togglePruning = getControlP5().addToggle("mUsePruning",
				mUsePruning, LEFT + 0, TOP + mPosY + 100, 15, 15);
		togglePruning.setValue(true);
		togglePruning.setCaptionLabel("Pruning");
		mToggles.add(togglePruning);
		final Toggle useAttribute = getControlP5().addToggle("mUseAttribute",
				mUseAttribute, LEFT + 0, TOP + mPosY + 120, 15, 15);
		useAttribute.setCaptionLabel("Use attribute value");
		mToggles.add(useAttribute);
		final Toggle useMoveDetection = getControlP5().addToggle(
				"mUseMoveDetection", mUseMoveDetection, LEFT + 0, TOP + mPosY + 140,
				15, 15);
		useMoveDetection.setCaptionLabel("Use move detection");
		mToggles.add(useMoveDetection);

		mXPathField = getControlP5().addTextfield("xpath", mParent.width - 250,
				TOP + 20, 200, 20);
		mXPathField.setCaptionLabel("XPath expression");
		mXPathField.setFocus(false);
		mXPathField.setAutoClear(false);
		mXPathField.setColorBackground(mParent.color(0)); // black
		mXPathField.setColorForeground(mParent.color(255)); // white
		mXPathField.setId(50);
		mXPathField.plugTo(this);

		// Add textfield for XML fragment input.
		mCtrl = getControlP5().addGroup("add XML fragment", 150, 25, 115);
		mCtrl.setVisible(false);
		mCtrl.close();

		mTextArea = getControlP5()
				.addTextfield("Add XML fragment", 0, 20, 400, 100);
		mTextArea.setColorBackground(mParent.color(0)); // black
		mTextArea.setColorForeground(mParent.color(255)); // white
		mTextArea.setGroup(mCtrl);

		final Button submit = getControlP5()
				.addButton("submit", 20, 0, 140, 80, 19);
		submit.plugTo(this);
		submit.setGroup(mCtrl);
		final Button commit = getControlP5().addButton("commit", 20, 120, 140, 80,
				19);
		commit.plugTo(this);
		commit.setGroup(mCtrl);
		final Button cancel = getControlP5().addButton("cancel", 20, 240, 140, 80,
				19);
		cancel.plugTo(this);
		cancel.setGroup(mCtrl);
	}

	/**
	 * Implements the {@link PApplet} draw() method.
	 */
	@Override
	public void draw() {
		if (getControlP5() != null) {
			try {
				mParent.pushMatrix();
				mIsHovered = false;

				if (isZoomingPanning()) {
					mIsZoomingPanning = true;
				}

				// This enables zooming/panning.
				transform();

				mParent.textSize(15f);
				mParent.colorMode(PConstants.HSB, 360, 100, 100, 100);
				mParent.noFill();
				mParent.ellipseMode(PConstants.RADIUS);
				mParent.strokeCap(PConstants.SQUARE);
				mParent.textLeading(14);
				mParent.textAlign(PConstants.LEFT, PConstants.TOP);
				mParent.smooth();

				if (mIsZoomingPanning || isSavePDF() || mFisheye || mInit) {
					LOGWRAPPER.debug("Without buffered image!");
					mParent.background(0, 0, getBackgroundBrightness());
					mParent.translate((float) mParent.width / 2f,
							(float) mParent.height / 2f);
					mParent.rotate(PApplet.radians(mRad));
					if (mDone) {
						drawItems(Draw.DRAW);
					}
					mParent.stroke(0);
					mParent.strokeWeight(2f);
					mParent.line(0, 0, mParent.width * 0.5f, 0);
				} else if (mDone) {
					LOGWRAPPER.debug("Buffered image!");

					if (mRadChanged) {
						mRadChanged = false;
						update(EResetZoomer.YES);
					}

					mLock.acquireUninterruptibly();
					mParent.image(mImg, 0, 0);
					mLock.release();
					LOGWRAPPER.debug("[draw()]: Available permits: "
							+ mLock.availablePermits());
				}

				mParent.textSize(15f);
				mParent.popMatrix();
				mParent.pushMatrix();
				mParent.strokeWeight(0);
				if (mUseDiffView == ViewType.DIFF && ViewType.DIFF.getValue()) {
					ViewUtilities.compareLegend(this);
				} else {
					ViewUtilities.color(this);
					if (mUseDiffView == ViewType.NODIFF && !mUseDiffView.getValue()) {
						mParent.text("Press 'o' to get a list of revisions to compare!",
								mParent.width - 300f, mParent.height - 50f);
					}
				}

				@SuppressWarnings("unchecked")
				final Model<SunburstContainer, SunburstItem> model = (Model<SunburstContainer, SunburstItem>) mControl
						.getModel();
				ViewUtilities.legend(this, model);

				mParent.popMatrix();
				mParent.pushMatrix();
				// This enables zooming/panning.
				transform();
				mParent.translate((float) mParent.width / 2f,
						(float) mParent.height / 2f);
				mParent.textSize(13f);
				mParent.strokeWeight(1f);

				// Mouse rollover.
				if (!isShowGUI() && !mCtrl.isVisible() && !mFisheye && mDone) {
					boolean doMouseOver = true;
					if (mRevisions != null && mRevisions.isOpen()) {
						doMouseOver = false;
					}

					if (doMouseOver) {
						// Mouse rollover, arc hittest vars.
						final boolean itemHit = rollover();
						if (itemHit) {
							mIsHovered = true;
							((Embedded) mParent).getView().hover(
									mControl.getModel().getItem(mHitTestIndex));
							mParent.pushMatrix();
							mParent.rotate(PApplet.radians(mRad));
							Draw.DRAW.drawHover(this, mHitItem);
							mParent.popMatrix();
						}

						// Depth level focus.
						if (mDepth <= mDepthMax) {
							final float firstRad = calcEqualAreaRadius(mDepth, mDepthMax);
							final float secondRad = calcEqualAreaRadius(mDepth + 1, mDepthMax);
							mParent.noFill();
							mParent.stroke(0, 0, 0, 30);
							mParent.strokeWeight(5.5f);
							mParent.ellipse(0, 0, firstRad, firstRad);
							mParent.ellipse(0, 0, secondRad, secondRad);
						}

						mParent.popMatrix();
						mParent.pushMatrix();
						mParent.translate((float) mParent.width / 2f,
								(float) mParent.height / 2f);
						mX = mParent.mouseX - mParent.width / 2f;
						mY = mParent.mouseY - mParent.height / 2f;
						textMouseOver();
					}
				}

				// Fisheye view.
				if (mDone && mFisheye && !isSavePDF()) { // In PDF mode cannot make
																									// pixel based
																									// transformations.
					// Fisheye transormation.
					fisheye(mParent.mouseX, mParent.mouseY, 120);
				}

				if (mZoomPanReset) {
					update(EResetZoomer.YES);
					mZoomPanReset = false;
					mIsZoomingPanning = false;
				}

			} catch (final RuntimeException e) {
				LOGWRAPPER.error(e.getMessage(), e);
			} finally {
				mParent.popMatrix();
			}

			ViewUtilities.drawGUI(getControlP5());
		}
	}

	@Override
	protected boolean rolloverInit() {
		final PVector mousePosition = getZoomMouseCoord();
		mX = mousePosition.x - mParent.width / 2f;
		mY = mousePosition.y - mParent.height / 2f;
		return true;
	}

	/**
	 * Fisheye transformation.
	 * 
	 * @param pXPos
	 *          X position of middle point of the transformation
	 * @param pYPos
	 *          Y position of middle point of the transformation
	 * @param pRadius
	 *          the radius to use
	 */
	private void fisheye(final int pXPos, final int pYPos, final int pRadius) {
		// Start point of rectangle to grab.
		final int tlx = pXPos - pRadius;
		final int tly = pYPos - pRadius;
		// Rectangle with pixels.
		final PImage pi = mParent.get(tlx, tly, pRadius * 2, pRadius * 2);
		for (int x = -pRadius; x < pRadius; x++) {
			for (int y = -pRadius; y < pRadius; y++) {
				// Rescale cartesian coords between -1 and 1.
				final float cx = (float) x / pRadius;
				final float cy = (float) y / pRadius;

				// Outside of the sphere -> skip.
				final float square = PApplet.sq(cx) + PApplet.sq(cy);
				if (square >= 1) {
					continue;
				}

				// Compute cz from cx & cy.
				final float cz = PApplet.sqrt(1 - square);

				// Cartesian coords cx, cy, cz -> spherical coords sx, sy, still in -1,
				// 1 range.
				final float sx = PApplet.atan(EFFECT_AMOUNT * cx / cz) * 2
						/ PConstants.PI;
				final float sy = PApplet.atan(EFFECT_AMOUNT * cy / cz) * 2
						/ PConstants.PI;

				// Spherical coords sx & sy -> texture coords.
				final int tx = tlx + (int) ((sx + 1) * pRadius);
				final int ty = tly + (int) ((sy + 1) * pRadius);

				// Set pixel value.
				pi.set(pRadius + x, pRadius + y, mParent.get(tx, ty));
			}
		}
		mParent.set(tlx, tly, pi);
	}

	/**
	 * Method to process event for cancel-button.
	 * 
	 * @param paramValue
	 *          change value
	 */
	public void cancel(final int paramValue) {
		mControl.cancel(paramValue);
	}

	/**
	 * Method to process event for submit-button.
	 * 
	 * @param pValue
	 *          change value
	 * @throws XMLStreamException
	 *           if the XML fragment isn't well formed
	 */
	public void submit(final int pValue) throws XMLStreamException {
		mControl.submit(pValue);
	}

	/**
	 * Method to process event for submit-button.
	 * 
	 * @param pValue
	 *          change value
	 * @throws XMLStreamException
	 *           if the XML fragment isn't well formed
	 */
	public void commit(final int pValue) throws XMLStreamException {
		mControl.commit(pValue);
	}

	@Override
	public void propertyChange(final PropertyChangeEvent pEvent) {
		super.propertyChange(pEvent);
		switch (pEvent.getPropertyName().toLowerCase()) {
		case "progress":
			assert pEvent.getNewValue() instanceof Integer;
			final int progress = (Integer) pEvent.getNewValue();
			assert progress >= 0 && progress <= 100;
			ViewUtilities.processGlassPaneEvents(mListener, (Embedded) mParent,
					progress);
			break;
		case "done":
			try {
				mLock.acquireUninterruptibly();
				resetZoom();
				if (mUseDiffView == ViewType.DIFF && ViewType.DIFF.getValue()
						&& mControl.getModel().getItemsSize() < ANIMATION_THRESHOLD) {
					mInit = true;
					EXECUTOR_SERVICE.submit(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							update(EResetZoomer.YES);
							return null;
						}
					});
				} else {
					mInit = false;
					update(EResetZoomer.YES);
				}
			} finally {
				mLock.release();
			}
			mDone = true;
			break;
		default:
			break;
		}
	}

	/**
	 * Set fisheye lense.
	 * 
	 * @param pState
	 *          true if fisheye lense should be used, false otherwise
	 */
	public void setFisheye(final boolean pState) {
		mFisheye = pState;
	}

	/**
	 * Set use arcs.
	 * 
	 * @param pState
	 *          true if arcs should be used, false otherwise
	 */
	public void setUseArc(final boolean pState) {
		mUseArc = pState;
	}

	@Override
	public void relocate() {
		if (mXPathField != null) {
			mXPathField.setPosition(mParent.width - 250, TOP + 20);
		}
		if (mRevisions != null) {
			mRevisions.setPosition(mParent.width - 250, 100);
		}
	}

	/**
	 * Set kind of view.
	 * 
	 * @param pViewKind
	 *          the kind of view
	 */
	public void setViewKind(final ViewType pViewKind) {
		mUseDiffView = checkNotNull(pViewKind);
	}
}
