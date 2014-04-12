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
package org.sirix.gui.view.smallmultiple;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.gui.ReadDB;
import org.sirix.gui.view.ViewUtilities;
import org.sirix.gui.view.smallmultiple.SmallmultipleView.Embedded;
import org.sirix.gui.view.sunburst.AbstractSunburstGUI;
import org.sirix.gui.view.sunburst.Draw;
import org.sirix.gui.view.sunburst.EGreyState;
import org.sirix.gui.view.sunburst.SunburstGUI;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.SunburstItemKeyEquivalence;
import org.sirix.gui.view.sunburst.ViewType;
import org.sirix.gui.view.sunburst.control.SunburstControl;

import processing.core.PApplet;
import processing.core.PConstants;
import processing.core.PGraphics;
import processing.core.PImage;

/**
 * GUI of the {@link SmallmultipleView}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SmallmultipleGUI extends AbstractSunburstGUI implements
		PropertyChangeListener {
	/** Instance of this class. */
	private static volatile SmallmultipleGUI mGUI;

	/** Equivalence relation. */
	private static final SunburstItemKeyEquivalence mKeyEquivalence = new SunburstItemKeyEquivalence();

	/** {@link SmallmultipleControl} reference. */
	private final SunburstControl mControl;

	/** {@link List} of {@link PGraphics} to buffer {@link SunburstItem}s. */
	private final List<ImageStore> mBufferedImages;

	/** {@link ImageStore} reference. */
	private transient ImageStore mImage;

	/** Index into the datastructure hold in the model. */
	private transient int mIndex;

	/** All available revisions which are plotted. */
	private transient long mRevisions;

	/**
	 * Private constructor.
	 * 
	 * @param pApplet
	 *          parent processing applet
	 * @param pReadDB
	 *          {@link ReadDB} instance
	 */
	private SmallmultipleGUI(final PApplet pEmbedded,
			final SunburstControl pControl, final ReadDB pReadDB) {
		super(pEmbedded, pControl, pReadDB);
		mDb = pReadDB;
		mControl = pControl;
		mUseDiffView = ViewType.DIFF;
		mUseDiffView.setValue(true);
		mDone = false;
		mBufferedImages = new ArrayList<>();
		setShowLines(true);
		setUseAttribute(true);
		setModificationWeight(1f);
	}

	/**
	 * Factory method (Singleton).
	 * 
	 * @param pApplet
	 *          parent processing applet
	 * @param pControl
	 *          {@link SunburstControl} implementation
	 * @param pReadDB
	 *          {@link ReadDB} instance
	 * @return a {@link SunburstGUI} singleton
	 */
	public static SmallmultipleGUI getInstance(final PApplet pApplet,
			final SunburstControl pControl, final ReadDB pReadDB) {
		if (mGUI == null) {
			synchronized (SmallmultipleGUI.class) {
				if (mGUI == null) {
					mGUI = new SmallmultipleGUI(pApplet, pControl, pReadDB);
				}
			}
		}
		return mGUI;
	}

	@Override
	public void draw() {
		if (mDone) {
			mParent.pushMatrix();
			mParent.colorMode(PConstants.HSB, 360, 100, 100, 100);
			mParent.noFill();
			mParent.ellipseMode(PConstants.RADIUS);
			mParent.strokeCap(PConstants.SQUARE);
			mParent.textLeading(14);
			mParent.textAlign(PConstants.LEFT, PConstants.CENTER);
			mParent.smooth();
			mParent.stroke(255f);
			mParent.strokeWeight(2f);

			int i = 1;
			mX = 0;
			mY = 0;

			if (mBufferedImages.size() != 0) {
				mLock.acquireUninterruptibly();

				mParent.fill(360);
				mParent.noStroke();
				mParent.rect(0, 0, (float) mParent.width, (float) mParent.height);

				for (final ImageStore imageStore : mBufferedImages) {
					final PImage buffer = imageStore.mBufferedImage;// .get((int)(mBuffer.width
																													// / 15f), 0,
					// (int)(mBuffer.width - mBuffer.width / 15f), mBuffer.height);
					mParent.image(buffer, mX, mY, buffer.width / 2f, buffer.height / 2f);
					mX += mBuffer.width / 2f;
					if (i % 2 == 0) {
						mX -= mBuffer.width / 2f;
						mY = mBuffer.height / 2f + 1;
					} else if (i % 3 == 0) {
						mX = 0;
					}
					i++;
				}
				mLock.release();

				mParent.fill(0);
				mParent.textAlign(PConstants.LEFT, PConstants.TOP);
				mParent.stroke(50f);
				mParent.strokeWeight(1.5f);
				circArrow(mParent.width * 0.5f, mParent.height * 0.5f, 50,
						PConstants.PI, PConstants.TWO_PI * 1.25f, 10);
				mParent.text(
						"direction",
						mParent.width * 0.5f - mParent.textWidth("direction") * 0.5f,
						mParent.height * 0.5f
								- (mParent.textAscent() + mParent.textDescent()) * 0.5f);
				mParent.stroke(0f);
				mParent.strokeWeight(0f);

				mParent.ellipseMode(PConstants.RADIUS);
				final SmallmultipleModel model = ((SmallmultipleModel) mControl
						.getModel());
				ViewUtilities.compareLegend(this);
				ViewUtilities.legend(this, model);

				mParent.textAlign(PConstants.LEFT, PConstants.CENTER);
				mParent.fill(360);
				mParent.noStroke();

				// Mouse rollover.
				if (!isShowGUI()) {
					// Mouse rollover, arc hittest vars.
					rollover();

					if (mHitItem != null && mHitItem.getGreyState() == EGreyState.NO) {
						mX = mParent.mouseX - mParent.width / 2f;
						mY = mParent.mouseY - mParent.height / 2f;

						mParent.translate((float) mParent.width / 2f,
								(float) mParent.height / 2f);

						// Hover items.
						mParent.pushMatrix();
						mParent.scale(0.5f);
						for (i = 0; i < mRevisions; i++) {
							final ImageStore imageStore = mBufferedImages.get(i);
							model.setItems(i);
							switch (i) {
							case 0:
								mParent.translate(-(float) mBuffer.width / 2f,
										-(float) mBuffer.height / 2f);
								break;
							case 1:
								mParent.translate((float) mBuffer.width, 0);
								break;
							case 2:
								mParent.translate(0, (float) mBuffer.height + 1);
								break;
							case 3:
								mParent.translate(-(float) mBuffer.width, 0);
								break;
							}
							mDepthMax = imageStore.mMaxDepth;
							mOldDepthMax = imageStore.mOldMaxDepth;
							for (final SunburstItem item : model) {
								if (mKeyEquivalence.equivalent(item, mHitItem)) {
									Draw.DRAW.drawHover(this, item);
									break;
								}
							}
						}
						mParent.popMatrix();

						// Rollover text.
						if (mHitItem.getGreyState() == EGreyState.NO) {
							mParent.strokeWeight(1f);
							textMouseOver();
						}
					}
				}
			}

			mParent.popMatrix();
			ViewUtilities.drawGUI(getControlP5());
		}
	}

	@Override
	protected boolean rolloverInit() {
		mIndex = 4;
		if (mParent.mouseY < mParent.height / 2f) {
			if (mParent.mouseX < mParent.width / 2f) {
				mIndex = 0;
				mX = mParent.mouseX * 2f + 1;
				mY = mParent.mouseY * 2f + 1;
			} else {
				if (mRevisions >= 2) {
					mIndex = 1;
					mX = (mParent.mouseX - mParent.width / 2f) * 2;
					mY = mParent.mouseY * 2f;
				}
			}
		} else {
			if (mParent.mouseX < mParent.width / 2f) {
				if (mRevisions >= 4) {
					mIndex = 3;
					mX = mParent.mouseX * 2f;
					mY = (mParent.mouseY - mParent.height / 2f) * 2;
				}
			} else {
				if (mRevisions >= 3) {
					mIndex = 2;
					mX = (mParent.mouseX - mParent.width / 2f) * 2 + 1;
					mY = (mParent.mouseY - mParent.height / 2f) * 2 + 1;
				}
			}
		}

		boolean retVal = false;
		if (mIndex < mRevisions) {
			retVal = true;
			((SmallmultipleModel) mControl.getModel()).setItems(mIndex);
		}

		mX -= mBuffer.width * 0.5f;
		mY -= mBuffer.height * 0.5f;

		return retVal;
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
			assert pEvent.getNewValue() instanceof Boolean;
			try {
				mLock.acquireUninterruptibly();
				update(EResetZoomer.YES);
				mImage = new ImageStore(mBuffer, mSelectedRev, mDepthMax, mOldDepthMax);
				mBufferedImages.add(mImage);
				final long revDiff = mDb.getSession().getMostRecentRevisionNumber()
						- mDb.getRevisionNumber();
				mRevisions = revDiff > 4 ? 4 : revDiff;
				if (mBufferedImages.size() == mRevisions) {
					Collections.sort(mBufferedImages, mImage);
					mDone = true;
				}
			} finally {
				((SmallmultipleControl) mControl).releaseLock();
				((SmallmultipleControl) mControl).getLatch().countDown();
				mLock.release();
			}
			break;
		case "newrev":
			assert pEvent.getNewValue() instanceof Integer;
			mSelectedRev = (Integer) pEvent.getNewValue();
			break;
		case "oldrev":
			assert pEvent.getNewValue() instanceof Integer;
			mOldSelectedRev = (Integer) pEvent.getNewValue();
			break;
		case "dotbrightness":
			assert pEvent.getNewValue() instanceof Float;
			setDotBrightness((Float) pEvent.getNewValue());
		case "saturation":
			assert pEvent.getNewValue() instanceof Float;
			setSaturation((Float) pEvent.getNewValue());
		default:
			break;
		}
	}

	/**
	 * 
	 * @param x
	 * @param y
	 * @param radius
	 * @param start
	 * @param stop
	 * @param arrowSize
	 */
	void circArrow(final float pX, final float pY, final float pRadius,
			final float pStart, final float pStop, final float pArrowSize) {
		mParent.ellipseMode(PConstants.CENTER);
		mParent.noFill();
		mParent.arc(pX, pY, pRadius * 2, pRadius * 2, pStart, pStop);

		float arrowX = pX + PApplet.cos(pStop) * pRadius;
		float arrowY = pY + PApplet.sin(pStop) * pRadius;

		float point1X = pX + (PApplet.cos(pStop) * pRadius)
				+ (PApplet.cos(pStop - PApplet.radians(pArrowSize * 5)) * (pArrowSize));
		float point1Y = pY + (PApplet.sin(pStop) * pRadius)
				+ (PApplet.sin(pStop - PApplet.radians(pArrowSize * 5)) * (pArrowSize));

		float point2X = pX
				+ (PApplet.cos(pStop) * pRadius)
				+ (PApplet.cos(pStop - PApplet.radians(-pArrowSize * 5)) * (-pArrowSize));
		float point2Y = pY
				+ (PApplet.sin(pStop) * pRadius)
				+ (PApplet.sin(pStop - PApplet.radians(-pArrowSize * 5)) * (-pArrowSize));

		mParent.line(arrowX, arrowY, point1X, point1Y);
		mParent.line(arrowX, arrowY, point2X, point2Y);
	}

	@Override
	protected void setup() {
	}

	@Override
	public void relocate() {
	}

	/** Stores an image buffer with it's revision for sorting. */
	private static final class ImageStore implements Comparator<ImageStore> {

		/** {@link PImage} to buffer {@link SunburstItem}. */
		private final PImage mBufferedImage;

		/** Revision. */
		private final long mRevision;

		/** Max depth of outer circle. */
		private final int mMaxDepth;

		/** Max depth of inner circle. */
		private final int mOldMaxDepth;

		/**
		 * Constructor.
		 * 
		 * @param pBuffer
		 *          {@link PGraphics} reference
		 * @param pRevision
		 *          current revision
		 */
		ImageStore(final PImage pBuffer, @Nonnegative final long pRevision,
				@Nonnegative final int pMaxDepth, @Nonnegative final int pOldMaxDepth) {
			assert pBuffer != null;
			assert pRevision >= 0;
			assert pMaxDepth >= 0;
			assert pOldMaxDepth >= 0;
			mBufferedImage = pBuffer;
			mRevision = pRevision;
			mMaxDepth = pMaxDepth;
			mOldMaxDepth = pOldMaxDepth;
		}

		@Override
		public int compare(@Nullable final ImageStore pFirst,
				@Nullable final ImageStore pSecond) {
			if (pFirst == null || pSecond == null) {
				return 0;
			}
			return pFirst.mRevision > pSecond.mRevision ? 1
					: pFirst.mRevision == pSecond.mRevision ? 0 : -1;
		}
	}

	@Override
	public void resetGUI() {
		mGUI = null;
	}

	@Override
	public PApplet getApplet() {
		return mParent;
	}
}
