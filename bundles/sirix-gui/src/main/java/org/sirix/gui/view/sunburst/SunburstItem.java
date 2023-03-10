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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Stack;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import javax.xml.namespace.QName;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Namespace;

import org.brackit.xquery.atomic.QNm;
import org.gicentre.utils.move.Ease;
import org.sirix.access.Utils;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.EHover;
import org.sirix.gui.view.VisualItem;
import org.sirix.gui.view.splines.BSpline;
import org.sirix.node.Kind;

import processing.core.PApplet;
import processing.core.PConstants;
import processing.core.PGraphics;
import processing.core.PVector;

import com.google.common.base.Equivalence;

/**
 * 
 * <p>
 * Represents one item in the Sunburst diagram.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstItem implements VisualItem {
	// Relations. ============================================
	/** Index to parent node. */
	private transient int mIndexToParent;

	// Arc and lines drawing vars. ===========================
	/** Color of item. */
	private transient int mCol;

	/** Color of relation line. */
	private transient int mLineCol;

	/** Relation line weight. */
	private transient float mLineWeight;

	// Angle variables. ======================================
	/** The start of the angle in radians. */
	private transient float mAngleStart;

	/** The center of the angle in radians. */
	private transient float mAngleCenter;

	/** The end of the angle in radians. */
	private transient float mAngleEnd;

	/** Radius of the current depth. */
	private transient float mRadius;

	/** Stroke weight of the arc. */
	private transient float mDepthWeight;

	/** X coordinate control point of the relation line. */
	private transient float mX;

	/** Y coordinate control point of the relation line. */
	private transient float mY;

	/** Distance between the two relation points (child/parent). */
	private transient float mArcLength;

	// Bezier controlpoints. =================================
	/** X coordinate of first bezier control point. */
	private float mC1X;

	/** Y coordinate of first bezier control point. */
	private float mC1Y;

	/** X coordinate of second bezier control point. */
	private float mC2X;

	/** Y coordinate of second bezier control point. */
	private float mC2Y;

	/** {@link QNm} of current node. */
	private final QNm mQName;

	/** {@link QNm} of old node. */
	private transient QNm mOldQName;

	/** Depth in the tree. */
	private transient int mDepth;

	/** Value used for the arc colors. */
	private transient float mValue;

	/** Global minimum value. */
	private transient float mMinValue;

	/** Global maximum value. */
	private transient float mMaxValue;

	/** Structural kind of node. */
	public enum EStructType {
		/** Node is a leaf node. */
		ISLEAFNODE,

		/** Node is an inner node. */
		ISINNERNODE,
	}

	/** Structural kind of node. */
	private final EStructType mStructKind;

	/**
	 * State which determines if current item is found by an XPath expression or
	 * not.
	 */
	private transient XPathState mXPathState = XPathState.ISNOTFOUND;

	/** Singleton {@link SunburstGUI} instance. */
	private transient AbstractSunburstGUI mGUI;

	/** Text string. */
	private final String mText;

	/** Old text string. */
	private transient String mOldText;

	/** Parent processing applet. */
	private final PApplet mParent;

	/** Kind of diff. */
	transient DiffType mDiff = DiffType.SAME;

	/** Determines if one must be subtracted. */
	private transient boolean mSubtract;

	/** Image to write to. */
	private transient PGraphics mGraphic;

	/** Modification count. */
	private transient int mModifications;

	/** Determines if item has to be greyed out or not. */
	private transient EGreyState mGreyState = EGreyState.NO;

	/** Determines if item should be colored or not. */
	public enum EColorNode {
		YES, NO
	}

	/** Instance to use. */
	private transient EColorNode mColorNode = EColorNode.YES;

	/** Determines the revision the item is in. */
	private long mRevision;

	/** Index of the node where the current one has been moved to. */
	private int mIndexMovedTo = -1;

	/** {@link List} of {@link Attribute}s. */
	private List<Attribute> mAttributes;

	/** {@link List} of {@link Namespace}s. */
	private List<Namespace> mNamespaces;

	/** Old node key. */
	private long mOldKey;

	/** Original depth of item without . */
	private int mOrigDepth;

	/**
	 * Temporary depth of item (normal depth before transition for modified nodes
	 * + transformation).
	 */
	private float mTmpDepth;

	/** Easing parameter. */
	private float mEasing = 0f;

	private long mNodeKey;

	private Kind mKind;

	/** Builder to setup the Items. */
	public static final class Builder {
		/** {@link PApplet} representing the core processing library. */
		private final PApplet mParent;

		/** {@link QNm} of current node. */
		private transient QNm mQName;

		/** {@link QNm} of old node. */
		private transient QNm mOldQName;

		/** {@link NodeRelations} reference. */
		private final NodeRelations mRelations;

		/** The start degree. */
		private final float mAngleStart;

		/** The extension of the angle. */
		private final float mExtension;

		/** Text string. */
		private transient String mText;

		/** Old text string. */
		private transient String mOldText;

		/** Kind of diff. */
		private transient DiffType mDiff = DiffType.SAME;

		/** Modification count. */
		private transient int mModifications;

		/** GUI which extends {@link AbstractSunburstGUI}. */
		private transient AbstractSunburstGUI mGUI;

		/** {@link List} of {@link Attribute}s. */
		private List<Attribute> mAttributes = Collections.emptyList();

		/** {@link List} of {@link Namespace}s. */
		private List<Namespace> mNamespaces = Collections.emptyList();

		/** Old node key. */
		private long mOldKey;

		private long mNodeKey;

		private Kind mKind;

		/**
		 * Constructor.
		 * 
		 * @param pApplet
		 *          the processing core library @see PApplet
		 * @param pAngleStart
		 *          the start degree
		 * @param pExtension
		 *          the extension of the angle
		 * @param pRelations
		 *          {@link NodeRelations} instance
		 * @param pReadDB
		 *          {@link ReadDB} instance
		 * @param pGUI
		 *          GUI which extends {@link AbstractSunburstGUI}
		 */
		public Builder(final PApplet pApplet, @NonNegative final float pAngleStart,
				@NonNegative final float pExtension,
				@NonNull final NodeRelations pRelations, @NonNull final ReadDB pReadDB,
				@NonNull final AbstractSunburstGUI pGUI) {
			mParent = pApplet;
			mAngleStart = pAngleStart;
			mExtension = pExtension;
			mRelations = pRelations;
			mGUI = pGUI;
		}

		/**
		 * Set modification count.
		 * 
		 * @param pModifications
		 *          counted modifications in subtree of current node
		 * 
		 * @return this builder
		 */
		public Builder setModifications(@NonNegative final int pModifications) {
			checkArgument(pModifications >= 0, "pModifications must be >= 0!");
			mModifications = pModifications;
			return this;
		}

		/**
		 * Set {@link QName}.
		 * 
		 * @param pQName
		 *          {@link QName} of the current node
		 * @return this builder
		 */
		public Builder setQName(final QNm pQName) {
			mQName = checkNotNull(pQName);
			return this;
		}

		/**
		 * Set attributes.
		 * 
		 * @param pAttributes
		 *          {@link List} of attributes
		 * @return this builder
		 */
		public Builder setAttributes(final List<Attribute> pAttributes) {
			mAttributes = checkNotNull(pAttributes);
			return this;
		}

		/**
		 * Set namespaces.
		 * 
		 * @param pNamespaces
		 *          {@link List} of namespaces
		 * @return this builder
		 */
		public Builder setNamespaces(final List<Namespace> pNamespaces) {
			mNamespaces = checkNotNull(pNamespaces);
			return this;
		}

		/**
		 * Set the node key.
		 * 
		 * @param pNodeKey
		 *          node key
		 * @return this builder
		 */
		public Builder setNodeKey(final @NonNegative long pNodeKey) {
			checkArgument(pNodeKey >= 0, "node key must be >= 0!");
			mNodeKey = pNodeKey;
			return this;
		}

		/**
		 * Set the node kind.
		 * 
		 * @param pKind
		 *          node kind
		 * @return this builder
		 */
		public Builder setKind(final @NonNegative Kind pKind) {
			mKind = checkNotNull(pKind);
			return this;
		}

		/**
		 * Set old {@link QNm}.
		 * 
		 * @param pOldQName
		 *          {@link QNm} of the current node
		 * @return this builder
		 */
		public Builder setOldQName(final QNm pOldQName) {
			mOldQName = checkNotNull(pOldQName);
			return this;
		}

		/**
		 * Set character content.
		 * 
		 * @param pText
		 *          text string in case of a text node
		 * @return this builder
		 */
		public Builder setText(final String pText) {
			mText = checkNotNull(pText);
			return this;
		}

		/**
		 * Set old character content.
		 * 
		 * @param pOldText
		 *          text string in case of a text node
		 * @return this builder
		 */
		public Builder setOldText(final String pOldText) {
			mOldText = checkNotNull(pOldText);
			return this;
		}

		/**
		 * Set kind of diff.
		 * 
		 * @param pDiff
		 *          {@link DiffType}
		 * @return this builder
		 */
		public Builder setDiff(final DiffType pDiff) {
			mDiff = checkNotNull(pDiff);
			return this;
		}

		/**
		 * Set old nodeKey.
		 * 
		 * @param pNodeKey
		 *          node key
		 * @return this builder
		 */
		public Builder setOldKey(@NonNegative long pNodeKey) {
			checkArgument(pNodeKey >= 0, "old nodeKey must be >= 0!");
			mOldKey = pNodeKey;
			return this;
		}

		/**
		 * Build a new sunburst item.
		 * 
		 * @return a new sunburst item
		 */
		public SunburstItem build() {
			if (mQName == null && mOldQName == null && mText == null
					&& mOldText == null) {
				throw new IllegalStateException();
			}
			return new SunburstItem(this);
		}
	}

	/**
	 * Copy constructor.
	 * 
	 * @param pItem
	 *          {@link SunburstItem} to copy
	 * @throws NullPointerException
	 *           if {@code pItem} is {@code null}
	 */
	public SunburstItem(final SunburstItem pItem) {
		mNodeKey = pItem.mNodeKey;
		mKind = pItem.mKind;
		mOrigDepth = pItem.mOrigDepth;
		mTmpDepth = mOrigDepth;
		mOldKey = pItem.mOldKey;
		mIndexMovedTo = pItem.mIndexMovedTo;
		mAttributes = pItem.mAttributes;
		mNamespaces = pItem.mNamespaces;
		mGUI = pItem.mGUI;
		mQName = pItem.mQName;
		mOldQName = pItem.mOldQName;
		mText = pItem.mText;
		mOldText = pItem.mOldText;
		mParent = pItem.mParent;
		mModifications = pItem.mModifications;
		mStructKind = pItem.mStructKind;
		mValue = pItem.mValue;
		mMinValue = pItem.mMinValue;
		mMaxValue = pItem.mMaxValue;
		mIndexToParent = pItem.mIndexToParent;
		mDepth = pItem.mDepth;
		mSubtract = pItem.mSubtract;
		mAngleStart = pItem.mAngleStart;
		mDiff = pItem.mDiff;
		mAngleCenter = pItem.mAngleCenter;
		mAngleEnd = pItem.mAngleEnd;
		mGreyState = pItem.mGreyState;
		mColorNode = pItem.mColorNode;
		mRevision = pItem.mRevision;
		mGraphic = pItem.mGraphic;
		mArcLength = pItem.mArcLength;
		mC1X = pItem.mC1X;
		mC2X = pItem.mC2X;
		mC1Y = pItem.mC1Y;
		mC2Y = pItem.mC2Y;
		mCol = pItem.mCol;
		mDepthWeight = pItem.mDepthWeight;
		mLineCol = pItem.mLineCol;
		mLineWeight = pItem.mLineWeight;
		mXPathState = pItem.mXPathState;
		mRadius = pItem.mRadius;
		mX = pItem.mX;
		mY = pItem.mY;
	}

	/**
	 * Constructor.
	 * 
	 * @param pBuilder
	 *          the Builder to build a new sunburst item
	 */
	private SunburstItem(final Builder pBuilder) {
		mKind = pBuilder.mKind;
		mNodeKey = pBuilder.mNodeKey;
		mOrigDepth = pBuilder.mRelations.mOrigDepth;
		mTmpDepth = mOrigDepth;
		mGUI = pBuilder.mGUI;
		mQName = pBuilder.mQName;
		mOldQName = pBuilder.mOldQName;
		mText = pBuilder.mText;
		mOldText = pBuilder.mOldText;
		mParent = pBuilder.mParent;
		mModifications = pBuilder.mModifications;
		mStructKind = pBuilder.mRelations.mStructKind;
		mValue = pBuilder.mRelations.mValue;
		mMinValue = pBuilder.mRelations.mMinValue;
		mMaxValue = pBuilder.mRelations.mMaxValue;
		mIndexToParent = pBuilder.mRelations.mIndexToParent;
		mDepth = pBuilder.mRelations.mDepth;
		mSubtract = pBuilder.mRelations.mSubtract;
		mDiff = pBuilder.mDiff;

		if (pBuilder.mAngleStart > PConstants.TWO_PI) {
			mAngleStart = PConstants.TWO_PI;
		} else {
			mAngleStart = pBuilder.mAngleStart; // + 0.003f;
		}

		if ((mAngleStart + pBuilder.mExtension * 0.5f) > PConstants.TWO_PI) {
			mAngleCenter = PConstants.TWO_PI;
		} else {
			mAngleCenter = mAngleStart + pBuilder.mExtension * 0.5f;
		}

		if (mAngleStart + pBuilder.mExtension > PConstants.TWO_PI) {
			mAngleEnd = PConstants.TWO_PI;
		} else {
			mAngleEnd = mAngleStart + pBuilder.mExtension; // - 0.003f;
		}

		assert mAngleEnd >= mAngleStart;
		mAttributes = pBuilder.mAttributes;
		mNamespaces = pBuilder.mNamespaces;
		mOldKey = pBuilder.mOldKey;
	}

	/**
	 * Set the kind of diff.
	 * 
	 * @param pDiff
	 *          kind of diff to set
	 */
	public void setDiff(final DiffType pDiff) {
		mDiff = checkNotNull(pDiff);
	}

	/**
	 * Set revision to which this item belongs.
	 * 
	 * @param pRevision
	 *          revision to set
	 */
	public void setRevision(@NonNegative final long pRevision) {
		checkArgument(pRevision >= 0, "paramRevision must be >= 0!");
		mRevision = pRevision;
	}

	/**
	 * Get revision.
	 * 
	 * @return revision number
	 */
	public long getRevision() {
		return mRevision;
	}

	/**
	 * Update item, called if the sirix storage has changed or the normalizations
	 * have changed.
	 * 
	 * @param pMappingMode
	 *          specifies the mapping mode
	 * @param pGraphic
	 *          offline buffer image
	 * @throws IllegalArgumentException
	 *           if {@code pMapping} < 1 or > 3
	 * @throws NullPointerException
	 *           if {@code pGraphic} is null
	 */
	@Override
	public void update(final Draw pDraw, @NonNegative final int pMappingMode,
			@NonNull final PGraphics pGraphic) {
		checkArgument(pMappingMode == 1 || pMappingMode == 2 || pMappingMode == 3);
		checkNotNull(pGraphic);
		mGraphic = pGraphic;
		if (mIndexToParent > -1 || mDepth > 0) {
			final int depthMax = mGUI.mDepthMax;

			if (pDraw == Draw.DRAW && !mGUI.isZoomingPanning() && mTmpDepth < mDepth) {
				mRadius = mGUI.calcEqualAreaRadius(mTmpDepth, depthMax);
				mDepthWeight = mGUI.calcEqualAreaRadius(mTmpDepth + 1, depthMax)
						- mRadius;
			} else {
				mRadius = mGUI.calcEqualAreaRadius(mDepth, depthMax);
				mDepthWeight = mGUI.calcEqualAreaRadius(mDepth + 1, depthMax) - mRadius;
			}
			mX = PApplet.cos(mAngleCenter) * mRadius;
			mY = PApplet.sin(mAngleCenter) * mRadius;

			// Chord.
			final float startX = PApplet.cos(mAngleCenter) * mRadius;
			final float startY = PApplet.sin(mAngleCenter) * mRadius;
			final float endX = PApplet.cos(mAngleEnd) * mRadius;
			final float endY = PApplet.sin(mAngleEnd) * mRadius;
			mArcLength = PApplet.dist(startX, startY, endX, endY);

			// Color mapings.
			float percent = 0;
			float value = mValue;
			float minValue = mMinValue;
			float maxValue = mMaxValue;

			if (mGUI.mUseDiffView == ViewType.DIFF && mGUI.mUseDiffView.getValue()
					&& mKind == Kind.ELEMENT) {
				value = (mValue - (float) mModifications) / mValue;
				minValue = 0;
				maxValue = 1;
			}
			if (minValue == 0 && maxValue == 0
					|| (minValue == maxValue && maxValue == value)) {
				percent = 0;
			} else {
				switch (pMappingMode) {
				case 1:
					percent = (float) (value - minValue) / (float) (maxValue - minValue);
					break;
				case 2:
					percent = (float) (PApplet.log(value) - PApplet.log(minValue))
							/ (float) (PApplet.log(maxValue) - PApplet.log(minValue));
					break;
				case 3:
					percent = (float) (PApplet.sqrt(value) - PApplet.sqrt(minValue))
							/ (float) (PApplet.sqrt(maxValue) - PApplet.sqrt(minValue));
					break;
				default:
					throw new IllegalStateException("Mapping mode unknown!");
				}
			}

			// Colors for element and other nodes.
			switch (mKind) {
			case ELEMENT:
				float bright = PApplet.lerp(mGUI.getInnerNodeBrightnessStart(),
						mGUI.getInnerNodeBrightnessEnd(), percent);
				mCol = pGraphic.color(0, 0, bright);

				// bright =
				// PApplet.lerp(mGUI.mInnerNodeStrokeBrightnessStart,
				// mGUI.mInnerNodeStrokeBrightnessEnd,
				// percent);
				mLineCol = pGraphic.color(0, 0, 0);
				break;
			case TEXT:
			case COMMENT:
			case PROCESSING_INSTRUCTION:
				final int from = pGraphic.color(mGUI.getHueStart(),
						mGUI.getSaturationStart(), mGUI.getBrightnessStart());
				final int to = pGraphic.color(mGUI.getHueEnd(),
						mGUI.getSaturationEnd(), mGUI.getBrightnessEnd());
				mCol = pGraphic.lerpColor(from, to, percent);

				mLineCol = mCol;
				break;
			default:
				throw new IllegalStateException("Node type currently not supported!");
			}

			// Calculate stroke weight for relations line.
			if (pDraw == Draw.DRAW && !mGUI.isZoomingPanning() && mTmpDepth < mDepth) {
				mLineWeight = PApplet.map(mTmpDepth, 0, depthMax,
						mGUI.getStrokeWeightStart(), mGUI.getStrokeWeightEnd());
			} else {
				mLineWeight = PApplet.map(mDepth, 0, depthMax,
						mGUI.getStrokeWeightStart(), mGUI.getStrokeWeightEnd());
			}
			if (mArcLength < mLineWeight) {
				mLineWeight = mArcLength * 0.95f;
			}

			// Calculate bezier controlpoints.
			if (mIndexToParent > -1) {
				if (pDraw == Draw.DRAW && !mGUI.isZoomingPanning()
						&& mTmpDepth < mDepth) {
					mC1X = PApplet.cos(mAngleCenter)
							* mGUI.calcEqualAreaRadius(mTmpDepth - 1, depthMax);
					mC1Y = PApplet.sin(mAngleCenter)
							* mGUI.calcEqualAreaRadius(mTmpDepth - 1, depthMax);
				} else {
					mC1X = PApplet.cos(mAngleCenter)
							* mGUI.calcEqualAreaRadius(mDepth - 1, depthMax);
					mC1Y = PApplet.sin(mAngleCenter)
							* mGUI.calcEqualAreaRadius(mDepth - 1, depthMax);
				}

				// Cast is safe.
				final SunburstItem parent = (SunburstItem) mGUI.mControl.getModel()
						.getItem(mIndexToParent);
				final float parentAngleCenter = parent.mAngleCenter;

				if (parent.getDepth() == 0) {
					mC2X = 0;
				} else {
					if (pDraw == Draw.DRAW && !mGUI.isZoomingPanning()
							&& mTmpDepth < mDepth) {
						mC2X = PApplet.cos(parentAngleCenter)
								* mGUI.calcEqualAreaRadius(mTmpDepth, depthMax);
					} else {
						mC2X = PApplet.cos(parentAngleCenter)
								* mGUI.calcEqualAreaRadius(mDepth, depthMax);
					}

					if (((parentAngleCenter >= PConstants.PI / 2f) && mAngleCenter < PConstants.PI / 2f)
							|| ((parentAngleCenter < 1.5f * PConstants.PI) && mAngleCenter >= 1.5f * PConstants.PI)) {
						mC2X *= -1;
					}
				}
				if (pDraw == Draw.DRAW && !mGUI.isZoomingPanning()
						&& mTmpDepth < mDepth) {
					mC2Y = PApplet.sin(parentAngleCenter)
							* mGUI.calcEqualAreaRadius(mTmpDepth, depthMax);
				} else {
					mC2Y = PApplet.sin(parentAngleCenter)
							* mGUI.calcEqualAreaRadius(mDepth, depthMax);
				}
			}

			if (mTmpDepth < mDepth) {
				mTmpDepth = PApplet.lerp(mTmpDepth, mDepth, Ease.cubicBoth(mEasing));
				mEasing += 0.01;
			}
		}
	}

	float getCurrArcRadius() {
		return mRadius;
	}

	// Draw methods ====================================
	/**
	 * Draw an arc.
	 * 
	 * @param pInnerNodeScale
	 *          scale of inner nodes
	 * @param pLeafScale
	 *          scale of leaf nodes
	 * @param pHover
	 *          determines if item currently is hovered or not
	 */
	void drawArc(@NonNegative final float pInnerNodeScale,
			@NonNegative final float pLeafScale, @NonNull final EHover pHover) {
		checkArgument(pInnerNodeScale >= 0f, "innernode arc scale must be >= 0!");
		checkArgument(pLeafScale >= 0f, "leaf scale must be >= 0!");
		checkNotNull(pHover);
		assert mGraphic != null;
		float arcRadius = 0;
		switch (mStructKind) {
		case ISLEAFNODE:
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.strokeWeight(mDepthWeight * pLeafScale);
			}
			mGraphic.strokeWeight(mDepthWeight * pLeafScale);
			arcRadius = mRadius + mDepthWeight * pLeafScale / 2;
			break;
		case ISINNERNODE:
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.strokeWeight(mDepthWeight * pInnerNodeScale);
			}
			mGraphic.strokeWeight(mDepthWeight * pInnerNodeScale);
			arcRadius = mRadius + mDepthWeight * pInnerNodeScale / 2;
			break;
		default:
			throw new AssertionError("Structural kind not known!");
		}

		mGreyState.setStroke(mGraphic, mGUI.mParent.recorder, mCol, pHover);
		if (mGreyState == EGreyState.NO && pHover != EHover.TRUE) {
			mXPathState.setStroke(mGraphic, mGUI.mParent.recorder, mCol, pHover);
		}

		if (pHover == EHover.TRUE) {
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.noFill();
			}
			mGraphic.noFill();
		}

		// mParent.arc(0, 0, arcRadius, arcRadius, mAngleStart, mAngleEnd);
		arcWrap(0, 0, arcRadius, arcRadius, mAngleStart, mAngleEnd); // normaly arc
																																	// should work
	}

	/**
	 * Fix for arc it seems that the arc functions has a problem with very tiny
	 * angles ... arcWrap is a quick hack to get rid of this problem.
	 * 
	 * @param pX
	 *          X position of middle point
	 * @param pY
	 *          Y position of middle point
	 * @param pW
	 *          width of ellipse
	 * @param pH
	 *          height of ellipse
	 * @param pA1
	 *          angle to start from
	 * @param pA2
	 *          angle to end
	 */
	void arcWrap(final float pX, final float pY, final float pW, final float pH,
			final float pA1, final float pA2) {
		if (mArcLength > 2.5) {
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.arc(pX, pY, pW, pH, pA1, pA2);
			}
			mGraphic.arc(pX, pY, pW, pH, pA1, pA2);
		} else {
			if (mGUI.mParent.recorder != null) {
				drawArc(mGUI.mParent.recorder, pW);
			}
			drawArc(mGraphic, pW);
		}
	}

	/**
	 * Draw arc if arc is too small because of processing bug.
	 * 
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 * @param pW
	 *          width of ellipse
	 */
	private void drawArc(final PGraphics pGraphic, @NonNegative final float pW) {
		assert pGraphic != null;
		assert pW >= 0f;
		pGraphic.pushMatrix();
		pGraphic.strokeWeight(mArcLength);
		pGraphic.rotate(mAngleCenter);
		pGraphic.translate(mRadius, 0);
		pGraphic.line(0, 0, (pW - mRadius) * 2, 0);
		pGraphic.popMatrix();
	}

	/**
	 * Draw current sunburst item as a rectangle.
	 * 
	 * @param pInnerNodeScale
	 *          scale of a non leaf node
	 * @param pLeafScale
	 *          scale of a leaf node
	 * @param pHover
	 *          determines if item currently is hovered or not
	 */
	void drawRect(@NonNegative final float pInnerNodeScale,
			@NonNegative final float pLeafScale, @NonNull final EHover pHover) {
		float rectWidth;
		switch (mStructKind) {
		case ISLEAFNODE:
			rectWidth = mRadius + mDepthWeight * pLeafScale / 2;
			break;
		case ISINNERNODE:
			rectWidth = mRadius + mDepthWeight * pInnerNodeScale / 2;
			break;
		default:
			throw new AssertionError("Structural kind not known!");
		}

		mXPathState.setStroke(mGraphic, mGUI.mParent.recorder, mCol, pHover);
		if (mGUI.mParent.recorder != null) {
			if (pHover == EHover.TRUE) {
				mGUI.mParent.recorder.noFill();
			}
			drawRect(mGUI.mParent.recorder, rectWidth);
		}
		if (pHover == EHover.TRUE) {
			mGraphic.noFill();
		}
		mGraphic.stroke(mCol);
		drawRect(mGraphic, rectWidth);
	}

	/**
	 * Draw a rectangle.
	 * 
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 * @param pRectWidth
	 *          the with of the rectangle
	 */
	private void drawRect(final PGraphics pGraphic, final float pRectWidth) {
		assert pGraphic != null;
		assert pRectWidth >= 0f;
		pGraphic.pushMatrix();
		pGraphic.strokeWeight(mArcLength);
		pGraphic.rotate(mAngleCenter);
		pGraphic.translate(mRadius, 0);
		pGraphic.line(0, 0, (pRectWidth - mRadius) * 2, 0);
		pGraphic.popMatrix();
	}

	/**
	 * Draw a dot which are the bezier-curve anchors.
	 * 
	 * @param pHover
	 *          determines if item currently is hovered or not
	 */
	void drawDot(final EHover pHover) {
		float diameter = mGUI.getDotSize();

		if (pHover == EHover.TRUE) {
			diameter = diameter * 2f;
		}

		if (mDepth > 0 && mArcLength < diameter) {
			diameter = mArcLength * 0.95f;
		}

		if (mGUI.mParent.recorder != null) {
			mGUI.mParent.recorder.noStroke();
		}
		mGraphic.noStroke();
		if (mGUI.mUseDiffView == ViewType.DIFF && ViewType.DIFF.getValue()
				&& mGUI.mSelectedRev != 0 && mGreyState != EGreyState.YES) {
			// Must be the same as in SmallMultiplesModel (Refactoring required!).
			final float factor = 25f;
			switch (mDiff) {
			case INSERTED:
				final int blue = mParent.color(200, mGUI.getSaturation() - factor
						* mRevision, mGUI.getDotBrightness() - factor * mRevision);
				if (mGUI.mParent.recorder != null) {
					mGUI.mParent.recorder.fill(blue);
				}
				mGraphic.fill(blue);
				break;
			case DELETED:
				final int red = mParent.color(360, mGUI.getSaturation() - factor
						* mRevision, mGUI.getDotBrightness() - factor * mRevision);
				if (mGUI.mParent.recorder != null) {
					mGUI.mParent.recorder.fill(red);
				}
				mGraphic.fill(red);
				break;
			case UPDATED:
				final int green = mParent.color(120, mGUI.getSaturation() - factor
						* mRevision, mGUI.getDotBrightness() - factor * mRevision);
				if (mGUI.mParent.recorder != null) {
					mGUI.mParent.recorder.fill(green);
				}
				mGraphic.fill(green);
				break;
			case MOVEDFROM:
			case MOVEDTO:
				final int yellow = mParent.color(60, mGUI.getSaturation() - factor
						* mRevision, mGUI.getDotBrightness() - factor * mRevision);
				if (mGUI.mParent.recorder != null) {
					mGUI.mParent.recorder.fill(yellow);
				}
				mGraphic.fill(yellow);
				break;
			case REPLACEDOLD:
				final int dot = mParent.color(250, mGUI.getSaturation() - factor
						* mRevision, mGUI.getDotBrightness() - factor * mRevision);
				if (mGUI.mParent.recorder != null) {
					mGUI.mParent.recorder.fill(dot);
				}
				mGraphic.fill(dot);
				mGraphic.ellipse(mX, mY, diameter, diameter);

				diameter *= 0.5;
			case REPLACEDNEW:
				final int color = mParent.color(290, mGUI.getSaturation() - factor
						* mRevision, mGUI.getDotBrightness() - factor * mRevision);
				if (mGUI.mParent.recorder != null) {
					mGUI.mParent.recorder.fill(color);
				}
				mGraphic.fill(color);
				break;
			default:
				// EDiff.SAME.
				dot();
			}

		} else if (mColorNode == EColorNode.YES) {
			dot();
		}
		if (mGUI.mParent.recorder != null) {
			mGUI.mParent.recorder.ellipse(mX, mY, diameter, diameter);
			mGUI.mParent.recorder.noFill();
		}
		mGraphic.ellipse(mX, mY, diameter, diameter);
		mGraphic.noFill();
	}

	/**
	 * Draw black or white dot determined through the background brightness.
	 */
	private void dot() {
		if (mGUI.mParent.recorder != null) {
			if (mGUI.getBackgroundBrightness() < 30) {
				mGUI.mParent.recorder.fill(0, 0, 20);
			} else {
				mGUI.mParent.recorder.fill(0, 0, 0);
			}
		}

		if (mGUI.getBackgroundBrightness() < 30) {
			mGraphic.fill(0, 0, 20);
		} else {
			mGraphic.fill(0, 0, 0);
		}
	}

	/**
	 * Draw a spline between the {@code movedFrom} and {@code movedTo} item.
	 */
	void drawMovedConnection(final EHover pHover) {
		checkNotNull(pHover);
		if (mDiff == DiffType.MOVEDFROM) {
			if (mGUI.mParent.recorder != null) {
				setStroke(mGUI.mParent.recorder, pHover);
			}
			setStroke(mGraphic, pHover);
			if (mGUI.mParent.recorder != null) {
				strokeWeight(mGUI.mParent.recorder, pHover);
			}
			strokeWeight(mGraphic, pHover);

			final SunburstItem movedTo = (SunburstItem) mGUI.mControl.getModel()
					.getItem(mIndexMovedTo);
			assert movedTo.getDiff() == DiffType.MOVEDTO;
			final float angle = PApplet.abs(PApplet.atan2(mY, mX)
					- PApplet.atan2(movedTo.mY, movedTo.mX));
			if (mGUI.mParent.recorder != null) {
				drawMoveRelation(angle, mGUI.mParent.recorder, movedTo);
			}
			drawMoveRelation(angle, mGraphic, movedTo);
		}
	}

	private void setStroke(final PGraphics pGraphic, final EHover pHover) {
		assert pGraphic != null;
		assert pHover != null;
		final int red = mParent.color(360, mGUI.getSaturation(),
				mGUI.getDotBrightness());
		if (pHover == EHover.TRUE) {
			pGraphic.stroke(red);
		} else {
			pGraphic.stroke(0);
		}
	}

	/**
	 * Set the stroke weight.
	 * 
	 * @param pGraphic
	 *          graphics buffer
	 * @param pHover
	 *          hovering
	 */
	private void strokeWeight(final PGraphics pGraphic, final EHover pHover) {
		assert pGraphic != null;
		assert pHover != null;
		if (pHover == EHover.TRUE) {
			pGraphic.strokeWeight(4f);
		} else {
			pGraphic.strokeWeight(1f);
		}
	}

	/**
	 * Draw the move relation.
	 * 
	 * @param pAngle
	 *          angle to denote if a hierarichal edge bundling technique should be
	 *          used or a straight line
	 * @param pGraphic
	 *          {@link PGraphics} reference
	 * @param pMovedTo
	 *          {@link SunburstItem} reference where the node has been moved to
	 */
	private void drawMoveRelation(final float pAngle, final PGraphics pGraphic,
			final SunburstItem pMovedTo) {
		assert pAngle >= 0f;
		assert pGraphic != null;
		assert pMovedTo != null;
		// if (pAngle > (float)(PConstants.PI / 2f)) {
		hierarichalEdgeBundles();
		// } else {
		// mGUI.drawArrow(mGraphic, mX, mY, pMovedTo.mX, pMovedTo.mY,
		// EDrawLine.YES);
		// }
	}

	/**
	 * Creates hierarchical edge bundle between moved nodes according to Danny
	 * Holsten's paper.
	 */
	private void hierarichalEdgeBundles() {
		// Get LCA.
		final SunburstItem lca = getLeastCommonAncestor();

		// Path from source to destination over longest common ancestor.
		final List<PVector> path = getPath(lca);

		// Create and draw spline.
		final BSpline bspline = new BSpline();
		if (mGUI.mParent.recorder != null) {
			bspline.draw(mGUI, mGUI.mParent.recorder, path);
		}
		bspline.draw(mGUI, mGraphic, path);
	}

	/**
	 * Get path from {@code movedFrom} to {@code movedTo} over common ancestor if
	 * path has less than 3 nodes, otherwise common ancestor is removed.
	 * 
	 * @return {@link List} of path elements of type {@link PVector}
	 */
	private List<PVector> getPath(final SunburstItem pLCA) {
		final List<PVector> path = new ArrayList<>();
		// Source to LCA.

		path.add(new PVector(mX, mY));
		final Equivalence<SunburstItem> equivalence = new SunburstItemKeyEquivalence();
		SunburstItem parent = ((SunburstItem) mGUI.mControl.getModel().getItem(
				mIndexToParent));
		while (!equivalence.equivalent(parent, pLCA)) {
			path.add(new PVector(parent.mX, parent.mY));
			parent = ((SunburstItem) mGUI.mControl.getModel().getItem(
					parent.getIndexToParent()));
		}
		// final int size = path.size();
		// path.add(new PVector(parent.mX, parent.mY));
		// Destination to LCA.
		final Stack<PVector> points = new Stack<>();
		parent = ((SunburstItem) mGUI.mControl.getModel().getItem(mIndexMovedTo));
		while (!equivalence.equivalent(parent, pLCA)) {
			points.add(new PVector(parent.mX, parent.mY));
			parent = ((SunburstItem) mGUI.mControl.getModel().getItem(
					parent.getIndexToParent()));
		}
		while (!points.empty()) {
			path.add(points.pop());
		}
		// if (path.size() < 4) {
		// path.add(size, new PVector(pLCA.getX(), pLCA.getY()));
		// }
		return path;
	}

	/** Get the least common ancestor for moved nodes {@code from => to}. */
	private SunburstItem getLeastCommonAncestor() {
		final Deque<SunburstItem> from = new ArrayDeque<>();
		SunburstItem fromItem = this;
		while (fromItem.getIndexToParent() != -1) {
			fromItem = ((SunburstItem) mGUI.mControl.getModel().getItem(
					fromItem.getIndexToParent()));
			from.push(fromItem);
		}

		final Deque<SunburstItem> to = new ArrayDeque<>();
		SunburstItem toItem = ((SunburstItem) mGUI.mControl.getModel().getItem(
				mIndexMovedTo));
		while (toItem.getIndexToParent() != -1) {
			toItem = ((SunburstItem) mGUI.mControl.getModel().getItem(
					toItem.getIndexToParent()));
			to.push(toItem);
		}

		final Equivalence<SunburstItem> equivalence = new SunburstItemKeyEquivalence();
		SunburstItem result = null;
		while (equivalence.equivalent(from.peek(), to.peek())) {
			result = from.pop();
			to.pop();
		}
		return result;
	}

	/**
	 * Draw a straight line from child to parent.
	 */
	void drawRelationLine() {
		if (mIndexToParent > -1) {
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.stroke(mLineCol);
			}
			mGraphic.stroke(mLineCol);
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.strokeWeight(mLineWeight);
			}
			mGraphic.strokeWeight(mLineWeight);
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder
						.line(mX, mY,
								((SunburstItem) mGUI.mControl.getModel()
										.getItem(mIndexToParent)).mX, ((SunburstItem) mGUI.mControl
										.getModel().getItem(mIndexToParent)).mY);
			}
			mGraphic.line(mX, mY,
					((SunburstItem) mGUI.mControl.getModel().getItem(mIndexToParent)).mX,
					((SunburstItem) mGUI.mControl.getModel().getItem(mIndexToParent)).mY);
		}
	}

	/**
	 * Draw a bezier curve from child to parent.
	 */
	void drawRelationBezier() {
		if (mIndexToParent > -1) {
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.stroke(mLineCol);
			}
			mGraphic.stroke(mLineCol);
			if (mLineWeight < 0) {
				mLineWeight *= -1;
			}
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.strokeWeight(mLineWeight);
			}
			mGraphic.strokeWeight(mLineWeight);
			final SunburstItem parent = (SunburstItem) mGUI.mControl.getModel()
					.getItem(mIndexToParent);
			if (mGUI.mParent.recorder != null) {
				mGUI.mParent.recorder.bezier(mX, mY, mC1X, mC1Y, mC2X, mC2Y, parent.mX,
						parent.mY);
			}
			mGraphic.bezier(mX, mY, mC1X, mC1Y, mC2X, mC2Y, parent.mX, parent.mY);
		}
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder().append("[Depth: ")
				.append(mDepth);
		if (mOldText != null) {
			builder.append("\n").append("old Text: ");
			appendText(builder, mOldText);
			builder.append("\n");
		} else if (mOldQName != null) {
			builder.append("\n").append("old QName: ");
			appendText(builder, Utils.buildName(mOldQName));
			builder.append("\n");
		}
		if (mQName != null) {
			appendWhiteSpace(builder);
			builder.append("QName: ");
			appendText(builder, Utils.buildName(mQName));
			builder.append("\n");
		} else if (mText != null) {
			appendWhiteSpace(builder);
			builder.append("Text: ");
			appendText(builder, mText);
			builder.append("\n");
		}
		updated(builder);
		if (mGUI.mUseAttribute) {
			for (final Attribute attr : mAttributes) {
				if ("name".equalsIgnoreCase(attr.getName().getLocalPart())) {
					builder.append("Name: ").append(attr.getValue()).append("\n");
				}
			}
		}
		if (mModifications > 0) {
			builder.append("ModifcationCount: ").append(mModifications);
		}
		if (mModifications != 0) {
			builder.append(" ");
		}
		builder.append("DescendantCount: ").append((int) mValue);
		builder.append(" NodeKey: ").append(mNodeKey);

		if (mDiff != null) {
			builder.append(" Diff: ").append(mDiff.toString());
		}
		builder.append("]");
		return builder.toString();
	}

	private void appendWhiteSpace(final StringBuilder pBuilder) {
		if (mOldText == null && mOldQName == null) {
			pBuilder.append("\n");
		}
	}

	private void appendText(final StringBuilder pBuilder, final String pText) {
		final int length = pText.length();
		if (length >= 80) {
			pBuilder.append(pText.substring(0, 20)).append("...");
		} else {
			pBuilder.append(pText);
		}
	}

	/**
	 * Node has been updated so append to {@link StringBuilder}.
	 * 
	 * @param pBuilder
	 *          {@link StringBuilder} reference
	 * @throws NullPointerException
	 *           if {@code pBuilder} is {@code null}
	 */
	public void updated(final StringBuilder pBuilder) {
		checkNotNull(pBuilder);
		if (mDiff != null
				&& (mDiff == DiffType.UPDATED || mDiff == DiffType.REPLACEDOLD || mDiff == DiffType.REPLACEDNEW)) {
			if (mGUI.mUseAttribute) {
				mGUI.processAttributes(pBuilder);
			}
		}
	}

	@Override
	public boolean equals(final Object pObj) {
		if (pObj == this) {
			return true;
		}
		if (!(pObj instanceof SunburstItem)) {
			return false;
		}
		final SunburstItem item = (SunburstItem) pObj;
		boolean retVal = (getQName() == null ? item.getQName() == null : getQName()
				.equals(item.getQName()));
		retVal = retVal
				&& (getText() == null ? item.getText() == null : getText().equals(
						item.getText()));
		retVal = retVal
				&& (getKey() == item.getKey() && mDiff == item.mDiff && mDepth == item.mDepth);

		return retVal;
	}

	@Override
	public int hashCode() {
		int result = 17;
		/*
		 * It takes a (64-bit) long l, exclusive-or's the top and bottom halves (of
		 * 32 bits each) into the bottom 32 bits of a 64-bit results, then takes
		 * only the bottom 32 bits with the (int) cast.
		 */
		result = 31 * result + (int) (getKey() ^ (getKey() >>> 32));
		result = 31 * result + mDiff.hashCode();
		result = 31 * result + mDepth;
		if (getQName() != null) {
			result = 31 * result + getQName().hashCode();
		}
		if (getText() != null) {
			result = 31 * result + getText().hashCode();
		}
		return result;
	}

	@Override
	public void hover(final PGraphics pGraphic) {
		if (mGreyState == EGreyState.NO) {
			final PGraphics graphic = mGraphic;
			mGraphic = checkNotNull(pGraphic);
			mGraphic.noFill();
			if (mDiff == DiffType.MOVEDFROM || mDiff == DiffType.MOVEDTO) {
				drawArcRec();

				if (mIndexMovedTo >= 0 && mIndexMovedTo < mGUI.mModel.getItemsSize()) {
					drawMovedConnection(EHover.TRUE);
				}
			} else {
				float tmpLineWeight = mLineWeight;
				int tmpLineCol = mLineCol;
				if (mGUI.isShowArcs()) {
					drawArcRec();
				}
				mLineWeight += 2f;
				mLineCol = mParent.color(360, mGUI.getSaturation(),
						mGUI.getDotBrightness());

				final int tmpIndexToParent = mIndexToParent;
				final float tmpmX = mX;
				final float tmpmY = mY;
				final float tmpmC1X = mC1X;
				final float tmpmC2X = mC2X;
				final float tmpmC1Y = mC1Y;
				final float tmpmC2Y = mC2Y;
				if (mGUI.isUseBezierLine()) {
					while (mIndexToParent != -1) {
						drawRelationBezier();
						final SunburstItem item = ((SunburstItem) mGUI.mControl.getModel()
								.getItem(mIndexToParent));
						setVariables(item);
						mC1X = item.mC1X;
						mC1Y = item.mC1Y;
						mC2X = item.mC2X;
						mC2Y = item.mC2Y;
					}
				} else {
					while (mIndexToParent != -1) {
						drawRelationLine();
						final SunburstItem item = ((SunburstItem) mGUI.mControl.getModel()
								.getItem(mIndexToParent));
						setVariables(item);
					}
				}
				mIndexToParent = tmpIndexToParent;
				mX = tmpmX;
				mY = tmpmY;
				mC1X = tmpmC1X;
				mC2X = tmpmC2X;
				mC1Y = tmpmC1Y;
				mC2Y = tmpmC2Y;

				mLineWeight = tmpLineWeight;
				mLineCol = tmpLineCol;
				drawDot(EHover.TRUE);
				mGraphic = graphic;
			}
		}
	}

	/** Draw an arc or rectangle depending on the GUI toggle value. */
	private void drawArcRec() {
		if (mGUI.mUseArc) {
			drawArc(mGUI.getInnerNodeArcScale(), mGUI.getLeafArcScale(), EHover.TRUE);
		} else {
			drawRect(mGUI.getInnerNodeArcScale(), mGUI.getLeafArcScale(), EHover.TRUE);
		}
	}

	/**
	 * Set some variables.
	 * 
	 * @param pItem
	 *          {@link SunburstItem} instance
	 */
	private void setVariables(final SunburstItem pItem) {
		assert pItem != null;
		mIndexToParent = pItem.mIndexToParent;
		mX = pItem.mX;
		mY = pItem.mY;
	}

	/**
	 * Note that this compareTo(Object) method doesn't reflect document order of
	 * the nodes.
	 * 
	 * @see Comparable#compareTo(Object)
	 * @throws NullPointerException
	 *           if {@code pItem} is {@code null}
	 */
	@Override
	public int compareTo(final VisualItem pItem) {
		checkNotNull(pItem);
		return this.getKey() > pItem.getKey() ? 1
				: this.getKey() == pItem.getKey() ? 0 : -1;
	}

	// Accessors ==========================================

	public PGraphics getGraphic() {
		return mGraphic;
	}

	/**
	 * Set XPath state.
	 * 
	 * @param pState
	 *          set state to this value
	 */
	@Override
	public void setXPathState(final XPathState pState) {
		mXPathState = checkNotNull(pState);
	}

	public long getOldKey() {
		return mOldKey;
	}

	/**
	 * Set greyout value.
	 * 
	 * @param pState
	 *          set state to this value
	 */
	@Override
	public void setGreyState(final EGreyState pState) {
		mGreyState = checkNotNull(pState);
	}

	/**
	 * Get angle start.
	 * 
	 * @return the angleStart.
	 */
	public float getAngleStart() {
		return mAngleStart;
	}

	/**
	 * Set angle start.
	 * 
	 * @param pAngleStart
	 *          start angle of item
	 */
	public void setAngleStart(final float pAngleStart) {
		checkArgument(pAngleStart >= 0f && pAngleStart <= PConstants.TWO_PI,
				"pStartAngle has to be between 0 and 2 * PI!");
		mAngleStart = pAngleStart;
	}

	/**
	 * Get angle end.
	 * 
	 * @return the angleEnd.
	 */
	public float getAngleEnd() {
		return mAngleEnd;
	}

	@Override
	public int getDepth() {
		return mDepth;
	}

	/**
	 * Get subtract.
	 * 
	 * @return true if one has to be subtracted
	 */
	public boolean getSubtract() {
		return mSubtract;
	}

	/**
	 * Get index of parent node.
	 * 
	 * @return index of parent node
	 */
	public int getIndexToParent() {
		return mIndexToParent;
	}

	/**
	 * Set modification count.
	 * 
	 * @param pModificationCount
	 *          new modification count
	 */
	public void setModificationCount(final int pModificationCount) {
		checkArgument(pModificationCount >= 1,
				"paramModificationCount must be >= 1!");
		mModifications = pModificationCount;
	}

	@Override
	public int getModificationCount() {
		return mModifications;
	}

	/**
	 * Set value.
	 * 
	 * @param pValue
	 *          new value
	 */
	public void setValue(final int pValue) {
		checkArgument(pValue >= 0, "pValue must be >= 0!");
		mValue = pValue;
	}

	/**
	 * Get value.
	 * 
	 * @return value
	 */
	public float getValue() {
		return mValue;
	}

	/**
	 * Get the kind of item.
	 * 
	 * @return kind of item
	 */
	public Kind getKind() {
		return mKind;
	}

	/**
	 * Set if node has to be colored or not.
	 * 
	 * @param pColorNode
	 *          enum which determines if node has to be colored or not
	 */
	@Override
	public void setColorNode(final EColorNode pColorNode) {
		mColorNode = checkNotNull(pColorNode);
	}

	public EColorNode getColorNode() {
		return mColorNode;
	}

	/**
	 * Set angle end.
	 * 
	 * @param pAngleEnd
	 *          new angle end
	 */
	public void setAngleEnd(final float pAngleEnd) {
		checkArgument(pAngleEnd > 0f && pAngleEnd <= PConstants.TWO_PI,
				"pAngleEnd must between > 0 and 2 * PI!");
		mAngleEnd = pAngleEnd;
	}

	@Override
	public long getKey() {
		return mNodeKey;
	}

	/**
	 * Get grey state.
	 * 
	 * @return grey state
	 */
	public EGreyState getGreyState() {
		return mGreyState;
	}

	@Override
	public DiffType getDiff() {
		return mDiff;
	}

	/**
	 * Get kind of structure node.
	 * 
	 * @return kind of structure of current item
	 */
	public EStructType getStructKind() {
		return checkNotNull(mStructKind);
	}

	/**
	 * Get {@link QName}.
	 * 
	 * @return {@code QName} or {@code null}
	 */
	public QNm getQName() {
		return mQName;
	}

	/**
	 * Get depth weight.
	 * 
	 * @return {@code depth weigth}
	 */
	public float getDepthWeight() {
		return mDepthWeight;
	}

	/**
	 * Get text.
	 * 
	 * @return {@code text value} or {@code null}
	 */
	public String getText() {
		return mText;
	}

	public void setIndexMovedTo(final int pIndex) {
		mIndexMovedTo = pIndex;
	}

	public int getIndexMovedTo() {
		return mIndexMovedTo;
	}

	public float getX() {
		return mX;
	}

	public float getY() {
		return mY;
	}

	public float getC1X() {
		return mC1X;
	}

	public float getC1Y() {
		return mC1Y;
	}

	public float getC2X() {
		return mC2X;
	}

	public float getC2Y() {
		return mC2Y;
	}

	public boolean hasAttributes() {
		return mAttributes.size() != 0;
	}

	public List<Attribute> getAttributes() {
		return mAttributes;
	}

	public boolean hasNamespaces() {
		return mNamespaces.size() != 0;
	}

	public List<Namespace> getNamespaces() {
		return mNamespaces;
	}

	public void setDepth(final int pDepth) {
		checkArgument(pDepth >= 0 && pDepth <= Integer.MAX_VALUE,
				"pDepth must be between 0 and Integer.MAX_VALUE!");
		mDepth = pDepth;
	}

	public void setAngleCenter(final float pAngleCenter) {
		checkArgument(pAngleCenter >= 0 && pAngleCenter <= PConstants.TWO_PI,
				"pAngleCenter must be between 0 and 2 * PI!");
		mAngleCenter = pAngleCenter;
	}

	public int getColor() {
		return mCol;
	}

	@Override
	public float getMinimum() {
		return mMinValue;
	}

	@Override
	public float getMaximum() {
		return mMaxValue;
	}

	@Override
	public void setMinimum(final float pMinimum) {
		checkArgument(pMinimum >= 0, "pMinimum must be >= 0!");
		mMinValue = pMinimum;
	}

	@Override
	public void setMaximum(final float pMaximum) {
		checkArgument(pMaximum >= 0, "pMinimum must be >= 0!");
		mMaxValue = pMaximum;
	}

	/**
	 * Set index to parent.
	 * 
	 * @param pIndexToParent
	 *          index to parent item
	 */
	public void setIndexToParent(final int pIndexToParent) {
		checkArgument(pIndexToParent >= -1, "pIndexToParent must be >= -1!");
		mIndexToParent = pIndexToParent;
	}

	/**
	 * Get the GUI associated with the item.
	 * 
	 * @return GUI instance associated with the item
	 */
	public AbstractSunburstGUI getGUI() {
		return mGUI;
	}

	/**
	 * Get line weight.
	 * 
	 * @return {@code line weight}
	 */
	public float getLineWeight() {
		return mLineWeight;
	}

	/**
	 * Get line color.
	 * 
	 * @return {@code line color}
	 */
	public int getLineColor() {
		return mLineCol;
	}

	/**
	 * Get old text value.
	 * 
	 * @return old text
	 */
	public String getOldText() {
		return mOldText;
	}

	/**
	 * Get old {@link QNm}.
	 * 
	 * @return old {@link QNm} instance
	 */
	public QNm getOldQName() {
		return mOldQName;
	}

	@Override
	public int getOriginalDepth() {
		return mOrigDepth;
	}

	/**
	 * Get temporary depth.
	 * 
	 * @return temporary depth
	 */
	public float getTmpDepth() {
		return mTmpDepth;
	}

	/**
	 * Set the original depth (before the semantic zoom dislocates the item).
	 * 
	 * @param pDepth
	 *          original depth
	 */
	public void setOriginalDepth(@NonNegative final int pDepth) {
		checkArgument(pDepth >= 0, "pDepth must be >= 0!");
		mOrigDepth = pDepth;
		mTmpDepth = mOrigDepth;
	}

	/**
	 * Set the temporal depth.
	 * 
	 * @param pDepth
	 *          new depth
	 */
	public void setTempDepth(final @NonNegative int pDepth) {
		checkArgument(pDepth >= 0, "pDepth must be >= 0!");
		mTmpDepth = pDepth;
	}
}
