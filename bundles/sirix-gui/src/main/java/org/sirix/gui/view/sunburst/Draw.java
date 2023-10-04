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

import javax.xml.stream.events.Attribute;

import io.brackit.query.atomic.QNm;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.gui.GUI;
import org.sirix.gui.view.EHover;
import org.sirix.gui.view.ViewUtilities;
import org.sirix.gui.view.sunburst.SunburstItem.EStructType;

import processing.core.PApplet;
import processing.core.PConstants;
import processing.core.PGraphics;

/**
 * Encapsulates drawing strategies.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum Draw {

	/** Draw directly. */
	DRAW {
		@Override
		public void drawStrategy(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem, final EDrawSunburst pDraw) {
			pDraw.drawStrategy(pGUI, pItem, this);
		}

		@Override
		public void drawRings(final AbstractSunburstGUI pGUI) {
			if (pGUI.mParent.recorder != null) {
				drawStaticRings(pGUI, pGUI.mParent.recorder);
			}
			drawStaticRings(pGUI, pGUI.mParent.g);
		}

		@Override
		public void update(final AbstractSunburstGUI pGUI, final SunburstItem pItem) {
			pItem.update(this, pGUI.getMappingMode(), pGUI.mParent.g);
		}

		@Override
		public void drawOldRevision(final AbstractSunburstGUI pGUI) {
			drawStaticOldRevision(pGUI, pGUI.mParent.g);
		}

		@Override
		public void drawNewRevision(final AbstractSunburstGUI pGUI) {
			drawStaticNewRevision(pGUI, pGUI.mParent.g);
		}

		@Override
		public void drawModificationRel(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem) {
			if (pGUI.mParent.recorder != null) {
				drawStaticModifcationRel(pGUI, pItem, pGUI.mParent.recorder);
			}
			drawStaticModifcationRel(pGUI, pItem, pGUI.mParent.g);
		}

		@Override
		public void drawLabel(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem) {
			if (pGUI.isShowArcs()
					&& (!pGUI.isShowLines() || pItem.getLineWeight() <= 0.5f)) {
				if (pGUI.mParent.recorder != null) {
					drawStaticLabel(pGUI, pGUI.mParent.recorder, pItem, EHovered.NO,
							Draw.DRAW);
				}
				drawStaticLabel(pGUI, pGUI.mParent.g, pItem, EHovered.NO, Draw.DRAW);
			}
		}

		@Override
		public void drawHover(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem) {
			// if (!pGUI.mInit && pItem.getGreyState() == EGreyState.NO) {
			if (pItem.getGreyState() == EGreyState.NO) {
				if (pGUI.mParent.recorder != null) {
					pItem.hover(pGUI.mParent.recorder);
					drawStaticLabel(pGUI, pGUI.mParent.recorder, pItem, EHovered.YES,
							Draw.DRAW);
				}
				pItem.hover(pGUI.mParent.g);
				drawStaticLabel(pGUI, pGUI.mParent.g, pItem, EHovered.YES, Draw.DRAW);
			}
		}

		@Override
		public void drawMovedRel(final SunburstItem pFrom) {
			pFrom.drawMovedConnection(EHover.FALSE);
		}
	},

	/** Draw into buffer. */
	UPDATEBUFFER {
		@Override
		public void drawStrategy(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem, final EDrawSunburst pDraw) {
			pDraw.drawStrategy(pGUI, pItem, this);
		}

		@Override
		public void drawRings(final AbstractSunburstGUI pGUI) {
			if (pGUI.mParent.recorder != null) {
				drawStaticRings(pGUI, pGUI.mParent.recorder);
			}
			drawStaticRings(pGUI, pGUI.getBuffer());
		}

		@Override
		public void update(final AbstractSunburstGUI pGUI, final SunburstItem pItem) {
			pItem.update(this, pGUI.getMappingMode(), pGUI.getBuffer());
		}

		@Override
		public void drawOldRevision(final AbstractSunburstGUI pGUI) {
			drawStaticOldRevision(pGUI, pGUI.getBuffer());
		}

		@Override
		public void drawNewRevision(final AbstractSunburstGUI pGUI) {
			drawStaticNewRevision(pGUI, pGUI.getBuffer());
		}

		@Override
		public void drawModificationRel(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem) {
			if (pGUI.mParent.recorder != null) {
				drawStaticModifcationRel(pGUI, pItem, pGUI.mParent.recorder);
			}
			drawStaticModifcationRel(pGUI, pItem, pGUI.getBuffer());
		}

		@Override
		public void drawLabel(AbstractSunburstGUI pGUI, SunburstItem pItem) {
			if (pGUI.isShowArcs()
					&& (!pGUI.isShowLines() || pItem.getLineWeight() <= 0.5f)) {
				if (pGUI.mParent.recorder != null) {
					drawStaticLabel(pGUI, pGUI.mParent.recorder, pItem, EHovered.NO,
							Draw.UPDATEBUFFER);
				}
				drawStaticLabel(pGUI, pGUI.getBuffer(), pItem, EHovered.NO,
						Draw.UPDATEBUFFER);
			}
		}

		@Override
		public void drawHover(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem) {
			if (pGUI.mParent.recorder != null) {
				pItem.hover(pGUI.mParent.recorder);
				drawStaticLabel(pGUI, pGUI.mParent.recorder, pItem, EHovered.YES,
						Draw.UPDATEBUFFER);
			}
			pItem.hover(pGUI.getBuffer());
			drawStaticLabel(pGUI, pGUI.getBuffer(), pItem, EHovered.YES,
					Draw.UPDATEBUFFER);
		}

		@Override
		public void drawMovedRel(final SunburstItem pFrom) {
			pFrom.drawMovedConnection(EHover.FALSE);
		}
	};

	/** Determine if current item is hovered. */
	enum EHovered {
		/** Yes, it's hovered. */
		YES {
			@Override
			void setFillStroke(final SunburstItem pItem, final PGraphics pGraphic) {
				if (pItem.getDepth() == 0) {
					pGraphic.fill(0);
					pGraphic.stroke(0f);
				} else {
					pGraphic.fill(400f);
					pGraphic.stroke(255f);
				}
			}
		},

		/** No, it's not hovered. */
		NO {
			@Override
			void setFillStroke(final SunburstItem pItem, final PGraphics pGraphic) {
				if (pItem.getColor() > -12000000) {
					pGraphic.fill(0);
					pGraphic.stroke(0f);
				} else {
					pGraphic.fill(400f);
					pGraphic.stroke(255f);
				}
			}
		};

		/**
		 * Set {@link PGraphics#fill(float)} and {@link PGraphics#stroke(float)}
		 * values according to the GUI background color.
		 * 
		 * @param pGUI
		 *          the GUI
		 * @param pGraphic
		 *          {@link PGraphics} reference
		 * @param pHovered
		 *          {@link EHovered} value
		 */
		abstract void setFillStroke(final SunburstItem pItem,
				final PGraphics pGraphic);
	}

	/**
	 * Draw a label.
	 * 
	 * @param pGUI
	 *          the GUI ({@link AbstractSunburstGUI})
	 * @param pGraphic
	 *          {@link PGraphics} reference
	 * @param pItem
	 *          {@link SunburstItem} reference
	 * @param pHovered
	 *          {@link EHovered} value
	 */
	private static void drawStaticLabel(final AbstractSunburstGUI pGUI,
			final PGraphics pGraphic, final SunburstItem pItem,
			final EHovered pHovered, final Draw pDraw) {
		assert pGUI != null;
		assert pGraphic != null;
		final float depth = pDraw == Draw.UPDATEBUFFER || pHovered == EHovered.YES ? pItem
				.getDepth() : pItem.getTmpDepth();
		final float startAngle = pItem.getAngleStart();
		final float endAngle = pItem.getAngleEnd();
		final float scale = pItem.getStructKind() == EStructType.ISINNERNODE ? pGUI
				.getInnerNodeArcScale() : pGUI.getLeafArcScale();
		if (scale >= 0.7) {
			float size;
			if (pGUI.mDepthMax > 8) {
				size = depth == 0 ? 17 : PApplet.map(depth, 0, pGUI.mDepthMax, 16, 12);
			} else {
				size = depth == 0 ? 17 : PApplet.map(depth, 0, pGUI.mDepthMax, 16, 15);
			}
			pGraphic.textSize(size);
			pGraphic.textLeading(0f);
			pGraphic.textAlign(PConstants.LEFT, PConstants.CENTER);
			String text = "";
			boolean found = false;
			final boolean useAttribute = pGUI.isUseAttribute();
			if (useAttribute) {
				for (final Attribute att : pItem.getAttributes()) {
					if ("name".equals(att.getName().getLocalPart())) {
						text = String.valueOf(att.getValue());
						found = true;
						break;
					}
				}
			}

			if (!found) {
				final DiffType diff = pItem.getDiff();
				if (diff == DiffType.REPLACEDOLD || diff == DiffType.DELETED
						|| diff == DiffType.MOVEDFROM) {
					final QNm oldQName = pItem.getOldQName();
					text = oldQName == null ? pItem.getOldText() : ViewUtilities
							.qNameToString(oldQName);
				} else {
					final QNm qName = pItem.getQName();
					text = qName == null ? pItem.getText() : ViewUtilities
							.qNameToString(qName);
				}
			}
			if (text.length() > 20) {
				text = new StringBuilder(text.substring(0, 20)).append("...")
						.toString();
			}

			// float arcRadius = pGUI.calcEqualAreaRadius(depth, pGUI.mDepthMax);
			float arcRadius = pItem.getCurrArcRadius();
			final float arc = draw(pGraphic, text, arcRadius, startAngle,
					EDisplay.NO, EReverseDirection.NO);

			// if (pHovered == EHovered.YES || pItem.getDiff() == EDiff.SAME ||
			// pItem.getDiff() ==
			// EDiff.SAMEHASH) {
			pHovered.setFillStroke(pItem, pGraphic);
			// }
			pGraphic.pushMatrix();
			// pGraphic.hint(PApplet.ENABLE_NATIVE_FONTS);

			if (depth == 0) {
				// Must be the root-Element.
				pGraphic.pushMatrix();
				pGraphic.text(text, 0 - pGraphic.textWidth(text) * 0.5f, -17f);
				pGraphic.popMatrix();
				pGraphic.noFill();
			} else if (arc < endAngle) {
				if ((startAngle + endAngle) / 2f < PConstants.PI) {
					// Bottom half.
					float radius = (pGUI.calcEqualAreaRadius(depth + 1, pGUI.mDepthMax) - arcRadius);
					radius *= scale;
					arcRadius += (0.5f * radius);

					draw(pGraphic, text, arcRadius, endAngle - ((endAngle - arc) * 0.5f),
							EDisplay.YES, EReverseDirection.YES);
				} else {
					// Top half.
					float radius = (pGUI.calcEqualAreaRadius(depth + 1, pGUI.mDepthMax) - arcRadius);
					radius *= scale;
					arcRadius += (0.5f * radius);

					draw(pGraphic, text, arcRadius, (endAngle - arc) * 0.5f + startAngle,
							EDisplay.YES, EReverseDirection.NO);
				}
			}
			pGraphic.popMatrix();
			pGraphic.textSize(15);
			pGraphic.textAlign(PConstants.LEFT, PConstants.TOP);
			pGraphic.noFill();
		}
	}

	/**
	 * Drawing revision ring which marks the border of nodes which are equal, that
	 * is the ring which denotes the border of unchanged nodes.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 */
	private static void drawStaticOldRevision(final AbstractSunburstGUI pGUI,
			final PGraphics pGraphic) {
		assert pGraphic != null;
		if (pGUI.mDepthMax <= pGUI.mOldDepthMax) {
			final int temp = pGUI.mDepthMax;
			pGUI.mDepthMax = pGUI.mOldDepthMax + 1;
			pGUI.mOldDepthMax = temp;
		}
		pGraphic.pushMatrix();
		pGraphic.textAlign(PConstants.LEFT, PConstants.CENTER);
		float arcRadius = calculateOldRadius(pGUI, pGraphic);
		if (pGUI.mParent.recorder != null) {
			drawRevision(pGUI, pGUI.mParent.recorder, arcRadius);
		}
		drawRevision(pGUI, pGraphic, arcRadius);
		final String text = new StringBuilder("matching nodes in revision ")
				.append(pGUI.mOldSelectedRev).append(" and revision ")
				.append(pGUI.mSelectedRev).toString();
		arcRadius = pGUI.calcEqualAreaRadius(pGUI.mOldDepthMax + 1, pGUI.mDepthMax);
		arcRadius += (pGUI.calcEqualAreaRadius(pGUI.mOldDepthMax + 2,
				pGUI.mDepthMax) - arcRadius) / 2;
		pGraphic.stroke(0f);
		pGraphic.fill(400f);
		final float arc = draw(pGraphic, text, arcRadius, 0, EDisplay.NO,
				EReverseDirection.NO);
		final float theta = PConstants.PI + 0.5f * PConstants.PI - 0.5f * arc;
		if (pGUI.mParent.recorder != null) {
			draw(pGUI.mParent.recorder, text, arcRadius, theta, EDisplay.YES,
					EReverseDirection.NO);
		}
		draw(pGraphic, text, arcRadius, theta, EDisplay.YES, EReverseDirection.NO);
		pGraphic.popMatrix();
	}

	/**
	 * Drawing new revision ring, that is the ring which denotes the border of
	 * changed nodes..
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 * @throws AssertionException
	 *           if {@code pGUI} or {@code pGraphic} is {@code null} and the "-ea"
	 *           VM p is set
	 */
	private static void drawStaticNewRevision(final AbstractSunburstGUI pGUI,
			final PGraphics pGraphic) {
		assert pGraphic != null;
		if (pGUI.mDepthMax > pGUI.mOldDepthMax + 1) {
			pGraphic.pushMatrix();
			pGraphic.textAlign(PConstants.LEFT, PConstants.CENTER);
			float arcRadius = calculateNewRadius(pGUI, pGraphic);
			if (pGUI.mParent.recorder != null) {
				drawRevision(pGUI, pGUI.mParent.recorder, arcRadius);
			}
			drawRevision(pGUI, pGraphic, arcRadius);
			final String text = new StringBuilder("changed nodes in revision ")
					.append(pGUI.mSelectedRev).append(" from revision ")
					.append(pGUI.mOldSelectedRev).toString();
			arcRadius = pGUI.calcEqualAreaRadius(pGUI.mDepthMax - 1, pGUI.mDepthMax);
			arcRadius += (pGUI.calcEqualAreaRadius(pGUI.mDepthMax, pGUI.mDepthMax) - arcRadius) / 2;
			pGraphic.stroke(0f);
			pGraphic.fill(400f);
			final float arc = draw(pGraphic, text, arcRadius, 0, EDisplay.NO,
					EReverseDirection.NO);
			final float theta = PConstants.PI + 0.5f * PConstants.PI - 0.5f * arc;
			if (pGUI.mParent.recorder != null) {
				draw(pGUI.mParent.recorder, text, arcRadius, theta, EDisplay.YES,
						EReverseDirection.NO);
			}
			draw(pGraphic, text, arcRadius, theta, EDisplay.YES, EReverseDirection.NO);
			pGraphic.popMatrix();
		}
	}

	/**
	 * Draw relation from modified subtree root-node to parent.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 * @param pItem
	 *          {@link SunburstItem} instance
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 */
	private static void drawStaticModifcationRel(final AbstractSunburstGUI pGUI,
			final SunburstItem pItem, final PGraphics pGraphic) {
		assert pItem != null;
		assert pGraphic != null;
		if (pGUI.mUseDiffView == ViewType.DIFF && ViewType.DIFF.getValue()
				&& pGUI.isShowArcs() && pItem.getDepth() == pGUI.mOldDepthMax + 2
				&& pItem.getIndexToParent() != -1) {
			final float factor = 25f;
			switch (pItem.mDiff) {
			case INSERTED:
				pGraphic.stroke(200,
						pGUI.getSaturation() - factor * pItem.getRevision(),
						pGUI.getDotBrightness() - factor * pItem.getRevision(), 30);
				break;
			case DELETED:
				pGraphic.stroke(360,
						pGUI.getSaturation() - factor * pItem.getRevision(),
						pGUI.getDotBrightness() - factor * pItem.getRevision(), 30);
				break;
			case UPDATED:
				pGraphic.stroke(120,
						pGUI.getSaturation() - factor * pItem.getRevision(),
						pGUI.getDotBrightness() - factor * pItem.getRevision(), 30);
				break;
			case MOVEDFROM:
			case MOVEDTO:
				pGraphic.stroke(60,
						pGUI.getSaturation() - factor * pItem.getRevision(),
						pGUI.getDotBrightness() - factor * pItem.getRevision(), 30);
				break;
			case REPLACEDOLD:
			case REPLACEDNEW:
				pGraphic.stroke(290,
						pGUI.getSaturation() - factor * pItem.getRevision(),
						pGUI.getDotBrightness() - factor * pItem.getRevision(), 30);
				break;
			}
			if (pItem.getGreyState() == EGreyState.YES) {
				pGraphic.stroke(0);
			}
			// It's save to cast.
			final SunburstItem parent = (SunburstItem) pGUI.mControl.getModel()
					.getItem(pItem.getIndexToParent());
			for (int i = parent.getDepth() + 1; i <= pGUI.mOldDepthMax; i++) {
				float radius = pGUI.calcEqualAreaRadius(i, pGUI.mDepthMax);
				final float depthWeight = pGUI.calcEqualAreaRadius(i + 1,
						pGUI.mDepthMax) - radius;
				pGraphic.strokeWeight(depthWeight);
				radius += (depthWeight / 2);
				pGraphic.arc(0, 0, radius, radius, pItem.getAngleStart(),
						pItem.getAngleEnd());
			}
			if (!pGUI.isShowLines()) {
				pGraphic.stroke(pItem.getLineColor());
				pGraphic.strokeWeight(pItem.getLineWeight());
				pGraphic.bezier(pItem.getX(), pItem.getY(), pItem.getC1X(),
						pItem.getC1Y(), pItem.getC2X(), pItem.getC2Y(), parent.getX(),
						parent.getY());
			}
		}
	}

	/**
	 * Drawing hierarchy rings.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 */
	private static void drawStaticRings(final AbstractSunburstGUI pGUI,
			final PGraphics pGraphic) {
		assert pGUI != null;
		assert pGraphic != null;
		int depthMax = pGUI.mDepthMax;
		if (pGUI.mUseDiffView == ViewType.NODIFF) {
			depthMax += 1;
		}
		for (int depth = 0; depth < depthMax; depth++) {
			final float radius = pGUI.calcEqualAreaRadius(depth, pGUI.mDepthMax);
			pGraphic.stroke(300f);
			pGraphic.arc(0, 0, radius, radius, 0, 2 * PConstants.PI);
		}
	}

	/**
	 * Draw text.
	 * 
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 * @param pItem
	 *          {@link SunburstItem} instance
	 * @param pRadius
	 *          arc radius
	 * @param pTheta
	 *          angle in radians where text starts
	 * @param pDisplay
	 *          determines if text should be displayed or not
	 * @param pReverseDirection
	 *          determines if text should be in the reverse direction or not
	 */
	private static float draw(final PGraphics pGraphic, final String pText,
			final float pArcRadius, final float pTheta, final EDisplay pDisplay,
			final EReverseDirection pReverseDrawDirection) {
		assert pGraphic != null;
		assert pText != null;
		assert pArcRadius >= 0;
		// assert pTheta >= 0f && pTheta <= PConstants.TWO_PI;
		assert pDisplay != null;
		assert pReverseDrawDirection != null;

		final String text = pText;

		// We must keep track of our position along the curve.
		float arclength = 0;
		// For every box.
		for (int i = 0; i < text.length(); i++) {
			// Instead of a constant width, we check the width of each character.
			final char currentChar = text.charAt(i);
			final float w = pGraphic.textWidth(currentChar) + 1; // Work around.

			// Each box is centered so we move half the width.
			arclength += currentChar != 'i' ? w * 0.5f : w;
			// Angle in radians is the arclength divided by the radius.
			// Starting on the left side of the circle by adding PI.
			final float theta = pReverseDrawDirection == EReverseDirection.YES ? pTheta
					- arclength / pArcRadius
					: pTheta + arclength / pArcRadius;

			pGraphic.pushMatrix();
			// Polar to cartesian coordinate conversion.
			pGraphic.translate(pArcRadius * PApplet.cos(theta),
					pArcRadius * PApplet.sin(theta));
			// Rotate the box.
			if (pReverseDrawDirection == EReverseDirection.YES) {
				pGraphic.rotate(theta - PConstants.PI / 2f); // rotation is offset by 90
																											// degrees
			} else {
				pGraphic.rotate(theta + PConstants.PI / 2f); // rotation is offset by 90
																											// degrees
			}
			if (pDisplay == EDisplay.YES) {
				pGraphic.text(currentChar, 0, 0);
			}
			pGraphic.popMatrix();
			// Move halfway again.
			arclength += w / 2f;
		}
		pGraphic.noFill();
		return pTheta + arclength / pArcRadius;
	}

	/**
	 * Calculate radius.
	 * 
	 * @param pGUI
	 *          {@link GUI} instance
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 * @return calculated radius
	 */
	private static float calculateOldRadius(final AbstractSunburstGUI pGUI,
			final PGraphics pGraphic) {
		assert pGUI != null;
		assert pGraphic != null;
		final int revisionDepth = pGUI.mOldDepthMax + 1;
		final float radius = pGUI
				.calcEqualAreaRadius(revisionDepth, pGUI.mDepthMax);
		final float depthWeight = pGUI.calcEqualAreaRadius(revisionDepth + 1,
				pGUI.mDepthMax) - radius;
		if (pGUI.mParent.recorder != null) {
			pGUI.mParent.recorder.strokeWeight(depthWeight);
		}
		pGraphic.strokeWeight(depthWeight);
		return radius + depthWeight / 2;
	}

	/**
	 * Calculate radius.
	 * 
	 * @param pGUI
	 *          {@link GUI} instance
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 * @return calculated radius
	 */
	private static float calculateNewRadius(final AbstractSunburstGUI pGUI,
			final PGraphics pGraphic) {
		assert pGraphic != null;

		final int revisionDepth = pGUI.mDepthMax - 1;
		final float radius = pGUI
				.calcEqualAreaRadius(revisionDepth, pGUI.mDepthMax);
		final float depthWeight = pGUI.calcEqualAreaRadius(revisionDepth + 1,
				pGUI.mDepthMax) - radius;
		if (pGUI.mParent.recorder != null) {
			pGUI.mParent.recorder.strokeWeight(depthWeight);
		}
		pGraphic.strokeWeight(depthWeight);
		return radius + depthWeight / 2;
	}

	/**
	 * Draw revision.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 * @param pGraphic
	 *          {@link PGraphics} instance
	 * @param pArcRadius
	 *          arc radius
	 */
	private static void drawRevision(final AbstractSunburstGUI pGUI,
			final PGraphics pGraphic, final float pArcRadius) {
		assert pGUI != null;
		assert pArcRadius >= 0;
		pGraphic.stroke(200f);
		pGraphic.arc(0, 0, pArcRadius, pArcRadius, 0, 2 * PConstants.PI);
		pGraphic.stroke(0f);
	}

	/**
	 * Draw arc.
	 * 
	 * @param pGUI
	 *          {@link GUI} instance
	 * @param pItem
	 *          {@link SunburstItem} instance
	 */
	protected void drawArc(final AbstractSunburstGUI pGUI,
			final SunburstItem pItem) {
		assert pGUI != null;
		assert pItem != null;
		if (pGUI.isShowArcs()) {
			if (pGUI.mUseArc) {
				pItem.drawArc(pGUI.getInnerNodeArcScale(), pGUI.getLeafArcScale(),
						EHover.FALSE);
			} else {
				pItem.drawRect(pGUI.getInnerNodeArcScale(), pGUI.getLeafArcScale(),
						EHover.FALSE);
			}
		}
	}

	/**
	 * Draw relation line.
	 * 
	 * @param pGUI
	 *          {@link GUI} instance
	 * @param pItem
	 *          {@link SunburstItem} instance
	 * @throws NullPointerException
	 *           if {@code pGUI} or {@code pItem} is {@code null}
	 */
	void drawRelation(final AbstractSunburstGUI pGUI, final SunburstItem pItem) {
		checkNotNull(pGUI);
		checkNotNull(pItem);
		if (pGUI.isShowLines()) {
			if (pGUI.isUseBezierLine()) {
				pItem.drawRelationBezier();
			} else {
				pItem.drawRelationLine();
			}
		}
	}

	/**
	 * Draw dot.
	 * 
	 * @param pItem
	 *          {@link SunburstItem} instance
	 */
	public void drawDot(final SunburstItem pItem) {
		pItem.drawDot(EHover.FALSE);
	}

	/**
	 * Update a {@link SunburstItem}.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} reference
	 * @param pItem
	 *          {@link SunburstItem} to update
	 */
	public abstract void update(final AbstractSunburstGUI pGUI,
			final SunburstItem pItem);

	/**
	 * Drawing strategy.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 * @param pItem
	 *          {@link SunburstItem} to draw
	 * @param pDraw
	 */
	public abstract void drawStrategy(final AbstractSunburstGUI pGUI,
			final SunburstItem pItem, final EDrawSunburst pDraw);

	/**
	 * Drawing strategy for hovering.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 * @param pItem
	 *          {@link SunburstItem} to draw
	 * @param pDraw
	 */
	public abstract void drawHover(final AbstractSunburstGUI pGUI,
			final SunburstItem pItem);

	/**
	 * Draw old revision ring.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 */
	public abstract void drawOldRevision(final AbstractSunburstGUI pGUI);

	/**
	 * Draw new revision ring.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} instance
	 */
	public abstract void drawNewRevision(final AbstractSunburstGUI pGUI);

	/**
	 * Draw label.
	 * 
	 * @param pItem
	 *          {@link SunburstItem} instance
	 * @param pGUI
	 *          {@link AbstractSunburstGUI} reference
	 */
	public abstract void drawLabel(final AbstractSunburstGUI pGUI,
			final SunburstItem pItem);

	/**
	 * Drawing hierarchy rings.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} reference
	 */
	public abstract void drawRings(final AbstractSunburstGUI pGUI);

	/**
	 * Draw modification relation to the root change.
	 * 
	 * @param pGUI
	 *          {@link SunburstGUI} reference
	 * @param pItem
	 *          {@link SunburstItem} reference
	 */
	public abstract void drawModificationRel(final AbstractSunburstGUI pGUI,
			final SunburstItem pItem);

	/**
	 * Draw relation spline between the cut point of a moved node and the paste
	 * point.
	 * 
	 * @param pFrom
	 *          {@link SunburstItem} reference (moved from)
	 */
	public abstract void drawMovedRel(final SunburstItem pFrom);

	/** Determines how to draw. */
	public enum EDrawSunburst {
		/** Normal sunburst view. */
		NORMAL {
			/** {@inheritDoc} */
			@Override
			void drawStrategy(final AbstractSunburstGUI pGUI,
					final SunburstItem pItem, final Draw pDraw) {
				pDraw.drawArc(pGUI, pItem);
				pDraw.drawRelation(pGUI, pItem);
				pDraw.drawDot(pItem);
			}
		},

		/** Compare sunburst view. */
		COMPARE {
			/** {@inheritDoc} */
			@Override
			void drawStrategy(final AbstractSunburstGUI pGUI,
					final SunburstItem pItem, final Draw pDraw) {
				pDraw.drawArc(pGUI, pItem);
			}
		};

		/**
		 * Drawing strategy.
		 * 
		 * @param pGUI
		 *          {@link SunburstGUI} instance
		 * @param pItem
		 *          {@link SunburstItem} to draw
		 * @param pDraw
		 *          determines if it has to be drawn into an offscreen buffer or
		 *          directly to the screen
		 */
		abstract void drawStrategy(final AbstractSunburstGUI pGUI,
				final SunburstItem pItem, final Draw pDraw);
	}

	/** Determines if text should be displayed or not. */
	private enum EDisplay {
		/** Yes it should be displayed. */
		YES,

		/** No it shouldn't be displayed. */
		NO
	}

	/** Determines if text direction should be reversed or not. */
	private enum EReverseDirection {
		/** Yes it should be displayed. */
		YES,

		/** No it shouldn't be displayed. */
		NO
	}
}
