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
package org.treetank.gui.view.sunburst;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import controlP5.ControlP5;
import controlP5.ControllerStyle;
import controlP5.Group;
import controlP5.Label;
import controlP5.Range;
import controlP5.Slider;
import controlP5.Textfield;
import controlP5.Toggle;

import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import javax.annotation.Nonnull;
import javax.xml.stream.events.Attribute;

import org.gicentre.utils.move.ZoomPan;
import org.slf4j.LoggerFactory;
import org.treetank.diff.DiffFactory.EDiff;
import org.treetank.gui.ReadDB;
import org.treetank.gui.view.IProcessingGUI;
import org.treetank.gui.view.IVisualItem;
import org.treetank.gui.view.model.AbsModel;
import org.treetank.gui.view.model.interfaces.IModel;
import org.treetank.gui.view.sunburst.EDraw.EDrawSunburst;
import org.treetank.gui.view.sunburst.control.ISunburstControl;
import org.treetank.utils.LogWrapper;
import processing.core.PApplet;
import processing.core.PConstants;
import processing.core.PFont;
import processing.core.PGraphics;
import processing.core.PImage;
import processing.core.PVector;

/**
 * Abstract Sunburst Processing GUI.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsSunburstGUI implements IProcessingGUI, PropertyChangeListener {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(AbsSunburstGUI.class));

  /** Draw labels if max depth is lower than this constant. */
  public static final int LABELS_DEPTH = 20;

  /** Listening thread pool. */
  private static final ListeningExecutorService POOL = MoreExecutors.listeningDecorator(Executors
    .newFixedThreadPool(Runtime.getRuntime().availableProcessors()));

  /** Current old depth for maximum old depth computation. */
  private volatile int mOldDepth;

  /** Model which implements {@link IModel} for the SunburstView. */
  protected IModel<SunburstContainer, SunburstItem> mModel;

  /** Determines if a line is drawn or not. */
  public enum EDrawLine {
    YES {
      @Override
      void drawLine(final PGraphics pGraphic, final float pX1, final float pY1, final float pX2,
        final Float pY2) {
        pGraphic.line(pX1, pY1, pX2, pY2);
      }
    },
    NO {
      @Override
      void drawLine(final PGraphics pGraphic, final float pX1, final float pY1, final float pX2,
        final Float pY2) {
        // Don't draw.
      }
    };

    /**
     * Draw a line.
     * 
     * @param pGraphic
     *          {@link PGraphics} reference to draw the line
     * @param pX1
     *          x coordinate of first vertex
     * @param pY1
     *          y coordinate of first vertex
     * @param pX2
     *          x coordinate of second vertex
     * @param pY2
     *          y coordinate of second vertex
     */
    abstract void drawLine(final PGraphics pGraphic, final float pX1, final float pY1, final float pX2,
      final Float pY2);
  }

  /** {@link Deque} of {@link PImage} instances for undo operation. */
  private final Deque<PImage> mImages;

  /**
   * Determines if currently mouse is pointing to an item.
   */
  protected boolean mIsHovered;

  /** Saturation of node points. */
  private float mSaturation = 100;

  /** Pixels from the left border of the processing view. */
  public static final int LEFT = 0;

  /** Pixels from the top border of the processing view. */
  public static final int TOP = 5;

  /** Y-position from the top. */
  protected transient int mPosY;

  /** {@link List} of {@link Slider}s. */
  protected final List<Slider> mSliders;

  /** {@link List} of {@link Range}s. */
  protected final List<Range> mRanges;

  /** {@link List} of {@link Toggle}s. */
  protected final List<Toggle> mToggles;

  /** Color mapping mode. */
  private transient int mMappingMode = 3;

  /** Hue start value. */
  private transient float mHueStart = 323;

  /** Hue end value. */
  private transient float mHueEnd = 273;

  /** Saturation start value. */
  private transient float mSaturationStart = 100;

  /** Saturation end value. */
  private transient float mSaturationEnd = 73;

  /** Brightness start value. */
  private transient float mBrightnessStart = 77;

  /** Brightness end value. */
  private transient float mBrightnessEnd = 33;

  /** Inner node brightness start value. */
  private transient float mInnerNodeBrightnessStart = 90;

  /** Inner node brightness end value. */
  private transient float mInnerNodeBrightnessEnd = 20;

  /** Inner node stroke brightness start value. */
  private transient float mInnerNodeStrokeBrightnessStart = 90;

  /** Inner node stroke brightness end value. */
  private transient float mInnerNodeStrokeBrightnessEnd = 20;

  /** Inner node arc scale. */
  private transient float mInnerNodeArcScale = 0.9f;

  /** Modification weight. */
  private transient float mModificationWeight = 0.8f;

  /** Stroke weight start. */
  private transient float mStrokeWeightStart = 0.0f;

  /** Stroke weight end. */
  private transient float mStrokeWeightEnd = 0.1f;

  /** Dot size. */
  transient float mDotSize = 3f;

  /** Dot brightness. */
  private transient float mDotBrightness = 80f;

  /** Show arcs. */
  private transient boolean mShowArcs = true;

  /** Maximum depth in the tree. */
  protected transient int mDepthMax;

  /** Determines if arcs should be used (Default: true). */
  public transient boolean mUseArc = true;

  /** Determines if a special attribute should be used for labels (Default: false). */
  public transient boolean mUseAttribute;

  /** {@link ZoomPan} instance to zoom in or out. */
  private transient ZoomPan mZoomer;

  /** Background brightness. */
  private transient float mBackgroundBrightness = 100f;

  /** Leaf node arc scale. */
  private transient float mLeafArcScale = 1.0f;

  /** {@link PGraphics} offscreen buffer. */
  protected transient PGraphics mBuffer;

  /** {@link PApplet} instance. */
  protected final PApplet mParent;

  /** Semaphore to lock draw() during updates. */
  protected final Semaphore mLock;

  /** Image to write into. */
  protected transient PImage mImg;

  /** Used to define the rotation. */
  protected transient int mRad;

  /**
   * Determines if bezier lines for connections between parent/child should be
   * used (Default: true).
   */
  private transient boolean mUseBezierLine = true;

  /** Determines if line relations between parent/child should be drawn. */
  private transient boolean mShowLines = true;

  /** {@link ControlP5} instance. */
  private final ControlP5 mControlP5;

  /** {@link ReadDB} instance. */
  protected transient ReadDB mDb;

  /** {@link ISunburstControl} implementation. */
  public final ISunburstControl mControl;

  /** Selected revision to compare. */
  protected transient long mSelectedRev;

  /** Selected revision to compare. */
  protected transient long mOldSelectedRev;

  /** Old maximum depth. */
  protected transient int mOldDepthMax;

  /** Determines if diff view should be used or not. */
  protected transient EView mUseDiffView = EView.NODIFF;

  /** Determines if current state should be saved as a PDF-file. */
  private transient boolean mSavePDF;

  /** Determines if SunburstGUI interface should be shown. */
  private transient boolean mShowGUI;

  /** Determines if model has done the work. */
  protected volatile boolean mDone;

  /** {@link GlassPaneListener} reference. */
  public final GlassPaneListener mListener;

  /** X position of the mouse cursor. */
  protected transient float mX;

  /** Y position of the mouse cursor. */
  protected transient float mY;

  /** {@link ControlP5} text field. */
  transient Textfield mXPathField;

  /** Item, which is currently clicked. */
  protected transient SunburstItem mHitItem;

  /** Hit test index. */
  protected volatile int mHitTestIndex = -1;

  /** Current angle of the mouse cursor to y axis. */
  protected transient float mAngle;

  /** Current depth of the mouse cursor. */
  protected transient int mDepth;

  /** Font used. */
  private final PFont mFont;

  /** Determines if pruning should be enabled or not. */
  transient boolean mUsePruning = true;

  /** Determines if move detection should be used. */
  transient boolean mUseMoveDetection;
  
  /** Determines initialization to draw animation. */
  transient volatile boolean mInit;
  
  /** Determines a threshold for the item count until which an animation is drawn. */
  static final int ANIMATION_THRESHOLD = 3_000;


  /**
   * Constructor.
   * 
   * @param paramApplet
   *          {@link PApplet} instance
   * @param paramControl
   *          {@link ISunburstControl} implementation
   * @param paramDb
   *          {@link ReadDB} instance
   */
  @SuppressWarnings("unchecked")
  protected AbsSunburstGUI(@Nonnull final PApplet paramApplet, @Nonnull final ISunburstControl paramControl,
    @Nonnull final ReadDB paramDb) {
    mParent = checkNotNull(paramApplet);
    mControl = checkNotNull(paramControl);
    mDb = checkNotNull(paramDb);
    mListener = new GlassPaneListener();
    mZoomer = new ZoomPan(mParent);
    mZoomer.setMouseMask(PConstants.CONTROL);
    mControlP5 = new ControlP5(mParent);
    mControlP5.disableShortcuts();
    mSliders = new LinkedList<Slider>();
    mRanges = new LinkedList<Range>();
    mToggles = new LinkedList<Toggle>();
    mLock = new Semaphore(1);
    mImages = new LinkedBlockingDeque<>();
    // Generic cast always working.
    mModel = (IModel<SunburstContainer, SunburstItem>)mControl.getModel();
    mFont =
      mParent.createFont("src" + File.separator + "main" + File.separator + "resources" + File.separator
        + "data" + File.separator + "miso-regular.ttf", 15);
    setupGUI();
  }

  /**
   * Reset the GUI instance to null.
   */
  public abstract void resetGUI();

  /** Initial setup of the GUI. */
  private void setupGUI() {
    mParent.textFont(mFont);
    mParent.smooth();
    mParent.background(255f);

    final int activeColor = mParent.color(0, 130, 164);
    mControlP5.setColorActive(activeColor);
    mControlP5.setColorBackground(mParent.color(170));
    mControlP5.setColorForeground(mParent.color(50));
    mControlP5.setColorLabel(mParent.color(50));
    mControlP5.setColorValue(mParent.color(255));

    final int len = 300;

    final Range hueRange =
      mControlP5.addRange("leaf node hue range", 0, 360, getHueStart(), getHueEnd(), LEFT, TOP + mPosY + 0,
        len, 15);
    mRanges.add(hueRange);
    final Range saturationRange =
      mControlP5.addRange("leaf node saturation range", 0, 100, getSaturationStart(), getSaturationEnd(),
        LEFT, TOP + mPosY + 20, len, 15);
    mRanges.add(saturationRange);
    final Range brightnessRange =
      mControlP5.addRange("leaf node brightness range", 0, 100, getBrightnessStart(), getBrightnessEnd(),
        LEFT, TOP + mPosY + 40, len, 15);
    mRanges.add(brightnessRange);

    mPosY += 70;

    final Range innerNodebrightnessRange =
      mControlP5.addRange("inner node brightness range", 0, 100, getInnerNodeBrightnessStart(),
        getInnerNodeBrightnessEnd(), LEFT, TOP + mPosY + 0, len, 15);
    mRanges.add(innerNodebrightnessRange);
    final Range innerNodeStrokeBrightnessRange =
      mControlP5.addRange("inner node stroke brightness range", 0, 100, getInnerNodeStrokeBrightnessStart(),
        getInnerNodeStrokeBrightnessEnd(), LEFT, TOP + mPosY + 20, len, 15);
    mRanges.add(innerNodeStrokeBrightnessRange);

    mPosY += 50;

    // name, minimum, maximum, default value (float), x, y, width, height
    final Slider innerNodeArcScale =
      mControlP5.addSlider("setInnerNodeArcScale", 0, 1, getInnerNodeArcScale(), LEFT, TOP + mPosY + 0, len,
        15);
    innerNodeArcScale.setCaptionLabel("innerNodeArcScale");
    mSliders.add(innerNodeArcScale);
    final Slider leafNodeArcScale =
      mControlP5.addSlider("setLeafArcScale", 0, 1, getLeafArcScale(), LEFT, TOP + mPosY + 20, len, 15);
    leafNodeArcScale.setCaptionLabel("leafNodeArcScale");
    mSliders.add(leafNodeArcScale);
    mPosY += 50;
    final Slider modWeight =
      mControlP5.addSlider("setModificationWeight", 0, 1, getModificationWeight(), LEFT, TOP + mPosY + 0,
        len, 15);
    modWeight.setCaptionLabel("modification weight");
    mSliders.add(modWeight);
    mPosY += 50;

    final Range strokeWeight =
      mControlP5.addRange("stroke weight range", 0, 10, getStrokeWeightStart(), getStrokeWeightEnd(), LEFT,
        TOP + mPosY + 0, len, 15);
    mRanges.add(strokeWeight);
    mPosY += 30;

    final Slider dotSize =
      mControlP5.addSlider("setDotSize", 0, 10, mDotSize, LEFT, TOP + mPosY + 0, len, 15);
    dotSize.setCaptionLabel("dot size");
    mSliders.add(dotSize);
    final Slider dotBrightness =
      mControlP5.addSlider("setDotBrightness", 0, 100, mDotBrightness, LEFT, TOP + mPosY + 20, len, 15);
    dotBrightness.setCaptionLabel("dot brightness");
    mSliders.add(dotBrightness);
    mPosY += 50;

    final Slider backgroundBrightness =
      mControlP5.addSlider("setBackgroundBrightness", 0, 100, getBackgroundBrightness(), LEFT, TOP + mPosY
        + 0, len, 15);
    backgroundBrightness.setCaptionLabel("background brightness");
    mSliders.add(backgroundBrightness);
    mPosY += 50;

    final Toggle showArcs = mControlP5.addToggle("isShowArcs", isShowArcs(), LEFT + 0, TOP + mPosY, 15, 15);
    showArcs.setCaptionLabel("show arcs");
    mToggles.add(showArcs);
    final Toggle showLines =
      mControlP5.addToggle("isShowLines", isShowLines(), LEFT + 0, TOP + mPosY + 20, 15, 15);
    showLines.setCaptionLabel("show lines");
    mToggles.add(showLines);
    final Toggle useBezier =
      mControlP5.addToggle("isUseBezierLine", isUseBezierLine(), LEFT + 0, TOP + mPosY + 40, 15, 15);
    useBezier.setCaptionLabel("Bezier / Line");
    mToggles.add(useBezier);

    setup();

    style();
  }

  /**
   * Template method. Allows additional GUI setup.
   */
  protected abstract void setup();

  /**
   * Style menu.
   */
  protected void style() {
    final Group ctrl = mControlP5.addGroup("menu", 15, 25, 35);
    ctrl.setColorLabel(mParent.color(255));
    ctrl.setColorBackground(mParent.color(100));
    ctrl.close();

    mParent.colorMode(PConstants.RGB, 255, 255, 255);
    final int backgroundColor = 0x99ffffff;
    int i = 0;
    for (final Slider slider : mSliders) {
      slider.setGroup(ctrl);
      slider.setId(i);
      final Label label = slider.getCaptionLabel();
      label.toUpperCase(true);
      label.setColor(mParent.color(0));
      label.setColorBackground(backgroundColor);
      final ControllerStyle style = label.getStyle();
      style.padding(4, 0, 1, 3);
      style.marginTop = -4;
      style.marginLeft = 0;
      style.marginRight = -14;
      slider.plugTo(mControl);
      i++;
    }

    i = 0;
    for (final Range range : mRanges) {
      range.setGroup(ctrl);
      range.setId(i);
      final Label label = range.getCaptionLabel();
      label.toUpperCase(true);
      label.setColor(mParent.color(0));
      label.setColorBackground(backgroundColor);
      final ControllerStyle style = label.getStyle();
      style.padding(4, 0, 1, 3);
      style.marginTop = -4;
      range.plugTo(mControl);
      i++;
    }

    i = 0;
    for (final Toggle toggle : mToggles) {
      toggle.setGroup(ctrl);
      toggle.setId(i);
      final Label label = toggle.getCaptionLabel();
      label.setColor(mParent.color(0));
      label.setColorBackground(backgroundColor);
      final ControllerStyle style = label.getStyle();
      style.padding(4, 3, 1, 3);
      style.marginTop = -19;
      style.marginLeft = 18;
      style.marginRight = 5;
      toggle.plugTo(mControl);
      i++;
    }

    mParent.colorMode(PConstants.HSB, 360, 100, 100);
    mParent.textLeading(14);
    mParent.textAlign(PConstants.LEFT, PConstants.TOP);
    mParent.cursor(PConstants.CROSS);
  }

  /** Update items as well as the buffered offscreen image. */
  @Override
  public void update() {
    LOGWRAPPER.debug("[update()]: Available permits: " + mLock.availablePermits());
    LOGWRAPPER.debug("parent width: " + mParent.width + " parent height: " + mParent.height);
    mZoomer.reset();
    setBuffer(mParent.createGraphics(mParent.width, mParent.height, PConstants.JAVA2D));
    mBuffer.beginDraw();
    mBuffer.textFont(mFont);
    updateBuffer();
    mBuffer.endDraw();
    updateImage();
  }

  /** Update the image used for drawing with the buffered image. */
  private void updateImage() {
    mParent.noLoop();
    mImg = mBuffer.get(0, 0, mBuffer.width, mBuffer.height);
    mParent.loop();
  }

  /**
   * Draws into an off-screen buffer.
   */
  private void updateBuffer() {
    mBuffer.pushMatrix();

    mBuffer.colorMode(PConstants.HSB, 360, 100, 100, 100);
    mBuffer.background(0, 0, getBackgroundBrightness());
    mBuffer.noFill();
    mBuffer.ellipseMode(PConstants.RADIUS);
    mBuffer.strokeCap(PConstants.SQUARE);
    mBuffer.smooth();
    mBuffer.translate((float)mBuffer.width / 2f, (float)mBuffer.height / 2f);
    mBuffer.rotate(PApplet.radians(mRad));

    // Draw items.
    drawItems(EDraw.UPDATEBUFFER);

    mBuffer.stroke(0);
    mBuffer.strokeWeight(2f);
    mBuffer.line(0, 0, mBuffer.width * 0.5f, 0);
    mBuffer.textSize(14f);
    mBuffer.fill(0f);
    mBuffer.textAlign(PConstants.LEFT, PConstants.BOTTOM);
    mBuffer.text("end", mBuffer.width * 0.5f - 60f, -20f + mBuffer.textAscent() - 2f);
    mBuffer.text("start", mBuffer.width * 0.5f - 60f, 20f);
    drawArrow(mBuffer, (int)Math.round(mBuffer.width * 0.5f - 80), 0, 30, PConstants.PI * 0.5f);

    mBuffer.popMatrix();
  }

  /**
   * Set database instance.
   * 
   * @param paramDb
   *          the {@link ReadDB} instance to set
   * @throws NullPointerException
   *           if {@code paramDb} is {@code null}
   */
  protected void updateDb(final ReadDB paramDb) {
    checkNotNull(paramDb);
    if (!mDb.equals(paramDb)) {
      mDb.close();
      mDb = paramDb;
    }
  }

  /**
   * Draw an arrow.
   * 
   * @param paramX
   *          x start position
   * @param paramY
   *          y start position
   * @param paramLen
   *          length of the arrow
   * @param paramAngle
   *          angle to rotate
   */
  protected void drawArrow(final PGraphics paramGraphic, final int paramX, final int paramY,
    final int paramLen, final float paramAngle) {
    checkNotNull(paramGraphic);
    paramGraphic.pushMatrix();
    paramGraphic.translate(paramX, paramY);
    paramGraphic.rotate(paramAngle);
    paramGraphic.line(0, 0, paramLen, 0);
    paramGraphic.line(paramLen, 0, paramLen - 10, 10);
    paramGraphic.line(paramLen, 0, paramLen - 10, -10);
    paramGraphic.popMatrix();
  }

  /**
   * Draw a single {@link SunburstItem}.
   * 
   * @param paramDraw
   *          drawing strategy
   * @param paramItem
   *          {@link SunburstItem} instance
   */
  protected void drawItem(final EDraw paramDraw, final SunburstItem paramItem) {
    checkNotNull(paramDraw);
    paramDraw.update(this, checkNotNull(paramItem));
    if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()) {
      paramDraw.drawModificationRel(this, paramItem);
      paramDraw.drawStrategy(this, paramItem, EDrawSunburst.COMPARE);
    } else {
      paramDraw.drawStrategy(this, paramItem, EDrawSunburst.NORMAL);
    }
    if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()) {
      paramDraw.drawRelation(this, paramItem);
      paramDraw.drawDot(paramItem);
      paramDraw.drawLabel(this, paramItem);
    }
  }

  /**
   * Draw {@link IVisualItem} instances on the screen.
   * 
   * @param paramDraw
   *          drawing strategy
   */
  @SuppressWarnings("unchecked")
  protected synchronized void drawItems(final EDraw paramDraw) {
    checkNotNull(paramDraw);
    if (!isShowArcs()) {
      paramDraw.drawRings(this);
    }

    mModel = (IModel<SunburstContainer, SunburstItem>)mControl.getModel();
    final Iterable<SunburstItem> items = mModel;
    for (final SunburstItem item : items) {
      paramDraw.update(this, item);

      if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()) {
        paramDraw.drawStrategy(this, item, EDrawSunburst.COMPARE);
      } else {
        paramDraw.drawStrategy(this, item, EDrawSunburst.NORMAL);
        if (item.getDepth() < LABELS_DEPTH) {
          paramDraw.drawLabel(this, item);
        }
      }
    }

    if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()) {
      paramDraw.drawNewRevision(this);
      paramDraw.drawOldRevision(this);

      for (final SunburstItem item : items) {
        if (mShowLines) {
          paramDraw.drawRelation(this, item);
        }
        paramDraw.drawModificationRel(this, item);
      }

      for (final SunburstItem item : items) {
        paramDraw.drawDot(item);
        if (item.getDepth() < LABELS_DEPTH) {
          paramDraw.drawLabel(this, item);
        }
      }

      if (mUseMoveDetection) {
        for (final SunburstItem item : items) {
          if (item.getDiff() == EDiff.MOVEDFROM && item.getIndexMovedTo() >= 0
            && item.getIndexMovedTo() < mModel.getItemsSize()) {
            paramDraw.drawMovedRel(item);
          }
        }
      }
    }
  }

  /**
   * Mouse over to display text.
   */
  protected void textMouseOver() {
    if (mHitTestIndex != -1) {
      String text = mHitItem.toString();

      int lines = 1;
      for (final char c : text.toCharArray()) {
        if (c == '\n') {
          lines++;
        }
      }
      if (lines > 1) {
        lines++;
      }
      // final StringBuilder builder = new StringBuilder().append("[Depth: ").append(mDepth);
      // if (isUseAttribute()) {
      // processAttributes(builder);
      // } else {
      // if (mHitItem.getQName() != null) {
      // final String name = ViewUtilities.qNameToString(mHitItem.getQName());
      // builder.append(" QName: ");
      // appendString(builder, name);
      // } else {
      // final String value = mHitItem.getText();
      // builder.append(" Text: ");
      // appendString(builder, value);
      // }
      // }
      // if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()) {
      // mHitItem.updated(builder);
      // builder.append(" ModificationCount: ").append(mHitItem.getModificationCount());
      // }
      // if (mHitItem.getQName() != null) {
      // builder.append(" DescendantCount: ");
      // builder.append((int)mHitItem.getValue());
      // } else {
      // assert mHitItem.getText() != null;
      // }
      //
      // if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()) {
      // builder.append(" Similarity: ");
      // final float value = mHitItem.getValue();
      //
      // if (mHitItem.getText() == null) {
      // builder.append((value - (float)mHitItem.getModificationCount()) / value);
      // } else {
      // builder.append(value);
      // }
      // }
      //
      // if (mHitItem.getStructKind() == EStructType.ISLEAFNODE && mHitItem.getText() != null) {
      // builder.append(" length: ").append(mHitItem.getText().length());
      // }
      // builder.append(" NodeKey: ").append(mHitItem.getItem().getKey()).append("]");
      // text = builder.toString();

      final int offset = 5;
      final float textWidth = mParent.textWidth(text) + 10f;
      final float textHeight = (mParent.textAscent() + mParent.textDescent()) * lines + 4;
      mParent.fill(0, 0, 0);
      if (mY + offset + textHeight > mParent.height / 2f) {
        mY = mY - textHeight - offset - 4;
      }
      if (mX + offset + textWidth > mParent.width / 2f) {
        // Exceeds right window border, thus align to the left of the current mouse location.
        mParent.rect(mX - textWidth + offset, mY + offset, textWidth, textHeight);
        mParent.fill(0, 0, 100);
        mParent.text(text, mX - textWidth + offset + 2, mY + offset + 2, textWidth, textHeight);
      } else {
        // Align to the right of the current mouse location.
        mParent.rect(mX + offset, mY + offset, textWidth, textHeight);
        mParent.fill(0, 0, 100);
        mParent.text(text, mX + offset + 2, mY + offset + 2, textWidth, textHeight);
      }
      mParent.noFill();
    }
  }

  /**
   * Process attributes for string representation.
   * 
   * @param pBuilder
   *          the builder instance to use
   */
  public void processAttributes(final StringBuilder pBuilder) {
    checkNotNull(pBuilder);
    for (final Attribute att : mHitItem.getAttributes()) {
      if ("name".equals(att.getName().getLocalPart())) {
        pBuilder.append(" Attribute: ");
        appendString(pBuilder, att.getValue());
        break;
      }
    }
  }

  /**
   * Append text to a {@link StringBuilder} instance.
   * 
   * @param pBuilder
   *          {@link StringBuilder} instance
   * @param pText
   *          text to append
   */
  private void appendString(final StringBuilder pBuilder, final String pText) {
    if (pText.length() > 20) {
      pBuilder.append(pText.substring(0, 20)).append("...");
    } else {
      pBuilder.append(pText);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void propertyChange(final PropertyChangeEvent paramEvent) {
    switch (paramEvent.getPropertyName().toLowerCase()) {
    case "updated":
      assert paramEvent.getNewValue() instanceof Boolean;
      final boolean updateOldDepthMax = (boolean)paramEvent.getNewValue();
      // Generic cast always safe.
      mModel = (IModel<SunburstContainer, SunburstItem>)mControl.getModel();
      int tmpOldDepth = mOldDepthMax;
      if (updateOldDepthMax) {
        final IVisualItem item = mModel.getItem(0);
        final int depth = item.getDepth();
        mOldDepthMax = new OldDepthMax(mModel.subList(0, mModel.getItemsSize()), depth).call();
      }

      if (tmpOldDepth > mOldDepthMax) {
        final int diffDepth = tmpOldDepth - mOldDepthMax;
        for (final SunburstItem item : mModel) {
          final int oldDepth = item.getDepth();
          if (oldDepth > tmpOldDepth) {
            // if (item.getDiff() != EDiff.SAME && item.getDiff() != EDiff.SAMEHASH) {
            item.setDepth(oldDepth - diffDepth);
          }
        }
        mDepthMax -= diffDepth;
        mModel.setNewDepthMax(mDepthMax - 2);
        mModel.setOldDepthMax(mOldDepthMax);
      }
      break;
    case "maxdepth":
      assert paramEvent.getNewValue() instanceof Integer;
      mDepthMax = (Integer)paramEvent.getNewValue();
      setDepthMax();
      break;
    case "oldmaxdepth":
      assert paramEvent.getNewValue() instanceof Integer;
      mOldDepthMax = (Integer)paramEvent.getNewValue();
      mUseDiffView = EView.DIFF;
      break;
    default:
      break;
    // if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()) {
    // EDraw.UPDATEBUFFER.drawNewRevision(this);
    // EDraw.UPDATEBUFFER.drawOldRevision(this);
    // }
    // mBuffer.stroke(0);
    // mBuffer.strokeWeight(2f);
    // mBuffer.line(0, 0, mBuffer.width, 0);
    // mBuffer.textSize(14f);
    // mBuffer.fill(0f);
    // mBuffer.text("end", mBuffer.width * 0.5f - 60f, -20f + mBuffer.textAscent() - 2f);
    // mBuffer.text("start", mBuffer.width * 0.5f - 60f, 20f);
    // drawArrow((int)Math.round(mBuffer.width * 0.5f - 80), 0, 30, PConstants.PI * 0.5f);
    //
    // mBuffer.popMatrix();
    // mBuffer.endDraw();
    // mParent.noLoop();
    //
    // try {
    // mLock.acquire();
    // mImg = mBuffer.get(0, 0, mBuffer.width, mBuffer.height);
    // } catch (final InterruptedException e) {
    // LOGWRAPPER.error(e.getMessage(), e);
    // } finally {
    // mLock.release();
    // mParent.loop();
    // }
    // assert paramEvent.getNewValue() instanceof Boolean;
    // mDone = true;
    // mGUI.getZoomer().reset();
    // mGUI.mZoomPanReset = true;
    // } else if (paramEvent.getPropertyName().equals("item")) {
    // assert paramEvent.getNewValue() instanceof SunburstItem;
    // final SunburstItem item = (SunburstItem)paramEvent.getNewValue();
    // if (item.getIndexToParent() == -1) {
    // getZoomer().reset();
    // setBuffer(mParent.createGraphics(mParent.width, mParent.height, PConstants.JAVA2D));
    // mBuffer.beginDraw();
    // mBuffer.pushMatrix();
    // mBuffer.colorMode(PConstants.HSB, 360, 100, 100, 100);
    // mBuffer.background(0, 0, getBackgroundBrightness());
    // mBuffer.noFill();
    // mBuffer.ellipseMode(PConstants.RADIUS);
    // mBuffer.strokeCap(PConstants.SQUARE);
    // mBuffer.smooth();
    // mBuffer.translate((float)mBuffer.width / 3f, (float)mBuffer.height / 3f);
    // mBuffer.rotate(PApplet.radians(mRad));
    // }
    // drawItem(EDraw.UPDATEBUFFER, item);
    }
  }

  /**
   * Rollover test.
   * 
   * @return true, if found, false otherwise
   */
  protected boolean rollover() {
    final boolean doRollover = rolloverInit();

    mHitTestIndex = -1;
    mHitItem = null;
    boolean retVal = false;

    if (doRollover) {
      mAngle = PApplet.atan2(mY, mX) + PApplet.radians(mRad);
      if (mAngle < 0) {
        mAngle = PApplet.map(mAngle, -PConstants.PI, 0, PConstants.PI, PConstants.TWO_PI);
      }
      LOGWRAPPER.debug("angle: " + mAngle);

      final float radius = PApplet.dist(0, 0, mX, mY);
      LOGWRAPPER.debug("radius: " + radius);

      // Calc mouse depth with mouse radius ... transformation of calcEqualAreaRadius().
      LOGWRAPPER.debug("depth max: " + mDepthMax);
      mDepth = PApplet.floor(PApplet.pow(radius, 2) * (mDepthMax + 1) / PApplet.pow(getInitialRadius(), 2));
      LOGWRAPPER.debug("depth: " + mDepth);

      int index = 0;
      final IModel<?, ?> model = mControl.getModel();
      for (final IVisualItem visualItem : model) {
        final SunburstItem item = (SunburstItem)visualItem;
        // Hittest, which arc is the closest to the mouse.
        // System.out.println("radians: " + PApplet.radians(mRad));
        float angleStart = item.getAngleStart() + PApplet.radians(mRad);
        // FIXME.
        // System.out.println("angleStart: " + item.getAngleStart());
        // if (angleStart > PConstants.TWO_PI) {
        // angleStart -= PConstants.TWO_PI;
        // System.out.println("start: " + angleStart);
        // }
        float angleEnd = item.getAngleEnd() + PApplet.radians(mRad);
        // if (angleEnd > PConstants.TWO_PI) {
        // angleEnd -= PConstants.TWO_PI;
        // System.out.println("end: " + angleEnd);
        // }
        if (item.getDepth() == mDepth && mAngle > angleStart && mAngle < angleEnd) {
          mHitTestIndex = index;
          mHitItem = item;
          retVal = true;
          LOGWRAPPER.debug("found: " + mHitTestIndex);
          break;
        }
        index++;
      }
    }

    return retVal;
  }

  /** Initialize rollover. */
  protected abstract boolean rolloverInit();

  /**
   * Get initial radius.
   * 
   * @return initial radius
   */
  protected float getInitialRadius() {
    return mParent.height / 2.2f;
  }

  /**
   * Calculate area so that radiuses have equal areas in each depth.
   * 
   * @param paramDepth
   *          the actual depth
   * @param paramDepthMax
   *          the maximum depth
   * @return calculated radius
   * @throws IllegalArgumentException
   *           if {@code paramDepth} < 0 or {@code paramDepthMax} < 0
   */
  protected float calcEqualAreaRadius(final float paramDepth, final int paramDepthMax) {
    checkArgument(paramDepth >= 0, "paramDepth must be greater or equal to 0!");
    checkArgument(paramDepthMax >= 0, "paramDepthMax must be greater or equal to 0!");
    return PApplet.sqrt(paramDepth * PApplet.pow(getInitialRadius(), 2) / (paramDepthMax + 1));
  }

  /**
   * Calculate area radius in a linear way.
   * 
   * @param paramDepth
   *          the actual depth
   * @param paramDepthMax
   *          the maximum depth
   * @return calculated radius
   * @throws IllegalArgumentException
   *           if {@code paramDepth} < 0 or {@code paramDepthMax} < 0
   */
  protected float calcAreaRadius(final int paramDepth, final int paramDepthMax) {
    checkArgument(paramDepth >= 0, "paramDepth must be greater or equal to 0!");
    checkArgument(paramDepthMax >= 0, "paramDepthMax must be greater or equal to 0!");
    return PApplet.map(paramDepth, 0, paramDepthMax + 1, 0, getInitialRadius());
  }

  /**
   * Get mapping mode.
   * 
   * @return mappingMode
   */
  public int getMappingMode() {
    return mMappingMode;
  }

  // ============================= Accessors ================================

  /** {@inheritDoc} */
  @Override
  public PApplet getApplet() {
    return mParent;
  }

  /**
   * Set point saturation.
   * 
   * @param paramSaturation
   *          the saturation to set
   */
  public void setSaturation(final float paramSaturation) {
    mSaturation = paramSaturation;
  }

  /**
   * Get point saturation.
   * 
   * @return saturation
   */
  public float getSaturation() {
    return mSaturation;
  }

  /**
   * Set hue start.
   * 
   * @param paramHueStart
   *          the hueStart to set
   */
  public void setHueStart(final float paramHueStart) {
    mHueStart = paramHueStart;
  }

  /**
   * Get hue start.
   * 
   * @return the hueStart
   */
  public float getHueStart() {
    return mHueStart;
  }

  /**
   * Get hue end.
   * 
   * @return hue end
   */
  public float getHueEnd() {
    return mHueEnd;
  }

  /**
   * Set hue end.
   * 
   * @param paramHueEnd
   *          the hueEnd
   */
  public void setHueEnd(final float paramHueEnd) {
    mHueEnd = paramHueEnd;
  }

  /**
   * Set saturation start.
   * 
   * @param paramSaturationStart
   *          the saturationStart to set
   */
  public void setSaturationStart(final float paramSaturationStart) {
    mSaturationStart = paramSaturationStart;
  }

  /**
   * Get saturation start.
   * 
   * @return the saturationStart
   */
  public float getSaturationStart() {
    return mSaturationStart;
  }

  /**
   * Set brightness start.
   * 
   * @param paramBrightnessStart
   *          the brightnessStart to set
   */
  public void setBrightnessStart(final float paramBrightnessStart) {
    mBrightnessStart = paramBrightnessStart;
  }

  /**
   * Get brightness start.
   * 
   * @return the brightnessStart
   */
  public float getBrightnessStart() {
    return mBrightnessStart;
  }

  /**
   * Set saturation end.
   * 
   * @param paramSaturationEnd
   *          the saturationEnd to set
   */
  public void setSaturationEnd(final float paramSaturationEnd) {
    mSaturationEnd = paramSaturationEnd;
  }

  /**
   * Get saturation end.
   * 
   * @return the saturationEnd
   */
  public float getSaturationEnd() {
    return mSaturationEnd;
  }

  /**
   * Set innerNodeBrightnessStart.
   * 
   * @param paramInnerNodeBrightnessStart
   *          the innerNodeBrightnessStart to set
   */
  public void setInnerNodeBrightnessStart(final float paramInnerNodeBrightnessStart) {
    mInnerNodeBrightnessStart = paramInnerNodeBrightnessStart;
  }

  /**
   * Get innerNodeBrightnessStart.
   * 
   * @return the innerNodeBrightnessStart
   */
  public float getInnerNodeBrightnessStart() {
    return mInnerNodeBrightnessStart;
  }

  /**
   * Set innerNodeStrokeBrightnessStart.
   * 
   * @param paramInnerNodeStrokeBrightnessStart
   *          the innerNodeStrokeBrightnessStart to set
   */
  public void setInnerNodeStrokeBrightnessStart(final float paramInnerNodeStrokeBrightnessStart) {
    mInnerNodeStrokeBrightnessStart = paramInnerNodeStrokeBrightnessStart;
  }

  /**
   * Get innerNodeStrokeBrightnessStart.
   * 
   * @return the innerNodeStrokeBrightnessStart
   */
  public float getInnerNodeStrokeBrightnessStart() {
    return mInnerNodeStrokeBrightnessStart;
  }

  /**
   * Set strokeWeightStart.
   * 
   * @param paramStrokeWeightStart
   *          the strokeWeightStart to set
   */
  public void setStrokeWeightStart(final float paramStrokeWeightStart) {
    mStrokeWeightStart = paramStrokeWeightStart;
  }

  /**
   * Get strokeWeightStart.
   * 
   * @return the strokeWeightStart
   */
  public float getStrokeWeightStart() {
    return mStrokeWeightStart;
  }

  /**
   * Set strokeWeightEnd.
   * 
   * @param paramStrokeWeightEnd
   *          the strokeWeightEnd to set
   */
  public void setStrokeWeightEnd(float paramStrokeWeightEnd) {
    mStrokeWeightEnd = paramStrokeWeightEnd;
  }

  /**
   * Get strokeWeightEnd.
   * 
   * @return the strokeWeightEnd
   */
  public float getStrokeWeightEnd() {
    return mStrokeWeightEnd;
  }

  /**
   * Set innerNodeStrokeBrightnessEnd.
   * 
   * @param paramInnerNodeStrokeBrightnessEnd
   *          the innerNodeStrokeBrightnessEnd to set
   */
  public void setInnerNodeStrokeBrightnessEnd(final float paramInnerNodeStrokeBrightnessEnd) {
    mInnerNodeStrokeBrightnessEnd = paramInnerNodeStrokeBrightnessEnd;
  }

  /**
   * Get innerNodeStrokeBrightnessEnd.
   * 
   * @return the innerNodeStrokeBrightnessEnd
   */
  public float getInnerNodeStrokeBrightnessEnd() {
    return mInnerNodeStrokeBrightnessEnd;
  }

  /**
   * Set brightness end.
   * 
   * @param paramBrightnessEnd
   *          the brightnessEnd to set
   */
  public void setBrightnessEnd(final float paramBrightnessEnd) {
    mBrightnessEnd = paramBrightnessEnd;
  }

  /**
   * Get brightnessEnd.
   * 
   * @return the brightnessEnd
   */
  public float getBrightnessEnd() {
    return mBrightnessEnd;
  }

  /**
   * Set innerNodeBrightnessEnd.
   * 
   * @param paramInnerNodeBrightnessEnd
   *          the innerNodeBrightnessEnd to set
   */
  public void setInnerNodeBrightnessEnd(final float paramInnerNodeBrightnessEnd) {
    mInnerNodeBrightnessEnd = paramInnerNodeBrightnessEnd;
  }

  /**
   * Get innerNodeBrightnessEnd.
   * 
   * @return the innerNodeBrightnessEnd
   */
  public float getInnerNodeBrightnessEnd() {
    return mInnerNodeBrightnessEnd;
  }

  /**
   * Get controlP5
   * 
   * @return the controlP5 instance
   */
  public ControlP5 getControlP5() {
    return mControlP5;
  }

  /**
   * Determines if {@link ZoomPan} is currently zooming/pannin.
   * 
   * @return {@code true} if it is zooming/panning currently, {@code false} otherwise
   */
  public boolean isZoomingPanning() {
    return mZoomer.isZooming() || mZoomer.isPanning();
  }

  /**
   * Get zooming mouse coordinate.
   * 
   * @return {@link PVector} instance, the zoomed mouse coordinate
   */
  public PVector getZoomMouseCoord() {
    return mZoomer.getMouseCoord();
  }

  /**
   * Reset zoom.
   */
  public void resetZoom() {
    mZoomer.reset();
  }

  /**
   * Mouse event for zooming.
   * 
   * @param pEvent
   *          the {@link MouseEvent} instance
   */
  public void zoomMouseEvent(final MouseEvent pEvent) {
    mZoomer.mouseEvent(pEvent);
  }

  /**
   * Transformation of zoomer.
   */
  public void transform() {
    mZoomer.transform();
  }

  /**
   * Set modification weight.
   * 
   * @param paramModificationWeight
   *          the modificationWeight to set
   */
  public void setModificationWeight(final float paramModificationWeight) {
    mModificationWeight = paramModificationWeight;
  }

  /**
   * Get modification weight.
   * 
   * @return the modificationWeight
   */
  public float getModificationWeight() {
    return mModificationWeight;
  }

  /**
   * Set bezier line.
   * 
   * @param paramUseBezierLine
   *          the useBezierLine to set
   */
  public void setUseBezierLine(final boolean paramUseBezierLine) {
    mUseBezierLine = paramUseBezierLine;
  }

  /**
   * Get isUseBezierLine.
   * 
   * @return the useBezierLine
   */
  public boolean isUseBezierLine() {
    return mUseBezierLine;
  }

  /**
   * Set showArcs.
   * 
   * @param paramShowArcs
   *          the showArcs to set
   */
  public void setShowArcs(final boolean paramShowArcs) {
    mShowArcs = paramShowArcs;
  }

  /**
   * Get isShowArcs.
   * 
   * @return the showArcs
   */
  public boolean isShowArcs() {
    return mShowArcs;
  }

  /**
   * Set show lines.
   * 
   * @param paramShowLines
   *          the showLines to set
   */
  public void setShowLines(final boolean paramShowLines) {
    mShowLines = paramShowLines;
  }

  /**
   * Get isShowLines.
   * 
   * @return the showLines
   */
  public boolean isShowLines() {
    return mShowLines;
  }

  /**
   * Set inner node arc scale.
   * 
   * @param paramInnerNodeArcScale
   *          the innerNodeArcScale to set
   */
  public void setInnerNodeArcScale(final float paramInnerNodeArcScale) {
    mInnerNodeArcScale = paramInnerNodeArcScale;
  }

  /**
   * Get inner node arc scale.
   * 
   * @return the innerNodeArcScale
   */
  public float getInnerNodeArcScale() {
    return mInnerNodeArcScale;
  }

  /**
   * Set leaf arc scale
   * 
   * @param paramLeafArcScale
   *          the leafArcScale to set
   */
  public void setLeafArcScale(final float paramLeafArcScale) {
    mLeafArcScale = paramLeafArcScale;
  }

  /**
   * @return the mLeafArcScale
   */
  public float getLeafArcScale() {
    return mLeafArcScale;
  }

  /**
   * @param paramDotSize
   *          the dotSize to set
   */
  public void setDotSize(final float paramDotSize) {
    mDotSize = paramDotSize;
  }

  /**
   * Get the dot size.
   * 
   * @return the dotSize
   */
  public float getDotSize() {
    return mDotSize;
  }

  /**
   * Set dot brightness.
   * 
   * @param paramDotBrightness
   *          the dotBrightness to set
   */
  public void setDotBrightness(final float paramDotBrightness) {
    mDotBrightness = paramDotBrightness;
  }

  /**
   * Get dot brightness.
   * 
   * @return the dotBrightness
   */
  public float getDotBrightness() {
    return mDotBrightness;
  }

  /**
   * Set the background brightness.
   * 
   * @param paramBackgroundBrightness
   *          the backgroundBrightness to set
   */
  public void setBackgroundBrightness(final float paramBackgroundBrightness) {
    mBackgroundBrightness = paramBackgroundBrightness;
  }

  /**
   * Get background brightness.
   * 
   * @return the paramBackgroundBrightness
   */
  public float getBackgroundBrightness() {
    return mBackgroundBrightness;
  }

  /**
   * Set mappingMode.
   * 
   * @param paramMappingMode
   *          the mappingMode to set
   */
  public void setMappingMode(final int paramMappingMode) {
    mMappingMode = paramMappingMode;
  }

  /**
   * Set buffer.
   * 
   * @param paramBuffer
   *          the buffer to set
   */
  public void setBuffer(final PGraphics paramBuffer) {
    mBuffer = checkNotNull(paramBuffer);
  }

  /**
   * Get buffer.
   * 
   * @return the buffer
   */
  public PGraphics getBuffer() {
    return mBuffer;
  }

  /**
   * Get processing parent.
   * 
   * @return {@link PApplet} reference
   */
  public PApplet getParent() {
    return mParent;
  }

  /**
   * Set savePDF
   * 
   * @param paramSavePDF
   *          the savePDF to set
   */
  public void setSavePDF(final boolean paramSavePDF) {
    mSavePDF = paramSavePDF;
  }

  /**
   * @return the mSavePDF
   */
  public boolean isSavePDF() {
    return mSavePDF;
  }

  /**
   * Get showGUI.
   * 
   * @param paramShowGUI
   *          the showGUI to set
   */
  public void setShowGUI(final boolean paramShowGUI) {
    mShowGUI = paramShowGUI;
  }

  /**
   * Get isShowGUI.
   * 
   * @return the showGUI
   */
  public boolean isShowGUI() {
    return mShowGUI;
  }

  /**
   * Get kind of view.
   * 
   * @return the kind of view
   */
  public EView getViewKind() {
    return mUseDiffView;
  }

  /**
   * Determines if a special attribute should be used for labels
   * 
   * @return {@code true} if it should be used, {@code false} otherwise
   */
  public boolean isUseAttribute() {
    return mUseAttribute;
  }

  /**
   * Set if a special attribute should be used for labels.
   * 
   * @param pUseAttribute
   *          {@code true} if it should be used, {@code false} otherwise
   */
  public void setUseAttribute(final boolean pUseAttribute) {
    mUseAttribute = pUseAttribute;
  }

  /**
   * GlassPaneMouseListener to disable mouse and keyboard listening
   * temporarily.
   * 
   * @author Johannes Lichtenberger, University of Konstanz
   * 
   */
  public static class GlassPaneListener implements MouseListener, KeyListener {
    @Override
    public void mouseClicked(MouseEvent e) {
    }

    @Override
    public void mousePressed(MouseEvent e) {
    }

    @Override
    public void mouseReleased(MouseEvent e) {
    }

    @Override
    public void mouseEntered(MouseEvent e) {
    }

    @Override
    public void mouseExited(MouseEvent e) {
    }

    @Override
    public void keyTyped(KeyEvent e) {
    }

    @Override
    public void keyPressed(KeyEvent e) {
    }

    @Override
    public void keyReleased(KeyEvent e) {
    }
  }

  /**
   * Draw an arrow.
   * 
   * @param pGraphic
   *          {@link PGraphics} reference to draw
   * @param px1
   *          x1 vertex
   * @param py1
   *          y1 vertex
   * @param px2
   *          x2 vertex
   * @param py2
   *          y2 vertex
   * @param paramDraw
   *          draw a line+arrow or just the arrow
   * @throws NullPointerException
   *           if {@code pGraphic} or {@code pDraw} is {@code null}
   */
  public void drawArrow(final PGraphics pGraphic, final float px1, final float py1, final float px2,
    final float py2, final EDrawLine pDraw) {
    checkNotNull(pGraphic);
    checkNotNull(pDraw);
    pDraw.drawLine(pGraphic, px1, py1, px2, py2);
    pGraphic.pushMatrix();
    pGraphic.translate(px2, py2);
    final float a = PApplet.atan2(px1 - px2, py2 - py1);
    pGraphic.rotate(a);
    pGraphic.line(0, 0, -7, -7);
    pGraphic.line(0, 0, 7, -7);
    pGraphic.popMatrix();
  }

  /**
   * Transforms a subtree into the new root.
   * 
   * @param pHitItemIndex
   *          index of the item to represent as the new root
   */
  public void transformRoot(final int pHitItemIndex) {
    LOGWRAPPER.debug("index of item hitted: " + pHitItemIndex);
    checkArgument(pHitItemIndex >= 0, "pHitItemIndex must be >=0!");
    checkNotNull(mBuffer);

    POOL.submit(new Callable<Void>() {
      @SuppressWarnings("unchecked")
      @Override
      public Void call() throws Exception {
        // Generic cast always working.
        mModel = (IModel<SunburstContainer, SunburstItem>)mControl.getModel();
        final SunburstItem item = mModel.getItem(pHitItemIndex);
        ListIterator<SunburstItem> items =
          ((AbsModel<SunburstContainer, SunburstItem>)mModel).listIterator(pHitItemIndex);
        final int oldRootDepth = item.getDepth();
        final float angleFactor = PConstants.TWO_PI / (item.getAngleEnd() - item.getAngleStart());
        // New root node.
        SunburstItem oldRoot = null;
        SunburstItem root = null;
        int depthMax = 0;
        int oldDepthMax = 0;
        if (items.hasNext()) {
          oldRoot = items.next();
          final int depth = (oldRoot.getDiff() == EDiff.SAME || oldRoot.getDiff() == EDiff.SAMEHASH) ? 0 : 2;
          depthMax = depth;
          SunburstItem.Builder builder =
            new SunburstItem.Builder(mParent, 0, PConstants.TWO_PI, new NodeRelations(depth, depth, oldRoot
              .getStructKind(), oldRoot.getValue(), oldRoot.getMinimum(), oldRoot.getValue(), -1), oldRoot
              .getGUI().mDb, oldRoot.getGUI());
          if (oldRoot.getText() != null) {
            builder.setText(oldRoot.getText());
          } else if (oldRoot.getQName() != null) {
            builder.setQName(oldRoot.getQName());
          }
          if (oldRoot.getOldText() != null) {
            builder.setOldText(oldRoot.getOldText());
          } else if (oldRoot.getOldQName() != null) {
            builder.setOldQName(oldRoot.getOldQName());
          }
          if (oldRoot.getAttributes() != null) {
            builder.setAttributes(oldRoot.getAttributes());
          }
          if (oldRoot.getNamespaces() != null) {
            builder.setNamespaces(oldRoot.getNamespaces());
          }
          if (oldRoot.getDiff() != null) {
            builder.setDiff(oldRoot.getDiff());
          }
          if (oldRoot.getItem() != null) {
            builder.setNode(oldRoot.getItem());
          }
          builder.setModifications(oldRoot.getModificationCount());
          root = builder.build();
        }
        final int endIndex =
          item.getText() == null ? pHitItemIndex + (int)oldRoot.getValue() - 1 : pHitItemIndex;
        final List<SunburstItem> newItems = new ArrayList<>((int)root.getValue());
        newItems.add(root);
        // endIndex exclusive.

        final List<SunburstItem> oldItems = mModel.subList(pHitItemIndex + 1, endIndex + 1);
        if (oldItems.size() > 0) {
          final int partitions =
            (int)Math.ceil(oldItems.size() / (double)Runtime.getRuntime().availableProcessors());
          final List<List<SunburstItem>> partitioned = Lists.partition(oldItems, partitions);
          final CountDownLatch latch = new CountDownLatch(partitioned.size());
          mOldDepth = 0;
          for (final List<SunburstItem> list : partitioned) {
            ListenableFuture<Integer> future = POOL.submit(new OldDepthMax(list, oldRootDepth));
            Futures.addCallback(future, new FutureCallback<Integer>() {
              @Override
              public void onSuccess(final Integer result) {
                mOldDepth = Math.max(mOldDepth, result);
                latch.countDown();
              }

              @Override
              public void onFailure(final Throwable t) {
                LOGWRAPPER.error(t.getMessage(), t);
              }
            });
          }
          latch.await();

          oldDepthMax = mOldDepth;
          items = ((AbsModel<SunburstContainer, SunburstItem>)mModel).listIterator(pHitItemIndex);
          items.next();

          for (int i = pHitItemIndex + 1; i <= endIndex && items.hasNext(); i++) {
            final SunburstItem oldChild = items.next();

            // for (final SunburstItem oldChild : newList) {
            final SunburstItem newChild = new SunburstItem(oldChild);
            float newStartAngle = (oldChild.getAngleStart() - oldRoot.getAngleStart()) * angleFactor;
            LOGWRAPPER.debug("angleStart: " + newStartAngle);
            if (newStartAngle > PConstants.TWO_PI) {
              newStartAngle = PConstants.TWO_PI;
            }
            newChild.setAngleStart(newStartAngle);

            float newEndAngle =
              (oldChild.getAngleEnd() - oldChild.getAngleStart()) * angleFactor + newChild.getAngleStart();
            LOGWRAPPER.debug("angleEnd: " + newEndAngle);
            if (newEndAngle > PConstants.TWO_PI) {
              newEndAngle = PConstants.TWO_PI;
            }
            newChild.setAngleEnd(newEndAngle);
            newChild.setAngleCenter(newChild.getAngleStart()
              + ((newChild.getAngleEnd() - newChild.getAngleStart()) / 2f));
            newChild.setIndexToParent(oldChild.getIndexToParent() - pHitItemIndex);
            final int newIndexMovedTo = oldChild.getIndexMovedTo() - pHitItemIndex;
            if (newIndexMovedTo >= 0 && (newIndexMovedTo < (endIndex - pHitItemIndex))
              && oldChild.getIndexMovedTo() > mHitTestIndex && oldChild.getIndexMovedTo() < endIndex) {
              newChild.setIndexMovedTo(newIndexMovedTo);
            } else {
              newChild.setIndexMovedTo(-1);
            }

            int newDepth = 0;
            LOGWRAPPER.debug("child depth: " + oldChild.getDepth());
            LOGWRAPPER.debug("parent depth: " + mModel.getItem(oldChild.getIndexToParent()).getDepth());
            final int parentDepth = mModel.getItem(oldChild.getIndexToParent()).getDepth();
            if ((parentDepth + 1) != oldChild.getDepth()) {
              newDepth = oldDepthMax + 2;
            } else {
              newDepth = newItems.get(newChild.getIndexToParent()).getDepth() + 1;
            }
            if (newDepth > depthMax) {
              depthMax = newDepth;
            }
            LOGWRAPPER.debug("newDepth: " + newDepth);
            newChild.setDepth(newDepth);
            newChild.setOriginalDepth(oldChild.getOriginalDepth() - oldRoot.getOriginalDepth());
            newItems.add(newChild);
          }
        }

        mControl.setItems(newItems);
        if (!(mUseDiffView == EView.DIFF && mUseDiffView.getValue())) {
          mModel.setMinMax();
        }
        mControl.setNewMaxDepth(depthMax);
        mControl.setOldMaxDepth(oldDepthMax);
        mDepthMax = depthMax;
        mOldDepthMax = oldDepthMax;
        setDepthMax();
        update();
        
        if (mUseDiffView == EView.DIFF && EView.DIFF.getValue()
        && mControl.getModel().getItemsSize() < ANIMATION_THRESHOLD) {
          mInit = true;
        }
        return null;
      }
    });
  }

  /** Set maximum depth. */
  protected void setDepthMax() {
    if (mUseDiffView == EView.DIFF) {
      if (mDepthMax < mOldDepthMax + 2) {
        mDepthMax = mOldDepthMax;
      }
      mDepthMax += 2;
    }
  }

  /**
   * Get if pruning is used.
   * 
   * @return {@code true}, if pruning is used, {@code false} otherwise
   */
  public boolean getUsePruning() {
    return mUsePruning;
  }

  /**
   * Set use move detection.
   * 
   * @param pUseMoveDetection
   *          new value
   */
  public void setUseMoveDetection(final boolean pUseMoveDetection) {
    mUseMoveDetection = pUseMoveDetection;
  }

  /**
   * Set use pruning.
   * 
   * @param pUsePruning
   *          new value
   */
  public void setUsePruning(final boolean pUsePruning) {
    mUsePruning = pUsePruning;
  }

  /** Undo operation. */
  public void undo() {
    mDone = false;
    mLock.acquireUninterruptibly();
    if (!mImages.isEmpty()) {
      mImg = mImages.pop();
    }
    mLock.release();
    mDone = true;
  }

  /** Push image on top of the stack for use with the undo operation. */
  public void pushImg() {
    mImages.push(mImg);
  }

  /** Inner class to calculate new oldMaxDepth. */
  class OldDepthMax implements Callable<Integer> {

    /** {@link List} of {@link SunburstItem}s. */
    private final List<SunburstItem> mItems;

    /** Root depth. */
    private final int mRootDepth;

    /**
     * Constructor.
     * 
     * @param pItems
     *          {@link SunburstItem} list to get new oldMaxDepth from
     * @param pRootDepth
     *          the root depth
     */
    OldDepthMax(final List<SunburstItem> pItems, final int pRootDepth) {
      checkArgument(pRootDepth >= 0, "pRootDepth must be >= 0!");
      mItems = checkNotNull(pItems);
      mRootDepth = pRootDepth;
    }

    @Override
    public Integer call() {
      int oldDepthMax = 0;
      for (final SunburstItem oldItem : mItems) {
        if (oldItem.getDiff() == EDiff.SAME || oldItem.getDiff() == EDiff.SAMEHASH) {
          int indexToParent = oldItem.getIndexToParent();
          SunburstItem tmpItem = oldItem;
          while (indexToParent != -1
            && (tmpItem.getDiff() == EDiff.SAME || tmpItem.getDiff() == EDiff.SAMEHASH)) {
            indexToParent = tmpItem.getIndexToParent();
            if (indexToParent != -1) {
              tmpItem = mModel.getItem(indexToParent);
            }
          }
          if (indexToParent == -1) {
            oldDepthMax = Math.max(oldDepthMax, oldItem.getDepth() - mRootDepth);
          }
        }
      }
      return oldDepthMax;
    }

  }

  /**
   * Get done.
   * 
   * @return {@code mDone} value
   */
  public boolean isDone() {
    return mDone;
  }
}
