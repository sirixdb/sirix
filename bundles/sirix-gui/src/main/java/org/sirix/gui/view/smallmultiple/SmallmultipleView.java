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

import static com.google.common.base.Preconditions.checkNotNull;

import java.awt.Dimension;
import java.awt.event.MouseEvent;

import javax.annotation.Nullable;
import javax.swing.JComponent;

import org.sirix.gui.GUIProp;
import org.sirix.gui.ProgressGlassPane;
import org.sirix.gui.view.AbstractView;
import org.sirix.gui.view.ProcessingEmbeddedView;
import org.sirix.gui.view.ProcessingView;
import org.sirix.gui.view.View;
import org.sirix.gui.view.ViewNotifier;
import org.sirix.gui.view.VisualItem;
import org.sirix.gui.view.VisualItemAxis;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.sunburst.AbstractSunburstGUI;
import org.sirix.gui.view.sunburst.SunburstView;
import org.sirix.gui.view.sunburst.model.SunburstModel;

import processing.core.PApplet;
import processing.core.PConstants;

import com.google.common.base.Optional;

/**
 * SmallMultiples view.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SmallmultipleView extends AbstractView implements View {

  /**
   * SerialUID.
   */
  private static final long serialVersionUID = 1L;

  // /** {@link LogWrapper}. */
  // private static final LogWrapper LOGWRAPPER = new LogWrapper(
  // LoggerFactory.getLogger(SmallMultiplesView.class));

  /** Name of the view. */
  private static final String NAME = "SmallMultiplesView";

  /** {@link SunburstView} instance. */
  private static SmallmultipleView mView;

  // /** {@link ViewNotifier} to notify views of changes. */
  // private final ViewNotifier mNotifier;
  //
  // /** {@link ReadDB} instance to interact with sirix. */
  // private transient ReadDB mDB;
  //
  // /** {@link GUI} reference. */
  // private final GUI mGUI;
  //
  // /** Embedded processing view. */
  // private transient Embedded mEmbed;

  /**
   * Constructor.
   * 
   * @param pNotifier
   *          {@link ViewNotifier} instance.
   */
  private SmallmultipleView(final ViewNotifier pNotifier) {
    super(pNotifier);
  }

  /**
   * Singleton factory method.
   * 
   * @param paramNotifier
   *          {@link ViewNotifier} to notify views of changes etc.pp.
   * @return {@link SmallmultipleView} instance
   */
  public static synchronized SmallmultipleView getInstance(final ViewNotifier paramNotifier) {
    if (mView == null) {
      mView = new SmallmultipleView(paramNotifier);
    }

    return mView;
  }

  /**
   * Not supported.
   * 
   * @see Object#clone()
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  @Override
  public boolean isVisible() {
    return GUIProp.EShowViews.SHOWSMALLMULTIPLES.getValue();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public JComponent component() {
    return this;
  }

  @Override
  public Dimension getPreferredSize() {
    final Dimension parentFrame = getGUI().getSize();
    return new Dimension(parentFrame.width, parentFrame.height - 55);
  }

  /** Embedded processing view. */
  final class Embedded extends PApplet implements ProcessingView {

    /**
     * Serial UID.
     */
    private static final long serialVersionUID = 1L;

    /** The sirix {@link SunburstModel}. */
    private transient SmallmultipleModel mModel;

    /** {@link ProcessingEmbeddedView} reference. */
    private transient ProcessingEmbeddedView mEmbeddedView;

    /** {@link SmallmultipleView} reference. */
    private final SmallmultipleView mView;

    /** {@link SmallmultipleControl} reference. */
    private transient SmallmultipleControl mControl;

    /** {@link ProgressGlassPane} reference. */
    private transient ProgressGlassPane mGlassPane;

    /**
     * Constructor.
     * 
     * @param paramView
     *          the enclosing view
     */
    public Embedded(final SmallmultipleView pView) {
      mView = checkNotNull(pView);
    }

    @Override
    public void setup() {
      size(getNotifier().getGUI().getSize().width - 10, getNotifier().getGUI().getSize().height - 80,
        PConstants.JAVA2D);
      refreshInit();
    }

    /** Setup processing view. */
    public void refreshInit() {
      // Set glass pane.
      mView.getGUI().setGlassPane(mGlassPane = new ProgressGlassPane());
      mGlassPane.setVisible(true);

      // Initialization with no draw() loop.
      noLoop();

      // Frame rate reduced to 30 per sec.
      frameRate(30);

      // Create Model.
      mModel = new SmallmultipleModel(this, getDB());

      // Create Controller.
      mControl = SmallmultipleControl.getInstance(this, mModel, getDB());
      mControl.refreshIncrementalTraversal();

      // Use embedded view.
      mEmbeddedView =
        ProcessingEmbeddedView.getInstance(this, mView, mControl.getGUIInstance(), mControl, getNotifier());
    }

    @Override
    public void draw() {
      if (mEmbeddedView != null) {
        mEmbeddedView.draw();
      }
    }

    @Override
    public void mouseEntered(final MouseEvent paramEvent) {
      if (mEmbeddedView != null) {
        mEmbeddedView.mouseEntered(paramEvent);
      }
    }

    @Override
    public void mouseExited(final MouseEvent paramEvent) {
      if (mEmbeddedView != null) {
        mEmbeddedView.mouseExited(paramEvent);
      }
    }

    @Override
    public void keyReleased() {
      if (mEmbeddedView != null) {
        mEmbeddedView.keyReleased();
      }
    }

    @Override
    public void mousePressed(final MouseEvent paramEvent) {
      if (mEmbeddedView != null) {
        mEmbeddedView.mousePressed(paramEvent);
      }
    }

    @Override
    public void refreshUpdate() {
      // mControl.refreshUpdate();
      mEmbeddedView.handleHLWeight();
    }

    /** Refresh. Thus sirix storage has been updated to a new revision. */
    void refresh() {
      getNotifier().update(mView, Optional.<VisualItemAxis> absent());
    }

    /** Update Processing GUI. */
    @Override
    public void update() {
      if (mEmbeddedView != null) {
        mEmbeddedView.updateGUI();
      }
    }

    @Override
    public SmallmultipleControl getController() {
      assert mControl != null;
      return mControl;
    }

    @Override
    public ProgressGlassPane getGlassPane() {
      assert mGlassPane != null;
      return mGlassPane;
    }

    @Override
    public View getView() {
      assert mView != null;
      return mView;
    }

    @Override
    public AbstractSunburstGUI getGUI() {
      return mControl.getGUIInstance();
    }

    @Override
    public ProcessingEmbeddedView getEmbeddedView() {
      return mEmbeddedView;
    }

    @Override
    public void setEmbeddedView(@Nullable final ProcessingEmbeddedView pEmbeddedView) {
      mEmbeddedView = pEmbeddedView;
    }

    @Override
    public PApplet getApplet() {
      return this;
    }

    @Override
    public boolean isFocused() {
      return focused;
    }

    @Override
    public boolean isDone() {
      return mControl.getGUIInstance().isDone();
    }

    @Override
    public Model<?, ?> getModel() {
      return mModel;
    }
  }

  @Override
  public void hover(final VisualItem paramItem) {
  }

  @Override
  public void resize() {
  }

  @Override
  protected ProcessingView getEmbeddedInstance() {
    return new Embedded(this);
  }
}
