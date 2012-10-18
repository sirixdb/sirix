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

package org.sirix.gui.view.sunburst.control;

import static com.google.common.base.Preconditions.checkNotNull;

import java.awt.event.MouseEvent;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.xml.stream.XMLStreamException;

import org.sirix.gui.ReadDB;
import org.sirix.gui.view.controls.AbstractControl;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.sunburst.AbstractSunburstGUI;
import org.sirix.gui.view.sunburst.AbstractSunburstGUI.EResetZoomer;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstItem;

import processing.core.PApplet;
import controlP5.ControlEvent;
import controlP5.Range;
import controlP5.Slider;
import controlP5.Toggle;

/**
 * Abstract class to simplify the implementation of {@link SunburstControl}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbstractSunburstControl extends AbstractControl implements SunburstControl {

  /** Used for the hybrid comparsion. */
  private transient CountDownLatch mLatch = new CountDownLatch(1);

  /** {@link Model} implementation. */
  protected transient Model<SunburstContainer, SunburstItem> mModel;

  /** {@link ReadDB} reference. */
  protected transient ReadDB mDb;

  /** The GUI. */
  protected final AbstractSunburstGUI mGUI;

  /** Processing {@link PApplet}. */
  protected final PApplet mParent;

  /**
   * Constructor.
   * 
   * @param paramParent
   *          {@link PApplet} reference
   * @param paramModel
   *          {@link Model} implementation
   * @param paramDb
   *          {@link ReadDB} reference
   */
  public AbstractSunburstControl(final PApplet paramParent,
    final Model<SunburstContainer, SunburstItem> paramModel, final ReadDB paramDb) {
    mParent = checkNotNull(paramParent);
    mDb = checkNotNull(paramDb);
    mModel = checkNotNull(paramModel);
    mGUI = getGUIInstance();
    mGUI.getControlP5().addListener(this);
    mGUI.getParent().addKeyListener(this);
    mModel.addPropertyChangeListener(mGUI);
  }

  /**
   * Get GUI instance.
   */
  protected abstract AbstractSunburstGUI getGUIInstance();

  @Override
  public void zoomEnded() {
  	mGUI.noAnimation();
  }
  
  @Override
  public void panEnded() {
  	mGUI.noAnimation();
  }
  
  /**
   * Implements processing mousePressed.
   * 
   * @param paramEvent
   *          The {@link MouseEvent}.
   * 
   * @see processing.core.PApplet#mousePressed
   */
  @Override
  public void mousePressed(final MouseEvent paramEvent) {
    mGUI.getControlP5().controlWindow.mouseEvent(paramEvent);
    mGUI.zoomMouseEvent(paramEvent);
  }

  /**
   * Called on every change of the GUI.
   * 
   * @param paramControlEvent
   *          the {@link ControlEvent}
   */
  @Override
  public void controlEvent(final ControlEvent paramControlEvent) {
    assert paramControlEvent != null;
    if (paramControlEvent.isController()) {
      if (paramControlEvent.getController().getId() == 50) {
        mModel.evaluateXPath(paramControlEvent.getController().getStringValue());
      } else {

        if (paramControlEvent.getController() instanceof Toggle) {
          final Toggle toggle = (Toggle)paramControlEvent.getController();
          switch (paramControlEvent.getController().getId()) {
          case 0:
            mGUI.setShowArcs(toggle.getState());
            break;
          case 1:
            mGUI.setShowLines(toggle.getState());
            break;
          case 2:
            mGUI.setUseBezierLine(toggle.getState());
            break;
          }
        } else if (paramControlEvent.getController() instanceof Slider) {
          switch (paramControlEvent.getController().getId()) {
          case 0:
            mGUI.setInnerNodeArcScale(paramControlEvent.getController().getValue());
            break;
          case 1:
            mGUI.setLeafArcScale(paramControlEvent.getController().getValue());
            break;
          case 2:
            mGUI.setModificationWeight(paramControlEvent.getController().getValue());
            break;
          case 3:
            mGUI.setDotSize(paramControlEvent.getController().getValue());
            break;
          case 4:
            mGUI.setDotBrightness(paramControlEvent.getController().getValue());
            break;
          case 5:
            mGUI.setBackgroundBrightness(paramControlEvent.getController().getValue());
            break;
          }
        } else if (paramControlEvent.getController() instanceof Range) {
          final float[] f = paramControlEvent.getController().getArrayValue();
          switch (paramControlEvent.getController().getId()) {
          case 0:
            mGUI.setHueStart(f[0]);
            mGUI.setHueEnd(f[1]);
            break;
          case 1:
            mGUI.setSaturationStart(f[0]);
            mGUI.setSaturationEnd(f[1]);
            break;
          case 2:
            mGUI.setBrightnessStart(f[0]);
            mGUI.setBrightnessEnd(f[1]);
            break;
          case 3:
            mGUI.setInnerNodeBrightnessStart(f[0]);
            mGUI.setInnerNodeBrightnessEnd(f[1]);
            break;
          case 4:
            mGUI.setInnerNodeStrokeBrightnessStart(f[0]);
            mGUI.setInnerNodeStrokeBrightnessEnd(f[1]);
            break;
          case 5:
            mGUI.setStrokeWeightStart(f[0]);
            mGUI.setStrokeWeightEnd(f[1]);
            break;
          }
        }

        mGUI.update(EResetZoomer.YES);
      }
    }
  }

  /**
   * Implements processing mouseEntered.
   * 
   * @param paramEvent
   *          The {@link MouseEvent}.
   * 
   * @see processing.core.PApplet#mouseEntered
   */
  @Override
  public void mouseEntered(final MouseEvent paramEvent) {
    mGUI.getParent().loop();
  }

  /**
   * Implements processing mouseExited.
   * 
   * @param paramEvent
   *          The {@link MouseEvent}.
   * 
   * @see processing.core.PApplet#mouseExited
   */
  @Override
  public void mouseExited(final MouseEvent paramEvent) {
    mGUI.getParent().noLoop();
  }

  @Override
  public void commit(final int paramValue) throws XMLStreamException {
  }

  @Override
  public void submit(final int paramValue) throws XMLStreamException {
  }

  @Override
  public void cancel(final int paramValue) {
  }

  @Override
  public final Model<SunburstContainer, SunburstItem> getModel() {
    return mModel;
  }

  /**
   * Get the {@link CountDownLatch} reference
   * 
   * @return the {@link CountDownLatch} reference
   */
  public CountDownLatch getLatch() {
    return mLatch;
  }

  public void setLatch(final CountDownLatch pLatch) {
    mLatch = checkNotNull(pLatch);
  }

  @Override
  public void setItems(final List<SunburstItem> pItems) {
    mModel.setItems(checkNotNull(pItems));
  }
}
