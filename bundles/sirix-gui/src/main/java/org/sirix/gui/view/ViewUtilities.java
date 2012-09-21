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

package org.sirix.gui.view;

import controlP5.ControlP5;

import java.io.File;
import java.lang.Thread.State;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.exception.SirixException;
import org.sirix.gui.ProgressGlassPane;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.model.interfaces.IModel;
import org.sirix.gui.view.smallmultiple.SmallmultipleModel;
import org.sirix.gui.view.sunburst.AbsSunburstGUI;
import org.sirix.gui.view.sunburst.AbsSunburstGUI.GlassPaneListener;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.model.SunburstCompareModel;
import org.sirix.gui.view.sunburst.model.SunburstModel;
import processing.core.PApplet;
import processing.core.PConstants;

/**
 * Provides some helper methods for views, which couldn't otherwise be encapsulated together.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class ViewUtilities {

  /** Path to save visualization as a PDF or PNG file. */
  public static final String SAVEPATH = "target" + File.separator;

  /** Private constructor. */
  private ViewUtilities() {
    // Just in case of a helper method tries to invoke the constructor.
    throw new AssertionError();
  }

  /**
   * Serialization compatible String representation of a {@link QName} retnkference.
   * 
   * @param pQName
   *          {@link QName} reference
   * @return the string representation
   */
  public static String qNameToString(@Nonnull final QName pQName) {
    String retVal;

    if (pQName.getPrefix().isEmpty()) {
      retVal = pQName.getLocalPart();
    } else {
      retVal = new StringBuilder(pQName.getPrefix()).append(":").append(pQName.getLocalPart()).toString();
    }

    return retVal;
  }

  /**
   * Refresh resource with latest revision.
   * 
   * @param pDb
   *          {@link ReadDB} instance which has to be closed
   * @return a new {@link ReadDB} instance
   * @throws SirixException
   *           if something went wrong while reading the newest revision
   */
  public static ReadDB refreshResource(@Nonnull final ReadDB pDb) throws SirixException {
    final File file = pDb.getDatabase().getDatabaseConfig().getFile();
    if (pDb != null) {
      pDb.close();
    }
    return new ReadDB(file);
  }

  /**
   * Format a timestamp.
   * 
   * @return formatted timestamp
   */
  public static String timestamp() {
    return String.format("%1$ty%1$tm%1$td_%1$tH%1$tM%1$tS", Calendar.getInstance());
  }

  /**
   * Draw controlP5 GUI.
   * 
   * @param pControlP5
   *          {@link ControlP5} instance
   */
  public static void drawGUI(@Nonnull final ControlP5 pControlP5) {
    pControlP5.show();
    pControlP5.draw();
  }

  /** Debugging threads. */
  public static void stackTraces() {
    Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
    Iterator<Thread> itr = map.keySet().iterator();
    while (itr.hasNext()) {
      Thread t = itr.next();
      StackTraceElement[] elem = map.get(t);
      System.out.print("\"" + t.getName() + "\"");
      System.out.print(" prio=" + t.getPriority());
      System.out.print(" tid=" + t.getId());
      State s = t.getState();
      String state = null;
      switch (s) {
      case NEW:
        state = "NEW";
        break;
      case BLOCKED:
        state = "BLOCKED";
        break;
      case RUNNABLE:
        state = "RUNNABLE";
        break;
      case TERMINATED:
        state = "TERMINATED";
        break;
      case TIMED_WAITING:
        state = "TIME WAITING";
        break;
      case WAITING:
        state = "WAITING";
        break;
      }
      System.out.println(" " + state + "\n");
      for (int i = 0; i < elem.length; i++) {
        System.out.println("  at ");
        System.out.print(elem[i].toString());
        System.out.println("\n");
      }
      System.out.println("----------------------------\n");
    }

  }

  /**
   * Legend of a SunburstView.
   * 
   * @param pGUI
   *          GUI which provides a SunburstView and therefore extends {@link AbsSunburstGUI}
   * @param applet
   *          Processing {@link applet} reference
   */
  public static void legend(@Nonnull final AbsSunburstGUI pGUI,
    final IModel<SunburstContainer, SunburstItem> pModel) {
    final PApplet applet = pGUI.getApplet();
    // applet.translate(0, 0);
    applet.textAlign(PConstants.LEFT, PConstants.TOP);
    applet.strokeWeight(0);

    if (pGUI.isShowArcs()) {
      applet.fill(pGUI.getHueStart(), pGUI.getSaturationStart(), pGUI.getBrightnessStart());
      applet.rect(20f, applet.height - 70f, 50, 17);
      color(pGUI);
      applet.text("-", 78, applet.height - 70f);
      applet.fill(pGUI.getHueEnd(), pGUI.getSaturationEnd(), pGUI.getBrightnessEnd());
      applet.rect(90f, applet.height - 70f, 50, 17);
      color(pGUI);
      if (pModel instanceof SunburstModel) {
        applet.text("text node length", 150f, applet.height - 70f);
      }
      if (pModel instanceof SunburstCompareModel || pModel instanceof SmallmultipleModel) {
        applet.text("text node similarity", 150f, applet.height - 70f);
      }
      applet.fill(0, 0, pGUI.getInnerNodeBrightnessStart());
      applet.rect(20f, applet.height - 50f, 50, 17);
      color(pGUI);
      applet.text("-", 78, applet.height - 50f);
      applet.fill(0, 0, pGUI.getInnerNodeBrightnessEnd());
      applet.rect(90f, applet.height - 50f, 50, 17);
      color(pGUI);
      if (pModel instanceof SunburstModel) {
        applet.text("descendant-or-self count of element nodes", 150f, applet.height - 50f);
      }
      if (pModel instanceof SunburstCompareModel || pModel instanceof SmallmultipleModel) {
        applet.text("element node similarity", 150f, applet.height - 50f);
      }
    }

    if (pGUI.isSavePDF()) {
      applet.translate(applet.width / 2f, applet.height / 2f);
      pGUI.setSavePDF(false);
      applet.endRecord();
      PApplet.println("saving to pdf – done");
    }
  }

  /**
   * Fill color which changes to white or black depending on the background brightness.
   * 
   * @param pGUI
   *          GUI which provides a SunburstView and therefore extends {@link AbsSunburstGUI}
   */
  public static void color(@Nonnull final AbsSunburstGUI pGUI) {
    final PApplet applet = pGUI.getApplet();
    if (pGUI.getBackgroundBrightness() > 40f) {
      applet.fill(0, 0, 0);
    } else {
      applet.fill(360, 0, 100);
    }
  }

  /**
   * Compare legend.
   * 
   * @param pGUI
   *          GUI which provides a SunburstView and therefore extends {@link AbsSunburstGUI}
   */
  public static void compareLegend(@Nonnull final AbsSunburstGUI pGUI) {
    if (pGUI.getDotSize() > 0) {
      final PApplet applet = pGUI.getApplet();
      applet.textAlign(PConstants.LEFT, PConstants.TOP);
      applet.fill(60, 100, pGUI.getDotBrightness());
      applet.ellipse(applet.width - 160f, applet.height - 136f, 8, 8);
      color(pGUI);
      applet.text("node moved", applet.width - 140f, applet.height - 146f);
      applet.fill(200, 100, pGUI.getDotBrightness());
      applet.ellipse(applet.width - 160f, applet.height - 113f, 8, 8);
      color(pGUI);
      applet.text("node inserted", applet.width - 140f, applet.height - 123f);
      applet.fill(360, 100, pGUI.getDotBrightness());
      applet.ellipse(applet.width - 160f, applet.height - 90f, 8, 8);
      color(pGUI);
      applet.text("node deleted", applet.width - 140f, applet.height - 100f);
      applet.fill(120, 100, pGUI.getDotBrightness());
      applet.ellipse(applet.width - 160f, applet.height - 67f, 8, 8);
      color(pGUI);
      applet.text("node updated", applet.width - 140f, applet.height - 77f);
      applet.fill(290, 100, pGUI.getDotBrightness());
      applet.ellipse(applet.width - 160f, applet.height - 44f, 8, 8);
      color(pGUI);
      applet.text("node replaced", applet.width - 140f, applet.height - 54f);
    }
  }

  /**
   * Process events for displaying the process in a GlassPane.
   * 
   * @param pListener
   *          {@link GlassPaneListener} to add
   * @param pView
   *          {@link IView} which "owns" the {@link ProgressGlassPane}
   * @param progress
   *          progress between 0 and 100
   * @throws NullPointerException
   *           if a reference is null
   * @throws IllegalArgumentException
   *           if the progress peter is invalid
   */
  public static void processGlassPaneEvents(@Nonnull final GlassPaneListener pListener,
    @Nonnull final IProcessingView pView, @Nonnegative final int progress) {
    assert pView instanceof PApplet;
    if (pView == null || pListener == null) {
      throw new NullPointerException("Reference parameters may not be null!");
    }
    if (progress < 0 || progress > 100) {
      throw new IllegalArgumentException("Process parameter must be between 0 and 100!");
    }
    final ProgressGlassPane pane = pView.getGlassPane();
    switch (progress) {
    case 100:
      pane.setVisible(false);
      pane.removeMouseListener(pListener);
      pane.removeKeyListener(pListener);
      if (!pView.isShowing()) {
        pView.setVisible(true);
      }
      ((PApplet)pView).requestFocusInWindow();
      break;
    case 0:
      if (pView.isShowing()) {
        pView.setVisible(false);
      }
      pane.setVisible(true);
      pane.setProgress(0);
      pane.addMouseListener(pListener);
      pane.addKeyListener(pListener);
      break;
    default:
      pane.setProgress(progress);
    }
  }
}
