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
import controlP5.ControlEvent;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.swing.JOptionPane;

import org.slf4j.LoggerFactory;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.ViewUtilities;
import org.sirix.gui.view.model.interfaces.IModel;
import org.sirix.gui.view.smallmultiple.SmallmultipleView.Embedded;
import org.sirix.gui.view.sunburst.AbsSunburstGUI;
import org.sirix.gui.view.sunburst.EPruning;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.control.AbsSunburstControl;
import org.sirix.utils.LogWrapper;
import processing.core.PApplet;
import processing.core.PConstants;

/**
 * Controller for the {@link SmallmultipleView}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SmallmultipleControl extends AbsSunburstControl {

  /** {@link LogWrapper}. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(SmallmultipleControl.class));

  /** Thread pool. */
  private static final ExecutorService POOL = Executors.newSingleThreadExecutor();

  /** Maximum of revisions to compare. */
  private static final int REVS_TO_COMPARE = 4;

  /** {@link SmallmultipleControl} singleton instance . */
  private static SmallmultipleControl mControl;

  /** Container with settings for the model. */
  private transient SunburstContainer mContainer;

  /** Locking of observable changes. */
  private final Semaphore mLock = new Semaphore(1);

  /** {@link SmallmultipleGUI} reference. */
  private final SmallmultipleGUI mSmallMultiplesGUI;

  /** Latch. */
  // private transient CountDownLatch mLatch = new CountDownLatch(1);

  /**
   * Private constructor.
   * 
   * @param pParent
   *          parent {@link Embedded} reference
   * @param pModel
   *          model which implements the {@link IModel} interface
   * @param pDb
   *          {@link ReadDB} reference
   */
  private SmallmultipleControl(@Nonnull final Embedded pParent,
    @Nonnull final IModel<SunburstContainer, SunburstItem> pModel, @Nonnull final ReadDB pDB) {
    super(pParent, pModel, pDB);
    mSmallMultiplesGUI = (SmallmultipleGUI)mGUI;
  }

  /**
   * Refresh differential traversal of the model.
   */
  public void refreshDifferentialTraversal() {
    POOL.submit(new Callable<Void>() {
      @Override
      public Void call() {
        final int lastRevision = getLastRevision();
        for (int j = mDb.getRevisionNumber() + 1, i = mDb.getRevisionNumber() + 1; i <= lastRevision
          && i < j + REVS_TO_COMPARE; i++) {
          traverse(j - 1, i, ECompare.DIFFERENTIAL);
        }
        return null;
      }
    });
  }

  /**
   * Refresh incremental traversal of the model.
   */
  public void refreshIncrementalTraversal() {
    POOL.submit(new Callable<Void>() {
      @Override
      public Void call() {
        final int lastRevision = getLastRevision();
        for (int j = mDb.getRevisionNumber() + 1, i = mDb.getRevisionNumber() + 1; i <= lastRevision
          && i < j + REVS_TO_COMPARE; i++) {
          traverse(i - 1, i, ECompare.INCREMENTAL);
        }
        return null;
      }
    });
  }

  /**
   * Invoke model to traverse the tree.
   * 
   * @param pNewRevision
   *          new revision
   * @param pOldRevision
   *          old revision
   * @param pCompare
   *          determines how to compare (but basically just discriminates between a hybrid diff and
   *          incremental/differential)
   */
  private void traverse(final int pOldRevision, final int pNewRevision, final ECompare pCompare) {
    assert pOldRevision >= 0;
    assert pNewRevision > 0;
    assert pCompare != null;
    mContainer = new SunburstContainer(mSmallMultiplesGUI, mModel);
    mContainer.setLock(mLock).setNewStartKey(mDb.getNodeKey()).setModWeight(
      mSmallMultiplesGUI.getModificationWeight()).setOldRevision(pOldRevision).setRevision(pNewRevision)
      .setDepth(0).setCompare(pCompare).setMoveDetection(true);
    if (mGUI.getUsePruning()) {
      mContainer.setPruning(EPruning.DIFF_WITHOUT_SAMEHASHES);
    }
    mModel.traverseTree(mContainer);
  }

  /**
   * Refresh hybrid traversal of the model.
   */
  public void refreshHybridTraversal() {
    POOL.submit(new Callable<Void>() {
      @Override
      public Void call() {
        final int lastRevision = getLastRevision();
        ECompare.HYBRID.setValue(true);
        mContainer = new SunburstContainer(mSmallMultiplesGUI, mModel);
        mContainer.setLatch(getLatch()).setLock(mLock).setNewStartKey(mDb.getNodeKey()).setModWeight(
          mSmallMultiplesGUI.getModificationWeight()).setOldRevision(mDb.getRevisionNumber()).setRevision(
          (lastRevision < mDb.getRevisionNumber() + REVS_TO_COMPARE) ? lastRevision : mDb.getRevisionNumber()
            + REVS_TO_COMPARE).setDepth(0).setCompare(ECompare.HYBRID).setPruning(EPruning.ITEMSIZE);

        mModel.traverseTree(mContainer);
        resetLatch();
        for (int j = mDb.getRevisionNumber() + 1, i = mDb.getRevisionNumber() + 1; i <= lastRevision
          && i < j + REVS_TO_COMPARE; i++) {
          mContainer = new SunburstContainer(mSmallMultiplesGUI, mModel);
          mModel.traverseTree(mContainer.setLatch(getLatch()).setLock(mLock).setNewStartKey(mDb.getNodeKey())
            .setModWeight(mSmallMultiplesGUI.getModificationWeight()).setOldRevision(i - 1).setRevision(i)
            .setDepth(0).setCompare(ECompare.HYBRID).setPruning(EPruning.ITEMSIZE));
          resetLatch();
        }
        return null;
      }
    });
  }

  /** Reset {@link CountDownLatch}. */
  private void resetLatch() {
    try {
      final boolean done = getLatch().await(5, TimeUnit.MINUTES);
      if (!done) {
        throw new IllegalStateException("Hybrid traversal Failed!");
      }
    } catch (final InterruptedException e) {
      LOGWRAPPER.error(e.getMessage(), e);
      JOptionPane.showMessageDialog(mGUI.getParent(), e, e.getMessage(), JOptionPane.ERROR_MESSAGE);
    } finally {
      setLatch(new CountDownLatch(1));
    }
  }

  /**
   * Get last revision of currently opened resource
   * 
   * @return last revision
   */
  private int getLastRevision() {
    return mDb.getSession().getLastRevisionNumber();
  }

  /**
   * Get singleton instance.
   * 
   * @param pParent
   *          reference of class which extends {@link PApplet}
   * @param pModel
   *          {@link IModel} reference
   * @param pDB
   *          {@link ReadDB} reference
   * @return singelton instance of this class
   */
  public static synchronized SmallmultipleControl getInstance(final Embedded pParent,
    final SmallmultipleModel pModel, final ReadDB pDB) {
    if (mControl == null) {
      mControl = new SmallmultipleControl(pParent, pModel, pDB);
    }
    return mControl;
  }

  /**
   * Is getting called from processings keyRealeased-method and implements it.
   * 
   * @see processing.core.PApplet#keyReleased()
   */
  @Override
  public void keyReleased() {
    switch (mSmallMultiplesGUI.getParent().key) {
    case 's':
    case 'S':
      // Save PNG.
      mSmallMultiplesGUI.getParent()
        .saveFrame(ViewUtilities.SAVEPATH + ViewUtilities.timestamp() + "_##.png");
      break;
    case 'p':
    case 'P':
      // Save PDF.
      mSmallMultiplesGUI.setSavePDF(true);
      PApplet.println("\n" + "saving to pdf – starting");
      mSmallMultiplesGUI.getParent().beginRecord(PConstants.PDF,
        ViewUtilities.SAVEPATH + ViewUtilities.timestamp() + ".pdf");
      mSmallMultiplesGUI.getParent().textMode(PConstants.SHAPE);
      break;
    }

    if (mSmallMultiplesGUI.isShowGUI()) {
      mSmallMultiplesGUI.getControlP5().getGroup("menu").open();
    } else {
      mSmallMultiplesGUI.getControlP5().getGroup("menu").close();
    }
  }

  @Override
  protected AbsSunburstGUI getGUIInstance() {
    return SmallmultipleGUI.getInstance(mParent, this, mDb);
  }

  /**
   * Get the processing applet.
   * 
   * @return {@link PApplet} instance
   */
  public PApplet getApplet() {
    return mParent;
  }

  /** Release lock. */
  public void releaseLock() {
    mLock.release();
  }

  @Override
  public void controlEvent(final ControlEvent pControlEvent) {
    super.controlEvent(checkNotNull(pControlEvent));
  }

  @Override
  public void resetControl() {
    // Intentionally reset static instance.
    mControl = null;
  }

  @Override
  public void setNewMaxDepth(final int pDepthMax) {
  }

  @Override
  public void setOldMaxDepth(final int pDepthMax) {
  }
}
