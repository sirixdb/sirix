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

package org.sirix.gui.view.sunburst.model;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.VisualItem;
import org.sirix.gui.view.VisualItemAxis;
import org.sirix.gui.view.model.AbstractModel;
import org.sirix.gui.view.model.TraverseCompareTree;
import org.sirix.gui.view.model.interfaces.Container;
import org.sirix.gui.view.sunburst.XPathState;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.SunburstView.Embedded;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import processing.core.PApplet;

import com.google.common.base.Optional;

/**
 * Model to compare revisions.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class SunburstCompareModel extends AbstractModel<SunburstContainer, SunburstItem> implements
  PropertyChangeListener {

  /** {@link LogWrapper}. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(SunburstCompareModel.class));

  /** {@link ExecutorService} reference to manage thread pool. */
  private static final ExecutorService mPool = Executors.newFixedThreadPool(Runtime.getRuntime()
    .availableProcessors());

  /** {@link BlockingQueue} for the results of a XPath query. */
//  private BlockingQueue<Long> mQueue;
  
  private List<Long> mResult;
  
  private CountDownLatch mLatch;

  /** {@link SunburstContainer} with some options for the traversal. */
  private SunburstContainer mContainer;

  /**
   * Constructor.
   * 
   * @param pApplet
   *          the processing {@link PApplet} core library
   * @param pDb
   *          {@link ReadDB} reference
   */
  public SunburstCompareModel(final PApplet pApplet, final ReadDB pDb) {
    super(pApplet, pDb);
  }

  @Override
  public synchronized void update(@Nonnull final Container<SunburstContainer> pContainer) {
    // Cast guaranteed to work.
    mContainer = (SunburstContainer)checkNotNull(pContainer);
    mLastItems.push(new ArrayList<SunburstItem>(mItems));
    mLastDepths.push(mLastMaxDepth);
    mLastOldDepths.push(mLastOldMaxDepth);
  }

  @Override
  public synchronized void traverseTree(@Nonnull final Container<SunburstContainer> pContainer) {
    // Cast guaranteed to work.
    mContainer = (SunburstContainer)checkNotNull(pContainer);
    final SunburstContainer container = (SunburstContainer)pContainer;
    if (container.getOldRevision() == -1) {
      container.setOldRevision(getDb().getRevisionNumber());
    }
    mPool.submit(new TraverseCompareTree(container));
  }

  @Override
  public synchronized final void evaluateXPath(@Nonnull final String pXPathExpression) {
    if (!pXPathExpression.isEmpty()) {
      final ReadDB db = mDb.get();
      mPool.submit(new TemporalXPathEvaluation(db.getSession(), db.getRevisionNumber(), mContainer
        .getRevision(), pXPathExpression));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void propertyChange(final PropertyChangeEvent pEvent) {
    switch (pEvent.getPropertyName().toLowerCase()) {
    case "oldrev":
      firePropertyChange("oldRev", null, (Integer)pEvent.getNewValue());
      break;
    case "newrev":
      firePropertyChange("newRev", null, (Integer)pEvent.getNewValue());
      break;
    case "oldmaxdepth":
      mLastOldMaxDepth = (Integer)pEvent.getNewValue();
      firePropertyChange("oldMaxDepth", null, mLastOldMaxDepth);
      break;
    case "maxdepth":
      mLastMaxDepth = (Integer)pEvent.getNewValue();
      firePropertyChange("maxDepth", null, mLastMaxDepth);
      break;
    case "done":
      firePropertyChange("done", null, true);
      mContainer.getLock().release();
      break;
    case "item":
      firePropertyChange("item", null, pEvent.getNewValue());
      break;
    case "items":
      mItems = (List<SunburstItem>)pEvent.getNewValue();
      firePropertyChange("items", null, mItems);
      ((Embedded)mParent).refresh(Optional.of(new VisualItemAxis(mItems)));
      break;
    case "progress":
      firePropertyChange("progress", null, pEvent.getNewValue());
      break;
    case "updated":
      firePropertyChange("updated", null, pEvent.getNewValue());
      break;
    }
  }

  /** XPath evaluation on an agglomeration. */
  private final class TemporalXPathEvaluation implements Callable<Void> {

    /** sirix {@link Session}. */
    private final Session mSession;

    /** New revision to open. */
    private final int mNewRevision;

    /** Old revision to open. */
    private final int mOldRevision;

    /** XPath query. */
    private final String mQuery;

    /**
     * Constructor.
     * 
     * @param pSession
     *          sirix {@link Session}
     * @param pRevision
     *          start revision to open
     * @param pKey
     *          key from which to start traversal
     * @param pQuery
     *          the XPath query
     * @throws AssertionError
     *           if {@code pSession} is {@code null}, {@code pNewRevision} is {@code less than 0},
     *           {@code pOldRevision} is {@code less than 0} or {@code pQuery} is {@code null}
     */
    private TemporalXPathEvaluation(final Session pSession, final int pNewRevision,
      final int pOldRevision, final String pQuery) {
      assert pSession != null;
      assert pOldRevision >= 0;
      assert pNewRevision >= 0;
      assert pQuery != null;
      mSession = pSession;
      mNewRevision = pNewRevision;
      mOldRevision = pOldRevision;
      mQuery = pQuery;
      mResult = Collections.synchronizedList(new ArrayList<Long>());//new LinkedBlockingQueue<>();
      mLatch = new CountDownLatch(3);
    }

    @Override
    public Void call() throws SirixException {
      final List<SunburstItem> sortedItems = new ArrayList<>(mItems);
      final ExecutorService executor = Executors.newFixedThreadPool(3);
      executor.submit(new XPathEvaluation(mSession, mOldRevision, mQuery));
      executor.submit(new XPathEvaluation(mSession, mNewRevision, mQuery));
      executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Collections.sort(sortedItems);
          mLatch.countDown();
          return null;
        }
      });
      executor.shutdown();
      
      try {
        mLatch.await(10, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }

//      final ExecutorService sameThreadExecutor = MoreExecutors.sameThreadExecutor();
//      final Future<List<Long>> xpathResultFuture = sameThreadExecutor.submit(new XPathResult());
//      sameThreadExecutor.shutdown();
//
//      int i = 0;
//      List<Long> xpathResult = null;
//      try {
//        xpathResult = new ArrayList<>(xpathResultFuture.get());
//      } catch (final InterruptedException | ExecutionException e) {
//        LOGWRAPPER.error(e.getMessage(), e);
//      }
      
      Collections.sort(mResult);

      // Initialize all items to ISNOTFOUND.
      for (final SunburstItem item : sortedItems) {
        item.setXPathState(XPathState.ISNOTFOUND);
      }
      final Iterator<Long> xpath = mResult.iterator();
      long nextResult = Integer.MIN_VALUE;
      if (xpath.hasNext()) {
        nextResult = xpath.next();
      }

      for (int i = 0; i < sortedItems.size() && nextResult != Integer.MIN_VALUE;) {
        VisualItem item = sortedItems.get(i);

        if (item.getKey() == nextResult) {
          i++;
          item.setXPathState(XPathState.ISFOUND);
          if (xpath.hasNext()) {
            item = sortedItems.get(i);
            if (item.getKey() == nextResult) {
              item.setXPathState(XPathState.ISFOUND);
            }
            nextResult = xpath.next();
          } else {
            nextResult = Integer.MIN_VALUE;
          }
        } else {
          if (item.getKey() > nextResult) {
            if (xpath.hasNext()) {
              nextResult = xpath.next();
            } else {
              nextResult = Integer.MIN_VALUE;
            }
          } else {
            i++;
          }
        }
      }

      firePropertyChange("done", null, true);
      return null;
    }
  }

  /** Evaluate an XPath query on one revision. */
  private final class XPathEvaluation implements Callable<Void> {

    /** sirix {@link Session}. */
    private final Session mSession;

    /** The revision to open. */
    private final int mRevision;

    /** XPath query. */
    private final String mQuery;

    /**
     * Constructor.
     * 
     * @param pSession
     *          sirix {@link Session}
     * @param pRevision
     *          the revision to open
     * @param pQuery
     *          XPath query
     */
    private XPathEvaluation(final Session pSession, final int pRevision, final String pQuery) {
      assert pSession != null;
      assert pRevision >= 0;
      assert pQuery != null;
      mSession = pSession;
      mRevision = pRevision;
      mQuery = pQuery;
    }

    @Override
    public Void call() throws Exception {
      // final XPathSelector selector =
      // new XPathEvaluator.Builder(mQuery, mSession).setRevision(mRevision).build().call();
      // for (final XdmItem item : selector) {
      // if (item instanceof XdmNode) {
      // final XdmNode node = (XdmNode)item;
      // final NodeWrapper wrapped = (NodeWrapper)node.getUnderlyingValue();
      // mQueue.put(wrapped.getKey());
      // }
      // }
      final NodeReadTrx rtx = mSession.beginNodeReadTrx(mRevision);
      final Axis axis = new XPathAxis(rtx, mQuery);
      for (final long nodeKey : axis) {
        mResult.add(nodeKey);
      }
//      mQueue.put(-1L);
      mLatch.countDown();
      rtx.close();
      return null;
    }
  }

//  /** Sort XPath results taken from a {@BlockingQueue}. */
//  private final class XPathResult implements Callable<List<Long>> {
//
//    /** {@link List} of nodeKeys which are the result of the XPath query on two revisions. */
//    private final List<Long> mXPathResult;
//
//    /** Constructor. */
//    private XPathResult() {
//      mXPathResult = new ArrayList<>();
//    }
//
//    @Override
//    public List<Long> call() throws AbsTTException, InterruptedException {
//      int counter = 0;
//      while (true) {
//        final long key = mQueue.take();
//        if (key == -1L) {
//          if (++counter == 2) {
//            break;
//          }
//        } else {
//          mXPathResult.add(key);
//          if (mXPathResult.size() >= 2) {
//            // for (int i = 0; i < mXPathResult.size(); i++) {
//            // if (key > mXPathResult.get(i)) {
//            int j = mXPathResult.size() - 1;
//            while (j > 0 && mXPathResult.get(j - 1) > key) {
//              mXPathResult.set(j, mXPathResult.get(j - 1));
//              j--;
//            }
//            if (!(j > 0 && mXPathResult.get(j - 1) == key)) {
//              mXPathResult.set(j, key);
//            }
//            // }
//            // }
//            // insertionSort();
//          }
//        }
//      }
//
//      return mXPathResult;
//    }
//
//    // /** Implementation of the InsertionSort algorithm. */
//    // private void insertionSort() {
//    // for (int i = 1, j = 1; i < mXPathResult.size(); i++) {
//    // final long newValue = mXPathResult.get(i);
//    // j = i;
//    // while (j > 0 && mXPathResult.get(j - 1) > newValue) {
//    // mXPathResult.set(j, mXPathResult.get(j - 1));
//    // j--;
//    // }
//    // if (!(j > 0 && mXPathResult.get(j - 1) == newValue)) {
//    // mXPathResult.set(j, newValue);
//    // }
//    // }
//    // }
//  }

  public Optional<VisualItemAxis> getItems(final int pIndex) {
    checkArgument(pIndex >= 0, "pIndex must be >= 0!");
    return Optional.of(new VisualItemAxis(mItems, pIndex - 1));
  }
}
