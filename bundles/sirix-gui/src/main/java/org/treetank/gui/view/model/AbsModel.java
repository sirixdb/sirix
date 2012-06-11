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
package org.treetank.gui.view.model;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.slf4j.LoggerFactory;
import org.treetank.api.INodeReadTrx;
import org.treetank.api.ISession;
import org.treetank.diff.DiffFactory.EDiff;
import org.treetank.exception.AbsTTException;
import org.treetank.gui.ReadDB;
import org.treetank.gui.view.AbsObservableComponent;
import org.treetank.gui.view.IVisualItem;
import org.treetank.gui.view.model.interfaces.IContainer;
import org.treetank.gui.view.model.interfaces.IModel;
import org.treetank.gui.view.sunburst.EXPathState;
import org.treetank.gui.view.sunburst.SunburstContainer;
import org.treetank.gui.view.sunburst.SunburstItem;
import org.treetank.node.ENode;
import org.treetank.service.xml.shredder.EInsert;
import org.treetank.service.xml.xpath.XPathAxis;
import org.treetank.utils.LogWrapper;
import processing.core.PApplet;

/**
 * Abstract model, to simplify implementation of the {@link IModel} interface
 * and share common methods among implementations.
 * 
 * Note that all methods fail fast and either throw a {@link NullPointerException} if a reference peter is
 * {@code null} or throw {@link IllegalArgumentException} in case the argument isn't valid.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param <S>
 *          type of generic argument for {@link SunburstContainer}
 * @param <T>
 *          type of {@link IVisualItem}s
 * 
 */
@Nonnull
public abstract class AbsModel<S, T extends IVisualItem> extends AbsObservableComponent implements
  IModel<S, T> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(AbsModel.class));

  /** {@link List} of items. */
  protected transient List<T> mItems;

  /** Treetank {@link INodeReadTrx}. */
  protected transient INodeReadTrx mRtx;

  /** Treetank {@link ISession}. */
  protected transient ISession mSession;

  /**
   * {@link Deque} with {@link List}s of a {@link IVisualItem} implementation
   * for undo operation.
   */
  protected transient Deque<List<T>> mLastItems;

  /** {@link Deque} with depths for undo operation. */
  protected transient Deque<Integer> mLastDepths;

  /** {@link Deque} with depths for undo operation. */
  protected transient Deque<Integer> mLastOldDepths;

  /** The last maximum depth. */
  protected transient @Nonnegative
  int mLastMaxDepth;

  /** The last maximum depth in the old revision. */
  protected transient @Nonnegative
  int mLastOldMaxDepth;

  /**
   * Determines if XML fragments should be inserted as first child or as right
   * sibling of the current node.
   */
  protected transient EInsert mInsert;

  /** The processing {@link PApplet} core library. */
  protected final PApplet mParent;

  /** {@link ReadDB} instance. */
  protected transient AtomicReference<ReadDB> mDb;

  /** Index of the current {@link SunburstItem} for the iterator. */
  private transient int mIndex;

  /**
   * Constructor.
   * 
   * @param pApplet
   *          the processing {@link PApplet} core library
   * @param pDb
   *          {@link ReadDB} reference
   */
  protected AbsModel(@Nonnull final PApplet pApplet, @Nonnull final ReadDB pDb) {
    checkNotNull(pApplet);
    checkNotNull(pDb);
    mParent = pApplet;
    try {
      mSession = pDb.getSession();
      mRtx = mSession.beginNodeReadTrx(pDb.getRevisionNumber());
      mRtx.moveTo(pDb.getNodeKey());
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    mItems = new ArrayList<T>();
    mLastItems = new ArrayDeque<List<T>>();
    mLastDepths = new ArrayDeque<Integer>();
    mLastOldDepths = new ArrayDeque<Integer>();
    mDb = new AtomicReference<ReadDB>(pDb);
  }

  /**
   * Shutdown {@link ExecutorService}.
   * 
   * @param pPool
   *          thread pool; {@link ExecutorService} instance
   */
  public final void shutdown(final ExecutorService pPool) {
    pPool.shutdown(); // Disable new tasks from being submitted.
  }

  @Override
  public final void updateDb(final ReadDB pDb, final IContainer<S> pContainer) {
    checkNotNull(pDb);
    checkNotNull(pContainer);
    setDb(pDb);
    try {
      mSession = pDb.getSession();
      mRtx = mSession.beginNodeReadTrx(pDb.getRevisionNumber());
      mRtx.moveTo(pDb.getNodeKey());
    } catch (final AbsTTException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    mItems = new ArrayList<T>();
    mLastItems = new ArrayDeque<List<T>>();
    mLastDepths = new ArrayDeque<Integer>();
    traverseTree(pContainer);
  }

  @Override
  public void evaluateXPath(final String pXPathExpression) {
    checkNotNull(pXPathExpression);

    // Initialize all items to ISNOTFOUND.
    for (final T item : mItems) {
      ((SunburstItem)item).setXPathState(EXPathState.ISNOTFOUND);
    }

    if (!pXPathExpression.isEmpty()) {
      new Thread(new XPathEvaluation(pXPathExpression)).start();
    }
  }

  @Override
  public final void undo() {
    if (!mLastItems.isEmpty()) {
      // Go back one index in history list.
      mItems = mLastItems.pop();
      mLastMaxDepth = mLastDepths.pop();
      firePropertyChange("maxDepth", null, mLastMaxDepth);
      if (!mLastOldDepths.isEmpty()) {
        mLastOldMaxDepth = mLastOldDepths.pop();
      }
      if (mLastOldMaxDepth != 0) {
        firePropertyChange("oldMaxDepth", null, mLastOldMaxDepth);
      }

      // firePropertyChange("done", null, true);
    }
  }

  @Override
  public T getItem(final int pIndex) throws IndexOutOfBoundsException {
    return mItems.get(pIndex);
  }

  @Override
  public boolean hasNext() {
    boolean retVal = false;
    if (mIndex < mItems.size() - 1) {
      retVal = true;
    }
    return retVal;
  }

  @Override
  public T next() {
    T item = null;
    if (mIndex > mItems.size()) {
      throw new NoSuchElementException();
    }
    item = mItems.get(mIndex);
    mIndex++;
    assert item != null;
    return item;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove operation not supported!");
  }

  @Override
  public Iterator<T> iterator() {
    return mItems.iterator();
  }

  /**
   * Get a {@link ListIterator} starting {@code next()} at the specified index.
   * 
   * @param pIndex
   *          start index
   * @return a {@link ListIterator} instance starting at the specified index
   * @throws IndexOutOfBoundsException
   *           if the index is out of range
   *           ({@code pIndex < 0 || pIndex > size()})
   * @see List#listIterator(int)
   */
  public ListIterator<T> listIterator(final int pIndex) {
    return mItems.listIterator(pIndex);
  }

  @Override
  public int getDepthMax() {
    int depthMax = 0;
    for (final T item : mItems) {
      if (item.getDiff() == EDiff.SAME || item.getDiff() == EDiff.SAMEHASH) {
        int indexToParent = item.getIndexToParent();
        T tmpItem = item;
        while (indexToParent != -1
          && (tmpItem.getDiff() == EDiff.SAME || tmpItem.getDiff() == EDiff.SAMEHASH)) {
          indexToParent = tmpItem.getIndexToParent();
          if (indexToParent != -1) {
            tmpItem = mItems.get(indexToParent);
          }
        }
        if (indexToParent == -1) {
          depthMax = Math.max(depthMax, item.getDepth());
        }
      }
    }
    mLastOldMaxDepth = depthMax;
    return depthMax;
  }

  @Override
  public void setMinMax() {
    int minLength = Integer.MAX_VALUE;
    int maxLength = Integer.MIN_VALUE;

    int minValue = 1;
    int maxValue = Integer.MIN_VALUE;

    for (final T item : mItems) {
      final String text = item.getText();
      if (text == null) {
        final int value = (int)item.getValue();
        if (value > maxValue) {
          maxValue = value;
        }
      } else {
        final int length = text.length();
        if (length > maxLength) {
          maxLength = length;
        }
        if (length < minLength) {
          minLength = length;
        }
      }
    }

    for (final T item : mItems) {
      if (item.getText() == null) {
        item.setMinimum(minValue);
        item.setMaximum(maxValue);
      } else {
        item.setMinimum(minLength);
        item.setMaximum(maxLength);
      }
    }
  }

  @Override
  public List<T> subList(final int pFromIndex, final int pToIndex) {
    return Collections.unmodifiableList(mItems.subList(pFromIndex, pToIndex));
  }

  @Override
  public int getItemsSize() {
    return mItems.size();
  }

  @Override
  public void setNewDepthMax(final int pDepthMax) {
    checkArgument(pDepthMax >= 0, "pDepthMax must be >= 0!");
    mLastMaxDepth = pDepthMax;
  }

  @Override
  public void setOldDepthMax(final int pDepthMax) {
    checkArgument(pDepthMax >= 0, "pDepthMax must be >= 0!");
    mLastOldMaxDepth = pDepthMax;
  }

  /** Traverse a tree (single revision). */
  private final class XPathEvaluation implements Runnable {
    /** Treetank {@link INodeReadTrx}. */
    private transient INodeReadTrx mRTX;

    /** Key from which to start traversal. */
    private final long mKey;

    /** XPath query. */
    private final String mQuery;

    /**
     * Constructor.
     * 
     * @param pQuery
     *          The XPath query.
     */
    private XPathEvaluation(final String pQuery) {
      this(mRtx.getNode().getNodeKey(), pQuery);
    }

    /**
     * Constructor.
     * 
     * @param pKey
     *          Key from which to start traversal.
     * @param pQuery
     *          The XPath query.
     */
    private XPathEvaluation(final long pKey, final String pQuery) {
      assert pKey >= 0;
      assert pQuery != null;
      mKey = pKey;
      mQuery = pQuery;
      try {
        mRTX = mSession.beginNodeReadTrx(mRtx.getRevisionNumber());
        mRTX.moveTo(mKey);
        if (mRTX.getNode().getKind() == ENode.ROOT_KIND) {
          mRTX.moveToFirstChild();
        }
      } catch (final AbsTTException exc) {
        exc.printStackTrace();
      }
    }

    @Override
    public void run() {
      try {
        final Set<Long> nodeKeys = new HashSet<Long>();
        final XPathAxis axis = new XPathAxis(mRTX, mQuery);

        // Save found node keys with descendants.
        while (axis.hasNext()) {
          axis.next();
          final long key = axis.getTransaction().getNode().getNodeKey();
          nodeKeys.add(key);
          // for (final AbsAxis desc = new DescendantAxis(axis.getTransaction()); desc.hasNext(); desc
          // .next()) {
          // nodeKeys.add(desc.getTransaction().getItem().getKey());
          // }
          axis.getTransaction().moveTo(key);
        }

        // Do the work.
        final int processors = Runtime.getRuntime().availableProcessors();
        final ExecutorService executor = Executors.newFixedThreadPool(processors);
        for (int fromIndex = 0; fromIndex < mItems.size(); fromIndex += (int)(mItems.size() / processors)) {
          int toIndex = fromIndex + (int)(mItems.size() / processors);
          if (toIndex >= mItems.size()) {
            toIndex = mItems.size() - 1;
          }
          executor.submit(new XPathSublistEvaluation(nodeKeys, mItems.subList(fromIndex, toIndex)));
        }

        shutdown(executor);
        firePropertyChange("done", null, true);
      } catch (final AbsTTException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
  }

  /** XPath sublist evaluation. */
  private final class XPathSublistEvaluation implements Runnable {

    /** Treetank {@link INodeReadTrx}. */
    private transient INodeReadTrx mRTX;

    /** {@link List} of a {@link IVisualItem} implementation. */
    private final List<T> mItems;

    /** {@link List} of node keys which are in the result. */
    private final Set<Long> mKeys;

    /**
     * Constructor.
     * 
     * @param pNodeKeys
     *          Keys of nodes, which are in the result of an XPath query
     * @param pSublist
     *          Sublist which has to be searched for matches
     */
    private XPathSublistEvaluation(final Set<Long> pNodeKeys, final List<T> pSublist) {
      assert pNodeKeys != null && pSublist != null;
      mKeys = pNodeKeys;
      mItems = pSublist;
      try {
        mRTX = mSession.beginNodeReadTrx(mRtx.getRevisionNumber());
        mRTX.moveTo(mRtx.getNode().getNodeKey());
      } catch (final AbsTTException exc) {
        exc.printStackTrace();
      }
    }

    @Override
    public void run() {
      for (final T item : mItems) {
        for (final long key : mKeys) {
          if (item.getKey() == key) {
            item.setXPathState(EXPathState.ISFOUND);
          }
        }
      }
    }
  }

  /**
   * Set insert for shredding.
   * 
   * @param pInsert
   *          determines how to insert an XML fragment
   */
  @Override
  public void setInsert(@Nonnull final EInsert pInsert) {
    mInsert = checkNotNull(pInsert);
  }

  /**
   * Get the parent.
   * 
   * @return the parent
   */
  public PApplet getParent() {
    assert mParent != null;
    return mParent;
  }

  /**
   * Set new {@link ReadDB} instance.
   * 
   * @param pDb
   *          the {@link ReadDB} instance to set
   */
  public synchronized void setDb(final ReadDB pDb) {
    checkNotNull(pDb);
    final ReadDB currDB = mDb.get();
    if (!currDB.equals(pDb)) {
      currDB.close();
      mDb.set(pDb);
    }
  }

  /**
   * Get database handle.
   * 
   * @return the database access
   */
  @Override
  public ReadDB getDb() {
    assert mDb != null;
    return mDb.get();
  }

  @Override
  public void setItems(final List<T> pItems) {
    mItems = checkNotNull(pItems);
  }
}
