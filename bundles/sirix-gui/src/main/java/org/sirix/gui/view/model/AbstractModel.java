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
package org.sirix.gui.view.model;

import static com.google.common.base.Preconditions.checkArgument;
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
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.brackit.xquery.update.Insert;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.exception.SirixException;
import org.sirix.gui.ReadDB;
import org.sirix.gui.view.AbstractObservableComponent;
import org.sirix.gui.view.VisualItem;
import org.sirix.gui.view.model.interfaces.Container;
import org.sirix.gui.view.model.interfaces.Model;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.XPathState;
import org.sirix.node.Kind;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;
import processing.core.PApplet;

/**
 * Abstract model, to simplify implementation of the {@link Model} interface and share common
 * methods among implementations.
 *
 * Note that all methods fail fast and either throw a {@link NullPointerException} if a reference
 * peter is {@code null} or throw {@link IllegalArgumentException} in case the argument isn't valid.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 * @param <S> type of generic argument for {@link SunburstContainer}
 * @param <T> type of {@link VisualItem}s
 *
 */
@NonNull
public abstract class AbstractModel<S, T extends VisualItem> extends AbstractObservableComponent
    implements Model<S, T> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(AbstractModel.class));

  /** {@link List} of items. */
  protected volatile List<T> mItems;

  /** Read-only transaction. */
  protected transient XmlNodeReadOnlyTrx mRtx;

  /** The resource manager. */
  protected transient XmlResourceManager mSession;

  /**
   * {@link Deque} with {@link List}s of a {@link VisualItem} implementation for undo operation.
   */
  protected transient Deque<List<T>> mLastItems;

  /** {@link Deque} with depths for undo operation. */
  protected transient Deque<Integer> mLastDepths;

  /** {@link Deque} with depths for undo operation. */
  protected transient Deque<Integer> mLastOldDepths;

  /** The last maximum depth. */
  protected transient @NonNegative int mLastMaxDepth;

  /** The last maximum depth in the old revision. */
  protected transient @NonNegative int mLastOldMaxDepth;

  /**
   * Determines if XML fragments should be inserted as first child or as right sibling of the current
   * node.
   */
  protected transient Insert mInsert;

  /** The processing {@link PApplet} core library. */
  protected final PApplet mParent;

  /** {@link ReadDB} instance. */
  protected transient AtomicReference<ReadDB> mDb;

  /** Index of the current {@link SunburstItem} for the iterator. */
  private transient int mIndex;

  /**
   * Constructor.
   *
   * @param pApplet the processing {@link PApplet} core library
   * @param pDb {@link ReadDB} reference
   */
  protected AbstractModel(final PApplet pApplet, @NonNull final ReadDB pDb) {
    checkNotNull(pApplet);
    checkNotNull(pDb);
    mParent = pApplet;
    try {
      mSession = pDb.getSession();
      mRtx = mSession.beginNodeReadTrx(pDb.getRevisionNumber());
      mRtx.moveTo(pDb.getNodeKey());
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    mItems = new ArrayList<>();
    mLastItems = new ArrayDeque<>();
    mLastDepths = new ArrayDeque<>();
    mLastOldDepths = new ArrayDeque<>();
    mDb = new AtomicReference<>(pDb);
  }

  /**
   * Shutdown {@link ExecutorService}.
   *
   * @param pPool thread pool; {@link ExecutorService} instance
   */
  public final void shutdown(final ExecutorService pPool) {
    pPool.shutdown(); // Disable new tasks from being submitted.
  }

  @Override
  public final void updateDb(final ReadDB pDb, @NonNull final Container<S> pContainer) {
    checkNotNull(pDb);
    checkNotNull(pContainer);
    setDb(pDb);
    try {
      mSession = pDb.getSession();
      mRtx = mSession.beginNodeReadTrx(pDb.getRevisionNumber());
      mRtx.moveTo(pDb.getNodeKey());
    } catch (final SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    mItems = new ArrayList<>();
    mLastItems = new ArrayDeque<>();
    mLastDepths = new ArrayDeque<>();
    traverseTree(pContainer);
  }

  @Override
  public void evaluateXPath(final String pXPathExpression) {
    checkNotNull(pXPathExpression);

    // Initialize all items to ISNOTFOUND.
    for (final T item : mItems) {
      ((SunburstItem) item).setXPathState(XPathState.ISNOTFOUND);
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
   * @param pIndex start index
   * @return a {@link ListIterator} instance starting at the specified index
   * @throws IndexOutOfBoundsException if the index is out of range (
   *         {@code pIndex < 0 || pIndex > size()})
   * @see List#listIterator(int)
   */
  public ListIterator<T> listIterator(final int pIndex) {
    return mItems.listIterator(pIndex);
  }

  @Override
  public int getDepthMax() {
    int depthMax = 0;
    for (final T item : mItems) {
      if (item.getDiff() == DiffType.SAME || item.getDiff() == DiffType.SAMEHASH) {
        int indexToParent = item.getIndexToParent();
        T tmpItem = item;
        while (indexToParent != -1 && (tmpItem.getDiff() == DiffType.SAME || tmpItem.getDiff() == DiffType.SAMEHASH)) {
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
        final int value = (int) item.getValue();
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
    /** sirix {@link NodeReadTrx}. */
    private transient XmlNodeReadTrx mRtx;

    /** Key from which to start traversal. */
    private final long mKey;

    /** XPath query. */
    private final String mQuery;

    /**
     * Constructor.
     *
     * @param pQuery The XPath query.
     */
    private XPathEvaluation(final String pQuery) {
      this(mRtx.getNodeKey(), pQuery);
    }

    /**
     * Constructor.
     *
     * @param pKey Key from which to start traversal.
     * @param pQuery The XPath query.
     */
    private XPathEvaluation(final long pKey, final String pQuery) {
      assert pKey >= 0;
      assert pQuery != null;
      mKey = pKey;
      mQuery = pQuery;
      try {
        mRtx = mSession.beginNodeReadTrx(mRtx.getRevisionNumber());
        mRtx.moveTo(mKey);
        if (mRtx.getKind() == Kind.DOCUMENT) {
          mRtx.moveToFirstChild();
        }
      } catch (final SirixException exc) {
        exc.printStackTrace();
      }
    }

    @Override
    public void run() {
      try {
        final Set<Long> nodeKeys = new HashSet<>();
        final XPathAxis axis = new XPathAxis(mRtx, mQuery);

        // Save found node keys with descendants.
        while (axis.hasNext()) {
          axis.next();
          final long key = axis.getTrx().getNodeKey();
          nodeKeys.add(key);
          axis.getTrx().moveTo(key);
        }

        // Do the work.
        final int processors = Runtime.getRuntime().availableProcessors();
        final ExecutorService executor = Executors.newFixedThreadPool(processors);
        for (int fromIndex = 0; fromIndex < mItems.size(); fromIndex += mItems.size() / processors) {
          int toIndex = fromIndex + mItems.size() / processors;
          if (toIndex >= mItems.size()) {
            toIndex = mItems.size() - 1;
          }
          executor.submit(new XPathSublistEvaluation(nodeKeys, mItems.subList(fromIndex, toIndex)));
        }

        shutdown(executor);
        firePropertyChange("done", null, true);
      } catch (final SirixException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
  }

  /** XPath sublist evaluation. */
  private final class XPathSublistEvaluation implements Runnable {

    /** sirix {@link NodeReadTrx}. */
    private transient NodeReadTrx mRTX;

    /** {@link List} of a {@link VisualItem} implementation. */
    private final List<T> mItems;

    /** {@link List} of node keys which are in the result. */
    private final Set<Long> mKeys;

    /**
     * Constructor.
     *
     * @param pNodeKeys Keys of nodes, which are in the result of an XPath query
     * @param pSublist Sublist which has to be searched for matches
     */
    private XPathSublistEvaluation(final Set<Long> pNodeKeys, final List<T> pSublist) {
      assert pNodeKeys != null && pSublist != null;
      mKeys = pNodeKeys;
      mItems = pSublist;
      try {
        mRTX = mSession.beginNodeReadTrx(mRtx.getRevisionNumber());
        mRTX.moveTo(mRtx.getNodeKey());
      } catch (final SirixException exc) {
        exc.printStackTrace();
      }
    }

    @Override
    public void run() {
      for (final T item : mItems) {
        for (final long key : mKeys) {
          if (item.getKey() == key) {
            item.setXPathState(XPathState.ISFOUND);
          }
        }
      }
    }
  }

  /**
   * Set insert for shredding.
   *
   * @param pInsert determines how to insert an XML fragment
   */
  @Override
  public void setInsert(final Insert pInsert) {
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
   * @param pDb the {@link ReadDB} instance to set
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
