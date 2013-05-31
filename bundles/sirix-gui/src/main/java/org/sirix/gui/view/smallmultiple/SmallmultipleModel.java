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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.gui.ReadDB;
import org.sirix.gui.view.model.AbstractModel;
import org.sirix.gui.view.model.interfaces.Container;
import org.sirix.gui.view.sunburst.Draw;
import org.sirix.gui.view.sunburst.EGreyState;
import org.sirix.gui.view.sunburst.SunburstContainer;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.control.SunburstControl;
import org.sirix.gui.view.sunburst.model.SunburstCompareModel;

import processing.core.PApplet;

/**
 * Small multiples model. Can be easily extended through the usage of composition.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SmallmultipleModel extends AbstractModel<SunburstContainer, SunburstItem> implements
  PropertyChangeListener {

  /** Thread pool. */
  private static final ExecutorService POOL = Executors.newFixedThreadPool(Runtime.getRuntime()
    .availableProcessors());

  /** {@link SunbburstCompareModel} reference. */
  private final SunburstCompareModel mModel;

  /** {@link SunburstContainer} reference. */
  private transient SunburstContainer mContainer;

  /** {@link List} of {@link SunburstItem}s. */
  private transient List<SunburstItem> mDiffItems;

  /** Dot brightness to set in the GUI. */
  private transient float mDotBrightness = 25f;

  /** Datastructure which captures {@link SunburstItem}s for each comparsion. */
  private transient List<SunburstListContainer> mCompItems;

  private long mRevision;

  // /** Datastructure which captures {@link SunburstItem}s for each comparsion. */
  // private transient Map<Integer, SunburstListContainer> mInitialCompItems;
  //
  // private transient boolean mFirst;
  //
  // /** Revision of the currently loaded revision. */
  // private transient long mOldRevision;

  /**
   * Constructor.
   * 
   * @param pApplet
   *          the processing {@link PApplet} core library
   * @param pDb
   *          {@link ReadDB} reference
   * @param pControl
   *          {@link SunburstControl} implementation
   */
  public SmallmultipleModel(final PApplet pApplet, @Nonnull final ReadDB pDb) {
    super(pApplet, pDb);
    mModel = new SunburstCompareModel(pApplet, pDb);
    mModel.addPropertyChangeListener(this);
    mCompItems = new ArrayList<>(4);
  }

  /**
   * Set current {@link List} of {@link SunburstItem}s for one SunburstView.
   * 
   * @param pIndex
   *          index which points in the datastructure
   */
  public void setItems(@Nonnegative final int pIndex) {
    checkArgument(pIndex >= 0 && pIndex < mCompItems.size());
    final SunburstListContainer container = mCompItems.get(pIndex);
    firePropertyChange("oldMaxDepth", null, container.mOldMaxDepth);
    firePropertyChange("maxDepth", null, container.mMaxDepth);
    mItems = container.mItems;
  }

  @Override
  public void update(final Container<SunburstContainer> pContainer) {
    mModel.update(checkNotNull(pContainer));
  }

  @Override
  public synchronized void traverseTree(final Container<SunburstContainer> pContainer) {
    mContainer = (SunburstContainer)pContainer;
    mModel.traverseTree(checkNotNull(pContainer));
  }

  @SuppressWarnings("unchecked")
  @Override
  public void propertyChange(final PropertyChangeEvent pEvent) {
    switch (pEvent.getPropertyName().toLowerCase()) {
    case "revision":
      mRevision = (int)pEvent.getNewValue();
      break;
    case "dotbrightness":
      firePropertyChange("dotBrightness", null, pEvent.getNewValue());
      break;
    case "saturation":
      firePropertyChange("saturation", null, pEvent.getNewValue());
      break;
    case "oldrev":
      firePropertyChange("oldRev", null, pEvent.getNewValue());
      break;
    case "newrev":
      firePropertyChange("newRev", null, pEvent.getNewValue());
      break;
    case "progress":
      firePropertyChange("progress", null, pEvent.getNewValue());
      break;
    case "oldmaxdepth":
      mLastOldMaxDepth = (Integer)pEvent.getNewValue();
      if (mContainer.getCompare() == ECompare.HYBRID) {
        if (ECompare.HYBRID.getValue()) {
          firePropertyChange("oldMaxDepth", null, mLastOldMaxDepth);
        }
      } else {
        firePropertyChange("oldMaxDepth", null, mLastOldMaxDepth);
      }
      break;
    case "maxdepth":
      mLastMaxDepth = (Integer)pEvent.getNewValue();

      if (mContainer.getCompare() == ECompare.HYBRID) {
        if (ECompare.HYBRID.getValue()) {
          firePropertyChange("maxDepth", null, mLastMaxDepth);
        }
      } else {
        firePropertyChange("maxDepth", null, mLastMaxDepth);
      }
      break;
    case "done":
      POOL.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          processDone();
          return null;
        }
      });
      break;
    case "items":
      mItems = (List<SunburstItem>)pEvent.getNewValue();
      break;
    }
  }

  /** Process the "done" event. */
  public void processDone() {
    switch (mContainer.getCompare()) {
    case DIFFERENTIAL:
    case INCREMENTAL:
      // Add new items.
      addItems();

      // Done.
      firePropertyChange("done", null, true);
      break;
    case HYBRID:
      if (ECompare.HYBRID.getValue()) {
        ECompare.HYBRID.setValue(false);
        mDiffItems = mItems;
        for (final SunburstItem item : mDiffItems) {
          item.setGreyState(EGreyState.YES);
          item.setRevision(mContainer.getOldRevision());
        }
        mContainer.getLock().release();
      } else {
        // Compare lists and grey out.
        compareLists(mItems, mDiffItems);
        mItems = mDiffItems;

        // Adjust dot brightness and saturation.
        firePropertyChange("dotBrightness", null, mDotBrightness);
        // Can be the same as mDotBrightness because both values range from 0 to 100.
        firePropertyChange("saturation", null, mDotBrightness);
        mDotBrightness += 25f;

        // Adapt revision.
        for (int j = 0; j < mCompItems.size(); j++) {
          final List<SunburstItem> prevItems = mCompItems.get(j).mItems;
          for (int i = 0; i < mItems.size(); i++) {
            prevItems.get(i).setRevision(mItems.get(i).getRevision());
          }
        }

        // Add new items.
        final List<SunburstItem> currItems = new ArrayList<>();
        for (int i = 0; i < mItems.size(); i++) {
          final SunburstItem newItem = new SunburstItem(mItems.get(i));
          newItem.update(Draw.UPDATEBUFFER, 2, mParent.g);
          currItems.add(newItem);
        }
        mCompItems.add(new SunburstListContainer(mLastMaxDepth, mLastOldMaxDepth, currItems, mContainer
          .getRevision()));

        // Done.
        firePropertyChange("done", null, true);
      }
      mContainer.getLatch().countDown();
      break;
    //
    // if (ECompare.HYBRID.getValue()) {
    // // Add new items.
    // if (mDiffItems == null) {
    // mDiffItems = new ArrayList<>();
    // for (int i = 0; i < mItems.size(); i++) {
    // final SunburstItem newItem = new SunburstItem(mItems.get(i));
    // newItem.update(2, mParent.g);
    // mDiffItems.add(newItem);
    // }
    // mOldRevision = mContainer.getOldRevision();
    // } else {
    // addDiffItems();
    // // mDiffItems.removeAll(mItems);
    // // System.out.println("diff items size: " + mDiffItems.size());
    // }
    //
    // // Add new items.
    // addItems(mInitialCompItems);
    //
    // mContainer.getLock().release();
    // AbsSunburstControl.mLatch.countDown();
    // } else {
    // // Reset counter.
    // if (mFirst) {
    // mFirst = false;
    // mCounter = 0;
    //
    // // Initialize items.
    // for (final SunburstItem item : mDiffItems) {
    // item.setGreyState(EGreyState.YES);
    // item.setRevision(mOldRevision);
    // }
    // }
    //
    // // Get the current list of items.
    // mItems = mInitialCompItems.get(mCounter).mItems;
    //
    // // Compare lists and grey out.
    // compareLists(mItems, mDiffItems);
    // mItems = mDiffItems;
    //
    // // Adapt revision.
    // for (int j = 0; j < mCompItems.size(); j++) {
    // final List<SunburstItem> prevItems = mCompItems.get(j).mItems;
    // for (int i = 0; i < mItems.size(); i++) {
    // if (i < prevItems.size()) {
    // prevItems.get(i).setRevision(mItems.get(i).getRevision());
    // }
    // }
    // }
    //
    // // Add new items.
    // addItems(mCompItems);
    //
    // // Adjust dot brightness and saturation.
    // firePropertyChange("dotBrightness", null, mDotBrightness);
    // // Can be the same as mDotBrightness because both values range from 0 to 100.
    // firePropertyChange("saturation", null, mDotBrightness);
    // mDotBrightness += 25f;
    //
    // // Done.
    // firePropertyChange("done", null, true);
    // }
    // break;
    }
  }

  // /**
  // * Add {@link SunburstItem}s which have been either changed by {@code startangle}/{@code endangle} or
  // * have been added.
  // */
  // private void addDiffItems() {
  // compare(mItems, mDiffItems, new AddItems());
  //
  // // Add items which are at the end of the list.
  // if (mItems.size() > mDiffItems.size()) {
  // for (int i = mItems.size(); i < mDiffItems.size();) {
  // mDiffItems.add(mItems.get(i));
  // }
  // }
  // }

  private void compare(final List<SunburstItem> pFirst, final List<SunburstItem> pSecond,
    final Function<Indexes> pFunction) {
    assert pFirst != null;
    assert pSecond != null;

    for (int i = 0, j = 0; i < pFirst.size() && j < pSecond.size();) {
      final SunburstItem firstItem = pFirst.get(i);
      final SunburstItem secondItem = pSecond.get(j);
      final Indexes indexes = pFunction.apply(firstItem, secondItem, i, j);
      i = indexes.mFirstIndex;
      j = indexes.mSecondIndex;
    }
  }

  /**
   * Add new items.
   */
  private void addItems() {
    final List<SunburstItem> currItems = new ArrayList<SunburstItem>();
    for (int i = 0; i < mItems.size(); i++) {
      final SunburstItem newItem = new SunburstItem(mItems.get(i));
      newItem.update(Draw.UPDATEBUFFER, 2, mParent.g);
      currItems.add(newItem);
    }
    mCompItems.add(new SunburstListContainer(mLastMaxDepth, mLastOldMaxDepth, currItems, mRevision));
    Collections.sort(mCompItems);
  }

  /**
   * Compare two {@link List}s and set {@link GreyState} appropriately as well as the revision to which the
   * {@link SunburstItem}s belong.
   * 
   * @param pFirst
   *          first list
   * @param pSecond
   *          second list
   */
  private void compareLists(final List<SunburstItem> pFirst, final List<SunburstItem> pSecond) {
    assert pFirst != null;
    assert pSecond != null;

    for (final SunburstItem item : pSecond) {
      if (item.getGreyState() == EGreyState.NO) {
        item.setRevision(item.getRevision() + 1);
      }
    }
    final List<SunburstItem> secondList = new ArrayList<SunburstItem>(pSecond);
    Collections.sort(pFirst);
    Collections.sort(secondList);
    compare(pFirst, secondList, new GreyState());
  }

  private static class GreyState implements Function<Indexes> {
    @Override
    public Indexes apply(final SunburstItem pFirst, final SunburstItem pSecond, int pIndexFirst,
      int pIndexSecond) {
      pSecond.setGreyState(EGreyState.NO);
      if (pFirst.equals(pSecond)) {
        pIndexFirst++;
        pIndexSecond++;
      } else {
        if (pFirst.getKey() > pSecond.getKey()) {
          pIndexSecond++;
        } else {
          pIndexFirst++;
        }
      }

      return new Indexes(pIndexFirst, pIndexSecond);
    };
  }

  // private class AddItems implements IFunction<Indexes> {
  //
  // /** {@link SunburstItemContentEquivalence} relation. */
  // private final SunburstItemContentEquivalence mEquivalence = new SunburstItemContentEquivalence();
  //
  // /** {@inheritDoc} */
  // @Override
  // public Indexes apply(final SunburstItem pFirst, final SunburstItem pSecond,
  // int pIndexFirst, int pIndexSecond) {
  //
  // if (pFirst.getKey() == pSecond.getKey()) {
  // if (!mEquivalence.doEquivalent(pFirst, pSecond)) {
  // mDiffItems.remove(pIndexSecond);
  // mDiffItems.add(pIndexSecond, pFirst);
  // }
  // pIndexFirst++;
  // pIndexSecond++;
  // } else {
  // mDiffItems.add(pIndexSecond++, pFirst);
  // // if (pIndexFirst == pIndexSecond) {
  // // pIndexFirst++;
  // // pIndexSecond++;
  // // } else if (pIndexFirst > pIndexSecond) {
  // // pIndexSecond++;
  // // } else {
  // // assert pIndexFirst < pIndexSecond;
  // // pIndexFirst++;
  // // }
  // }
  //
  // return new Indexes(pIndexFirst, pIndexSecond);
  // };
  // }
}
