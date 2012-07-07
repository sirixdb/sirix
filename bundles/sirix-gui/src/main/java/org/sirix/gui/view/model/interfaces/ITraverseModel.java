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

package org.sirix.gui.view.model.interfaces;

import com.google.common.base.Optional;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import org.sirix.api.INodeReadTrx;
import org.sirix.gui.view.sunburst.Item;
import org.sirix.gui.view.sunburst.axis.SunburstDescendantAxis;
import org.sirix.gui.view.sunburst.SunburstItem;
import org.sirix.gui.view.sunburst.model.Modification;

/**
 * Interface which has to be implemented from traversal models. That is a class which encapsulates all
 * stuff related to traverse a {@link Resource}/a document tree.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public interface ITraverseModel extends IObservable {

  /**
   * Is sent to observers once all descendant-or-self counts for each node have been submitted to a
   * {@link ExecutorService} and fired.
   */
  int DESCENDANTS_DONE = -1;

  /** Depth to prune. */
  int DEPTH_TO_PRUNE = 2;

  /** Factor to add to weighting of modifications. */
  int FACTOR = 30;

  /** Angle to prune in radians. */
  double ANGLE_TO_PRUNE = 0.02d;

  /**
   * Create a {@link SunburstItem} used as a callback method in {@link SunburstDescendantAxis}.
   * 
   * @param pItem
   *          {@link Item} reference
   * @param pDepth
   *          current depth in the tree
   * @param pIndex
   *          index of the current item
   * @return child extension
   * @throws NullPointerException
   *           if pItem is {@code null}
   * @throws IllegalArgumentException
   *           if {@code paramDepth < 0} or {@code paramIndex < 0}
   * 
   */
  float createSunburstItem(@Nonnull Item pItem, int pDepth, int pIndex);

  /**
   * Write a descendants per node in a {@link BlockingQueue}.
   * 
   * @param pRtx
   *          optional {@link INodeReadTrx} instance
   * @throws ExecutionException
   *           if execution fails
   * @throws InterruptedException
   *           if task gets interrupted
   * @throws NullPointerException
   *           if {@code paramRtx} is {@code null}
   */
  void descendants(@Nonnull Optional<INodeReadTrx> pRtx) throws InterruptedException, ExecutionException;

  /**
   * Get minimum and maximum global text length.
   * 
   * @param pNewRtx
   *          sirix {@link INodeReadTrx} instance (on new revision)
   * @param pOldRtx
   *          sirix {@link INodeReadTrx} instance (on old revision / optional)
   * @throws NullPointerException
   *           if {@code pNewRtx} or {@code pOldRtx} is {@code null}
   */
  void getMinMaxTextLength(@Nonnull INodeReadTrx pNewRtx, @Nonnull Optional<INodeReadTrx> pOldRtx);

  /**
   * Get if current item has been pruned or not.
   * 
   * @return true if it has been pruned, false otherwise
   */
  boolean getIsPruned();

  /**
   * Get modification count for each node stored in a {@link BlockingQueue}.
   * 
   * @return {@link BlockingQueue} instance
   */
  BlockingQueue<Future<Modification>> getModificationQueue();
}
