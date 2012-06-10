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

package org.treetank.service.xml.xpath.concurrent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.treetank.api.INodeReadTrx;
import org.treetank.axis.AbsAxis;
import org.treetank.settings.EFixed;

/**
 * <h1>ConcurrentAxis</h1>
 * <p>
 * Realizes in combination with the <code>ConurrentAxisHelper</code> the concurrent evaluation of pipeline
 * steps. The given axis is uncoupled from the main thread by embedding it in a Runnable that uses its one
 * transaction and stores all the results to a queue. The ConcurrentAxis gets the computed results from that
 * queue one by one on every hasNext() call and sets the main-transaction to it. As soon as the end of the
 * computed result sequence is reached (marked by the NULL_NODE_KEY), the ConcurrentAxis returns
 * <code>false</code>.
 * </p>
 * <p>
 * This framework is working according to the producer-consumer-principle, where the ConcurrentAxisHelper and
 * its encapsulated axis is the producer and the ConcurrentAxis with its callees is the consumer This can be
 * used by any class that implements the IAxis interface. Note: Make sure that the used class is thread-safe.
 * </p>
 */
public class ConcurrentAxis extends AbsAxis {

  /**
   * Axis that is running in an own thread and produces results for this axis.
   */
  private final AbsAxis mProducer;

  /**
   * Queue that stores result keys already computed by the producer. End of
   * the result sequence is marked by the NULL_NODE_KEY.
   */
  private final BlockingQueue<Long> mResults;

  /** Capacity of the mResults queue. */
  private final int M_CAPACITY = 10;

  /** Has axis already been called? */
  private boolean mFirst;

  /** Runnable in which the producer is running. */
  private Runnable task;

  /** Is axis already finished and has no results left? */
  private boolean mFinished;

  /** Size of thread pool for executor service. */
  private static int THREADPOOLSIZE = 2;

  /** Executor Service holding the execution plan for future tasks. */
  public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(THREADPOOLSIZE);

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param paramRtx
   *          Exclusive (immutable) trx to iterate with.
   * @param paramChildAxis
   *          Producer axis.
   */
  public ConcurrentAxis(final INodeReadTrx paramRtx, final AbsAxis paramChildAxis) {
    super(paramRtx);
    mResults = new ArrayBlockingQueue<Long>(M_CAPACITY);
    mFirst = true;
    mProducer = paramChildAxis;
    task = new ConcurrentAxisHelper(getTransaction(), mProducer, mResults);
    mFinished = false;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void reset(final long nodeKey) {

    super.reset(nodeKey);
    mFirst = true;
    mFinished = false;

    if (mProducer != null) {
      mProducer.reset(nodeKey);
    }
    if (mResults != null) {
      mResults.clear();
    }

    if (task != null) {
      task = new ConcurrentAxisHelper(getTransaction(), mProducer, mResults);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized boolean hasNext() {

    resetToLastKey();

    // start producer on first call
    if (mFirst) {
      mFirst = false;
      ConcurrentAxis.EXECUTOR.submit(task);
    }

    if (mFinished) {
      resetToStartKey();
      return false;
    }

    // long result = IReadTransaction.NULL_NODE_KEY;
    long result = EFixed.NULL_NODE_KEY.getStandardProperty();

    try {
      // get result from producer as soon as it is available
      result = mResults.take();
      // System.out.println("get: " + d.getC() +" "+ result );
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }

    // NULL_NODE_KEY marks end of the sequence computed by the producer
    if (result != EFixed.NULL_NODE_KEY.getStandardProperty()) {
      getTransaction().moveTo(result);
      return true;
    }

    mFinished = true;
    resetToStartKey();
    // EXECUTOR.shutdown();
    return false;

  }

  /**
   * @return true, if axis still has results left
   */
  public boolean isFinished() {
    return mFinished;
  }

}
