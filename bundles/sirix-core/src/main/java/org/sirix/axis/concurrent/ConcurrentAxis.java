/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.axis.concurrent;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.checkerframework.checker.index.qual.NonNegative;

import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.axis.AbstractAxis;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Realizes in combination with the <code>ConurrentAxisHelper</code> the concurrent evaluation of
 * pipeline steps. The given axis is uncoupled from the main thread by embedding it in a Runnable
 * that uses its one transaction and stores all the results to a queue. The ConcurrentAxis gets the
 * computed results from that queue one by one on every hasNext() call and sets the main-transaction
 * to it. As soon as the end of the computed result sequence is reached (marked by the
 * NULL_NODE_KEY), the ConcurrentAxis returns <code>false</code>.
 * </p>
 * <p>
 * This framework is working according to the producer-consumer-principle, where the
 * ConcurrentAxisHelper and its encapsulated axis is the producer and the ConcurrentAxis with its
 * callees is the consumer. This can be used by any class that implements the IAxis interface. Note:
 * Make sure that the used class is thread-safe.
 * </p>
 */
public final class ConcurrentAxis<R extends NodeCursor & NodeReadOnlyTrx> extends AbstractAxis {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(ConcurrentAxis.class));

  /** Axis that is running in an own thread and produces results for this axis. */
  private final Axis producer;

  /**
   * Queue that stores result keys already computed by the producer. End of the result sequence is
   * marked by the NULL_NODE_KEY.
   */
  private final BlockingQueue<Long> results;

  /** Capacity of the mResults queue. */
  private final int M_CAPACITY = 200;

  /** Has axis already been called? */
  private boolean first;

  /** Runnable in which the producer is running. */
  private Runnable task;

  /** Is axis already finished and has no results left? */
  private boolean finished;

  /** Executor Service holding the execution plan for future tasks. */
  public ExecutorService executorService;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param rtx exclusive (immutable) trx to iterate with
   * @param childAxis producer axis
   */
  public ConcurrentAxis(final R rtx, final Axis childAxis) {
    super(rtx);
    if (rtx == childAxis.getTrx()) {
      throw new IllegalArgumentException(
          "The filter must be bound to another transaction but on the same revision/node!");
    }
    results = new ArrayBlockingQueue<>(M_CAPACITY);
    first = true;
    producer = checkNotNull(childAxis);
    task = new ConcurrentAxisHelper(producer, results);
    executorService = Executors.newSingleThreadExecutor();
    finished = false;
  }

  @Override
  public synchronized void reset(final @NonNegative long nodeKey) {
    super.reset(nodeKey);
    first = true;
    finished = false;

    if (executorService != null) {
      executorService = Executors.newSingleThreadExecutor();
    }
    if (producer != null) {
      producer.reset(nodeKey);
    }
    if (results != null) {
      results.clear();
    }
    if (task != null) {
      task = new ConcurrentAxisHelper(producer, results);
    }
  }

  @Override
  protected long nextKey() {
    // Start producer on first call.
    if (first) {
      first = false;
      executorService.submit(task);
    }

    if (finished) {
      return done();
    }

    long result = Fixed.NULL_NODE_KEY.getStandardProperty();

    try {
      // Get result from producer as soon as it is available.
      result = results.take();
    } catch (final InterruptedException e) {
      LOGGER.warn(e.getMessage(), e);
    }

    // NULL_NODE_KEY marks end of the sequence computed by the producer.
    if (result != Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return result;
    }

    finished = true;
    return done();
  }

  /**
   * Signals that axis traversal is done, that is {@code hasNext()} must return false. Is callable
   * from subclasses which implement {@link #nextKey()} to signal that the axis-traversal is done and
   * {@link #hasNext()} must return false.
   *
   * @return null node key to indicate that the travesal is done
   */
  @Override
  protected long done() {
    executorService.shutdown();
    try {
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * Determines if axis has more results to deliver or not.
   *
   * @return {@code true}, if axis still has results left, {@code false} otherwise
   */
  public boolean isFinished() {
    return finished;
  }
}
