/*
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

import java.util.concurrent.BlockingQueue;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.Axis;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Is the helper for the ConcurrentAxis and realizes the concurrent evaluation of pipeline steps by
 * decoupling the given axis from the main thread and storing its results in a blocking queue so
 * establish a producer-consumer-relationship between the ConcurrentAxis and this one.
 * </p>
 * <p>
 * This axis should only be used and instantiated by the ConcurrentAxis. Find more information on
 * how to use this framework in the ConcurrentAxis documentation.
 * </p>
 */
public class ConcurrentAxisHelper implements Runnable {

  /** Logger. */
  public static final LogWrapper LOGWRAPPER =
      new LogWrapper(LoggerFactory.getLogger(ConcurrentAxisHelper.class));

  /** {@link Axis} that computes the results. */
  private final Axis axis;

  /**
   * Queue that stores result keys already computed by this axis. End of the result sequence is
   * marked by the NULL_NODE_KEY. This is used for communication with the consumer.
   */
  private BlockingQueue<Long> results;

  /**
   * Bind axis step to transaction. Make sure to create a new ReadTransaction instead of using the
   * parameter rtx. Because of concurrency every axis has to have it's own transaction.
   * 
   * @param axis Axis to bind with
   * @param results queue which has results related to the axis
   */
  public ConcurrentAxisHelper(final Axis axis, @NonNull final BlockingQueue<Long> results) {
    this.axis = requireNonNull(axis);
    this.results = requireNonNull(results);
  }

  @Override
  public void run() {
    // Compute all results of the given axis and store the results in the queue.
    while (axis.hasNext()) {
      final long nodeKey = axis.next();
      try {
        // Store result in queue as soon as there is space left.
        results.put(nodeKey);
        // Wait until next thread arrives and exchange blocking queue.
      } catch (final InterruptedException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }

    try {
      // Mark end of result sequence by the NULL_NODE_KEY.
      results.put(Fixed.NULL_NODE_KEY.getStandardProperty());
    } catch (final InterruptedException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }
}
