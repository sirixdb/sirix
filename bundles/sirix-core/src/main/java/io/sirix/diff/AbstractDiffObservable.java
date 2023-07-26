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

package io.sirix.diff;

import java.util.HashSet;
import java.util.Set;

import io.sirix.exception.SirixException;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Implements {@link DiffObservable}, which can be used for all classes, which implement the
 * {@link DiffObservable} interface.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
abstract class AbstractDiffObservable implements DiffObservable {

  /**
   * {@link Set} of observers, which want to be notified of the encountered differences.
   */
  private final Set<DiffObserver> diffObservers;

  /**
   * Default constructor.
   */
  AbstractDiffObservable() {
    diffObservers = new HashSet<>();
  }

  @Override
  public final void fireDiff(final DiffFactory.DiffType diff, final @NonNull long newNodeKey,
      final @NonNull long oldNodeKey, final @NonNull DiffDepth depth) {
    for (final DiffObserver observer : diffObservers) {
      observer.diffListener(diff, newNodeKey, oldNodeKey, depth);
    }
  }

  @Override
  public final void done() throws SirixException {
    for (final DiffObserver observer : diffObservers) {
      observer.diffDone();
    }
  }

  @Override
  public final void addObserver(final DiffObserver observer) {
    diffObservers.add(observer);
  }

  @Override
  public final void removeObserver(final DiffObserver observer) {
    diffObservers.remove(observer);
  }
}
