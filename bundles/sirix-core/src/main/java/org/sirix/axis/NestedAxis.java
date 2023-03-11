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

package org.sirix.axis;

import static java.util.Objects.requireNonNull;

import org.sirix.api.Axis;

/**
 * <p>
 * Chains two axis operations.
 * </p>
 */
public final class NestedAxis extends AbstractAxis {

  /** Parent axis. */
  private final Axis parentAxis;

  /** Child axis to apply to each node found with parent axis. */
  private final Axis childAxis;

  /** Is it the first run of parent axis? */
  private boolean isFirst;

  /**
   * Constructor initializing internal state.
   *
   * @param parentAxis inner nested axis
   * @param childAxis outer nested axis
   */
  public NestedAxis(final Axis parentAxis, final Axis childAxis) {
    super(parentAxis.getCursor());
    this.parentAxis = requireNonNull(parentAxis);
    this.childAxis = requireNonNull(childAxis);
    isFirst = true;
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    if (parentAxis != null) {
      parentAxis.reset(nodeKey);
    }
    if (childAxis != null) {
      childAxis.reset(nodeKey);
    }
    isFirst = true;
  }

  @Override
  protected long nextKey() {
    // Make sure that parent axis is moved for the first time.
    if (isFirst) {
      isFirst = false;
      if (parentAxis.hasNext()) {
        childAxis.reset(parentAxis.next());
      } else {
        return done();
      }
    }

    // Execute child axis for each node found with parent axis.
    boolean hasNext;
    while (!(hasNext = childAxis.hasNext())) {
      if (parentAxis.hasNext()) {
        childAxis.reset(parentAxis.next());
      } else {
        break;
      }
    }
    if (hasNext) {
      return childAxis.next();
    }

    return done();
  }
}
