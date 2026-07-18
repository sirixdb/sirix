/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package io.sirix.node;

import io.sirix.node.interfaces.FlyweightNode;

/**
 * Base class for flyweight (page-direct) node implementations, owning the field-offset scratch
 * array used when serializing a record to a page heap. The array is serialize-only state — reads
 * never touch it — so it is allocated lazily on first serialization instead of once per node
 * shell (which would cost an {@code int[]} on every record read).
 *
 * @author Johannes Lichtenberger
 */
public abstract class AbstractFlyweightNode implements FlyweightNode {

  /** Offset array reused across serializations; lazily allocated because reads never need it. */
  private int[] heapOffsets;

  /**
   * Number of entries in this record kind's field-offset table (the per-kind
   * {@code FIELD_COUNT} from {@link io.sirix.page.NodeFieldLayout}).
   *
   * @return the field count
   */
  protected abstract int heapOffsetFieldCount();

  /**
   * Get the lazily-allocated field-offset scratch array for use with the static
   * {@code writeNewRecord} methods and {@code serializeToHeap}.
   *
   * @return the reused offset array
   */
  public final int[] getHeapOffsets() {
    int[] offsets = heapOffsets;
    if (offsets == null) {
      offsets = new int[heapOffsetFieldCount()];
      heapOffsets = offsets;
    }
    return offsets;
  }
}
