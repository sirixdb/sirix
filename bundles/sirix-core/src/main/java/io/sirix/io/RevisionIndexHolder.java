/*
 * Copyright (c) 2023, Sirix
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
package io.sirix.io;

/**
 * Thread-safe holder for {@link RevisionIndex} supporting N readers + 1 writer pattern.
 * 
 * <p>
 * <b>Concurrency Model:</b>
 * <ul>
 * <li>Multiple readers can call {@link #get()} concurrently</li>
 * <li>Single writer calls {@link #update(RevisionIndex)} during commit</li>
 * <li>Readers see a consistent snapshot from when they acquired the reference</li>
 * </ul>
 * 
 * <p>
 * <b>Memory Ordering:</b> The volatile field ensures:
 * <ol>
 * <li>Visibility: readers see the latest index after volatile read</li>
 * <li>Ordering: all writes to index arrays happen-before the volatile write</li>
 * <li>Atomicity: reference assignment is atomic</li>
 * </ol>
 * 
 * <p>
 * In-flight readers on an old index will complete safely since {@link RevisionIndex} is immutable.
 * They simply won't see newly committed revisions, which is consistent with snapshot isolation
 * semantics.
 * 
 * @author Johannes Lichtenberger
 * @since 1.0.0
 */
public final class RevisionIndexHolder {

  /**
   * Volatile reference to current index. Ensures visibility and ordering across threads.
   */
  private volatile RevisionIndex index;

  /**
   * Create a new holder with an empty index.
   */
  public RevisionIndexHolder() {
    this.index = RevisionIndex.EMPTY;
  }

  /**
   * Create a new holder with the given index.
   * 
   * @param index initial index (must not be null)
   * @throws NullPointerException if index is null
   */
  public RevisionIndexHolder(RevisionIndex index) {
    if (index == null) {
      throw new NullPointerException("index must not be null");
    }
    this.index = index;
  }

  /**
   * Get the current revision index.
   * 
   * <p>
   * This is a volatile read, ensuring the caller sees the most recently published index. The returned
   * index is immutable and safe to use even if another thread updates the holder.
   * 
   * @return current RevisionIndex (never null)
   */
  public RevisionIndex get() {
    return index;
  }

  /**
   * Update the revision index.
   * 
   * <p>
   * This is a volatile write, ensuring the new index is visible to all subsequent readers. Should
   * only be called by the commit thread while holding the commit lock.
   * 
   * <p>
   * <b>Thread Safety:</b> This method is NOT synchronized. The caller (commit code) must ensure
   * single-writer semantics via external locking (e.g., commitLock in AbstractResourceSession).
   * 
   * @param newIndex the new index to publish (must not be null)
   * @throws NullPointerException if newIndex is null
   */
  public void update(RevisionIndex newIndex) {
    if (newIndex == null) {
      throw new NullPointerException("newIndex must not be null");
    }
    this.index = newIndex; // Volatile write
  }

  /**
   * Atomically update the index by adding a new revision.
   * 
   * <p>
   * Convenience method that combines {@link RevisionIndex#withNewRevision(long, long)} with
   * {@link #update(RevisionIndex)}. The caller must still hold the commit lock.
   * 
   * @param offset file offset of the new revision
   * @param timestamp commit timestamp of the new revision (epoch millis)
   */
  public void addRevision(long offset, long timestamp) {
    RevisionIndex current = this.index;
    RevisionIndex updated = current.withNewRevision(offset, timestamp);
    this.index = updated; // Volatile write
  }
}

