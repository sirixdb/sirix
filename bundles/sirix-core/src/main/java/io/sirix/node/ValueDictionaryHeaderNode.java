/**
 * Copyright (c) 2026.
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

package io.sirix.node;

import io.sirix.node.interfaces.DataRecord;
import io.sirix.utils.ToStringHelper;

/**
 * The header of one global projection value dictionary namespace — the record that says how many
 * values the namespace holds and where its forward directory lives.
 *
 * <p>It sits at local key 0 of the namespace, so a reader that knows only the namespace can find
 * everything else with one read. Everything it carries is derived state that a reader cannot
 * reconstruct without scanning the whole namespace, which is exactly what a header is for.
 *
 * <p>{@link #getDirectoryCoversMaxId()} is the load-bearing field for correctness. The forward
 * directory is produced in one pass at the end of a build; a later revision that appends values
 * without rebuilding it leaves ids above that watermark unreachable by a value probe. A probe must
 * therefore treat "not found" as authoritative only when the directory covers every id, and
 * otherwise decline — the alternative is answering "this value does not exist" about a value that
 * does, which turns a fast path into a wrong answer.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryHeaderNode implements DataRecord {

  /** Layout version of the namespace; readers reject anything they do not know. */
  public static final int VERSION = 1;

  private final long nodeKey;

  private final int version;

  /** Ids {@code 1..entryCount} are live; {@code entryCount + 1} is the next id to mint. */
  private final int entryCount;

  /** Node key the id space is anchored at; id {@code i} lives at {@code entryBase + stride * i}. */
  private final long entryBase;

  /** Node key of directory block 0, or {@code 0} when no directory has been written. */
  private final long directoryBase;

  /** How many directory blocks follow {@link #directoryBase}. */
  private final int directoryBlockCount;

  /** The highest id the directory indexes; a forward probe is only authoritative up to it. */
  private final int directoryCoversMaxId;

  /**
   * Constructor.
   *
   * @param nodeKey the node key, which is the namespace base (local key 0)
   * @param version the layout version
   * @param entryCount how many values the namespace holds
   * @param entryBase node key the id space is anchored at
   * @param directoryBase node key of directory block 0, or {@code 0} for "no directory"
   * @param directoryBlockCount how many directory blocks exist
   * @param directoryCoversMaxId the highest id the directory indexes
   * @throws IllegalArgumentException if any count is negative
   */
  public ValueDictionaryHeaderNode(final long nodeKey, final int version, final int entryCount,
      final long entryBase, final long directoryBase, final int directoryBlockCount,
      final int directoryCoversMaxId) {
    if (entryCount < 0 || directoryBlockCount < 0 || directoryCoversMaxId < 0 || directoryBase < 0
        || entryBase < 0) {
      throw new IllegalArgumentException("value dictionary header counts must not be negative: entryCount="
          + entryCount + " entryBase=" + entryBase + " directoryBase=" + directoryBase + " directoryBlockCount="
          + directoryBlockCount + " directoryCoversMaxId=" + directoryCoversMaxId);
    }
    this.nodeKey = nodeKey;
    this.version = version;
    this.entryCount = entryCount;
    this.entryBase = entryBase;
    this.directoryBase = directoryBase;
    this.directoryBlockCount = directoryBlockCount;
    this.directoryCoversMaxId = directoryCoversMaxId;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.VALUE_DICTIONARY_HEADER;
  }

  public int getVersion() {
    return version;
  }

  public int getEntryCount() {
    return entryCount;
  }

  public long getEntryBase() {
    return entryBase;
  }

  public long getDirectoryBase() {
    return directoryBase;
  }

  public int getDirectoryBlockCount() {
    return directoryBlockCount;
  }

  public int getDirectoryCoversMaxId() {
    return directoryCoversMaxId;
  }

  /** Whether a forward probe may report "absent" rather than declining. */
  public boolean isDirectoryComplete() {
    return directoryBlockCount > 0 && directoryCoversMaxId >= entryCount;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int hashCode() {
    int result = version;
    result = 31 * result + entryCount;
    result = 31 * result + Long.hashCode(entryBase);
    result = 31 * result + Long.hashCode(directoryBase);
    result = 31 * result + directoryBlockCount;
    return 31 * result + directoryCoversMaxId;
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ValueDictionaryHeaderNode other && version == other.version
        && entryCount == other.entryCount && entryBase == other.entryBase
        && directoryBase == other.directoryBase
        && directoryBlockCount == other.directoryBlockCount && directoryCoversMaxId == other.directoryCoversMaxId;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("nodeKey", nodeKey)
                         .add("version", version)
                         .add("entryCount", entryCount)
                         .add("entryBase", entryBase)
                         .add("directoryBase", directoryBase)
                         .add("directoryBlockCount", directoryBlockCount)
                         .add("directoryCoversMaxId", directoryCoversMaxId)
                         .toString();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }
}
