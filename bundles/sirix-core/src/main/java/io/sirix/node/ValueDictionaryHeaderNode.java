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
 * The header of one global projection value dictionary namespace.
 *
 * <p>It sits at local key 0 of the namespace, so a reader that knows only the namespace can find
 * everything else with one read. Everything it carries is derived state that a reader cannot
 * reconstruct without scanning the whole namespace, which is exactly what a header is for.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class ValueDictionaryHeaderNode implements DataRecord {

  /** Layout version of the namespace; readers reject anything they do not know. */
  public static final int VERSION = 0;

  private final long nodeKey;

  private final int version;

  /** Ids {@code 1..entryCount} are live; {@code entryCount + 1} is the next id to mint. */
  private final int entryCount;

  private final long forwardRootKey;

  private final long reverseRootKey;

  private final int generation;

  /** {@code false} for an {@link #unknownLayout(long, int)} carrier this build cannot interpret. */
  private final boolean currentLayout;

  /**
   * Constructor.
   *
   * @param nodeKey the node key, which is the namespace base (local key 0)
   * @param version the layout version
   * @param entryCount how many values the namespace holds
   * @param forwardRootKey root of the hash-prefix radix directory
   * @param reverseRootKey root of the id-prefix radix directory
   * @param generation number of successful append generations
   * @throws IllegalArgumentException if any count is negative
   */
  public ValueDictionaryHeaderNode(final long nodeKey, final int version, final int entryCount,
      final long forwardRootKey, final long reverseRootKey, final int generation) {
    if (nodeKey <= 0 || version != VERSION || entryCount < 0 || forwardRootKey < 0
        || reverseRootKey < 0 || generation < 0
        || (entryCount == 0) != (forwardRootKey == 0 && reverseRootKey == 0)) {
      throw new IllegalArgumentException("invalid value dictionary header");
    }
    this.nodeKey = nodeKey;
    this.version = version;
    this.entryCount = entryCount;
    this.forwardRootKey = forwardRootKey;
    this.reverseRootKey = reverseRootKey;
    this.generation = generation;
    this.currentLayout = true;
  }

  private ValueDictionaryHeaderNode(final long nodeKey, final int version) {
    this.nodeKey = nodeKey;
    this.version = version;
    this.entryCount = 0;
    this.forwardRootKey = 0;
    this.reverseRootKey = 0;
    this.generation = 0;
    this.currentLayout = false;
  }

  /**
   * A header whose serialized layout version this build cannot interpret. Only the version is
   * carried — the payload behind it is unreadable by definition. Every consumer declines it
   * ({@code GlobalValueDictionary#header} answers {@code null}), and re-serializing it is refused
   * so a newer build's data is never overwritten with a lossy reconstruction.
   *
   * @throws IllegalArgumentException for a negative version — that is corruption, not a future
   *         layout, and corruption stays loud
   */
  public static ValueDictionaryHeaderNode unknownLayout(final long nodeKey, final int version) {
    if (nodeKey <= 0 || version < 0 || version == VERSION) {
      throw new IllegalArgumentException("not an unknown-layout value dictionary header: version " + version);
    }
    return new ValueDictionaryHeaderNode(nodeKey, version);
  }

  /** Whether this build can interpret the header's layout ({@link #getVersion()} == {@link #VERSION}). */
  public boolean isCurrentLayout() {
    return currentLayout;
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

  public long getForwardRootKey() {
    return forwardRootKey;
  }

  public long getReverseRootKey() {
    return reverseRootKey;
  }

  public int getGeneration() {
    return generation;
  }

  /** Whether a forward probe may report "absent" rather than declining. */
  public boolean isDirectoryComplete() {
    return entryCount == 0
        ? forwardRootKey == 0 && reverseRootKey == 0
        : forwardRootKey > 0 && reverseRootKey > 0;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int hashCode() {
    int result = version;
    result = 31 * result + entryCount;
    result = 31 * result + Long.hashCode(forwardRootKey);
    result = 31 * result + Long.hashCode(reverseRootKey);
    return 31 * result + generation;
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ValueDictionaryHeaderNode other && version == other.version
        && entryCount == other.entryCount && forwardRootKey == other.forwardRootKey
        && reverseRootKey == other.reverseRootKey && generation == other.generation;
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("nodeKey", nodeKey)
                         .add("version", version)
                         .add("entryCount", entryCount)
                         .add("forwardRootKey", forwardRootKey)
                         .add("reverseRootKey", reverseRootKey)
                         .add("generation", generation)
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
