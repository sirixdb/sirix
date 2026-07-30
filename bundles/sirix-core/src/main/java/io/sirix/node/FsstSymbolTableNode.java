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

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * One FSST symbol table, stored as a record so that it is versioned like any other.
 *
 * <p>A symbol table is not derivable from the pages that use it: the compressed bytes on a page are
 * meaningless without the exact table they were encoded against. That makes it the one piece of
 * string state that <em>must</em> be versioned. Storing it as a record in the name dictionary's
 * trie gets that for free — copy-on-write keeps every revision's table reachable from that
 * revision's root, so a page written at revision N still decodes after revision N+1 builds a new
 * one.
 *
 * <p>The alternative — the symbol table embedded in every page, which is what the write path did
 * before — costs the table's bytes once per page, rebuilds it once per page, and re-parses it once
 * per page on read. Building it per page is also why it never paid for itself: the table needs a
 * few dozen samples of real strings before it beats the raw bytes, and a single page rarely
 * supplies them.
 *
 * <p>Tables are immutable once written. A rebuild allocates a <em>new</em> node key rather than
 * overwriting an existing table, because overwriting would silently corrupt every page in every
 * earlier revision that still points at it.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class FsstSymbolTableNode implements DataRecord {

  private final long nodeKey;

  /**
   * The serialized symbol table, in the layout
   * {@code [numSymbols:1][symbolLengths:numSymbols][symbolData…]} that
   * {@link io.sirix.utils.FSSTCompressor#parseSymbolTable} expects.
   */
  private final byte[] table;

  /**
   * Constructor.
   *
   * @param nodeKey the node key, which is also the dictionary id pages refer to
   * @param table the serialized symbol table; never {@code null} and never empty, since an empty
   *        table means "do not compress" and is represented by the absence of a reference rather
   *        than by a record
   * @throws IllegalArgumentException if {@code table} is empty
   */
  public FsstSymbolTableNode(final long nodeKey, final byte[] table) {
    this.nodeKey = nodeKey;
    this.table = requireNonNull(table, "table must not be null");
    if (table.length == 0) {
      throw new IllegalArgumentException(
          "an empty symbol table must not be stored; pages omit the reference instead");
    }
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.FSST_SYMBOL_TABLE;
  }

  /**
   * The serialized symbol table.
   *
   * <p>Returned directly rather than copied: this sits on the read path of every compressed string
   * and the array is treated as immutable throughout — {@link io.sirix.utils.FSSTCompressor} only
   * ever reads it. Callers must not modify it.
   *
   * @return the serialized symbol table
   */
  public byte[] getTable() {
    return table;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(table);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof FsstSymbolTableNode other && Arrays.equals(table, other.table);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this).add("nodeKey", nodeKey).add("tableBytes", table.length).toString();
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
