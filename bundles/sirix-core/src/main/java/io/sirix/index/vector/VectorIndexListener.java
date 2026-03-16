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

package io.sirix.index.vector;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.access.trx.node.IndexController;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.jspecify.annotations.Nullable;

/**
 * Intentionally minimal change listener for vector indexes. Vector indexes are
 * populated and deleted exclusively via the explicit API
 * ({@link io.sirix.index.vector.VectorIndex#insertVector} and
 * {@link io.sirix.index.vector.VectorIndex#deleteVector}) rather than through
 * the document change listener pathway. This listener implements the
 * {@link ChangeListener} and {@link PathNodeKeyChangeListener} contracts as
 * no-ops so it can be registered in the standard index lifecycle.
 *
 * <p>Automatic deletion of vector entries when document nodes are removed would
 * require a reverse mapping from document node keys to HNSW node keys, which is
 * not yet implemented.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class VectorIndexListener implements ChangeListener, PathNodeKeyChangeListener {

  private final IndexDef indexDef;

  /**
   * Creates a new vector index listener for the given index definition.
   *
   * @param indexDef the vector index definition (must not be null)
   * @throws IllegalArgumentException if indexDef is null
   */
  public VectorIndexListener(final IndexDef indexDef) {
    if (indexDef == null) {
      throw new IllegalArgumentException("indexDef must not be null");
    }
    this.indexDef = indexDef;
  }

  /**
   * Get the index definition.
   *
   * @return the index definition
   */
  public IndexDef getIndexDef() {
    return indexDef;
  }

  @Override
  public void listen(final IndexController.ChangeType type, final ImmutableNode node,
      final long pathNodeKey) {
    // No-op: vector indexes are populated explicitly.
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey,
      final NodeKind nodeKind, final long pathNodeKey, final @Nullable QNm name,
      final @Nullable Str value) {
    // No-op: vector indexes are populated explicitly.
  }
}
