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
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.vector.hnsw.HnswGraph;
import io.sirix.index.vector.hnsw.HnswParams;
import io.sirix.index.vector.ops.VectorDistanceType;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

/**
 * Change listener for vector indexes that handles deletion of document nodes.
 *
 * <p>When a document node that has a corresponding vector entry is deleted,
 * this listener performs reverse lookup to find the HNSW-internal node key
 * and executes full graph repair via {@link HnswGraph#delete(long)}.</p>
 *
 * <p>Insert operations are not handled by this listener -- vector indexes
 * are populated via the {@link VectorIndexBuilder} during index creation
 * or via the explicit {@link VectorIndex#insertVector} API.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class VectorIndexListener implements ChangeListener, PathNodeKeyChangeListener {

  /** The metadata node is always at key 1. */
  private static final long METADATA_NODE_KEY = 1L;

  private final IndexDef indexDef;
  private final StorageEngineWriter storageEngineWriter;
  private final LongSet matchingPCRs;

  /**
   * Creates a new vector index listener.
   *
   * @param indexDef              the vector index definition (must not be null)
   * @param storageEngineWriter   the storage engine writer for page-level operations
   * @param pathSummaryReader     the path summary reader for resolving PCRs
   * @throws IllegalArgumentException if any argument is null
   */
  public VectorIndexListener(final IndexDef indexDef,
      final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader) {
    if (indexDef == null) {
      throw new IllegalArgumentException("indexDef must not be null");
    }
    if (storageEngineWriter == null) {
      throw new IllegalArgumentException("storageEngineWriter must not be null");
    }
    if (pathSummaryReader == null) {
      throw new IllegalArgumentException("pathSummaryReader must not be null");
    }
    this.indexDef = indexDef;
    this.storageEngineWriter = storageEngineWriter;
    this.matchingPCRs = pathSummaryReader.getPCRsForPaths(indexDef.getPaths());
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
    if (type != IndexController.ChangeType.DELETE) {
      return;
    }
    if (matchingPCRs.isEmpty() || !matchingPCRs.contains(pathNodeKey)) {
      return;
    }
    deleteVectorForDocumentNode(node.getNodeKey());
  }

  @Override
  public void listen(final IndexController.ChangeType type, final long nodeKey,
      final NodeKind nodeKind, final long pathNodeKey, final @Nullable QNm name,
      final @Nullable Str value) {
    if (type != IndexController.ChangeType.DELETE) {
      return;
    }
    if (matchingPCRs.isEmpty() || !matchingPCRs.contains(pathNodeKey)) {
      return;
    }
    deleteVectorForDocumentNode(nodeKey);
  }

  /**
   * Performs reverse lookup and graph repair deletion for a document node.
   *
   * @param documentNodeKey the document-level node key being deleted
   */
  private void deleteVectorForDocumentNode(final long documentNodeKey) {
    final int indexNumber = indexDef.getID();
    final int dimension = indexDef.getDimension();
    final String distanceType = indexDef.getDistanceType();

    final PageBackedVectorStore store = new PageBackedVectorStore(
        storageEngineWriter, indexNumber, dimension, distanceType);
    store.loadMetadata(METADATA_NODE_KEY);

    final long hnswNodeKey = store.findNodeKeyByDocumentKey(documentNodeKey);
    if (hnswNodeKey == -1L) {
      return; // No vector indexed for this document node.
    }

    final VectorDistanceType distType = VectorDistanceType.valueOf(distanceType);
    final HnswParams params = HnswParams.builder(dimension, distType)
        .m(indexDef.getHnswM())
        .efConstruction(indexDef.getHnswEfConstruction())
        .build();

    final HnswGraph graph = new HnswGraph(store, params);
    graph.delete(hnswNodeKey);
  }
}
