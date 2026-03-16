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

import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.IndexDef;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.immutable.json.ImmutableObjectNullNode;
import io.sirix.node.immutable.json.ImmutableObjectNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectStringNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Builder for vector indexes that traverses JSON document trees and indexes
 * arrays matching configured paths as vector embeddings.
 *
 * <p>On visiting an {@link ImmutableArrayNode} whose path class record (PCR)
 * matches one of the paths in the {@link IndexDef}, the builder traverses
 * the array's children via the read-only transaction, collects numeric values
 * into a float buffer, and inserts the resulting vector into the HNSW graph
 * via the {@link VectorIndex}.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class VectorIndexBuilder implements JsonNodeVisitor {

  private final IndexDef indexDef;
  private final StorageEngineWriter storageEngineWriter;
  private final JsonNodeReadOnlyTrx rtx;
  private final VectorIndex vectorIndex;
  private final LongSet matchingPCRs;

  /**
   * Creates a new vector index builder.
   *
   * @param storageEngineWriter the storage engine writer for page-level operations
   * @param indexDef            the vector index definition (must not be null)
   * @param rtx                 the read-only transaction for tree navigation
   * @param vectorIndex         the vector index implementation for inserting vectors
   * @param pathSummaryReader   the path summary reader for resolving PCRs
   * @throws IllegalArgumentException if any argument is null
   */
  public VectorIndexBuilder(final StorageEngineWriter storageEngineWriter,
      final IndexDef indexDef, final JsonNodeReadOnlyTrx rtx,
      final VectorIndex vectorIndex, final PathSummaryReader pathSummaryReader) {
    if (storageEngineWriter == null) {
      throw new IllegalArgumentException("storageEngineWriter must not be null");
    }
    if (indexDef == null) {
      throw new IllegalArgumentException("indexDef must not be null");
    }
    if (rtx == null) {
      throw new IllegalArgumentException("rtx must not be null");
    }
    if (vectorIndex == null) {
      throw new IllegalArgumentException("vectorIndex must not be null");
    }
    if (pathSummaryReader == null) {
      throw new IllegalArgumentException("pathSummaryReader must not be null");
    }
    this.storageEngineWriter = storageEngineWriter;
    this.indexDef = indexDef;
    this.rtx = rtx;
    this.vectorIndex = vectorIndex;
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
  public VisitResult visit(final ImmutableJsonDocumentRootNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableObjectNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableObjectKeyNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableArrayNode node) {
    final long pathNodeKey = node.getPathNodeKey();
    if (matchingPCRs.isEmpty() || !matchingPCRs.contains(pathNodeKey)) {
      return VisitResultType.CONTINUE;
    }

    // Save cursor position.
    final long savedKey = rtx.getNodeKey();
    rtx.moveTo(node.getNodeKey());

    final int dim = indexDef.getDimension();
    final float[] buffer = new float[dim];
    int count = 0;

    if (rtx.moveToFirstChild()) {
      do {
        if (rtx.isNumberValue() && count < dim) {
          buffer[count] = rtx.getNumberValue().floatValue();
          count++;
        }
      } while (rtx.moveToRightSibling());
    }

    // Restore cursor.
    rtx.moveTo(savedKey);

    if (count == dim) {
      vectorIndex.insertVector(storageEngineWriter, indexDef, node.getNodeKey(), buffer);
    }

    return VisitResultType.SKIPSUBTREE;
  }

  @Override
  public VisitResult visit(final ImmutableObjectBooleanNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableObjectNumberNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableObjectStringNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableObjectNullNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableBooleanNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableNumberNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableStringNode node) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public VisitResult visit(final ImmutableNullNode node) {
    return VisitResultType.CONTINUE;
  }
}
