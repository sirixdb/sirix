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

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;

/**
 * Interface for vector index lifecycle operations: creation, insertion, and k-NN search.
 *
 * <p>Implementations are responsible for managing the underlying HNSW graph and
 * PageBackedVectorStore. Vector indexes are typically populated explicitly via
 * {@link #insertVector} rather than through automatic document change listeners.</p>
 *
 * @author Johannes Lichtenberger
 */
public interface VectorIndex {

  /**
   * Creates the vector index tree and initializes metadata.
   *
   * @param writer       the storage engine writer for page-level operations
   * @param indexDef      the index definition (must be of type VECTOR)
   * @param databaseType the database type (JSON or XML)
   * @throws IllegalArgumentException if indexDef is not a vector index
   */
  void createIndex(StorageEngineWriter writer, IndexDef indexDef, DatabaseType databaseType);

  /**
   * Inserts a vector embedding into the HNSW graph for the given document node.
   *
   * @param writer          the storage engine writer
   * @param indexDef        the vector index definition
   * @param documentNodeKey the document node key this vector belongs to
   * @param vector          the float embedding vector (must match indexDef dimension)
   * @throws IllegalArgumentException if vector dimension does not match indexDef
   */
  void insertVector(StorageEngineWriter writer, IndexDef indexDef,
      long documentNodeKey, float[] vector);

  /**
   * Searches for the k nearest neighbors to the query vector.
   *
   * @param reader   the storage engine reader for the target revision
   * @param indexDef the vector index definition
   * @param query    the query vector (must match indexDef dimension)
   * @param k        the number of nearest neighbors to return
   * @return the search result containing document node keys and distances
   * @throws IllegalArgumentException if k <= 0 or query dimension does not match
   */
  VectorSearchResult searchKnn(StorageEngineReader reader, IndexDef indexDef,
      float[] query, int k);
}
