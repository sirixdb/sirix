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

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.IndexDef;
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

/**
 * Stub builder for vector indexes. Vector indexes are typically populated
 * explicitly via the API (not by traversing the document tree), so this
 * builder is a no-op that simply implements the {@link JsonNodeVisitor}
 * contract.
 *
 * <p>Future implementations may support automatic vector extraction from
 * document nodes matching a path pattern defined in the {@link IndexDef}.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class VectorIndexBuilder implements JsonNodeVisitor {

  private final IndexDef indexDef;

  /**
   * Creates a new vector index builder for the given index definition.
   *
   * @param indexDef the vector index definition (must not be null)
   * @throws IllegalArgumentException if indexDef is null
   */
  public VectorIndexBuilder(final IndexDef indexDef) {
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
    return VisitResultType.CONTINUE;
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
