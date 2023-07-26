/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
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

package io.sirix.api.visitor;

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
 * Interface which must be implemented from visitors to implement functionality based on the visitor
 * pattern.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public interface JsonNodeVisitor extends NodeVisitor {
  /**
   * Do something when visiting a {@link ImmutableArrayNode}.
   *
   * @param node the {@link ImmutableArrayNode}
   * @return the result of visiting the {@link ImmutableArrayNode}
   */
  default VisitResult visit(ImmutableArrayNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting a {@link ImmutableObjectNode}.
   *
   * @param node the {@link ImmutableObjectNode}
   * @return the result of visiting the {@link ImmutableObjectNode}
   */
  default VisitResult visit(ImmutableObjectNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting an {@link ImmutableObjectKeyNode}.
   *
   * @param node the {@link ImmutableObjectKeyNode}
   * @return the result of visiting the {@link ImmutableObjectKeyNode}
   */
  default VisitResult visit(ImmutableObjectKeyNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting a {@link ImmutableObjectBooleanNode}.
   *
   * @param node the {@link ImmutableObjectBooleanNode}
   * @return the result of visiting the {@link ImmutableObjectBooleanNode}
   */
  default VisitResult visit(ImmutableObjectBooleanNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting a {@link ImmutableBooleanNode}.
   *
   * @param node the {@link ImmutableBooleanNode}
   * @return the result of visiting the {@link ImmutableBooleanNode}
   */
  default VisitResult visit(ImmutableBooleanNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * ImmutableDocumentRoot Do something when visiting a {@link ImmutableStringNode}.
   *
   * @param node the {@link ImmutableStringNode}
   * @return the result of visiting the {@link ImmutableStringNode}
   */
  default VisitResult visit(ImmutableStringNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting a {@link ImmutableNullNode}.
   *
   * @param node the {@link ImmutableNullNode}
   * @return the result of visiting the {@link ImmutableNumberNode}
   */
  default VisitResult visit(ImmutableNumberNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting a {@link ImmutableNullNode}.
   *
   * @param node the {@link ImmutableNullNode}
   * @return the result of visiting the {@link ImmutableNullNode}
   */
  default VisitResult visit(ImmutableNullNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * ImmutableDocumentRoot Do something when visiting a {@link ImmutableObjectStringNode}.
   *
   * @param node the {@link ImmutableObjectStringNode}
   * @return the result of visiting the {@link ImmutableObjectStringNode}
   */
  default VisitResult visit(ImmutableObjectStringNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting a {@link ImmutableObjectNumberNode}.
   *
   * @param node the {@link ImmutableObjectNumberNode}
   * @return the result of visiting the {@link ImmutableObjectNumberNode}
   */
  default VisitResult visit(ImmutableObjectNumberNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting a {@link ImmutableObjectNullNode}.
   *
   * @param node the {@link ImmutableObjectNullNode}
   * @return the result of visiting the {@link ImmutableObjectNullNode}
   */
  default VisitResult visit(ImmutableObjectNullNode node) {
    return VisitResultType.CONTINUE;
  }

  /**
   * Do something when visiting the {@link ImmutableJsonDocumentRootNode}.
   *
   * @param node the {@link ImmutableJsonDocumentRootNode}
   * @return the result of visiting the {@link ImmutableJsonDocumentRootNode}
   */
  default VisitResult visit(ImmutableJsonDocumentRootNode node) {
    return VisitResultType.CONTINUE;
  }
}
