/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.diff;

import java.util.Objects;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.node.NodeKind;

/**
 * Json diff.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
final class JsonDiff extends AbstractDiff<JsonNodeReadOnlyTrx, JsonNodeTrx> {

  /**
   * Constructor.
   *
   * @param builder {@link DiffFactory.Builder} reference
   */
  public JsonDiff(final DiffFactory.Builder<JsonNodeReadOnlyTrx, JsonNodeTrx> builder) {
    super(builder);
  }

  @Override
  NodeKind documentNode() {
    return NodeKind.JSON_DOCUMENT;
  }

  @Override
  boolean checkNodes(final JsonNodeReadOnlyTrx newRtx, final JsonNodeReadOnlyTrx oldRtx) {
    boolean found = false;
    if (newRtx.getNodeKey() == oldRtx.getNodeKey() && newRtx.getParentKey() == oldRtx.getParentKey()
        && newRtx.getKind() == oldRtx.getKind()) {
      found = checkNamesOrValues(newRtx, oldRtx);
    }
    return found;
  }

  private boolean checkNamesOrValues(final JsonNodeReadOnlyTrx newRtx, final JsonNodeReadOnlyTrx oldRtx) {
    boolean found = false;
    switch (newRtx.getKind()) {
      case ARRAY:
      case OBJECT:
      case NULL_VALUE:
        found = true;
        break;
      // iter#32: OBJECT_NAMED_OBJECT and OBJECT_NAMED_ARRAY are pure key-role records
      // (no primitive payload) — name equality alone determines equality.
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY:
        if (checkNamesForEquality(newRtx, oldRtx))
          found = true;
        break;
      // iter#32: fused OBJECT_NAMED_* primitives carry BOTH the key-name role AND the
      // primitive value. Equality requires matching names AND matching values, otherwise
      // an in-place primitive update (e.g. setStringValue on a fused record) goes
      // undetected and produces zero diffs.
      case OBJECT_NAMED_BOOLEAN:
        if (checkNamesForEquality(newRtx, oldRtx) && newRtx.getBooleanValue() == oldRtx.getBooleanValue())
          found = true;
        break;
      case OBJECT_NAMED_NUMBER:
        if (checkNamesForEquality(newRtx, oldRtx) && Objects.equals(newRtx.getNumberValue(), oldRtx.getNumberValue()))
          found = true;
        break;
      case OBJECT_NAMED_STRING:
        if (checkNamesForEquality(newRtx, oldRtx) && Objects.equals(newRtx.getValue(), oldRtx.getValue()))
          found = true;
        break;
      case OBJECT_NAMED_NULL:
        // Null payload is constant — only the name can change.
        if (checkNamesForEquality(newRtx, oldRtx))
          found = true;
        break;
      case BOOLEAN_VALUE:
        if (newRtx.getBooleanValue() == oldRtx.getBooleanValue())
          found = true;
        break;
      case NUMBER_VALUE:
        if (Objects.equals(newRtx.getNumberValue(), oldRtx.getNumberValue()))
          found = true;
        break;
      case STRING_VALUE:
        if (Objects.equals(newRtx.getValue(), oldRtx.getValue()))
          found = true;
        break;
      // $CASES-OMITTED$
      default:
        // Do nothing.
    }
    return found;
  }

  private boolean checkNamesForEquality(JsonNodeReadOnlyTrx newRtx, JsonNodeReadOnlyTrx oldRtx) {
    return newRtx.getNameKey() == oldRtx.getNameKey();
  }

  @Override
  void emitNonStructuralDiff(final JsonNodeReadOnlyTrx newRtx, final JsonNodeReadOnlyTrx oldRtx, final DiffDepth depth,
      final DiffFactory.DiffType diff) {}

  @Override
  boolean checkNodeNamesOrValues(final JsonNodeReadOnlyTrx newRtx, final JsonNodeReadOnlyTrx oldRtx) {
    return checkNamesOrValues(newRtx, oldRtx);
  }
}
