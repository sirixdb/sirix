/*
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

package io.sirix.utils;

import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.exception.SirixException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * <p>
 * This class creates a JSON test document.
 * </p>
 *
 * <pre>
 * <code>
 * {
 *   "foo": ["bar", null, 2.33],
 *   "bar": { "hello": "world", "helloo": true },
 *   "baz": "hello",
 *   "tada": [{"foo":"bar"},{"baz":false},"boo",{},[]]
 * }
 * </code>
 * </pre>
 */
public final class JsonDocumentCreator {

  public static final String JSON =
      "{\"foo\":[\"bar\",null,2.33],\"bar\":{\"hello\":\"world\",\"helloo\":true},\"baz\":\"hello\",\"tada\":[{\"foo\":\"bar\"},{\"baz\":false},\"boo\",{},[]]}";

  /**
   * Private Constructor, not used.
   */
  private JsonDocumentCreator() {
    throw new AssertionError("Not permitted to call constructor!");
  }

  /**
   * Create simple test document containing all supported node kinds.
   *
   * @param wtx {@link JsonNodeTrx} to write to
   * @throws SirixException if anything weird happens
   */
  public static void create(final JsonNodeTrx wtx) {
    assertNotNull(wtx);
    assertTrue(wtx.moveToDocumentRoot());

    wtx.insertObjectAsFirstChild();

    // P2 fusion: insertObjectRecordAsFirstChild("foo", ArrayValue) emits a single fused
    // OBJECT_NAMED_ARRAY record. The cursor lands ON the fused record, NOT on a separate
    // ARRAY child. Subsequent insertStringValueAsFirstChild inserts as the fused-array's
    // first child, so one moveToParent puts us back on the fused record (OBJECT_NAMED_ARRAY).
    wtx.insertObjectRecordAsFirstChild("foo", new ArrayValue())
       .insertStringValueAsFirstChild("bar")
       .insertNullValueAsRightSibling()
       .insertNumberValueAsRightSibling(2.33);

    wtx.moveToParent();

    assert wtx.isObjectKey();

    // Note: with iter#32 fusion, primitive-valued fields land on a single
    // OBJECT_NAMED_* record (which plays the OBJECT_KEY role). The cursor is
    // already on that fused record after the insert, so the legacy "moveToParent
    // from primitive child up to OBJECT_KEY" hop is no longer needed.
    // P2 fusion: insertObjectRecordAsRightSibling("bar", ObjectValue) emits a single fused
    // OBJECT_NAMED_OBJECT record. Cursor lands on the fused record. Inner field inserts as its
    // first child. After insert sequence cursor is on "helloo" (OBJECT_NAMED_BOOLEAN under the
    // fused parent). One moveToParent → fused "bar" (OBJECT_NAMED_OBJECT, isObjectKey == true).
    wtx.insertObjectRecordAsRightSibling("bar", new ObjectValue())
       .insertObjectRecordAsFirstChild("hello", new StringValue("world"));
    wtx.insertObjectRecordAsRightSibling("helloo", new BooleanValue(true));
    wtx.moveToParent();

    assert wtx.isObjectKey();

    wtx.insertObjectRecordAsRightSibling("baz", new StringValue("hello"));

    assert wtx.isObjectKey();

    // P2 fusion: insertObjectRecordAsRightSibling("tada", ArrayValue) → fused OBJECT_NAMED_ARRAY.
    // Cursor on fused. Then insertObjectAsFirstChild() inserts an OBJECT as first array element
    // — cursor on that OBJECT. Then insertObjectRecordAsFirstChild emits the inner record under
    // the OBJECT. moveToParent from inner ("foo" fused) → outer OBJECT.
    wtx.insertObjectRecordAsRightSibling("tada", new ArrayValue())
       .insertObjectAsFirstChild()
       .insertObjectRecordAsFirstChild("foo", new StringValue("bar"));
    wtx.moveToParent();

    assert wtx.isObject();

    wtx.insertObjectAsRightSibling().insertObjectRecordAsFirstChild("baz", new BooleanValue(false));
    wtx.moveToParent();

    assert wtx.isObject();

    wtx.insertStringValueAsRightSibling("boo");
    wtx.insertObjectAsRightSibling();
    wtx.insertArrayAsRightSibling();

    wtx.moveToDocumentRoot();
  }
}
