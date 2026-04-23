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
package io.sirix.service.json.shredder;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration round-trip for iter#29 OBJECT_NAMED_* fusion (task #62).
 *
 * <p>Shreds a small JSON document whose every object field carries a primitive value,
 * then verifies via {@link io.sirix.api.json.JsonNodeReadOnlyTrx} that each field is
 * materialised as a single fused {@code OBJECT_NAMED_*} record — NOT the legacy
 * {@code OBJECT_KEY + primitive-child} pair — and that {@code getName()}, {@code getValue()}
 * and the Option-A synthetic {@code moveToFirstChild()} path behave correctly.
 */
public final class JsonShredFusedRoundTripTest {

  private static final String PRIMITIVE_FIELDS_JSON =
      "[{\"a\":1,\"b\":\"x\",\"c\":true,\"d\":null}]";

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void shredPrimitiveFields_producesObjectNamedKinds() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final JsonResourceSession session =
             database.beginResourceSession(JsonTestHelper.RESOURCE)) {

      try (final var wtx = session.beginNodeTrx()) {
        final var reader = JsonShredder.createStringReader(PRIMITIVE_FIELDS_JSON);
        new JsonShredder.Builder(wtx, reader, InsertPosition.AS_FIRST_CHILD)
            .commitAfterwards()
            .fuseNamedPrimitives()
            .build()
            .call();
      }

      try (final var rtx = session.beginNodeReadOnlyTrx()) {
        verifyFusedNodes(rtx);
      }
    }
  }

  private static void verifyFusedNodes(final io.sirix.api.json.JsonNodeReadOnlyTrx rtx) {
      // Navigate: DOCUMENT → ARRAY → OBJECT → first field (a)
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild(), "document root should have a child");
      assertEquals(NodeKind.ARRAY, rtx.getKind(), "root child should be ARRAY");
      assertTrue(rtx.moveToFirstChild(), "array should have a child");
      assertEquals(NodeKind.OBJECT, rtx.getKind(), "array child should be OBJECT");
      assertTrue(rtx.moveToFirstChild(), "object should have its first field");

      // Field "a" → OBJECT_NAMED_NUMBER with value 1
      assertEquals(NodeKind.OBJECT_NAMED_NUMBER, rtx.getKind(),
          "first field should be OBJECT_NAMED_NUMBER");
      assertNotNull(rtx.getName(), "fused node must expose a name");
      assertEquals("a", rtx.getName().getLocalName(),
          "fused number node should return field name 'a'");

      // iter#31 Option B: fused node is a leaf. Both name and inline primitive value
      // are read directly off the fused record — no synthetic child emitted, no
      // indirection through moveToFirstChild.
      assertFalse(rtx.hasFirstChild(), "fused node is a leaf (no children)");
      assertTrue(rtx.isNumberValue(), "fused number node recognised as number");
      assertEquals(1L, rtx.getNumberValue().longValue(), "fused number inline value is 1");
      assertEquals("1", rtx.getValue(), "getValue() on fused number");

      // Field "b" → OBJECT_NAMED_STRING with value "x"
      assertTrue(rtx.moveToRightSibling(), "should have a right sibling 'b'");
      assertEquals(NodeKind.OBJECT_NAMED_STRING, rtx.getKind(),
          "second field should be OBJECT_NAMED_STRING");
      assertEquals("b", rtx.getName().getLocalName(),
          "fused string node should return field name 'b'");
      assertTrue(rtx.isStringValue());
      assertEquals("x", rtx.getValue(), "fused string inline value");

      // Field "c" → OBJECT_NAMED_BOOLEAN with value true
      assertTrue(rtx.moveToRightSibling());
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, rtx.getKind());
      assertEquals("c", rtx.getName().getLocalName());
      assertTrue(rtx.isBooleanValue());
      assertTrue(rtx.getBooleanValue(), "fused boolean inline value should be true");

      // Field "d" → OBJECT_NAMED_NULL
      assertTrue(rtx.moveToRightSibling());
      assertEquals(NodeKind.OBJECT_NAMED_NULL, rtx.getKind());
      assertEquals("d", rtx.getName().getLocalName());
      assertTrue(rtx.isNullValue());
      assertEquals("null", rtx.getValue());

      // No more siblings — we should be on the last field.
      assertFalse(rtx.hasRightSibling(),
          "last object field should have no right sibling");

      // Walk back up: fused → OBJECT → ARRAY → DOCUMENT_ROOT.
      assertTrue(rtx.moveToParent());
      assertEquals(NodeKind.OBJECT, rtx.getKind());
      assertTrue(rtx.moveToParent());
      assertEquals(NodeKind.ARRAY, rtx.getKind());
  }
}
