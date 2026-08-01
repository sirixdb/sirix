/*
 * Copyright (c) 2024, Sirix Contributors
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

package io.sirix.access.trx.node;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.InternalJsonNodeReadOnlyTrx;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cursor decodes each structural key (first child, right sibling, left sibling, parent) at most
 * once per position and remembers it until the next reposition. That makes a read-only traversal
 * markedly cheaper — a full-document serialization asks for the same two keys about three times per
 * node — but it also means every repositioning entry point has to drop what it remembered.
 *
 * <p>These tests pin the observable contract the cache must not break: a key read twice at one
 * position agrees with itself and with an independent cursor, no position is ever answered with the
 * previous position's key, {@code hasX()} and {@code getXKey()} never disagree, and a write
 * transaction — which mutates the record under its own cursor without repositioning, and therefore
 * must not cache at all — still reports what it just wrote.
 *
 * @author Johannes Lichtenberger
 */
final class StructuralKeyCacheTest {

  private static final String RESOURCE = "structuralKeyCacheResource";

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** Nested on purpose: the walk below has to cross both sibling chains and descents. */
  private static final String DOCUMENT = """
      {"a":1,"b":[2,3,{"c":"x","d":[4,5]}],"e":{"f":null,"g":true},"h":"tail"}""";

  @TempDir
  Path tempDir;

  private Database<JsonResourceSession> database;

  @BeforeEach
  void setUp() {
    final Path dbPath = tempDir.resolve("structural-key-cache-db");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.close();
    }
  }

  private void shredDocument() {
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(DOCUMENT));
      wtx.commit();
    }
  }

  /**
   * Every node of the document, visited by a walking cursor, must report exactly what a second
   * cursor that jumps straight to that node reports — and must report it identically on a repeated
   * ask. A key remembered from the previous position, or one remembered across a move, shows up
   * here as a mismatch.
   */
  @Test
  void everyPositionAnswersWithItsOwnKeys() {
    shredDocument();

    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx walker = session.beginNodeReadOnlyTrx();
         final JsonNodeReadOnlyTrx probe = session.beginNodeReadOnlyTrx()) {

      walker.moveToDocumentRoot();
      final DescendantAxis axis = new DescendantAxis(walker, IncludeSelf.YES);

      int visited = 0;
      while (axis.hasNext()) {
        final long nodeKey = axis.nextLong();
        visited++;

        // The probe reaches the node cold, so its first read is necessarily a decode.
        assertTrue(probe.moveTo(nodeKey), "probe must reach node " + nodeKey);

        assertEquals(probe.getFirstChildKey(), walker.getFirstChildKey(), "first child of " + nodeKey);
        assertEquals(probe.getRightSiblingKey(), walker.getRightSiblingKey(), "right sibling of " + nodeKey);
        assertEquals(probe.getLeftSiblingKey(), walker.getLeftSiblingKey(), "left sibling of " + nodeKey);
        assertEquals(probe.getParentKey(), walker.getParentKey(), "parent of " + nodeKey);

        // Asked again at the same position — this is the read the cache is meant to serve.
        assertEquals(probe.getFirstChildKey(), walker.getFirstChildKey(), "repeat first child of " + nodeKey);
        assertEquals(probe.getRightSiblingKey(), walker.getRightSiblingKey(), "repeat right sibling of " + nodeKey);
        assertEquals(probe.getLeftSiblingKey(), walker.getLeftSiblingKey(), "repeat left sibling of " + nodeKey);
        assertEquals(probe.getParentKey(), walker.getParentKey(), "repeat parent of " + nodeKey);

        // The predicates are answered from the same remembered values, so they must not drift
        // from the keys themselves.
        assertEquals(walker.getFirstChildKey() != NULL_KEY, walker.hasFirstChild(), "hasFirstChild of " + nodeKey);
        assertEquals(walker.getRightSiblingKey() != NULL_KEY, walker.hasRightSibling(), "hasRightSibling of " + nodeKey);
        assertEquals(walker.getLeftSiblingKey() != NULL_KEY, walker.hasLeftSibling(), "hasLeftSibling of " + nodeKey);
        assertEquals(walker.getParentKey() != NULL_KEY, walker.hasParent(), "hasParent of " + nodeKey);
      }

      assertTrue(visited > 10, "the fixture document must exercise more than a handful of nodes");
    }
  }

  /**
   * The predicates are the first thing a traversal asks, so they are also the first thing that can
   * populate the cache. Asking them BEFORE the keys must leave the keys correct.
   */
  @Test
  void predicateFirstThenKeyAgree() {
    shredDocument();

    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      rtx.moveToDocumentRoot();
      final DescendantAxis axis = new DescendantAxis(rtx, IncludeSelf.YES);

      while (axis.hasNext()) {
        final long nodeKey = axis.nextLong();

        final boolean hasFirstChild = rtx.hasFirstChild();
        final boolean hasRightSibling = rtx.hasRightSibling();
        final boolean hasLeftSibling = rtx.hasLeftSibling();

        assertEquals(hasFirstChild, rtx.getFirstChildKey() != NULL_KEY, "first child of " + nodeKey);
        assertEquals(hasRightSibling, rtx.getRightSiblingKey() != NULL_KEY, "right sibling of " + nodeKey);
        assertEquals(hasLeftSibling, rtx.getLeftSiblingKey() != NULL_KEY, "left sibling of " + nodeKey);

        // And the move the predicate promised must actually be available.
        if (hasFirstChild) {
          final long firstChildKey = rtx.getFirstChildKey();
          assertTrue(rtx.moveToFirstChild(), "moveToFirstChild of " + nodeKey);
          assertEquals(firstChildKey, rtx.getNodeKey(), "moveToFirstChild landed elsewhere from " + nodeKey);
          assertTrue(rtx.moveTo(nodeKey));
        }
      }
    }
  }

  /**
   * A failed move leaves the cursor where it was. Whatever it remembered about that position must
   * still be right afterwards — dropping the remembered keys is fine, serving the would-be target's
   * keys is not.
   */
  @Test
  void failedMoveLeavesTheKeysIntact() {
    shredDocument();

    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.moveToFirstChild());

      final long nodeKey = rtx.getNodeKey();
      final long firstChildKey = rtx.getFirstChildKey();
      final long rightSiblingKey = rtx.getRightSiblingKey();
      final long leftSiblingKey = rtx.getLeftSiblingKey();
      final long parentKey = rtx.getParentKey();

      assertFalse(rtx.moveTo(rtx.getMaxNodeKey() + 1_000), "move to a nonexistent node must fail");

      assertEquals(nodeKey, rtx.getNodeKey(), "a failed move must not reposition the cursor");
      assertEquals(firstChildKey, rtx.getFirstChildKey());
      assertEquals(rightSiblingKey, rtx.getRightSiblingKey());
      assertEquals(leftSiblingKey, rtx.getLeftSiblingKey());
      assertEquals(parentKey, rtx.getParentKey());
    }
  }

  /**
   * {@code setCurrentNode} repositions without going through {@code moveTo}, so it is the one
   * entry point a structural-key cache is likely to be forgotten at.
   */
  @Test
  void restoringASavedNodeDropsTheKeysOfWhereWeWere() {
    shredDocument();

    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final InternalJsonNodeReadOnlyTrx internalRtx = (InternalJsonNodeReadOnlyTrx) rtx;

      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());
      final ImmutableNode savedNode = internalRtx.getCurrentNode();
      final long savedKey = rtx.getNodeKey();
      final long savedFirstChildKey = rtx.getFirstChildKey();
      final long savedRightSiblingKey = rtx.getRightSiblingKey();
      final long savedParentKey = rtx.getParentKey();

      // Move somewhere with different structural keys and read them, so a cache is populated.
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.moveToRightSibling());
      rtx.getFirstChildKey();
      rtx.getRightSiblingKey();
      rtx.getParentKey();

      internalRtx.setCurrentNode(savedNode);

      assertEquals(savedKey, rtx.getNodeKey());
      assertEquals(savedFirstChildKey, rtx.getFirstChildKey());
      assertEquals(savedRightSiblingKey, rtx.getRightSiblingKey());
      assertEquals(savedParentKey, rtx.getParentKey());
    }
  }

  /**
   * A write transaction mutates the record under its own cursor in place, without repositioning, so
   * it must never answer from a remembered key. Inserting members one after another rewrites the
   * parent's first-child link every time; if the cursor had cached that link, the chain would be
   * built against a stale head and members would go missing.
   */
  @Test
  void writeTransactionSeesItsOwnStructuralWrites() {
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {

      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"));
      wtx.moveToDocumentRoot();
      assertTrue(wtx.moveToFirstChild());
      final long objectKey = wtx.getNodeKey();

      assertEquals(NULL_KEY, wtx.getFirstChildKey(), "the fixture object starts empty");
      assertFalse(wtx.hasFirstChild());

      long previousHead = NULL_KEY;
      for (int i = 0; i < 8; i++) {
        assertTrue(wtx.moveTo(objectKey));
        wtx.insertObjectRecordAsFirstChild("member" + i, new StringValue("value" + i));

        assertTrue(wtx.moveTo(objectKey));
        final long head = wtx.getFirstChildKey();
        assertTrue(wtx.hasFirstChild(), "the object has members after insert " + i);
        assertTrue(head != previousHead, "insert " + i + " must publish a new first child");

        assertTrue(wtx.moveTo(head));
        assertEquals(previousHead, wtx.getRightSiblingKey(),
            "the new head must link to the previous head after insert " + i);
        previousHead = head;
      }

      wtx.commit();
    }

    // And the committed chain reads back complete through a read-only cursor, which DOES cache.
    try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.moveToFirstChild());

      int members = 1;
      while (rtx.hasRightSibling()) {
        assertTrue(rtx.moveToRightSibling());
        members++;
      }
      assertEquals(8, members, "every inserted member must be reachable through the sibling chain");
    }
  }
}
