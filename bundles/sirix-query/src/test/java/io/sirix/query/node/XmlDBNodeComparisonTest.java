package io.sirix.query.node;

import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XmlDBNodeComparisonTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"false, true", "false, false", "true, true", "true, false"})
  void relationshipsMatchTreeRegardlessOfCursorSharing(final boolean storeDeweyIds, final boolean sharedCursor) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<seed/>"));
      assertNotNull(collection.add("tree", new DocumentParser("<r><a/><b/><c><d/></c></r>")));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("tree");
          final XmlNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx();
          final XmlNodeReadOnlyTrx otherTrx = session.beginNodeReadOnlyTrx()) {
        assertEquals(storeDeweyIds, session.getResourceConfig().areDeweyIDsStored);
        final XmlDBNode document = new XmlDBNode(trx, collection);
        final XmlDBNode root = document.getFirstChild();
        final XmlDBNode a = root.getFirstChild();
        final XmlDBNode b = a.getNextSibling();
        final XmlDBNode c = b.getNextSibling();
        final XmlDBNode d = c.getFirstChild();
        final XmlDBNode[] nodes = {document, root, a, b, c, d};
        final int[] parents = {-1, 0, 1, 1, 1, 4};
        for (final XmlDBNode node : nodes) {
          assertNotNull(node);
          assertSame(trx, node.getTrx());
        }
        for (int first = 0; first < nodes.length; first++) {
          for (int second = 0; second < nodes.length; second++) {
            final XmlDBNode other;
            if (sharedCursor) {
              other = nodes[second];
            } else {
              assertTrue(otherTrx.moveTo(nodes[second].getNodeKey()));
              other = new XmlDBNode(otherTrx, collection);
            }
            final boolean descendant = isDescendant(first, second, parents);
            final boolean ancestor = isDescendant(second, first, parents);
            final boolean sibling = first != second && parents[first] >= 0 && parents[first] == parents[second];
            final String pair = first + " compared with " + second;
            assertEquals(first == second, nodes[first].isSelfOf(other), pair);
            assertEquals(first == parents[second], nodes[first].isParentOf(other), pair);
            assertEquals(parents[first] == second, nodes[first].isChildOf(other), pair);
            assertEquals(descendant, nodes[first].isDescendantOf(other), pair);
            assertEquals(first == second || descendant, nodes[first].isDescendantOrSelfOf(other), pair);
            assertEquals(ancestor, nodes[first].isAncestorOf(other), pair);
            assertEquals(ancestor || (first == second && !storeDeweyIds), nodes[first].isAncestorOrSelfOf(other), pair);
            assertEquals(sibling, nodes[first].isSiblingOf(other), pair);
            assertEquals(sibling && first < second, nodes[first].isPrecedingSiblingOf(other), pair);
            assertEquals(sibling && first > second, nodes[first].isFollowingSiblingOf(other), pair);
            assertEquals(first < second && !ancestor, nodes[first].isPrecedingOf(other), pair);
            assertEquals(first > second && !descendant, nodes[first].isFollowingOf(other), pair);
            assertEquals(first == 0, nodes[first].isDocumentOf(other), pair);
            assertEquals(Integer.signum(first - second), Integer.signum(nodes[first].cmp(other)), pair);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void attributesAndNamespacesAreExcludedAsAxisResultsAndSiblings(final boolean storeDeweyIds) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<seed/>"));
      assertNotNull(collection.add("tree", new DocumentParser("<r xmlns:p='urn:p' id='r' other='x'><a/><b/></r>")));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("tree");
          final XmlNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        final XmlDBNode root = new XmlDBNode(trx, collection).getFirstChild();
        final XmlDBNode child = root.getFirstChild();
        final XmlDBNode attribute = root.getAttribute(new QNm("id"));
        final XmlDBNode otherAttribute = root.getAttribute(new QNm("other"));
        assertTrue(root.getTrx().moveToNamespace(0));
        final XmlDBNode namespace = new XmlDBNode(trx, collection);
        final XmlDBNode[] nodes = {root, child, attribute, otherAttribute, namespace};
        for (final XmlDBNode nonStructural : new XmlDBNode[] {attribute, otherAttribute, namespace}) {
          assertNotNull(nonStructural);
          for (final XmlDBNode node : nodes) {
            assertFalse(nonStructural.isSiblingOf(node));
            assertFalse(node.isSiblingOf(nonStructural));
            assertFalse(nonStructural.isPrecedingSiblingOf(node));
            assertFalse(node.isPrecedingSiblingOf(nonStructural));
            assertFalse(nonStructural.isFollowingSiblingOf(node));
            assertFalse(node.isFollowingSiblingOf(nonStructural));
            assertFalse(nonStructural.isPrecedingOf(node));
            assertFalse(nonStructural.isFollowingOf(node));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, true", "false, false", "true, true", "true, false"})
  void attributesAndNamespacesCanBeOrderingContexts(final boolean storeDeweyIds, final boolean sharedCursor) {
    final String[] documents = {"<r><a/><b id='x'/></r>", "<r><a id='x'/><b/></r>",
        "<r><a/><b xmlns:p='urn:p'/></r>", "<r><a xmlns:p='urn:p'/><b/></r>",
        "<r><a id='x'><d/></a><b/></r>", "<r><a xmlns:p='urn:p'><d/></a><b/></r>"};
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<seed/>"));
      for (int index = 0; index < documents.length; index++) {
        final String resource = "tree" + index;
        assertNotNull(collection.add(resource, new DocumentParser(documents[index])));
        try (final XmlResourceSession session = collection.getDatabase().beginResourceSession(resource);
            final XmlNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx();
            final XmlNodeReadOnlyTrx otherTrx = session.beginNodeReadOnlyTrx()) {
          assertEquals(storeDeweyIds, session.getResourceConfig().areDeweyIDsStored);
          final XmlDBNode root = new XmlDBNode(trx, collection).getFirstChild();
          final XmlDBNode a = root.getFirstChild();
          final XmlDBNode b = a.getNextSibling();
          final boolean preceding = index == 0 || index == 2;
          final XmlDBNode owner = preceding ? b : a;
          final XmlDBNode result = index < 4 ? (preceding ? a : b) : a.getFirstChild();
          final XmlDBNode sharedContext;
          if (index == 0 || index == 1 || index == 4) {
            sharedContext = owner.getAttribute(new QNm("id"));
          } else {
            assertTrue(owner.getTrx().moveToNamespace(0));
            sharedContext = new XmlDBNode(trx, collection);
          }
          assertNotNull(sharedContext);
          final XmlDBNode context;
          if (sharedCursor) {
            context = sharedContext;
            assertSame(trx, context.getTrx());
          } else {
            assertTrue(otherTrx.moveTo(sharedContext.getNodeKey()));
            context = new XmlDBNode(otherTrx, collection);
          }
          assertEquals(preceding, result.isPrecedingOf(context), documents[index]);
          assertEquals(!preceding, result.isFollowingOf(context), documents[index]);
          assertFalse(context.isPrecedingOf(result), documents[index]);
          assertFalse(context.isFollowingOf(result), documents[index]);
          assertFalse(owner.isPrecedingOf(context), documents[index]);
          assertFalse(owner.isFollowingOf(context), documents[index]);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rootsInDifferentResourcesAreNotAncestorOrSelf(final boolean storeDeweyIds) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<r/>"));
      assertNotNull(collection.add("second", new DocumentParser("<r/>")));
      try (final XmlResourceSession firstSession = collection.getDatabase().beginResourceSession("resource1");
          final XmlResourceSession secondSession = collection.getDatabase().beginResourceSession("second");
          final XmlNodeReadOnlyTrx firstTrx = firstSession.beginNodeReadOnlyTrx();
          final XmlNodeReadOnlyTrx secondTrx = secondSession.beginNodeReadOnlyTrx()) {
        assertEquals(storeDeweyIds, firstSession.getResourceConfig().areDeweyIDsStored);
        assertEquals(storeDeweyIds, secondSession.getResourceConfig().areDeweyIDsStored);
        assertNotEquals(firstSession.getResourceConfig().getID(), secondSession.getResourceConfig().getID());
        assertDistinctRootsAreNotAncestorOrSelf(new XmlDBNode(firstTrx, collection).getFirstChild(),
            new XmlDBNode(secondTrx, collection).getFirstChild());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unchangedRootsInDifferentRevisionsAreNotAncestorOrSelf(final boolean storeDeweyIds) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<r/>"));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("resource1")) {
        assertEquals(storeDeweyIds, session.getResourceConfig().areDeweyIDsStored);
        try (final XmlNodeTrx writer = session.beginNodeTrx()) {
          writer.commit();
        }
        assertEquals(2, session.getMostRecentRevisionNumber());
        try (final XmlNodeReadOnlyTrx firstTrx = session.beginNodeReadOnlyTrx(1);
            final XmlNodeReadOnlyTrx secondTrx = session.beginNodeReadOnlyTrx(2)) {
          assertNotEquals(firstTrx.getRevisionNumber(), secondTrx.getRevisionNumber());
          assertDistinctRootsAreNotAncestorOrSelf(new XmlDBNode(firstTrx, collection).getFirstChild(),
              new XmlDBNode(secondTrx, collection).getFirstChild());
        }
      }
    }
  }

  private static void assertDistinctRootsAreNotAncestorOrSelf(final XmlDBNode first, final XmlDBNode second) {
    assertNotNull(first);
    assertNotNull(second);
    assertEquals(new QNm("r"), first.getName());
    assertEquals(new QNm("r"), second.getName());
    assertEquals(first.getNodeKey(), second.getNodeKey());
    assertEquals(first.getDeweyID(), second.getDeweyID());
    assertFalse(first.isSelfOf(second));
    assertFalse(second.isSelfOf(first));
    assertFalse(first.isAncestorOrSelfOf(second));
    assertFalse(second.isAncestorOrSelfOf(first));
  }

  private BasicXmlDBStore configuredStore(final boolean storeDeweyIds) {
    return BasicXmlDBStore.newBuilder()
                          .location(directory)
                          .storageType(StorageType.FILE_CHANNEL)
                          .storeDeweyIds(storeDeweyIds)
                          .build();
  }

  private static boolean isDescendant(final int node, final int ancestor, final int[] parents) {
    for (int parent = parents[node]; parent >= 0; parent = parents[parent]) {
      if (parent == ancestor) {
        return true;
      }
    }
    return false;
  }
}
