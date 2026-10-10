package io.sirix.query.node;

import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.ResourceConfiguration;
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
            assertEquals(ancestor || first == second, nodes[first].isAncestorOrSelfOf(other), pair);
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
    final String[] documents = {"<r><a/><b id='x'/></r>", "<r><a id='x'/><b/></r>", "<r><a/><b xmlns:p='urn:p'/></r>",
        "<r><a xmlns:p='urn:p'/><b/></r>", "<r><a id='x'><d/></a><b/></r>", "<r><a xmlns:p='urn:p'><d/></a><b/></r>"};
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
          final XmlDBNode owner = preceding
              ? b
              : a;
          final XmlDBNode result = index < 4
              ? (preceding
                  ? a
                  : b)
              : a.getFirstChild();
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

  @ParameterizedTest
  @CsvSource({"false, true", "false, false", "true, true", "true, false"})
  void attributesAndNamespacesHaveStructuralAncestors(final boolean storeDeweyIds, final boolean sharedCursor) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection =
          store.create("collection", new DocumentParser("<r xmlns:p='urn:p' id='r'><a xmlns:q='urn:q' id='a'/></r>"));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("resource1");
          final XmlNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx();
          final XmlNodeReadOnlyTrx otherTrx = session.beginNodeReadOnlyTrx()) {
        final XmlDBNode document = new XmlDBNode(trx, collection);
        final XmlDBNode root = document.getFirstChild();
        final XmlDBNode child = root.getFirstChild();
        final XmlDBNode[] ancestors = {document, root, child};
        final XmlDBNode[] nonStructural = {root.getAttribute(new QNm("id")), namespace(root, 0),
            child.getAttribute(new QNm("id")), namespace(child, 0)};
        for (int index = 0; index < nonStructural.length; index++) {
          assertNotNull(nonStructural[index]);
          final XmlDBNode other;
          if (sharedCursor) {
            other = nonStructural[index];
          } else {
            assertTrue(otherTrx.moveTo(nonStructural[index].getNodeKey()));
            other = new XmlDBNode(otherTrx, collection);
          }
          for (int ancestor = 0; ancestor < ancestors.length; ancestor++) {
            final boolean expected = ancestor < 2 || index >= 2;
            final boolean owner = (index < 2 && ancestor == 1) || (index >= 2 && ancestor == 2);
            assertEquals(owner, ancestors[ancestor].isParentOf(other));
            assertEquals(ancestor == 0, ancestors[ancestor].isDocumentOf(other));
            if (index % 2 == 0) {
              assertEquals(owner, other.isAttributeOf(ancestors[ancestor]));
            }
            assertEquals(expected, ancestors[ancestor].isAncestorOf(other));
            assertEquals(expected, ancestors[ancestor].isAncestorOrSelfOf(other));
            assertFalse(other.isAncestorOf(ancestors[ancestor]));
            assertFalse(other.isAncestorOrSelfOf(ancestors[ancestor]));
            assertFalse(other.isDescendantOf(ancestors[ancestor]));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, true", "false, false", "true, true", "true, false"})
  void documentOrderIncludesNamespacesAndAttributesAtEveryDepth(final boolean storeDeweyIds,
      final boolean sharedCursor) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection =
          store.create("collection", new DocumentParser("<r xmlns:p='urn:p' xmlns:q='urn:q' id='r' other='x'>"
              + "<a xmlns:s='urn:s' id='a' other='y'><d id='d'/></a><b/></r>"));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("resource1");
          final XmlNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx();
          final XmlNodeReadOnlyTrx otherTrx = session.beginNodeReadOnlyTrx()) {
        final XmlDBNode document = new XmlDBNode(trx, collection);
        final XmlDBNode root = document.getFirstChild();
        final XmlDBNode a = root.getFirstChild();
        final XmlDBNode b = a.getNextSibling();
        final XmlDBNode d = a.getFirstChild();
        final XmlDBNode[] nodes = {document, root, namespace(root, 0), namespace(root, 1),
            root.getAttribute(new QNm("id")), root.getAttribute(new QNm("other")), a, namespace(a, 0),
            a.getAttribute(new QNm("id")), a.getAttribute(new QNm("other")), d, d.getAttribute(new QNm("id")), b};
        for (final XmlDBNode node : nodes) {
          assertNotNull(node);
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
            assertEquals(Integer.signum(first - second), Integer.signum(nodes[first].cmp(other)),
                first + " compared with " + second);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"database, false, false", "database, true, true", "database, false, true", "database, true, false",
      "resource, false, false", "resource, true, true", "resource, false, true", "resource, true, false",
      "revision, false, false", "revision, true, true"})
  void relationshipsRequireTheSameDocument(final String difference, final boolean firstDeweyIds,
      final boolean secondDeweyIds) {
    final String document =
        "<r xmlns:r='urn:r' id='r'>" + "<a xmlns:p='urn:p' id='a'><d/></a><b xmlns:q='urn:q' id='b'/></r>";
    try (final BasicXmlDBStore store = configuredStore(firstDeweyIds)) {
      assertNotNull(store.create("first", new DocumentParser(document)));
    }
    try (final BasicXmlDBStore store = configuredStore(secondDeweyIds)) {
      final XmlDBCollection firstCollection = store.lookup("first");
      assertNotNull(firstCollection);
      final XmlDBCollection secondCollection = difference.equals("database")
          ? store.create("second", new DocumentParser(document))
          : firstCollection;
      final String secondResource = difference.equals("resource")
          ? "second"
          : "resource1";
      if (difference.equals("resource")) {
        assertNotNull(secondCollection.add(secondResource, new DocumentParser(document)));
      }
      if (difference.equals("revision")) {
        try (final XmlResourceSession session = firstCollection.getDatabase().beginResourceSession("resource1");
            final XmlNodeTrx writer = session.beginNodeTrx()) {
          writer.commit();
        }
      }
      try (
          final XmlNodeReadOnlyTrx firstTrx =
              firstCollection.getDatabase().beginResourceSession("resource1").beginNodeReadOnlyTrx(1);
          final XmlNodeReadOnlyTrx secondTrx = secondCollection.getDatabase()
                                                               .beginResourceSession(secondResource)
                                                               .beginNodeReadOnlyTrx(difference.equals("revision")
                                                                   ? 2
                                                                   : 1)) {
        final XmlDBNode firstDocument = new XmlDBNode(firstTrx, firstCollection);
        final XmlDBNode secondDocument = new XmlDBNode(secondTrx, secondCollection);
        final ResourceConfiguration firstConfig = firstTrx.getResourceSession().getResourceConfig();
        final ResourceConfiguration secondConfig = secondTrx.getResourceSession().getResourceConfig();
        assertEquals(firstDeweyIds, firstConfig.areDeweyIDsStored);
        assertEquals(secondDeweyIds, secondConfig.areDeweyIDsStored);
        if (difference.equals("database")) {
          assertNotEquals(firstConfig.getDatabaseId(), secondConfig.getDatabaseId());
          assertEquals(firstConfig.getID(), secondConfig.getID());
        } else {
          assertEquals(firstConfig.getDatabaseId(), secondConfig.getDatabaseId());
          if (difference.equals("resource")) {
            assertNotEquals(firstConfig.getID(), secondConfig.getID());
          } else {
            assertEquals(firstConfig.getID(), secondConfig.getID());
            assertNotEquals(firstTrx.getRevisionNumber(), secondTrx.getRevisionNumber());
          }
        }
        final XmlDBNode[] firstNodes = relationshipNodes(firstDocument);
        final XmlDBNode[] secondNodes = relationshipNodes(secondDocument);
        for (int index = 0; index < firstNodes.length; index++) {
          assertNotNull(firstNodes[index]);
          assertNotNull(secondNodes[index]);
          assertEquals(firstNodes[index].getNodeKey(), secondNodes[index].getNodeKey());
          for (final XmlDBNode other : secondNodes) {
            assertUnrelated(firstNodes[index], other);
            assertUnrelated(other, firstNodes[index]);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void collidingKeysInDifferentTreesDoNotCreateRelationships(final boolean storeDeweyIds) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds)) {
      final XmlDBCollection collection = store.create("collection", new DocumentParser("<r><a/><b><d/></b></r>"));
      assertNotNull(collection.add("second", new DocumentParser("<r><c><d/><e/></c></r>")));
      try (
          final XmlNodeReadOnlyTrx firstTrx =
              collection.getDatabase().beginResourceSession("resource1").beginNodeReadOnlyTrx();
          final XmlNodeReadOnlyTrx secondTrx =
              collection.getDatabase().beginResourceSession("second").beginNodeReadOnlyTrx()) {
        final XmlDBNode firstRoot = new XmlDBNode(firstTrx, collection).getFirstChild();
        final XmlDBNode a = firstRoot.getFirstChild();
        final XmlDBNode b = a.getNextSibling();
        final XmlDBNode firstD = b.getFirstChild();
        final XmlDBNode c = new XmlDBNode(secondTrx, collection).getFirstChild().getFirstChild();
        final XmlDBNode secondD = c.getFirstChild();
        assertEquals(b.getNodeKey(), secondD.getNodeKey());
        assertUnrelated(firstD, secondD);
        assertUnrelated(secondD, firstD);
        assertUnrelated(a, secondD);
        assertUnrelated(secondD, a);
      }
    }
  }

  private static XmlDBNode[] relationshipNodes(final XmlDBNode document) {
    final XmlDBNode root = document.getFirstChild();
    final XmlDBNode a = root.getFirstChild();
    final XmlDBNode b = a.getNextSibling();
    return new XmlDBNode[] {document, root, a, b, a.getFirstChild(), root.getAttribute(new QNm("id")),
        namespace(root, 0), a.getAttribute(new QNm("id")), namespace(a, 0), b.getAttribute(new QNm("id")),
        namespace(b, 0)};
  }

  private static void assertUnrelated(final XmlDBNode first, final XmlDBNode second) {
    final String pair = first.getNodeKey() + " compared with " + second.getNodeKey();
    assertFalse(first.isSelfOf(second), pair);
    assertFalse(first.isParentOf(second), pair);
    assertFalse(first.isChildOf(second), pair);
    assertFalse(first.isDescendantOf(second), pair);
    assertFalse(first.isDescendantOrSelfOf(second), pair);
    assertFalse(first.isAncestorOf(second), pair);
    assertFalse(first.isAncestorOrSelfOf(second), pair);
    assertFalse(first.isSiblingOf(second), pair);
    assertFalse(first.isPrecedingSiblingOf(second), pair);
    assertFalse(first.isFollowingSiblingOf(second), pair);
    assertFalse(first.isPrecedingOf(second), pair);
    assertFalse(first.isFollowingOf(second), pair);
    assertFalse(first.isAttributeOf(second), pair);
    assertFalse(first.isDocumentOf(second), pair);
  }

  private static XmlDBNode namespace(final XmlDBNode owner, final int index) {
    final XmlNodeReadOnlyTrx trx = owner.getTrx();
    assertTrue(trx.moveToNamespace(index));
    return new XmlDBNode(trx, owner.getCollection());
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
