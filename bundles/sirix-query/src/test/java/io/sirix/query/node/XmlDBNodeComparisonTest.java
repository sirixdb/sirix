package io.sirix.query.node;

import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            assertEquals(first == second || ancestor, nodes[first].isAncestorOrSelfOf(other), pair);
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
  void attributesAndNamespacesAreExcludedFromSiblingAndOrderingAxes(final boolean storeDeweyIds) {
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
            assertFalse(node.isPrecedingOf(nonStructural));
            assertFalse(nonStructural.isFollowingOf(node));
            assertFalse(node.isFollowingOf(nonStructural));
          }
        }
      }
    }
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
