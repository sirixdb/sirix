package io.sirix.query.node;

import io.brackit.query.node.parser.DocumentParser;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.settings.VersioningType;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class XmlNodeRelationshipRegressionTest {
  private static final boolean[][] DESCENDANT = {{false, false, false, false, false, false, false},
      {true, false, false, false, false, false, false}, {true, true, false, false, false, false, false},
      {true, true, true, false, false, false, false}, {true, true, false, false, false, false, false},
      {true, true, false, false, true, false, false}, {true, true, false, false, false, false, false}};

  private static final int[] PARENT = {-1, 0, 1, 2, 1, 4, 1};

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void relationshipsWithoutDeweyIdsDoNotMoveTheComparisonTarget(final VersioningType versioning) {
    try (final BasicXmlDBStore store =
        BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).storeDeweyIds(false).build()) {
      final XmlDBCollection collection = store.create("relationships",
          new DocumentParser("<r><child><leaf/></child><other><nested/></other><tail/></r>"));
      final XmlResourceSession session = collection.getDocument(1).getTrx().getResourceSession();
      for (final boolean writerBacked : new boolean[] {false, true}) {
        try (final XmlNodeReadOnlyTrx trx = writerBacked
            ? session.beginNodeTrx()
            : session.beginNodeReadOnlyTrx(1)) {
          final XmlDBNode document = new XmlDBNode(trx, collection);
          final XmlDBNode root = document.getFirstChild();
          final XmlDBNode child = root.getFirstChild();
          final XmlDBNode leaf = child.getFirstChild();
          final XmlDBNode other = child.getNextSibling();
          final XmlDBNode nested = other.getFirstChild();
          final XmlDBNode tail = other.getNextSibling();
          final XmlDBNode[] nodes = {document, root, child, leaf, other, nested, tail};
          for (int source = 0; source < nodes.length; source++) {
            for (int target = 0; target < nodes.length; target++) {
              final String pair = source + " -> " + target + ", writer=" + writerBacked;
              final boolean self = source == target;
              assertEquals(DESCENDANT[source][target], nodes[source].isDescendantOf(nodes[target]), pair);
              assertEquals(self || DESCENDANT[source][target], nodes[source].isDescendantOrSelfOf(nodes[target]), pair);
              assertEquals(DESCENDANT[target][source], nodes[source].isAncestorOf(nodes[target]), pair);
              assertEquals(self || DESCENDANT[target][source], nodes[source].isAncestorOrSelfOf(nodes[target]), pair);
              final boolean unrelated = !self && !DESCENDANT[source][target] && !DESCENDANT[target][source];
              assertEquals(unrelated && source < target, nodes[source].isPrecedingOf(nodes[target]), pair);
              assertEquals(unrelated && source > target, nodes[source].isFollowingOf(nodes[target]), pair);
              final boolean siblings = !self && PARENT[source] >= 0 && PARENT[source] == PARENT[target];
              assertEquals(siblings && source < target, nodes[source].isPrecedingSiblingOf(nodes[target]), pair);
              assertEquals(siblings && source > target, nodes[source].isFollowingSiblingOf(nodes[target]), pair);
            }
          }
        }
      }
    }
  }
}
