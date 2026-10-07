package io.sirix.query.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.Query;
import io.brackit.query.jdm.Iter;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

final class XmlAliasIdentityRegressionTest {
  private static final String XML = "<r xmlns:p='urn:p' a='1'><c>old</c><tail/></r>";
  private static final String VARIABLES = "declare variable $a external; declare variable $b external; ";
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void aliasesShareStoredNodeIdentityAcrossHistory(final VersioningType versioning) throws IOException {
    for (final boolean deweyIds : new boolean[] {false, true}) {
      try (
          final BasicXmlDBStore store = BasicXmlDBStore.newBuilder()
                                                       .location(directory.resolve("ids-" + deweyIds))
                                                       .versioningType(versioning)
                                                       .storeDeweyIds(deweyIds)
                                                       .build();
          final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
          final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
        final XmlDBCollection collection = store.create("data", new DocumentParser(XML));
        final XmlDBNode text = collection.getDocument(1).getFirstChild().getFirstChild().getFirstChild();
        try (final XmlNodeTrx writer = text.getTrx().getResourceSession().beginNodeTrx()) {
          assertTrue(writer.moveTo(text.getNodeKey()));
          writer.setValue("new");
          writer.commit();
        }
        final Path database = store.getLocation().resolve("data");
        final Path link = store.getLocation().resolve("linked");
        Files.createSymbolicLink(link, database);
        final String[] aliases =
            {database.toString(), store.getLocation().resolve(".").resolve("data").toString(), link.toString()};
        for (final String name : aliases) {
          try (final XmlDBCollection alias = store.lookup(name)) {
            assertNotSame(collection, alias);
            for (int revision = 1; revision <= 2; revision++) {
              final XmlDBNode[] original = nodes(collection.getDocument(revision));
              final XmlDBNode[] other = nodes(alias.getDocument(revision));
              for (int i = 0; i < original.length; i++) {
                assertTrue(original[i].isSelfOf(other[i]), name + ": " + i);
                assertTrue(other[i].isSelfOf(original[i]));
                assertEquals(0, original[i].cmp(other[i]));
                assertEquals(original[i].hashCode(), other[i].hashCode());
                assertTrue(original[i].isAncestorOrSelfOf(other[i]));
              }
              assertTrue(original[1].isParentOf(other[2]));
              assertTrue(other[2].isChildOf(original[1]));
              assertTrue(original[0].isAncestorOf(other[3]));
              assertTrue(other[3].isDescendantOf(original[0]));
              assertTrue(original[2].isSiblingOf(other[4]));
              assertTrue(original[2].isPrecedingSiblingOf(other[4]));
              assertTrue(other[4].isFollowingSiblingOf(original[2]));
              assertTrue(other[5].isAttributeOf(original[1]));
              assertTrue(original[0].isDocumentOf(other[6]));
              for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 5; j++) {
                  assertEquals(Integer.signum(i - j), Integer.signum(original[i].cmp(other[j])));
                }
              }
              // Namespace nodes precede every descendant, even with no Dewey IDs and aliases.
              assertTrue(original[6].cmp(other[3]) < 0);
              assertTrue(other[3].cmp(original[6]) > 0);
              context.bind(new QNm("a"), original[6]);
              context.bind(new QNm("b"), other[3]);
              assertEquals(new Int64(2), new Query(chain, VARIABLES + "count($a | $b)").execute(context));
              try (final Iter union = new Query(chain, VARIABLES + "$b | $a").execute(context).iterate()) {
                assertEquals(original[6], union.next());
                assertEquals(other[3], union.next());
                assertEquals(null, union.next());
              }
              context.bind(new QNm("a"), original[1]);
              context.bind(new QNm("b"), other[1]);
              assertEquals(Bool.TRUE, new Query(chain, VARIABLES + "$a is $b").execute(context));
              assertEquals(new Int64(1), new Query(chain, VARIABLES + "count($a | $b)").execute(context));
              assertEquals(new Int64(1), new Query(chain, VARIABLES + "count($a/@a | $b/@a)").execute(context));
              assertFalse(original[1].isSelfOf(alias.getDocument(revision == 1
                  ? 2
                  : 1).getFirstChild()));
            }
          }
          assertEquals("new",
              collection.getDocument(2).getFirstChild().getFirstChild().getFirstChild().getValue().stringValue());
          assertEquals("old", text.getValue().stringValue());
        }
        final XmlDBNode distinct = store.create("other", new DocumentParser(XML)).getDocument(1).getFirstChild();
        final XmlDBNode root = collection.getDocument(1).getFirstChild();
        assertFalse(root.isSelfOf(distinct));
        assertFalse(root.isAncestorOrSelfOf(distinct));
        assertTrue(root.cmp(distinct) != 0);
        context.bind(new QNm("a"), root);
        context.bind(new QNm("b"), distinct);
        assertEquals(Bool.FALSE, new Query(chain, VARIABLES + "$a is $b").execute(context));
        assertEquals(new Int64(2), new Query(chain, VARIABLES + "count($a | $b)").execute(context));
      }
    }
  }

  private static XmlDBNode[] nodes(final XmlDBNode document) {
    final XmlDBNode root = document.getFirstChild();
    final XmlDBNode child = root.getFirstChild();
    final XmlDBNode text = child.getFirstChild();
    final XmlDBNode tail = child.getNextSibling();
    final XmlDBNode attribute = root.getAttribute(new QNm("a"));
    final XmlNodeReadOnlyTrx reader = root.getTrx();
    assertTrue(reader.moveToNamespace(0));
    final XmlDBNode namespace = new XmlDBNode(reader, document.getCollection());
    return new XmlDBNode[] {document, root, child, text, tail, attribute, namespace};
  }
}
