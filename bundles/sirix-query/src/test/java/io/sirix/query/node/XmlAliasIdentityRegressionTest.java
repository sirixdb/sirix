package io.sirix.query.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Kind;
import io.brackit.query.jdm.StructuredItem;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.update.op.OpType;
import io.brackit.query.update.op.UpdateOp;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  @ParameterizedTest
  @MethodSource("updateConfigurations")
  void aliasPendingUpdatesShareOneWriter(final VersioningType versioning, final CommitStrategy strategy,
      final boolean deweyIds, final int suppliedWriter) throws IOException {
    checkUpdate(versioning, strategy, deweyIds, suppliedWriter, true, "rename-delete", "<r><a/></r>",
        "(rename node $a/r/a as 'z', delete node $b/r/a)", "<r/>");
    checkUpdate(versioning, strategy, deweyIds, suppliedWriter, true, "content",
        "<r><a>old</a><b>before</b></r>",
        "(replace value of node $a/r/a with 'first', replace value of node $b/r/b with 'second')",
        "<r><a>first</a><b>second</b></r>");
  }

  @ParameterizedTest
  @MethodSource("updateConfigurations")
  void sameNameAttributeReplacementUsesTheExecutionView(final VersioningType versioning,
      final CommitStrategy strategy, final boolean deweyIds, final int suppliedWriter) throws IOException {
    for (final boolean alias : new boolean[] {false, true}) {
      checkUpdate(versioning, strategy, deweyIds, suppliedWriter, alias, "attribute", "<r a='old' keep='yes'/>",
          "(replace node $a/r/@a with attribute a {'new'}, insert node <c/> into $b/r)",
          "<r keep=\"yes\" a=\"new\"><c xmlns=\"\"/></r>");
    }
  }

  private void checkUpdate(final VersioningType versioning, final CommitStrategy strategy, final boolean deweyIds,
      final int suppliedWriter, final boolean useAlias, final String name, final String xml, final String update,
      final String expected) throws IOException {
    final String configuration = name + "-" + strategy + "-" + deweyIds + "-" + suppliedWriter + "-" + useAlias;
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder()
                                                    .location(directory.resolve(configuration))
                                                    .versioningType(versioning)
                                                    .storeDeweyIds(deweyIds)
                                                    .build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, strategy)) {
      final XmlDBCollection collection = store.create("data", new DocumentParser(xml));
      final Path link = store.getLocation().resolve("linked");
      Files.createSymbolicLink(link, store.getLocation().resolve("data"));
      try (final XmlDBCollection alias = store.lookup(link.toString())) {
        final XmlDBNode original = collection.getDocument(1);
        final XmlDBNode other = useAlias ? alias.getDocument(1) : original;
        final XmlResourceSession firstSession = original.getTrx().getResourceSession();
        final XmlResourceSession secondSession = other.getTrx().getResourceSession();
        if (useAlias) {
          assertNotSame(firstSession, secondSession);
        }
        final String before = serialize(original);
        final AtomicInteger commits = new AtomicInteger();
        try (final XmlNodeTrx supplied = suppliedWriter == 0 ? null
            : (suppliedWriter == 1 ? firstSession : secondSession).beginNodeTrx(1)) {
          if (supplied != null) {
            supplied.addPreCommitHook(unused -> commits.incrementAndGet());
          }
          context.bind(new QNm("a"), original);
          context.bind(new QNm("b"), other);
          new Query(chain, VARIABLES + update).execute(context);
          assertEquals(before, serialize(original));
          assertEquals(before, serialize(other));
          if (strategy == CommitStrategy.EXPLICIT) {
            assertEquals(1, firstSession.getMostRecentRevisionNumber());
            assertEquals(0, commits.get());
            try (final XmlNodeTrx writer = firstSession.getNodeTrx().orElseGet(
                () -> secondSession.getNodeTrx().orElseThrow())) {
              if (supplied != null) {
                assertSame(supplied, writer);
                if (useAlias) {
                  assertTrue((suppliedWriter == 1 ? secondSession : firstSession).getNodeTrx().isEmpty());
                }
              }
              writer.commit();
            }
          } else if (supplied != null) {
            assertTrue(supplied.isClosed());
          }
          if (supplied != null) {
            assertEquals(1, commits.get());
          }
        }
        assertEquals(2, firstSession.getMostRecentRevisionNumber());
        assertEquals(2, secondSession.getMostRecentRevisionNumber());
        assertEquals(expected, serialize(collection.getDocument(2)));
        assertEquals(expected, serialize(alias.getDocument(2)));
        assertEquals(before, serialize(original));
        assertEquals(before, serialize(other));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void privateWriterComparisonsPreserveSnapshotIdentity(final VersioningType versioning)
      throws IOException {
    for (final boolean deweyIds : new boolean[] {false, true}) {
      try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder()
                                                      .location(directory.resolve("scoped-" + deweyIds))
                                                      .versioningType(versioning)
                                                      .storeDeweyIds(deweyIds)
                                                      .build()) {
        final XmlDBCollection collection = store.create("data", new DocumentParser(XML));
        final Path link = store.getLocation().resolve("linked");
        Files.createSymbolicLink(link, store.getLocation().resolve("data"));
        try (final XmlDBCollection alias = store.lookup(link.toString());
            final XmlNodeTrx writer = alias.getDocument(1).getTrx().getResourceSession().beginNodeTrx(1)) {
          final XmlDBNode[] originals = nodes(collection.getDocument(1));
          final XmlDBNode[] aliases = nodes(alias.getDocument(1));
          writer.moveToDocumentRoot();
          final XmlDBNode[] live = nodes(new XmlDBNode(writer, alias));
          writer.beginAtomicOperation();
          try {
            for (int i = 0; i < originals.length; i++) {
              final XmlDBNode source = originals[i];
              final XmlDBNode otherAlias = aliases[i];
              final XmlDBNode target = source.writerView(writer);
              final XmlNodeReadOnlyTrx reader = source.getTrx();
              final int snapshotHash = source.hashCode();
              target.applyUpdate(new UpdateOp() {
                @Override
                public XmlDBNode getTarget() {
                  return source;
                }

                @Override
                public OpType getType() {
                  return OpType.RENAME;
                }

                @Override
                public void apply() {
                  apply(source);
                }

                @Override
                public void apply(final StructuredItem executionTarget) {
                  final XmlDBNode executing = (XmlDBNode) executionTarget;
                  assertSame(target, executing);
                  assertSame(writer, executing.getTrx());
                  assertSame(reader, source.getTrx());
                  assertEquals(1, otherAlias.getTrx().getRevisionNumber());
                  assertEquals(snapshotHash, source.hashCode());
                  assertEquals(snapshotHash, otherAlias.hashCode());
                  assertFalse(source.isSelfOf(executing));
                  assertNotEquals(0, source.cmp(executing));
                  for (final XmlDBNode node : live) {
                    checkRelationships(target, node, executing, node);
                    checkRelationships(node, target, node, executing);
                    checkRelationships(source, node, otherAlias, node);
                    checkRelationships(node, source, node, otherAlias);
                  }
                }
              });
              assertSame(reader, source.getTrx());
              assertEquals(snapshotHash, source.hashCode());
              assertTrue(source.isSelfOf(otherAlias));
              assertFalse(source.isSelfOf(target));
              assertNotEquals(0, source.cmp(target));
              assertEquals(1, source.getTrx().getRevisionNumber());
              assertEquals(1, otherAlias.getTrx().getRevisionNumber());
            }
          } finally {
            writer.endAtomicOperation();
          }
        }
      }
    }
  }

  private static void checkRelationships(final XmlDBNode expected, final XmlDBNode expectedOther,
      final XmlDBNode actual, final XmlDBNode actualOther) {
    assertEquals(expected.isSelfOf(expectedOther), actual.isSelfOf(actualOther));
    assertEquals(expected.equals(expectedOther), actual.equals(actualOther));
    assertEquals(expected.cmp(expectedOther), actual.cmp(actualOther));
    assertEquals(expected.isParentOf(expectedOther), actual.isParentOf(actualOther));
    assertEquals(expected.isChildOf(expectedOther), actual.isChildOf(actualOther));
    assertEquals(expected.isDescendantOf(expectedOther), actual.isDescendantOf(actualOther));
    assertEquals(expected.isDescendantOrSelfOf(expectedOther), actual.isDescendantOrSelfOf(actualOther));
    assertEquals(expected.isAncestorOf(expectedOther), actual.isAncestorOf(actualOther));
    assertEquals(expected.isAncestorOrSelfOf(expectedOther), actual.isAncestorOrSelfOf(actualOther));
    assertEquals(expected.isSiblingOf(expectedOther), actual.isSiblingOf(actualOther));
    assertEquals(expected.isPrecedingSiblingOf(expectedOther), actual.isPrecedingSiblingOf(actualOther));
    assertEquals(expected.isFollowingSiblingOf(expectedOther), actual.isFollowingSiblingOf(actualOther));
    assertEquals(expected.isPrecedingOf(expectedOther), actual.isPrecedingOf(actualOther));
    assertEquals(expected.isFollowingOf(expectedOther), actual.isFollowingOf(actualOther));
    if (expected.getKind() != Kind.DOCUMENT) {
      assertEquals(expected.isAttributeOf(expectedOther), actual.isAttributeOf(actualOther));
    }
    assertEquals(expected.isDocumentOf(expectedOther), actual.isDocumentOf(actualOther));
    assertEquals(expected.isDocumentRoot(), actual.isDocumentRoot());
    assertEquals(expected.isRoot(), actual.isRoot());
    assertEquals(expected.isNextOf(expectedOther), actual.isNextOf(actualOther));
    assertEquals(expected.isPreviousOf(expectedOther), actual.isPreviousOf(actualOther));
    assertEquals(expected.isFutureOf(expectedOther), actual.isFutureOf(actualOther));
    assertEquals(expected.isFutureOrSelfOf(expectedOther), actual.isFutureOrSelfOf(actualOther));
    assertEquals(expected.isEarlierOf(expectedOther), actual.isEarlierOf(actualOther));
    assertEquals(expected.isEarlierOrSelfOf(expectedOther), actual.isEarlierOrSelfOf(actualOther));
    assertEquals(expected.isLastOf(expectedOther), actual.isLastOf(actualOther));
    assertEquals(expected.isFirstOf(expectedOther), actual.isFirstOf(actualOther));
  }

  private static List<Arguments> updateConfigurations() {
    final List<Arguments> configurations = new ArrayList<>(48);
    for (final VersioningType versioning : VersioningType.values()) {
      for (final CommitStrategy strategy : CommitStrategy.values()) {
        for (final boolean deweyIds : new boolean[] {false, true}) {
          for (int suppliedWriter = 0; suppliedWriter < 3; suppliedWriter++) {
            configurations.add(Arguments.of(versioning, strategy, deweyIds, suppliedWriter));
          }
        }
      }
    }
    return configurations;
  }

  private static String serialize(final XmlDBNode document) {
    final BrackitQueryContext context = new BrackitQueryContext();
    context.setContextItem(document);
    final StringWriter output = new StringWriter();
    new Query(new CompileChain(), "$$").serialize(context, new PrintWriter(output));
    return output.toString();
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
