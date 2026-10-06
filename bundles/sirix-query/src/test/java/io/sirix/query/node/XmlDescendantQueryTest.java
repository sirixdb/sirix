package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.translator.SirixTranslator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XmlDescendantQueryTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"false, true", "true, true", "false, false", "true, false"})
  void mixedDepthDescendantPlansPreserveOrderBeforePositionalPredicates(final boolean storeDeweyIds,
      final boolean pathSummary) {
    final String[] documents = {"<r><a><b><hit id='1'/></b><hit id='2'/><b><hit id='3'/></b></a></r>",
        "<r><a><hit id='self'><b><hit id='1'><hit id='2'/><deep><hit id='3'/></deep></hit></b>"
            + "<hit id='4'><b><hit id='5'/></b></hit><b><hit id='6'/></b></hit></a></r>",
        "<hit id='1'><b><hit id='2'/></b><hit id='3'/><b><hit id='4'/></b></hit>"};
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds, pathSummary);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      for (int index = 0; index < documents.length; index++) {
        final String resource = "tree" + index;
        assertNotNull(collection.add(resource, new DocumentParser(documents[index])));
        try (final XmlResourceSession session = collection.getDatabase().beginResourceSession(resource);
            final XmlNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          XmlDBNode start = new XmlDBNode(rtx, collection);
          if (index < 2) {
            start = start.getFirstChild().getFirstChild();
            if (index == 1) {
              start = start.getFirstChild();
            }
          }
          for (final String axis : new String[] {"descendant", "descendant-or-self"}) {
            final String[] expected = index == 1
                ? (axis.equals("descendant-or-self")
                    ? new String[] {"self", "1", "2", "3", "4", "5", "6"}
                    : new String[] {"1", "2", "3", "4", "5", "6"})
                : (index == 0
                    ? new String[] {"1", "2", "3"}
                    : new String[] {"1", "2", "3", "4"});
            assertOrderedMatches(chain, context, start, axis, expected);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, true", "true, true", "false, false", "true, false"})
  void editedDescendantPlansUseTreeOrderRatherThanNodeKeyOrder(final boolean storeDeweyIds, final boolean pathSummary) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds, pathSummary);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      assertNotNull(collection.add("tree", new DocumentParser(
          "<r><a><hit id='self'><b><hit id='1'/></b><hit id='3'/><b><hit id='4'/></b></hit></a></r>")));
      try (final XmlResourceSession session = collection.getDatabase().beginResourceSession("tree")) {
        session.getNodeTrx().ifPresent(XmlNodeTrx::close);
        try (final XmlNodeTrx writer = session.beginNodeTrx()) {
          writer.moveToDocumentRoot();
          assertTrue(writer.moveToFirstChild());
          assertTrue(writer.moveToFirstChild());
          assertTrue(writer.moveToFirstChild());
          assertTrue(writer.moveToFirstChild());
          final long branchKey = writer.getNodeKey();
          assertTrue(writer.moveToRightSibling());
          final long existingChild = writer.getNodeKey();
          assertTrue(writer.moveTo(branchKey));
          writer.insertElementAsRightSibling(new QNm("hit"));
          assertTrue(writer.getNodeKey() > existingChild);
          assertEquals(existingChild, writer.getRightSiblingKey());
          writer.insertAttribute(new QNm("id"), "2");

          assertTrue(writer.moveTo(branchKey));
          assertTrue(writer.moveToFirstChild());
          final long existingNested = writer.getNodeKey();
          assertTrue(writer.moveTo(branchKey));
          writer.insertElementAsFirstChild(new QNm("hit"));
          assertTrue(writer.getNodeKey() > existingNested);
          assertEquals(existingNested, writer.getRightSiblingKey());
          writer.insertAttribute(new QNm("id"), "0");
          writer.commit();
        }
        try (final XmlNodeReadOnlyTrx oldTrx = session.beginNodeReadOnlyTrx(1);
            final XmlNodeReadOnlyTrx newTrx = session.beginNodeReadOnlyTrx(2)) {
          final XmlDBNode oldStart = new XmlDBNode(oldTrx, collection).getFirstChild().getFirstChild().getFirstChild();
          final XmlDBNode newStart = new XmlDBNode(newTrx, collection).getFirstChild().getFirstChild().getFirstChild();
          for (final String axis : new String[] {"descendant", "descendant-or-self"}) {
            for (final int revision : new int[] {1, 2, 1}) {
              final String[] expected = revision == 1
                  ? (axis.equals("descendant-or-self")
                      ? new String[] {"self", "1", "3", "4"}
                      : new String[] {"1", "3", "4"})
                  : (axis.equals("descendant-or-self")
                      ? new String[] {"self", "0", "1", "2", "3", "4"}
                      : new String[] {"0", "1", "2", "3", "4"});
              assertOrderedMatches(chain, context, revision == 1
                  ? oldStart
                  : newStart, axis, expected);
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, true", "true, true", "false, false", "true, false"})
  void namedDescendantPlansStartAtTheirNonRootContext(final boolean storeDeweyIds, final boolean pathSummary) {
    final String[] documents = {"<r><a><hit id='h1'/></a></r>", "<r><a><branch><hit id='h1'/></branch></a><noise/></r>",
        "<r><a><left><hit id='h1'/></left><right><hit id='h2'/></right></a></r>",
        "<r><a><hit id='h1'/><branch><hit id='h2'/></branch></a></r>",
        "<r><a><hit id='self'><branch><hit id='child'/></branch></hit></a></r>", "<r><a><hit id='self'/></a></r>"};
    final String[] descendants = {"h1", "h1", "h1,h2", "h1,h2", "child", ""};
    try (
        final BasicXmlDBStore store = BasicXmlDBStore.newBuilder()
                                                     .location(directory)
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .storeDeweyIds(storeDeweyIds)
                                                     .buildPathSummary(pathSummary)
                                                     .build();
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      for (int index = 0; index < documents.length; index++) {
        final String resource = "tree" + index;
        assertNotNull(collection.add(resource, new DocumentParser(documents[index])));
        final String start = "xn:doc('collection','" + resource + "')/r/a" + (index >= 4
            ? "/hit"
            : "");
        for (final String axis : new String[] {"descendant", "descendant-or-self"}) {
          final String query = "string-join(for $hit in " + start + "/" + axis + "::hit return string($hit/@id), ',')";
          final String expected = axis.equals("descendant-or-self") && index >= 4
              ? (index == 4
                  ? "self,child"
                  : "self")
              : descendants[index];
          assertEquals(expected, ((Str) new Query(chain, query).evaluate(context)).stringValue(), query);
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, true", "true, true", "false, false", "true, false"})
  void namedDescendantPlansExcludeUnrelatedAndNonElementSelfContexts(final boolean storeDeweyIds,
      final boolean pathSummary) {
    final String[] documents = {"<r><a/><hit id='outside'/></r>",
        "<r><left><hit id='left'/></left><right><hit id='right'/></right><b><a/></b></r>",
        "<r><b><a id='context'><deep><hit id='inside'/></deep></a><hit id='outside'/></b></r>",
        "<r><a><hit id='inside'/></a><branch><hit id='outside'/></branch></r>",
        "<r hit='attribute'><hit id='outside'/></r>", "<r><a>text</a><hit id='outside'/></r>",
        "<hit id='root'><hit id='nested'/></hit>"};
    final String[] starts = {"/r/a", "/r/b/a", "/r/b/a", "/r/a", "/r/@hit", "/r/a/text()", ""};
    final String[] expected = {"", "", "hit:inside", "hit:inside", "", "", "hit:root,hit:nested"};
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds, pathSummary);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      for (int index = 0; index < documents.length; index++) {
        final String resource = "tree" + index;
        assertNotNull(collection.add(resource, new DocumentParser(documents[index])));
        for (final String axis : new String[] {"descendant", "descendant-or-self"}) {
          final String query = "string-join(for $hit in xn:doc('collection','" + resource + "')" + starts[index] + "/"
              + axis + "::hit return concat(name($hit), ':', string($hit/@id)), ',')";
          assertEquals(expected[index], ((Str) new Query(chain, query).evaluate(context)).stringValue(), query);
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, true", "true, true", "false, false", "true, false"})
  void namedDescendantPlansMatchExpandedQNamesRegardlessOfPrefix(final boolean storeDeweyIds,
      final boolean pathSummary) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds, pathSummary);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      assertNotNull(collection.add("tree", new DocumentParser("<r xmlns:p='urn:scope' xmlns:q='urn:scope'>"
          + "<a><p:hit id='inside'/></a><outside><q:hit id='outside'/></outside></r>")));
      for (final String axis : new String[] {"descendant", "descendant-or-self"}) {
        final String query =
            "declare namespace q='urn:scope'; " + "string-join(for $hit in xn:doc('collection','tree')/r/a/" + axis
                + "::q:hit return string($hit/@id), ',')";
        assertEquals("inside", ((Str) new Query(chain, query).evaluate(context)).stringValue(), query);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false, true", "true, true", "false, false", "true, false"})
  void compiledDescendantPlansKeepMatchesWithinTheirDocumentRevisionAndContext(final boolean storeDeweyIds,
      final boolean pathSummary) {
    try (final BasicXmlDBStore store = configuredStore(storeDeweyIds, pathSummary);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      assertNotNull(collection.add("empty", new DocumentParser("<r><a/></r>")));
      assertNotNull(collection.add("present",
          new DocumentParser("<r><a><hit id='present'/></a><a><hit id='second'/></a><b><hit id='b'/></b></r>")));
      final XmlDBCollection other =
          store.create("other", new DocumentParser("<r><a><deep><hit id='other'/></deep></a></r>"));
      try (final XmlResourceSession emptySession = collection.getDatabase().beginResourceSession("empty");
          final XmlResourceSession presentSession = collection.getDatabase().beginResourceSession("present");
          final XmlResourceSession otherSession = other.getDatabase().beginResourceSession("resource1")) {
        emptySession.getNodeTrx().ifPresent(XmlNodeTrx::close);
        try (final XmlNodeTrx writer = emptySession.beginNodeTrx()) {
          writer.moveToDocumentRoot();
          assertTrue(writer.moveToFirstChild());
          assertTrue(writer.moveToFirstChild());
          writer.insertElementAsFirstChild(new QNm("hit"));
          writer.insertAttribute(new QNm("id"), "revision");
          writer.commit();
        }
        try (final XmlNodeReadOnlyTrx oldTrx = emptySession.beginNodeReadOnlyTrx(1);
            final XmlNodeReadOnlyTrx newTrx = emptySession.beginNodeReadOnlyTrx(2);
            final XmlNodeReadOnlyTrx presentTrx = presentSession.beginNodeReadOnlyTrx();
            final XmlNodeReadOnlyTrx otherTrx = otherSession.beginNodeReadOnlyTrx()) {
          final XmlDBNode oldA = new XmlDBNode(oldTrx, collection).getFirstChild().getFirstChild();
          final XmlDBNode newA = new XmlDBNode(newTrx, collection).getFirstChild().getFirstChild();
          final XmlDBNode firstA = new XmlDBNode(presentTrx, collection).getFirstChild().getFirstChild();
          final XmlDBNode secondA = firstA.getNextSibling();
          final XmlDBNode b = secondA.getNextSibling();
          final XmlDBNode otherA = new XmlDBNode(otherTrx, other).getFirstChild().getFirstChild();
          assertEquals(firstA.getTrx().getPathNodeKey(), secondA.getTrx().getPathNodeKey());
          assertEquals(oldA.getTrx().getPathNodeKey(), newA.getTrx().getPathNodeKey());
          assertEquals(oldA.getTrx().getPathNodeKey(), otherA.getTrx().getPathNodeKey());
          assertEquals(emptySession.getResourceConfig().getID(), otherSession.getResourceConfig().getID());
          assertNotEquals(emptySession.getResourceConfig().getDatabaseId(),
              otherSession.getResourceConfig().getDatabaseId());
          for (final String axis : new String[] {"descendant", "descendant-or-self"}) {
            final Query query = descendantQuery(chain, axis);
            assertMatches("", query, context, oldA);
            assertMatches("hit:revision", query, context, newA);
            assertMatches("", query, context, oldA);
            assertMatches("hit:present", query, context, firstA);
            assertMatches("hit:second", query, context, secondA);
            assertMatches("hit:b", query, context, b);
            assertMatches("hit:other", query, context, otherA);
            assertMatches("hit:present", query, context, firstA);
            assertMatches("", query, context, oldA);
          }
        }
      }
    }
  }

  @Test
  void compiledChildPlansDoNotReuseAnotherResourcesEmptyMatches() {
    final String children = "<n/>".repeat(SirixTranslator.CHILD_THRESHOLD + 1);
    try (final BasicXmlDBStore store = configuredStore(false, true);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("collection");
      assertNotNull(collection.add("empty", new DocumentParser("<r>" + children + "</r>")));
      assertNotNull(collection.add("present", new DocumentParser("<r><hit id='child'/>" + children + "</r>")));
      try (final XmlResourceSession emptySession = collection.getDatabase().beginResourceSession("empty");
          final XmlResourceSession presentSession = collection.getDatabase().beginResourceSession("present");
          final XmlNodeReadOnlyTrx emptyTrx = emptySession.beginNodeReadOnlyTrx();
          final XmlNodeReadOnlyTrx presentTrx = presentSession.beginNodeReadOnlyTrx()) {
        final XmlDBNode emptyRoot = new XmlDBNode(emptyTrx, collection).getFirstChild();
        final XmlDBNode presentRoot = new XmlDBNode(presentTrx, collection).getFirstChild();
        assertEquals(emptyRoot.getTrx().getPathNodeKey(), presentRoot.getTrx().getPathNodeKey());
        assertTrue(emptyRoot.getTrx().getChildCount() > SirixTranslator.CHILD_THRESHOLD);
        assertTrue(presentRoot.getTrx().getChildCount() > SirixTranslator.CHILD_THRESHOLD);
        final Query query = descendantQuery(chain, "child");
        assertMatches("", query, context, emptyRoot);
        assertMatches("hit:child", query, context, presentRoot);
        assertMatches("", query, context, emptyRoot);
      }
    }
  }

  private BasicXmlDBStore configuredStore(final boolean storeDeweyIds, final boolean pathSummary) {
    return BasicXmlDBStore.newBuilder()
                          .location(directory)
                          .storageType(StorageType.FILE_CHANNEL)
                          .storeDeweyIds(storeDeweyIds)
                          .buildPathSummary(pathSummary)
                          .build();
  }

  private static Query descendantQuery(final SirixCompileChain chain, final String axis) {
    return new Query(chain, "declare variable $start external; string-join(for $hit in $start/" + axis
        + "::hit return concat(name($hit), ':', string($hit/@id)), ',')");
  }

  private static void assertOrderedMatches(final SirixCompileChain chain, final SirixQueryContext context,
      final XmlDBNode start, final String axis, final String[] expected) {
    context.bind(new QNm("start"), start);
    final String prolog = "xquery version \"1.0\"; declare variable $start external; ";
    final String path = "$start/" + axis + "::hit";
    final String allMatches = prolog + "string-join(for $hit in " + path + " return string($hit/@id), ',')";
    assertEquals(String.join(",", expected), ((Str) new Query(chain, allMatches).evaluate(context)).stringValue(),
        allMatches);
    for (int position = 1; position <= expected.length + 1; position++) {
      final String query = prolog + "string(" + path + "[" + position + "]/@id)";
      assertEquals(position <= expected.length
          ? expected[position - 1]
          : "", ((Str) new Query(chain, query).evaluate(context)).stringValue(), query);
    }
  }

  private static void assertMatches(final String expected, final Query query, final SirixQueryContext context,
      final XmlDBNode node) {
    context.bind(new QNm("start"), node);
    assertEquals(expected, ((Str) query.evaluate(context)).stringValue());
  }
}
