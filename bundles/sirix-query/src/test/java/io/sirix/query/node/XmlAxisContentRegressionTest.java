package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.ErrorCode;
import io.brackit.query.BrackitQueryContext;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Una;
import io.brackit.query.jdm.Kind;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.update.op.ReplaceElementContentOp;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.settings.VersioningType;
import java.nio.file.Path;
import java.io.StringWriter;
import java.io.PrintWriter;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class XmlAxisContentRegressionTest {
  private static final String XML =
      "<r keep='yes'><target a='v'>before<a/>between<b><c/></b>after<!--note--><?pi data?></target><tail/></r>";
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void documentAxesAtEveryRevision(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("axes", new DocumentParser(XML));
      try (final XmlNodeTrx trx = collection.getDocument().getTrx().getResourceSession().beginNodeTrx()) {
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.setValue("changed");
        trx.commit();
      }
      for (int revision = 1; revision <= 2; revision++) {
        context.setContextItem(collection.getDocument(revision));
        for (final String start : new String[] {"r", "r/target", "r/target/b/c", "r/target/text()", "r/@keep"}) {
          assertCount(chain, context, start + "/ancestor::document-node()", 1);
          assertCount(chain, context, start + "/ancestor-or-self::document-node()", 1);
          assertCount(chain, context, start + "/parent::document-node()", start.equals("r")
              ? 1
              : 0);
        }
        assertCount(chain, context, "r/ancestor::element()", 0);
        assertCount(chain, context, "r/ancestor-or-self::element()", 1);
        assertCount(chain, context, "r/target/b/c/ancestor::element()", 3);
        assertCount(chain, context, "r/target/b/c/ancestor-or-self::element()", 4);
        assertCount(chain, context, "parent::document-node()", 0);
        assertCount(chain, context, "ancestor::document-node()", 0);
        assertCount(chain, context, "ancestor-or-self::document-node()", 1);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void translatorDependentDocumentKindTests(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("kind-tests", new DocumentParser(XML));
      try (final XmlNodeTrx trx = collection.getDocument().getTrx().getResourceSession().beginNodeTrx()) {
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.moveToFirstChild();
        trx.setValue("changed");
        trx.commit();
      }
      for (int revision = 1; revision <= 2; revision++) {
        context.setContextItem(collection.getDocument(revision));
        for (final String start : new String[] {"r", "r/target", "r/target/b/c", "r/target/text()", "r/@keep"}) {
          assertCount(chain, context, start + "/parent::node()", 1);
          assertCount(chain, context, start + "/parent::document-node(element(r))", start.equals("r")
              ? 1
              : 0);
          assertCount(chain, context, start + "/ancestor::document-node(element(r))", 1);
          assertCount(chain, context, start + "/ancestor-or-self::document-node(element(r))", 1);
          assertCount(chain, context, start + "/ancestor::document-node(element(other))", 0);
        }
        assertCount(chain, context, "r/ancestor::node()", 1);
        assertCount(chain, context, "r/ancestor-or-self::node()", 2);
        assertCount(chain, context, "r/target/b/c/ancestor::node()", 4);
        assertCount(chain, context, "r/target/b/c/ancestor-or-self::node()", 5);
        assertCount(chain, context, "parent::node()", 0);
        assertCount(chain, context, "ancestor::node()", 0);
        assertCount(chain, context, "ancestor-or-self::document-node(element(r))", 1);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void replaceElementValue(final VersioningType versioning) {
    checkUpdate(versioning, "replace value of node r/target with 'new'",
        "<r keep=\"yes\"><target a=\"v\">new</target><tail/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void replaceElementValueAtomizesSource(final VersioningType versioning) {
    checkUpdate(versioning, "replace value of node (for $n in r/target return $n) with ('one', 42, 'three')",
        "<r keep=\"yes\"><target a=\"v\">one 42 three</target><tail/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void replaceAttributeValueKeepsBrackitBehavior(final VersioningType versioning) {
    checkUpdate(versioning, "replace value of node r/target/@a with 'new'",
        XML.replace("'", "\"").replace("a=\"v\"", "a=\"new\"").replace("<!--note-->", "<!-- note -->"));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rejectsInvalidTargetsBeforeOpeningWriter(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("invalid", new DocumentParser(XML));
      final XmlDBNode document = collection.getDocument(1);
      context.setContextItem(document);
      final QueryException empty = assertThrows(QueryException.class,
          () -> new Query(chain, "replace value of node r/missing with 'new'").execute(context));
      assertEquals(ErrorCode.ERR_UPDATE_INSERT_TARGET_IS_EMPTY_SEQUENCE, empty.getCode());
      for (final String target : new String[] {"(r/target, r/tail)", "42"}) {
        final QueryException error = assertThrows(QueryException.class,
            () -> new Query(chain, "replace value of node " + target + " with 'new'").execute(context));
        assertEquals(ErrorCode.ERR_UPDATE_REPLACE_TARGET_NOT_A_EATCP_NODE, error.getCode());
      }
      assertFalse(document.getTrx().getResourceSession().hasRunningNodeWriteTrx());
      assertEquals(1, document.getTrx().getResourceSession().getMostRecentRevisionNumber());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void replaceRootElementValue(final VersioningType versioning) {
    checkUpdate(versioning, "replace value of node r with 'new'", "<r keep=\"yes\">new</r>", false);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void replaceElementValueWithEmptyString(final VersioningType versioning) {
    checkUpdate(versioning, "replace value of node r/target with ''", "<r keep=\"yes\"><target a=\"v\"/><tail/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void replaceChildren(final VersioningType versioning) {
    checkUpdate(versioning,
        "for $child in (r/target/text(), r/target/*, r/target/comment(), r/target/processing-instruction()) return replace node $child with <new/>",
        "<r keep=\"yes\"><target a=\"v\"><new xmlns=\"\"/><new xmlns=\"\"/><new xmlns=\"\"/><new xmlns=\"\"/><new xmlns=\"\"/><new xmlns=\"\"/><new xmlns=\"\"/></target><tail/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void transactionContentReplacement(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning)) {
      final XmlDBCollection collection = store.create("api", new DocumentParser(XML));
      final XmlDBNode original = collection.getDocument(1);
      final long rootKey = original.getFirstChild().getNodeKey();
      final long targetKey = original.getFirstChild().getFirstChild().getNodeKey();
      try (final XmlNodeTrx trx = original.getTrx().getResourceSession().beginNodeTrx()) {
        assertTrue(trx.moveTo(targetKey));
        final XmlDBNode target = new XmlDBNode(trx, collection);
        new ReplaceElementContentOp(target, new Una("new")).apply();
        assertEquals(targetKey, target.getNodeKey());
        assertEquals(Kind.TEXT, target.getFirstChild().getKind());
        assertEquals("new", target.getFirstChild().getValue().stringValue());
        trx.commit();
      }
      assertDocument(collection, 2, "<r keep=\"yes\"><target a=\"v\">new</target><tail/></r>", rootKey, targetKey);
      assertDocument(collection, 1, XML.replace("'", "\"").replace("<!--note-->", "<!-- note -->"), rootKey, targetKey);
    }
  }

  private void checkUpdate(final VersioningType versioning, final String update, final String expected) {
    checkUpdate(versioning, update, expected, true);
  }

  private void checkUpdate(final VersioningType versioning, final String update, final String expected,
      final boolean retainsTarget) {
    try (final BasicXmlDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("update", new DocumentParser(XML));
      final XmlDBNode original = collection.getDocument(1);
      final long rootKey = original.getFirstChild().getNodeKey();
      final long targetKey = original.getFirstChild().getFirstChild().getNodeKey();
      context.setContextItem(original);
      new Query(chain, update).execute(context);
      assertDocument(collection, 2, expected, rootKey, retainsTarget
          ? targetKey
          : -1);
      assertDocument(collection, 1, XML.replace("'", "\"").replace("<!--note-->", "<!-- note -->"), rootKey, targetKey);
    }
  }

  private static void assertDocument(final XmlDBCollection collection, final int revision, final String expected,
      final long rootKey, final long targetKey) {
    final XmlDBNode document = collection.getDocument(revision);
    assertEquals(rootKey, document.getFirstChild().getNodeKey());
    if (targetKey >= 0) {
      assertEquals(targetKey, document.getFirstChild().getFirstChild().getNodeKey());
    }
    final BrackitQueryContext context = new BrackitQueryContext();
    context.setContextItem(document);
    final StringWriter output = new StringWriter();
    new Query(new CompileChain(), "$$").serialize(context, new PrintWriter(output));
    assertEquals(expected, output.toString());
  }

  private static void assertCount(final SirixCompileChain chain, final SirixQueryContext context, final String path,
      final long expected) {
    assertEquals(expected, ((Int64) new Query(chain, "xs:long(count(" + path + "))").execute(context)).longValue(),
        path);
  }

  private BasicXmlDBStore newStore(final VersioningType versioning) {
    return BasicXmlDBStore.newBuilder().location(directory).versioningType(versioning).build();
  }
}
