package io.sirix.query.node;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.ErrorCode;
import io.brackit.query.BrackitQueryContext;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.atomic.Una;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Kind;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.update.op.ReplaceElementContentOp;
import io.brackit.query.update.op.UpdateOp;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.expression.SirixReplaceValue;
import io.sirix.settings.VersioningType;
import java.nio.file.Path;
import java.io.StringWriter;
import java.io.PrintWriter;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.function.Executable;
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
  void contentReplacementSkipsDetachedDeletes(final VersioningType versioning) {
    checkUpdate(versioning,
        "(replace value of node r/target with 'new', delete nodes r/target/node())",
        "<r keep=\"yes\"><target a=\"v\">new</target><tail/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void contentReplacementSkipsDetachedDescendants(final VersioningType versioning) {
    checkUpdate(versioning,
        "(replace value of node r/target with 'new', replace value of node r/target/b with 'descendant')",
        "<r keep=\"yes\"><target a=\"v\">new</target><tail/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void descendantContentReplacementBeforeAncestor(final VersioningType versioning) {
    checkUpdate(versioning,
        "(replace value of node r/target/b with 'descendant', replace value of node r/target with 'new')",
        "<r keep=\"yes\"><target a=\"v\">new</target><tail/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void contentReplacementSkipsReplacedNodes(final VersioningType versioning) {
    checkUpdate(versioning,
        "(replace node r/target with <new/>, replace value of node r/target with 'detached')",
        "<r keep=\"yes\"><new xmlns=\"\"/><tail/></r>", false);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void writerBackedContentReplacementSkipsDetachedTargets(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning)) {
      final BrackitQueryContext context = new BrackitQueryContext();
      final XmlDBCollection collection = store.create("writer-targets", new DocumentParser(XML));
      final XmlDBNode original = collection.getDocument(1);
      final long rootKey = original.getFirstChild().getNodeKey();
      final long targetKey = original.getFirstChild().getFirstChild().getNodeKey();
      try (final XmlNodeTrx trx = original.getTrx().getResourceSession().beginNodeTrx()) {
        assertTrue(trx.moveTo(targetKey));
        final XmlDBNode target = new XmlDBNode(trx, collection);
        final XmlDBNode child = target.getFirstChild().getNextSibling();
        new SirixReplaceValue(new Str("new"), target).evaluateToItem(context, null);
        new SirixReplaceValue(new Str("detached"), child).evaluateToItem(context, null);
        for (final UpdateOp operation : context.getUpdateList().list()) {
          operation.apply();
        }
        child.delete();
        assertFalse(trx.isClosed());
        trx.commit();
      }
      assertDocument(collection, 2, "<r keep=\"yes\"><target a=\"v\">new</target><tail/></r>", rootKey, targetKey);
      assertDocument(collection, 1, XML.replace("'", "\"").replace("<!--note-->", "<!-- note -->"), rootKey, targetKey);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void detachedApiTargetsCannotMutateTheCurrentCursor(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning)) {
      for (final boolean writerBacked : new boolean[] {false, true}) {
        final XmlDBCollection collection = store.create("detached-" + writerBacked, new DocumentParser(XML));
        final XmlDBNode original = collection.getDocument(1);
        final long rootKey = original.getFirstChild().getNodeKey();
        final long targetKey = original.getFirstChild().getFirstChild().getNodeKey();
        final XmlDBNode replacement = original.getFirstChild().getLastChild();
        final long survivorKey = replacement.getNodeKey();
        try (final XmlNodeTrx trx = original.getTrx().getResourceSession().beginNodeTrx()) {
          assertTrue(trx.moveTo(targetKey));
          final XmlDBNode target = writerBacked
              ? new XmlDBNode(trx, collection)
              : original.getFirstChild().getFirstChild();
          final XmlDBNode text = target.getFirstChild();
          assertTrue(trx.moveTo(targetKey));
          trx.remove();
          assertTrue(trx.moveTo(survivorKey));
          target.delete();
          text.delete();
          final QNm name = new QNm("changed");
          final Str value = new Str("changed");
          for (final Executable operation : new Executable[] {
              () -> target.setName(name), () -> text.setValue(value),
              () -> target.append(Kind.ELEMENT, name, null), () -> target.append(replacement),
              () -> target.append(new DocumentParser("<new/>")),
              () -> target.prepend(Kind.ELEMENT, name, null), () -> target.prepend(replacement),
              () -> target.prepend(new DocumentParser("<new/>")),
              () -> target.insertBefore(Kind.ELEMENT, name, null), () -> target.insertBefore(replacement),
              () -> target.insertBefore(new DocumentParser("<new/>")),
              () -> target.insertAfter(Kind.ELEMENT, name, null), () -> target.insertAfter(replacement),
              () -> target.insertAfter(new DocumentParser("<new/>")),
              () -> target.setAttribute(name, value), () -> target.setAttribute(replacement),
              () -> target.deleteAttribute(name), () -> target.replaceWith(Kind.ELEMENT, name, null),
              () -> target.replaceWith(replacement), () -> target.replaceWith(new DocumentParser("<new/>"))}) {
            assertThrows(DocumentException.class, operation);
            assertFalse(trx.isClosed());
            assertEquals(survivorKey, trx.getNodeKey());
            assertEquals(0, trx.getAttributeCount());
          }
          if (writerBacked) {
            for (final Executable read : new Executable[] {target::getKind, target::getName, target::getValue,
                target::getParent, target::getFirstChild, target::getLastChild, target::getChildren,
                target::getSubtree, target::getAttributes, target::getNextSibling, target::getPreviousSibling,
                target::getTrx, target::getRtx}) {
              assertThrows(DocumentException.class, read);
              assertEquals(survivorKey, trx.getNodeKey());
            }
          } else {
            assertEquals(Kind.ELEMENT, target.getKind());
            assertEquals("target", target.getName().getLocalName());
          }
          final XmlDBNode survivor = new XmlDBNode(trx, collection);
          survivor.setName(new QNm("survivor"));
          survivor.setAttribute(new QNm("keep"), new Str("yes"));
          trx.commit();
        }
        assertDocument(collection, 2, "<r keep=\"yes\"><survivor keep=\"yes\"/></r>", rootKey, -1);
        assertDocument(collection, 1, XML.replace("'", "\"").replace("<!--note-->", "<!-- note -->"), rootKey, targetKey);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void invalidConsumedReplacementTextCannotLeavePartialWrites(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("invalid-text", new DocumentParser(XML));
      final XmlDBNode original = collection.getDocument(1);
      final long rootKey = original.getFirstChild().getNodeKey();
      final long targetKey = original.getFirstChild().getFirstChild().getNodeKey();
      final XmlResourceSession session = original.getTrx().getResourceSession();
      final QNm replacement = new QNm("replacement");
      for (final String source : new String[] {"$replacement", "($replacement, 42)", "text {$replacement}"}) {
        final Query query = new Query(chain,
            "declare variable $replacement external; replace value of node r/target with " + source);
        for (final String invalid : new String[] {"\u0000", "\u0001", "\u0008", "\u000B", "\u000C", "\u000E",
            "\u001F", "\uD800", "\uDC00", "\uFFFE", "\uFFFF"}) {
          final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
          context.setContextItem(original);
          context.bind(replacement, new Str("prefix" + invalid + "suffix"));
          assertThrows(DocumentException.class, () -> query.execute(context));
          assertTrue(context.getUpdateList() == null || context.getUpdateList().list().isEmpty());
          assertFalse(session.hasRunningNodeWriteTrx());
          assertEquals(1, session.getMostRecentRevisionNumber());
        }
      }
      final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store);
      context.setContextItem(original);
      new Query(chain, "replace value of node r/target/@a with 'valid'").execute(context);
      assertDocument(collection, 2,
          XML.replace("'", "\"").replace("a=\"v\"", "a=\"valid\"").replace("<!--note-->", "<!-- note -->"), rootKey, targetKey);
      assertDocument(collection, 1, XML.replace("'", "\"").replace("<!--note-->", "<!-- note -->"), rootKey, targetKey);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void validXmlWhitespaceAndSupplementaryCharactersAreAccepted(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning);
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStore(store)) {
      final XmlDBCollection collection = store.create("valid-text", new DocumentParser(XML));
      context.setContextItem(collection.getDocument(1));
      final String value = "\t\n\r\uD83D\uDE00";
      context.bind(new QNm("replacement"), new Str(value));
      new Query(chain,
          "declare variable $replacement external; replace value of node r/target with $replacement")
              .execute(context);
      assertEquals(value, collection.getDocument(2).getFirstChild().getFirstChild().getValue().stringValue());
    }
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
  void replaceElementValueWithEmptySequence(final VersioningType versioning) {
    checkUpdate(versioning, "replace value of node r/target with ()", "<r keep=\"yes\"><target a=\"v\"/><tail/></r>");
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
