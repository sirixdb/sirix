package io.sirix.query.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Una;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Kind;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.update.UpdateList;
import io.brackit.query.update.op.DeleteOp;
import io.brackit.query.update.op.ReplaceElementContentOp;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.exception.SirixUsageException;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.settings.VersioningType;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

final class XmlPendingUpdateRegressionTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void detachedPendingTargets(final VersioningType versioning) {
    checkUpdate(versioning, "content-delete", "<r><a/></r>", "(replace value of node r with 'new', delete node r/a)",
        "<r>new</r>");
    checkUpdate(versioning, "nested-content", "<r><a><b/></a></r>",
        "(replace value of node r/a with 'outer', replace value of node r/a/b with 'inner')", "<r><a>outer</a></r>");
    checkUpdate(versioning, "node-content", "<r><a><b/></a></r>",
        "(replace node r/a with <new/>, replace value of node r/a/b with 'inner')", "<r><new xmlns=\"\"/></r>");
    checkUpdate(versioning, "nested-node", "<r><a><b/></a></r>",
        "(replace node r/a with <new/>, replace node r/a/b with <other/>)", "<r><new xmlns=\"\"/></r>");
    checkUpdate(versioning, "node-delete", "<r><a><b/></a></r>", "(replace node r/a with <new/>, delete node r/a/b)",
        "<r><new xmlns=\"\"/></r>");
    checkUpdate(versioning, "nested-delete", "<r><a><b/></a></r>", "(delete node r/a, delete node r/a/b)", "<r/>");
    checkUpdate(versioning, "duplicate-delete", "<r><a/></r>", "(delete node r/a, delete node r/a)", "<r/>");
    checkUpdate(versioning, "attribute-delete", "<r><a x='v'/></r>",
        "(replace node r/a with <new/>, delete node r/a/@x)", "<r><new xmlns=\"\"/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void textTargetsSurviveUntilNormalization(final VersioningType versioning) {
    final String xml = "<r>left<a/>right</r>";
    checkUpdate(versioning, "replace-replace", xml,
        "(replace node r/a with 'middle', replace node r/a/following-sibling::text() with 'changed')",
        "<r>leftmiddlechanged</r>");
    checkUpdate(versioning, "replace-delete", xml,
        "(replace node r/a with 'middle', delete node r/a/following-sibling::text())", "<r>leftmiddle</r>");
    checkUpdate(versioning, "delete-delete", xml, "(delete node r/a, delete node r/a/following-sibling::text())",
        "<r>left</r>");
    checkUpdate(versioning, "insert-before", xml,
        "(insert node 'before' before r/a/following-sibling::text(), replace node r/a/following-sibling::text() with 'changed')",
        "<r>left<a/>beforechanged</r>");
    checkUpdate(versioning, "insert-after", xml,
        "(insert node 'after' after r/a/preceding-sibling::text(), replace node r/a/preceding-sibling::text() with 'changed')",
        "<r>changedafter<a/>right</r>");
    checkUpdate(versioning, "insert-first", xml,
        "(insert node 'first' as first into r, replace node r/a/preceding-sibling::text() with 'changed')",
        "<r>firstchanged<a/>right</r>");
    checkUpdate(versioning, "insert-last", xml,
        "(insert node 'last' as last into r, replace node r/a/following-sibling::text() with 'changed')",
        "<r>left<a/>changedlast</r>");
    checkUpdate(versioning, "empty-replace", xml,
        "(replace value of node r/a/preceding-sibling::text() with '', replace node r/a/preceding-sibling::text() with 'changed')",
        "<r>changed<a/>right</r>");
    checkUpdate(versioning, "empty-normalize", xml,
        "(replace value of node r/a/preceding-sibling::text() with '', replace node r/a with 'middle', replace node r/a/following-sibling::text() with 'changed')",
        "<r>middlechanged</r>");
    checkUpdate(versioning, "empty-between-text", xml,
        "(replace value of node r/a/following-sibling::text() with '', insert node 'last' as last into r, replace node r/a with 'middle')",
        "<r>leftmiddlelast</r>");
    checkUpdate(versioning, "empty-attribute", "<r keep='yes'><a/></r>", "replace value of node r/@keep with ''",
        "<r keep=\"\"><a/></r>");
    checkUpdate(versioning, "empty-comment", "<r><!--old--><a/></r>", "replace value of node r/comment() with ''",
        "<r><!--  --><a/></r>");
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void suppliedUpdatesRetainSnapshotsAndIgnoreRepeatedDeletion(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning, "supplied");
        final SirixQueryContext context =
            SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT)) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r keep='yes'><a>old</a></r>"));
      final XmlDBNode original = collection.getDocument(1);
      final XmlDBNode root = original.getFirstChild();
      final XmlDBNode child = root.getFirstChild();
      final long rootKey = root.getNodeKey();
      final String before = serialize(original);
      try (final XmlNodeTrx writer = original.getTrx().getResourceSession().beginNodeTrx()) {
        assertTrue(writer.moveTo(child.getNodeKey()));
        final XmlDBNode liveChild = new XmlDBNode(writer, collection);
        final UpdateList updates = new UpdateList();
        final DeleteOp deletion = new DeleteOp(child);
        final Object identity = deletion.getTargetIdentity();
        updates.append(new ReplaceElementContentOp(root, new Una("new")));
        updates.append(deletion);
        context.setUpdateList(updates);
        context.applyUpdates();
        assertEquals(identity, updates.list().get(1).getTargetIdentity());
        assertEquals(1, root.getTrx().getRevisionNumber());
        assertEquals(before, serialize(original));
        child.delete();
        liveChild.delete();
        writer.commit();
      }
      assertEquals(rootKey, collection.getDocument(2).getFirstChild().getNodeKey());
      assertEquals("<r keep=\"yes\">new</r>", serialize(collection.getDocument(2)));
      assertEquals(before, serialize(collection.getDocument(1)));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void missingTargetsRemainNoOpsAcrossExplicitBatches(final VersioningType versioning) {
    final String xml = "<r><a>old</a></r>";
    for (final boolean deweyIds : new boolean[] {false, true}) {
      for (final AfterCommitState state : new AfterCommitState[] {AfterCommitState.KEEP_OPEN,
          AfterCommitState.KEEP_OPEN_ASYNC_COMMIT, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH}) {
        try (
            final BasicXmlDBStore store =
                BasicXmlDBStore.newBuilder()
                               .location(directory.resolve("missing-" + state + "-" + deweyIds))
                               .versioningType(versioning)
                               .storageType(StorageType.FILE_CHANNEL)
                               .storeDeweyIds(deweyIds)
                               .build();
            final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
            final SirixQueryContext first =
                SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT);
            final SirixQueryContext second =
                SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT);
            final SirixQueryContext third =
                SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT)) {
          final XmlDBCollection collection = store.create("data", new DocumentParser(xml));
          final XmlDBNode original = collection.getDocument(1);
          final long rootKey = original.getFirstChild().getNodeKey();
          try (final XmlNodeTrx writer = original.getTrx().getResourceSession().beginNodeTrx(1, state)) {
            first.setContextItem(original);
            new Query(chain, "delete node r/a").execute(first);
            second.setContextItem(original);
            new Query(chain, "delete node r/a").execute(second);
            third.setContextItem(original);
            new Query(chain, "replace value of node r/a with 'ignored'").execute(third);
            assertEquals(1, original.getTrx().getResourceSession().getMostRecentRevisionNumber());
            assertEquals(xml, serialize(original));
            writer.commit();
          }
          assertEquals(2, original.getTrx().getResourceSession().getMostRecentRevisionNumber());
          assertEquals(rootKey, collection.getDocument(2).getFirstChild().getNodeKey());
          assertEquals("<r/>", serialize(collection.getDocument(2)));
          assertEquals(xml, serialize(original));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void missingTargetsStillParticipateInConflictChecks(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning, "missing-conflict");
        final SirixQueryContext first =
            SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT);
        final SirixQueryContext second =
            SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT)) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r><a/></r>"));
      final XmlDBNode original = collection.getDocument(1);
      final XmlDBNode child = original.getFirstChild().getFirstChild();
      try (final XmlNodeTrx writer = original.getTrx().getResourceSession().beginNodeTrx()) {
        first.addPendingUpdate(new DeleteOp(child));
        first.applyUpdates();
        second.addPendingUpdate(new ReplaceElementContentOp(child, new Una("first")));
        second.addPendingUpdate(new ReplaceElementContentOp(child, new Una("second")));
        assertThrows(QueryException.class, second::applyUpdates);
        assertThrows(SirixUsageException.class, writer::commit);
        writer.rollback();
      }
      assertEquals(1, original.getTrx().getResourceSession().getMostRecentRevisionNumber());
      assertEquals("<r><a/></r>", serialize(original));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void ordinaryWritesStillMergeTextAndRejectEmptyAppend(final VersioningType versioning) {
    try (final BasicXmlDBStore store = newStore(versioning, "ordinary")) {
      final XmlDBCollection collection = store.create("data", new DocumentParser("<r>left<a/>right</r>"));
      final XmlDBNode original = collection.getDocument(1);
      final long rootKey = original.getFirstChild().getNodeKey();
      try (final XmlNodeTrx writer = original.getTrx().getResourceSession().beginNodeTrx()) {
        assertTrue(writer.moveTo(rootKey));
        writer.moveToFirstChild();
        writer.insertTextAsRightSibling("added");
        assertEquals("leftadded", writer.getValue());
        writer.moveToRightSibling();
        writer.moveToRightSibling();
        writer.insertTextAsLeftSibling("before");
        assertEquals("beforeright", writer.getValue());
        writer.moveTo(rootKey);
        writer.insertTextAsFirstChild("first");
        assertEquals("firstleftadded", writer.getValue());
        writer.moveToRightSibling();
        writer.remove();
        writer.moveTo(rootKey);
        assertEquals(1, writer.getChildCount());
        writer.commit();
      }
      assertEquals("<r>firstleftaddedbeforeright</r>", serialize(collection.getDocument(2)));
      assertThrows(SirixUsageException.class,
          () -> collection.getDocument(2).getFirstChild().append(Kind.TEXT, null, new Una("")));
    }
  }

  private void checkUpdate(final VersioningType versioning, final String name, final String xml, final String update,
      final String expected) {
    for (final boolean deweyIds : new boolean[] {false, true}) {
      for (final CommitStrategy strategy : CommitStrategy.values()) {
        try (final BasicXmlDBStore store = newStore(versioning, name + "-" + strategy + "-ids-" + deweyIds, deweyIds);
            final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
            final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, strategy)) {
          final XmlDBCollection collection = store.create("data", new DocumentParser(xml));
          final XmlDBNode original = collection.getDocument(1);
          final long rootKey = original.getFirstChild().getNodeKey();
          final String before = serialize(original);
          context.setContextItem(original);
          new Query(chain, update).execute(context);
          assertEquals(before, serialize(original), name);
          if (strategy == CommitStrategy.EXPLICIT) {
            try (final XmlNodeTrx writer = original.getTrx().getResourceSession().getNodeTrx().orElseThrow()) {
              writer.commit();
            }
          }
          assertEquals(rootKey, collection.getDocument(2).getFirstChild().getNodeKey(), name);
          assertEquals(expected, serialize(collection.getDocument(2)), name);
          assertEquals(before, serialize(collection.getDocument(1)), name);
        }
      }
    }
  }

  private BasicXmlDBStore newStore(final VersioningType versioning, final String name) {
    return newStore(versioning, name, true);
  }

  private BasicXmlDBStore newStore(final VersioningType versioning, final String name, final boolean deweyIds) {
    return BasicXmlDBStore.newBuilder()
                          .location(directory.resolve(name))
                          .versioningType(versioning)
                          .storeDeweyIds(deweyIds)
                          .build();
  }

  private static String serialize(final XmlDBNode document) {
    final BrackitQueryContext context = new BrackitQueryContext();
    context.setContextItem(document);
    final StringWriter output = new StringWriter();
    new Query(new CompileChain(), "$$").serialize(context, new PrintWriter(output));
    return output.toString();
  }
}
