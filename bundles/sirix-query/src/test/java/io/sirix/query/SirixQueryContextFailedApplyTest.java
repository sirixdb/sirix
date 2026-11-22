package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.atomic.QNm;
import io.brackit.query.update.op.OpType;
import io.brackit.query.update.op.UpdateOp;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.exception.SirixUsageException;
import io.sirix.query.json.JsonDBItem;
import java.util.Optional;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.query.SirixQueryContext.CommitStrategy;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A query whose pending updates fail while being applied must not leave behind a writer that the
 * apply opened: a later query would otherwise reuse it and commit the failed query's partial
 * changes, or find it rollback-only. Writers the caller already held stay under caller control.
 */
final class SirixQueryContextFailedApplyTest {
  /** The first insert applies; the positional insert then fails while being applied. */
  private static final String FAILING_JSON_UPDATE =
      "(insert json {'z':1} into $$, insert json 9 into $$.arr at position 99)";

  /** The insert applies; the two renames of one node then fail as conflicting updates. */
  private static final String FAILING_XML_UPDATE =
      "(insert node <c/> into $$/a, rename node $$/a as 'b', rename node $$/a as 'd')";

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(CommitStrategy.class)
  void failedJsonApplyReleasesTheWriterItOpenedWithoutItsChanges(final CommitStrategy strategy) {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, strategy)) {
      final JsonDBCollection collection = store.create("json", "data", "{\"a\":1,\"arr\":[1,2]}");
      context.setContextItem(collection.getDocument("data"));
      assertThrows(QueryException.class, () -> new Query(chain, FAILING_JSON_UPDATE).execute(context));

      final JsonResourceSession session = collection.getDocument("data").getResourceSession();
      assertTrue(session.getNodeTrx().isEmpty(), "the failed apply's writer must be released");
      assertEquals(1, session.getMostRecentRevisionNumber());

      context.setUpdateList(null);
      context.setContextItem(collection.getDocument("data"));
      new Query(chain, "insert json {'c':3} into $$").execute(context);
      if (strategy == CommitStrategy.EXPLICIT) {
        try (final JsonNodeTrx writer = session.getNodeTrx().orElseThrow()) {
          writer.commit();
        }
      }
      assertEquals(2, session.getMostRecentRevisionNumber());
      context.setContextItem(collection.getDocument("data"));
      assertEquals("{\"a\":1,\"arr\":[1,2],\"c\":3}", serialize(chain, context));
    }
  }

  @Test
  void failedJsonApplyLeavesACallerHeldWriterOpen() {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final SirixQueryContext context =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, CommitStrategy.EXPLICIT)) {
      final JsonDBCollection collection = store.create("json", "data", "{\"a\":1,\"arr\":[1,2]}");
      final JsonResourceSession session = collection.getDocument("data").getResourceSession();
      final JsonNodeTrx held = session.beginNodeTrx();
      try {
        context.setContextItem(collection.getDocument("data"));
        assertThrows(QueryException.class, () -> new Query(chain, FAILING_JSON_UPDATE).execute(context));
        assertFalse(held.isClosed(), "a writer the caller held before the query is not the query's to close");
        assertSame(held, session.getNodeTrx().orElseThrow());
      } finally {
        held.rollback();
        held.close();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(CommitStrategy.class)
  void failedXmlApplyReleasesTheWriterItOpenedWithoutItsChanges(final CommitStrategy strategy) {
    try (final BasicXmlDBStore store = BasicXmlDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithNodeStore(store);
        final SirixQueryContext context = SirixQueryContext.createWithNodeStoreAndCommitStrategy(store, strategy)) {
      final XmlDBCollection collection = store.create("xml", new DocumentParser("<a><b/></a>"));
      context.setContextItem(collection.getDocument());
      assertThrows(QueryException.class, () -> new Query(chain, FAILING_XML_UPDATE).execute(context));

      final XmlResourceSession session = collection.getDocument().getTrx().getResourceSession();
      assertTrue(session.getNodeTrx().isEmpty(), "the failed apply's writer must be released");
      assertEquals(1, session.getMostRecentRevisionNumber());

      context.setUpdateList(null);
      context.setContextItem(collection.getDocument());
      new Query(chain, "insert node <e/> into $$/a").execute(context);
      if (strategy == CommitStrategy.EXPLICIT) {
        try (final XmlNodeTrx writer = session.getNodeTrx().orElseThrow()) {
          writer.commit();
        }
      }
      assertEquals(2, session.getMostRecentRevisionNumber());
      context.setContextItem(collection.getDocument());
      assertEquals("<a><b/><e xmlns=\"\"/></a>", serialize(chain, context));
    }
  }

  @Test
  void aWriterThatCannotBeReleasedLeavesTheApplyFailureReported() {
    final JsonResourceSession session = mock(JsonResourceSession.class);
    final JsonNodeTrx writer = mock(JsonNodeTrx.class);
    final SirixUsageException releaseFailure = new SirixUsageException("rollback refused");
    when(writer.rollback()).thenThrow(releaseFailure);
    // No writer before the apply; the failing operation opened one.
    when(session.getNodeTrx()).thenReturn(Optional.empty(), Optional.of(writer));
    final JsonNodeReadOnlyTrx reader = mock(JsonNodeReadOnlyTrx.class);
    when(reader.getResourceSession()).thenReturn(session);
    final JsonDBItem target = mock(JsonDBItem.class);
    when(target.getTrx()).thenReturn(reader);
    final QueryException applyFailure = new QueryException(new QNm("apply failed"));
    final UpdateOp operation = mock(UpdateOp.class);
    when(operation.getTarget()).thenReturn(target);
    when(operation.getType()).thenReturn(OpType.INSERT_INTO);
    doThrow(applyFailure).when(operation).apply();

    try (final SirixQueryContext context = SirixQueryContext.create()) {
      context.addPendingUpdate(operation);
      final QueryException reported = assertThrows(QueryException.class, context::applyUpdates);
      assertSame(applyFailure, reported);
      assertSame(releaseFailure, reported.getSuppressed()[0]);
    }
  }

  private static String serialize(final SirixCompileChain chain, final SirixQueryContext context) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      new Query(chain, "$$").serialize(context, writer);
    }
    return out.toString();
  }
}
