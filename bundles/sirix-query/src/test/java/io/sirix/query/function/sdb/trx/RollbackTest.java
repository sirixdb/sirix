package io.sirix.query.function.sdb.trx;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.brackit.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RollbackTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void testRollbackViaLowLevelApi() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder("mydoc.jn").build());

      try (final var manager = database.beginResourceSession("mydoc.jn"); final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"original\"]"));
        wtx.commit();
        // Modify but then rollback
        wtx.moveTo(2);
        wtx.setStringValue("modified");
        final long revisionBeforeRollback = wtx.getRevisionNumber();
        wtx.rollback();
        assertTrue(revisionBeforeRollback > 0, "Revision number before rollback should be positive");
      }
    }

    // Verify only 1 revision exists after rollback (the rolled-back revision was not committed)
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String query = "jn:doc('json-path1','mydoc.jn')";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("[\"original\"]", out.toString());
      }
    }
  }

  @Test
  public void testRollbackPreservesOriginalData() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder("mydoc.jn").build());

      try (final var manager = database.beginResourceSession("mydoc.jn"); final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"original\"]"));
        wtx.commit();
        // Modify but don't commit — will be rolled back
        wtx.moveTo(2);
        wtx.setStringValue("modified");
        wtx.rollback();
      }
    }

    // Verify the original data is preserved after rollback
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      final String query = "jn:doc('json-path1','mydoc.jn')";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        assertEquals("[\"original\"]", out.toString());
      }
    }
  }
}
