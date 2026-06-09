package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for {@code jn:open($db, $resource, $pointInTime)} — opening a JSON resource as of a
 * wall-clock instant ({@link io.sirix.query.function.jn.io.DocByPointInTime}).
 */
public final class DocByPointInTimeJsonTest {

  private static final Path SIRIX_DB_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setup() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static String run(final SirixQueryContext ctx, final SirixCompileChain chain, final String query)
      throws IOException {
    final var seq = new Query(chain, query).execute(ctx);
    final var buf = IOUtils.createBuffer();
    try (final var serializer = new StringSerializer(buf)) {
      serializer.setFormat(true).serialize(seq);
    }
    return buf.toString().trim();
  }

  /**
   * A point in time before the resource's first revision must yield the empty sequence — the
   * resource did not exist yet, so there is nothing to return (regression: previously the first
   * revision's data was returned anachronistically).
   */
  @Test
  public void test_whenPointInTimeBeforeFirstRevision_thenEmpty() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      // The revisions above are committed "now"; 2000-01-01 predates all of them.
      final var result = run(ctx, chain, "jn:open('json-path1','mydoc.jn', xs:dateTime('2000-01-01T00:00:00Z'))");

      assertEquals("", result);
    }
  }

  /**
   * A point in time after the first revision must still return the document (here a far-future
   * instant resolves to the most recent revision).
   */
  @Test
  public void test_whenPointInTimeAfterFirstRevision_thenDocument() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final var result = run(ctx, chain, "jn:open('json-path1','mydoc.jn', xs:dateTime('2100-01-01T00:00:00Z'))");

      assertFalse(result.isEmpty(), "expected the resource document, got the empty sequence");
    }
  }
}
