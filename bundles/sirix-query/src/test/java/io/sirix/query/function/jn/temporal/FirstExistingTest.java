package io.sirix.query.function.jn.temporal;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import io.sirix.JsonTestHelper;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class FirstExistingTest {

  private static final Path SIRIX_DB_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setup() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** OBJECT inserted in rev2 via insertSubtreeAsFirstChild on the first array. */
  private static final long REV2_INSERTED_OBJECT_KEY = SetupRevisions.FUSED ? 18L : 26L;

  /** Nodekey that exists in rev1 but was removed in rev3 — the "helloo" field record. */
  private static final long HELLOO_KEY = SetupRevisions.FUSED ? 8L : 11L;

  /** ARRAY inserted in rev4 via insertArrayAsRightSibling of the last "tada" array item. */
  private static final long REV4_INSERTED_ARRAY_KEY = SetupRevisions.FUSED ? 20L : 29L;

  @Test
  public void test_whenRevisionsAndNodeExists_getRevision() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final var allTimesQuery = "sdb:revision(jn:first-existing(sdb:select-item(jn:doc('json-path1','mydoc.jn'), "
          + REV2_INSERTED_OBJECT_KEY + ")))";
      final var allTimesSeq = new Query(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }

      assertEquals(2L, Long.valueOf(buf.toString()));
    }
  }

  @Test
  public void test_whenNodeDoesNotExist_getRevision() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final var allTimesQuery =
          "sdb:revision(jn:first-existing(sdb:select-item(jn:doc('json-path1','mydoc.jn',2), "
              + HELLOO_KEY + ")))";
      final var allTimesSeq = new Query(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }

      assertEquals(1L, Long.valueOf(buf.toString()));
    }
  }

  @Test
  public void test_whenExistsInMostRecentRevision_getRevision() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final var allTimesQuery = "sdb:revision(jn:first-existing(sdb:select-item(jn:doc('json-path1','mydoc.jn'), "
          + REV4_INSERTED_ARRAY_KEY + ")))";
      final var allTimesSeq = new Query(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }

      assertEquals(5L, Long.valueOf(buf.toString()));
    }
  }
}
