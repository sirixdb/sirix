package io.sirix.query.function.jn.temporal;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.Query;
import io.brackit.query.util.io.IOUtils;
import io.brackit.query.util.serialize.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import io.sirix.JsonTestHelper;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class LastExistingTest {

  private static final Path SIRIX_DB_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setup() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test_whenRevisionsAndNodeExists_getRevision() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final var allTimesQuery = "sdb:revision(jn:last-existing(sdb:select-item(jn:doc('json-path1','mydoc.jn'), 26)))";
      final var allTimesSeq = new Query(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }

      assertEquals(5L, Long.valueOf(buf.toString()));
    }
  }

  @Test
  public void test_whenNodeDoesNotExistInLastIndexedRevision_getRevision() throws IOException {
    try (final var store = BasicJsonDBStore.newBuilder().location(SIRIX_DB_PATH.getParent()).build();
         final var ctx = SirixQueryContext.createWithJsonStore(store);
         final var chain = SirixCompileChain.createWithJsonStore(store)) {

      SetupRevisions.setupRevisions(ctx, chain);

      final var allTimesQuery = "sdb:revision(jn:last-existing(sdb:select-item(jn:doc('json-path1','mydoc.jn',2), 11)))";
      final var allTimesSeq = new Query(chain, allTimesQuery).execute(ctx);

      final var buf = IOUtils.createBuffer();
      try (final var serializer = new StringSerializer(buf)) {
        serializer.setFormat(true).serialize(allTimesSeq);
      }

      assertEquals(3L, Long.valueOf(buf.toString()));
    }
  }
}
