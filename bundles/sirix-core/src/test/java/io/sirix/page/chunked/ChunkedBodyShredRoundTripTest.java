/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import com.google.gson.stream.JsonReader;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.page.PageKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The chunk-framed body under the real writer: shredded JSON, committed, reopened, read back.
 *
 * <p>
 * The hand-built pages in {@link ChunkedBodyWireFormatTest} can only reach the degenerate body
 * shape, because a record with a made-up kind id is exactly what the template pool refuses. Only
 * documents that went through the shredder produce the deduped shape — a template pool, per-slot
 * template ids, the hash-elision bitmap, the parentKey and pathNodeKey columns, the value and
 * name-key elision sections — which is the META content that actually ships. This test writes the
 * same document both ways and requires both to read back as the document.
 */
@DisplayName("Chunk-framed body through the shredder")
final class ChunkedBodyShredRoundTripTest {

  /**
   * Repeated object shape, so offset-table dedup fires and the elision levers have something to do.
   */
  private static final String JSON = "{\"records\":["
      + "{\"name\":\"Alice\",\"age\":30,\"dept\":\"Eng\",\"active\":true,\"tags\":[\"red\",\"green\"]},"
      + "{\"name\":\"Bob\",\"age\":25,\"dept\":\"Sales\",\"active\":false,\"tags\":[\"blue\"]},"
      + "{\"name\":\"Carol\",\"age\":42,\"dept\":\"Eng\",\"active\":true,\"tags\":[]},"
      + "{\"name\":\"Dave\",\"age\":51,\"dept\":\"Mkt\",\"active\":false,\"tags\":[\"red\"]},"
      + "{\"name\":\"Erin\",\"age\":28,\"dept\":\"Eng\",\"active\":true,\"tags\":[\"green\",\"blue\"]}"
      + "],\"version\":1,\"schema\":\"v2\"}";

  private boolean previouslyEnabled;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    previouslyEnabled = ChunkedBodyConfig.enabled();
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setEnabledForTesting(previouslyEnabled);
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("a deduped page shredded with chunked bodies reads back as the document")
  void chunkedShredRoundTrips() throws Exception {
    PageKind.resetChunkedBodyStats();
    final String monolith = shredAndReadBack(JsonTestHelper.PATHS.PATH1.getFile(), false);
    final long chunkedBeforeTheChunkedRun = PageKind.chunkedBodiesWritten();
    final String chunked = shredAndReadBack(JsonTestHelper.PATHS.PATH2.getFile(), true);

    assertTrue(PageKind.chunkedBodiesWritten() > chunkedBeforeTheChunkedRun,
        "no page was written with a chunked body — the flag never reached the writer, so this test"
            + " would have proven nothing");
    assertTrue(PageKind.chunkedDedupBodiesWritten() > 0,
        "every chunked page took the degenerate shape; the deduped META content — template pool,"
            + " columns, elision sections — went untested");
    assertTrue(PageKind.chunkedBodiesRead() > 0, "no chunked body was read back");
    JSONAssert.assertEquals(JSON, monolith, true);
    JSONAssert.assertEquals(JSON, chunked, true);
    JSONAssert.assertEquals(monolith, chunked, true);
  }

  /**
   * A page's fragment chain can hold both body formats at once: the format is chosen per
   * serialization, so the first commit after the writer switches leaves a delta in the new framing on
   * top of a full page in the old one. Combine reads decoded slots and bitmaps, neither of which
   * knows about framing — this test is what says so.
   */
  @Test
  @DisplayName("a fragment chain mixing both body formats combines correctly")
  void mixedFormatFragmentChain() throws Exception {
    PageKind.resetChunkedBodyStats();
    final Path file = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(file));
    try (final var db = Databases.openJsonDatabase(file)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .byteHandlerPipeline(new ByteHandlerPipeline())
                                             .build());

      // Revision 1: monolith bodies.
      ChunkedBodyConfig.setEnabledForTesting(false);
      try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = session.beginNodeTrx()) {
        try (final Reader src = new StringReader(JSON); final JsonReader jsonReader = new JsonReader(src)) {
          new JsonShredder.Builder(trx, jsonReader, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
      }

      // Revision 2: the same pages rewritten, now chunk-framed, as deltas over revision 1.
      ChunkedBodyConfig.setEnabledForTesting(true);
      try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = session.beginNodeTrx()) {
        trx.moveToDocumentRoot();
        trx.moveToFirstChild();
        trx.insertObjectRecordAsFirstChild("addedInRevisionTwo", new StringValue("added"));
        trx.commit();
      }
      assertTrue(PageKind.chunkedBodiesWritten() > 0, "revision 2 wrote no chunked body");
    }

    ChunkedBodyConfig.setEnabledForTesting(false);
    try (final var db = Databases.openJsonDatabase(file);
        final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
        final Writer first = new StringWriter();
        final Writer second = new StringWriter()) {
      new JsonSerializer.Builder(session, first).revisions(new int[] {1}).build().call();
      JSONAssert.assertEquals(JSON, first.toString(), true);

      new JsonSerializer.Builder(session, second).revisions(new int[] {2}).build().call();
      assertTrue(PageKind.chunkedBodiesRead() > 0, "revision 2 was served without reading a chunked body");
      assertTrue(second.toString().contains("addedInRevisionTwo"),
          "revision 2 lost the record the chunked fragment defines: " + second);
      assertTrue(second.toString().contains("Alice"),
          "revision 2 lost the records the monolith fragment defines: " + second);
    }
  }

  /** Shred the document into a fresh database with the given body format, then read it back. */
  private static String shredAndReadBack(final Path file, final boolean chunkedBody) throws Exception {
    final boolean previous = ChunkedBodyConfig.setEnabledForTesting(chunkedBody);
    try {
      if (!Files.exists(file)) {
        Databases.createJsonDatabase(new DatabaseConfiguration(file));
      }
      try (final var db = Databases.openJsonDatabase(file)) {
        final var resourceConfig = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                        .byteHandlerPipeline(new ByteHandlerPipeline()) // no outer LZ4:
                                                                                                        // the body
                                                                                                        // speaks for
                                                                                                        // itself
                                                        .build();
        db.createResource(resourceConfig);
        try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
            final var trx = session.beginNodeTrx()) {
          try (final Reader src = new StringReader(JSON); final JsonReader jsonReader = new JsonReader(src)) {
            new JsonShredder.Builder(trx, jsonReader, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
          }
        }
      }
      // Reopened in a second session, so the pages come off disk rather than out of the writer's
      // cache — the only way the body format is actually exercised on the read side.
      try (final var db = Databases.openJsonDatabase(file);
          final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
          final Writer out = new StringWriter()) {
        new JsonSerializer.Builder(session, out).build().call();
        return out.toString();
      }
    } finally {
      ChunkedBodyConfig.setEnabledForTesting(previous);
    }
  }
}
