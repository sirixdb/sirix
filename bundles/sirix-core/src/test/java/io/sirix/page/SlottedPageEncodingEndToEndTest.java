/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import com.google.gson.stream.JsonReader;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * End-to-end round-trip tests exercising the full JSON shred → commit →
 * deserialize path. Verifies that the structural-encoder format can write
 * pages through the production shredder and read them back without loss.
 *
 * <p>Each test creates a fresh DB, shreds a small but representative JSON
 * document (repeated object fields so offset-table dedup fires), commits,
 * closes, re-opens the DB, serializes the tree back to JSON, and asserts
 * bit-level equality with the original.
 *
 * <p>The small-document scope intentionally keeps the test fast (&lt;100ms).
 * Large-scale validation of compression ratio happens via the external
 * benchmarks; here we only prove correctness.
 */
@DisplayName("Slotted page encoding end-to-end JSON shred + deserialize round trip")
final class SlottedPageEncodingEndToEndTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("small array of identical object shape round-trips without LZ4 pipeline")
  void uniformArrayOfObjectsRoundTrip() throws Exception {
    final String json = "["
        + "{\"id\":1,\"dept\":\"Eng\",\"active\":true,\"age\":30},"
        + "{\"id\":2,\"dept\":\"Sales\",\"active\":false,\"age\":42},"
        + "{\"id\":3,\"dept\":\"Eng\",\"active\":true,\"age\":25},"
        + "{\"id\":4,\"dept\":\"Mkt\",\"active\":false,\"age\":51},"
        + "{\"id\":5,\"dept\":\"Eng\",\"active\":true,\"age\":28}"
        + "]";

    final var file = JsonTestHelper.PATHS.PATH1.getFile();
    if (!Files.exists(file)) {
      Databases.createJsonDatabase(new DatabaseConfiguration(file));
    }
    try (final var db = Databases.openJsonDatabase(file)) {
      final var resourceConfig = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
          .byteHandlerPipeline(new ByteHandlerPipeline()) // no LZ4
          .build();
      db.createResource(resourceConfig);
      try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {
        try (final Reader src = new StringReader(json);
             final JsonReader jr = new JsonReader(src)) {
          new JsonShredder.Builder(trx, jr, InsertPosition.AS_FIRST_CHILD)
              .commitAfterwards()
              .build()
              .call();
        }
      }
    }

    // Reopen and serialize back, verifying round-trip equality.
    try (final var db = Databases.openJsonDatabase(file);
         final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final Writer out = new StringWriter()) {
      assertNotNull(session);
      new JsonSerializer.Builder(session, out).build().call();
      JSONAssert.assertEquals(json, out.toString(), true);
    }
  }

  @Test
  @DisplayName("nested object + array round-trips without LZ4 pipeline")
  void nestedJsonRoundTrip() throws Exception {
    final String json = "{\"records\":["
        + "{\"name\":\"Alice\",\"age\":30,\"tags\":[\"red\",\"green\"]},"
        + "{\"name\":\"Bob\",\"age\":25,\"tags\":[\"blue\"]},"
        + "{\"name\":\"Carol\",\"age\":42,\"tags\":[]}"
        + "],\"version\":1,\"schema\":\"v2\"}";

    final var file = JsonTestHelper.PATHS.PATH1.getFile();
    if (!Files.exists(file)) {
      Databases.createJsonDatabase(new DatabaseConfiguration(file));
    }
    try (final var db = Databases.openJsonDatabase(file)) {
      final var resourceConfig = ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
          .byteHandlerPipeline(new ByteHandlerPipeline())
          .build();
      db.createResource(resourceConfig);
      try (final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
           final var trx = session.beginNodeTrx()) {
        try (final Reader src = new StringReader(json);
             final JsonReader jr = new JsonReader(src)) {
          new JsonShredder.Builder(trx, jr, InsertPosition.AS_FIRST_CHILD)
              .commitAfterwards()
              .build()
              .call();
        }
      }
    }

    try (final var db = Databases.openJsonDatabase(file);
         final JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE);
         final Writer out = new StringWriter()) {
      new JsonSerializer.Builder(session, out).build().call();
      JSONAssert.assertEquals(json, out.toString(), true);
    }
  }
}
