/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import io.sirix.service.xml.serialize.XmlSerializer;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** End-to-end gates for authoritative cached linkage across pages and async TIL identities. */
final class DocumentRecordLocationCacheIntegrationTest {

  private static final String RESOURCE = "document-location-cache";
  private static final int ASYNC_FLUSH_THRESHOLD = 64;
  private static final String OVERFLOW_TEXT = "overflow-payload-" + "x".repeat(96 * 1024);

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void arbitraryJsonCrossesCacheAndRecordPagesWithPinnedOverflowAndColdReopen() throws Exception {
    final String document = jsonDocument();
    final Path databasePath = temporaryDirectory.resolve("json-database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(resourceConfiguration());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
           final JsonNodeTrx wtx = session.beginNodeTrx(ASYNC_FLUSH_THRESHOLD,
               AfterCommitState.KEEP_OPEN_ASYNC_FLUSH);
           final var parser = JacksonJsonShredder.createStringParser(document)) {
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).build().call();
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
         final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final StringWriter output = new StringWriter(document.length());
      new JsonSerializer.Builder(session, output).build().call();
      JSONAssert.assertEquals(document, output.toString(), true);
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void arbitraryXmlCrossesCacheAndRecordPagesWithPinnedOverflowAndColdReopen() throws Exception {
    final String document = xmlDocument();
    final Path databasePath = temporaryDirectory.resolve("xml-database");
    Databases.createXmlDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath)) {
      database.createResource(resourceConfiguration());
      try (final XmlResourceSession session = database.beginResourceSession(RESOURCE);
           final XmlNodeTrx wtx = session.beginNodeTrx(ASYNC_FLUSH_THRESHOLD,
               AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(document), XmlNodeTrx.Commit.No);
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<XmlResourceSession> database = Databases.openXmlDatabase(databasePath);
         final XmlResourceSession session = database.beginResourceSession(RESOURCE);
         final ByteArrayOutputStream output = new ByteArrayOutputStream(document.length())) {
      XmlSerializer.newBuilder(session, output).build().call();
      final String serializedDocument = output.toString(StandardCharsets.UTF_8);
      assertEquals(document, serializedDocument.replace("<!-- ", "<!--").replace(" -->", "-->"),
          "XmlSerializer pads comment delimiters without changing the stored comment value");
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
  }

  private static ResourceConfiguration resourceConfiguration() {
    return ResourceConfiguration.newBuilder(RESOURCE)
                                .storeDiffs(false)
                                .hashKind(HashType.NONE)
                                .buildPathSummary(false)
                                .useDeweyIDs(false)
                                .versioningApproach(VersioningType.FULL)
                                .storageType(StorageType.FILE_CHANNEL)
                                .build();
  }

  private static String jsonDocument() {
    final StringBuilder document = new StringBuilder(160 * 1024)
        .append("{\"overflow\":\"")
        .append(OVERFLOW_TEXT)
        .append("\",\"rows\":[");
    for (int i = 0; i < 220; i++) {
      if (i > 0) {
        document.append(',');
      }
      switch (i & 3) {
        case 0 -> document.append("{\"id\":").append(i)
            .append(",\"nested\":{\"flag\":true,\"values\":[")
            .append(i).append(',').append(i + 1).append(",null]}}");
        case 1 -> document.append('[').append(i)
            .append(",{\"name\":\"row-").append(i).append("\"},[true,false]]");
        case 2 -> document.append("{\"id\":").append(i)
            .append(",\"text\":\"value-").append(i).append("\",\"empty\":{}}");
        case 3 -> document.append("[null,").append(i).append(",\"tail-").append(i).append("\"]");
        default -> throw new AssertionError();
      }
    }
    return document.append("]}").toString();
  }

  private static String xmlDocument() {
    final StringBuilder document = new StringBuilder(160 * 1024)
        .append("<root><overflow>")
        .append(OVERFLOW_TEXT)
        .append("</overflow>");
    for (int i = 0; i < 220; i++) {
      document.append("<row id=\"").append(i).append("\"><name>row-").append(i)
          .append("</name><values><v>").append(i).append("</v><v>").append(i + 1)
          .append("</v></values>");
      if ((i & 15) == 0) {
        document.append("<!--comment-").append(i).append("--><?row processing-").append(i).append("?>");
      }
      document.append("</row>");
    }
    return document.append("</root>").toString();
  }
}
