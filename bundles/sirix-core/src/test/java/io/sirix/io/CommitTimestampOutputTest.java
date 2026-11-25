package io.sirix.io;

import com.google.gson.JsonParser;
import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.serialize.JsonLimitedSerializer;
import io.sirix.service.json.serialize.JsonRecordSerializer;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.xml.serialize.XmlSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * VER-201: public commit timestamp text preserves every stored microsecond with ISO_INSTANT
 * formatting.
 */
final class CommitTimestampOutputTest {
  private static final String[] ISO_TIMES =
      {"2021-03-04T05:06:07Z", "2021-03-04T05:06:07.123Z", "2021-03-04T05:06:07.123456Z"};

  @TempDir
  Path directory;

  @Test
  void jsonSerializersPreserveIsoTimestampPrecision() {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openJsonDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("res").customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertArrayAsFirstChild();
          writer.insertStringValueAsFirstChild("value");
          for (final String text : ISO_TIMES) {
            writer.commit(null, Instant.parse(text));
          }
        }
        final var full = new StringWriter();
        JsonSerializer.newBuilder(session, full, 1, 2, 3).serializeTimestamp(true).build().call();
        final var limited = new StringWriter();
        JsonLimitedSerializer.newBuilder(session, limited, 1, 2, 3).serializeTimestamp(true).build().call();
        final var records = new StringWriter();
        JsonRecordSerializer.newBuilder(session, 1, records, 1, 2, 3).serializeTimestamp(true).build().call();
        for (final String output : new String[] {full.toString(), limited.toString(), records.toString()}) {
          final var revisions = JsonParser.parseString(output).getAsJsonObject().getAsJsonArray("sirix");
          assertEquals(3, revisions.size(), output);
          for (int index = 0; index < ISO_TIMES.length; index++) {
            final String emitted = revisions.get(index).getAsJsonObject().get("revisionTimestamp").getAsString();
            assertEquals(ISO_TIMES[index], emitted, output);
            try (final var reader = session.beginNodeReadOnlyTrx(index + 1)) {
              assertEquals(reader.getRevisionTimestamp(), Instant.parse(emitted));
            }
          }
        }
      }
    }
  }

  @Test
  void xmlSerializerPreservesIsoTimestampPrecision() throws Exception {
    Databases.createXmlDatabase(new DatabaseConfiguration(directory));
    try (final var database = Databases.openXmlDatabase(directory)) {
      database.createResource(ResourceConfiguration.newBuilder("res").customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("res")) {
        try (final var writer = session.beginNodeTrx()) {
          writer.insertElementAsFirstChild(new QNm("value"));
          for (final String text : ISO_TIMES) {
            writer.commit(null, Instant.parse(text));
          }
        }
        for (final boolean rest : new boolean[] {false, true}) {
          final var output = new ByteArrayOutputStream();
          final var builder = XmlSerializer.newBuilder(session, output, 1, 2, 3).serializeTimestamp(true);
          if (rest) {
            builder.emitRESTful().emitRESTSequence();
          }
          builder.build().call();
          final var factory = DocumentBuilderFactory.newInstance();
          factory.setNamespaceAware(true);
          final var document = factory.newDocumentBuilder().parse(new ByteArrayInputStream(output.toByteArray()));
          final var items = document.getElementsByTagName(rest
              ? "rest:item"
              : "sdb:sirix-item");
          assertEquals(3, items.getLength(), output.toString());
          for (int index = 0; index < ISO_TIMES.length; index++) {
            final String emitted = ((Element) items.item(index)).getAttribute(rest
                ? "rest:revisionTimestamp"
                : "sdb:revisionTimestamp");
            assertEquals(ISO_TIMES[index], emitted);
            try (final var reader = session.beginNodeReadOnlyTrx(index + 1)) {
              assertEquals(reader.getRevisionTimestamp(), Instant.parse(emitted));
            }
          }
        }
      }
    }
  }
}
