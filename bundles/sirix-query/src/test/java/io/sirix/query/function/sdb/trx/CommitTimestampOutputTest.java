package io.sirix.query.function.sdb.trx;

import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.brackit.query.atomic.QNm;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.io.StringWriter;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * VER-201: public commit timestamp text preserves every stored microsecond with ISO_INSTANT
 * formatting.
 */
final class CommitTimestampOutputTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"2021-03-04T05:06:07Z", "2021-03-04T05:06:07.123Z", "2021-03-04T05:06:07.123456Z"})
  void timestampFunctionPreservesIsoTimestampPrecision(final String text) throws Exception {
    final Path json = directory.resolve("json");
    Databases.createJsonDatabase(new DatabaseConfiguration(json));
    try (final var database = Databases.openJsonDatabase(json)) {
      database.createResource(ResourceConfiguration.newBuilder("res").customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("res"); final var writer = session.beginNodeTrx()) {
        writer.insertStringValueAsFirstChild("value");
        writer.commit(null, Instant.parse(text));
      }
    }
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithJsonStore(store);
        final var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var output = new StringWriter();
      try (final var printer = new PrintWriter(output)) {
        new Query(chain, "sdb:timestamp(jn:doc('json','res'))").serialize(context, printer);
      }
      assertEquals(text, output.toString());
      assertEquals(Instant.parse(text), Instant.parse(output.toString()));
      final String function = "sdb:timestamp(jn:doc('json','res'))";
      assertEquals("true", new Query(chain, function + " instance of xs:dateTime").evaluate(context).toString());
      assertEquals("true", new Query(chain, function + " eq xs:dateTime('" + text + "')").evaluate(context).toString());
    }
    final Path xml = directory.resolve("xml");
    Databases.createXmlDatabase(new DatabaseConfiguration(xml));
    try (final var database = Databases.openXmlDatabase(xml)) {
      database.createResource(ResourceConfiguration.newBuilder("res").customCommitTimestamps(true).build());
      try (final var session = database.beginResourceSession("res"); final var writer = session.beginNodeTrx()) {
        writer.insertElementAsFirstChild(new QNm("value"));
        writer.commit(null, Instant.parse(text));
      }
    }
    try (final var store = BasicXmlDBStore.newBuilder().location(directory).build();
        final var context = SirixQueryContext.createWithNodeStore(store);
        final var chain = SirixCompileChain.createWithNodeStore(store)) {
      final var output = new StringWriter();
      try (final var printer = new PrintWriter(output)) {
        new Query(chain, "sdb:timestamp(xn:doc('xml','res'))").serialize(context, printer);
      }
      assertEquals(text, output.toString());
      assertEquals(Instant.parse(text), Instant.parse(output.toString()));
      final String function = "sdb:timestamp(xn:doc('xml','res'))";
      assertEquals("true", new Query(chain, function + " instance of xs:dateTime").evaluate(context).toString());
      assertEquals("true", new Query(chain, function + " eq xs:dateTime('" + text + "')").evaluate(context).toString());
    }
  }
}
