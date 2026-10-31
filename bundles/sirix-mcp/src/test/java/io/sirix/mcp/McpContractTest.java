package io.sirix.mcp;

import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.brackit.query.jdm.Type;
import io.brackit.query.function.json.JSONFun;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexDef.DbType;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.service.json.serialize.JsonSerializer;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import io.sirix.service.xml.serialize.XmlSerializer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static io.brackit.query.util.path.Path.parse;
import static io.brackit.query.util.path.PathParser.Type.JSON;

class McpContractTest {
  private static final Instant FIRST = Instant.parse("2026-01-01T00:00:00Z");
  private static final String DOC = "jn:doc('db','public')";
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  @TempDir
  Path directory;

  @BeforeEach
  void createDatabase() {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("db")));
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"))) {
      for (final String name : List.of("public", "secret")) {
        db.createResource(ResourceConfiguration.newBuilder(name)
                                               .storageType(StorageType.FILE_CHANNEL)
                                               .customCommitTimestamps(true)
                                               .build());
        try (final var session = db.beginResourceSession(name); final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"name\":\"Alice\",\"age\":1}"),
              JsonNodeTrx.Commit.NO);
          writer.commit(null, FIRST);
        }
      }
    }
  }

  private ToolHandlers handlers(final boolean readOnly, final boolean restricted, final int count, final int size)
      throws IOException {
    return handlers(directory, readOnly, restricted
        ? Map.of("db", List.of("public"))
        : Map.of(), count, size);
  }

  private ToolHandlers handlers(final Path root, final boolean readOnly,
      final Map<String, List<String>> resources, final int count, final int size) throws IOException {
    final var config = new McpServerConfig("test", "1", "stdio", root.toString(), readOnly, List.of(), List.of(),
        resources, count, size, false, false, false, null);
    return new ToolHandlers(config, new AccessControl(config), new OutputSanitizer(config),
        new SnapshotRegistry(root), AuditLog.create(config));
  }

  private static CallToolRequest request(final Map<String, Object> parameters) {
    final var args = new LinkedHashMap<String, Object>();
    args.put("database", "db");
    args.put("resource", "public");
    args.putAll(parameters);
    return new CallToolRequest("test", args);
  }

  private static String text(final CallToolResult result) {
    return ((TextContent) result.content().getFirst()).text();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> structured(final CallToolResult result) {
    return (Map<String, Object>) result.structuredContent();
  }

  private int head() {
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"));
        final var session = db.beginResourceSession("public")) {
      return session.getMostRecentRevisionNumber();
    }
  }

  private void modify() {
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"));
        final var session = db.beginResourceSession("public");
        final var writer = session.beginNodeTrx()) {
      assertThat(writer.moveTo(2)).isTrue();
      writer.setStringValue("Bob");
      assertThat(writer.moveTo(3)).isTrue();
      writer.setNumberValue(2);
      writer.commit(null, FIRST.plusSeconds(10));
    }
  }

  @Test
  void queryMutationDiffGuardedRevertAndColdReopen() throws Exception {
    final var h = handlers(false, false, 100, 4096);
    assertThat(text(h.query(null, request(Map.of("query", DOC + ".name"))))).contains("Alice");
    assertThat(h.createSnapshot(null, request(Map.of("name", "baseline"))).isError()).isFalse();
    final var mutation = h.update(null, request(Map.of("nodeKey", 2, "value", "Bob")));
    assertThat(mutation.isError()).isFalse();
    assertThat(head()).isEqualTo(2);
    assertThat(text(h.query(null, request(Map.of("query", DOC + ".name"))))).contains("Bob");
    for (final Map<String, Object> selector : List.<Map<String, Object>>of(Map.of("revision", 1),
        Map.of("snapshot", "baseline"), Map.of("timestamp", FIRST.plusSeconds(1).toString()))) {
      final var args = new LinkedHashMap<>(selector);
      args.put("query", DOC + ".name");
      final var historical = h.query(null, request(args));
      assertThat(historical.isError()).as(text(historical)).isFalse();
      assertThat(text(historical)).contains("Alice").doesNotContain("Bob");
      assertThat(structured(historical)).containsEntry("revision", 1);
      args.put("query", "$$.name");
      assertThat(text(h.query(null, request(args)))).contains("Alice");
    }
    assertThat(
        h.query(null, request(Map.of("query", "jn:doc('db','public',2).name", "revision", 1))).isError()).isTrue();
    assertThat(text(h.query(null, request(Map.of("query", DOC + ".name"))))).contains("Bob").doesNotContain("Alice");
    final var diff = h.diff(null, request(Map.of("from_revision", 1, "to_revision", 2)));
    assertThat(diff.isError()).as(text(diff)).isFalse();
    assertThat(MAPPER.readTree(text(diff)).get("diffs").isArray()).isTrue();
    assertThat(structured(diff)).containsEntry("truncated", false);
    final var stale = h.revert(null, request(Map.of("snapshot", "baseline", "expected_head", 1)));
    assertThat(stale.isError()).isTrue();
    assertThat(structured(stale)).containsEntry("code", "stale_head").containsEntry("previousHead", 2);
    assertThat(head()).isEqualTo(2);
    final var revert = h.revert(null, request(Map.of("snapshot", "baseline", "expected_head", 2)));
    assertThat(revert.isError()).as(text(revert)).isFalse();
    assertThat(structured(revert)).containsEntry("revision", 3).containsEntry("previousHead", 2);
    final var cold = handlers(false, false, 100, 4096);
    assertThat(text(cold.listSnapshots(null, request(Map.of())))).contains("baseline");
    assertThat(text(cold.query(null, request(Map.of("query", DOC + ".name"))))).contains("Alice");
    assertThat(text(cold.query(null, request(Map.of("query", DOC + ".name", "revision", 2))))).contains("Bob");
    assertThat(cold.createSnapshot(null, request(Map.of("name", "after"))).isError()).isFalse();
    assertThat(text(handlers(true, false, 100, 4096).listSnapshots(null, request(Map.of())))).contains("baseline")
                                                                                             .contains("after");
  }

  @Test
  void historicalTemporalAccessDenialsAreErrorsInsteadOfEmptyCounts() throws Exception {
    modify();
    final var setup = handlers(false, false, 100, 4096);
    assertThat(setup.createSnapshot(null, request(Map.of("name", "second", "revision", 2))).isError()).isFalse();
    for (final boolean readOnly : new boolean[] {false, true}) {
      final var h = handlers(readOnly, true, 100, 4096);
      for (final Map<String, Object> selector : List.<Map<String, Object>>of(Map.of("revision", 2),
          Map.of("snapshot", "second"), Map.of("timestamp", FIRST.plusSeconds(10).toString()))) {
        for (final String function : List.of("all-times", "past")) {
          final var args = new LinkedHashMap<>(selector);
          args.put("query", "count(jn:" + function + "($$))");
          final var denied = h.query(null, request(args));
          assertThat(denied.isError()).as(function + ": " + text(denied)).isTrue();
          assertThat(text(h.query(null, request(Map.of("query", DOC + ".name", "revision", 2))))).contains("Bob");
        }
      }
      assertThat(structured(h.query(null, request(Map.of("query", "count(jn:all-times($$))")))))
          .containsEntry("items", List.of("2"));
      assertThat(structured(h.query(null, request(Map.of("query", "count(jn:past($$))")))))
          .containsEntry("items", List.of("1"));
      assertThat(structured(h.query(null, request(Map.of("query", "count(jn:all-times($$))", "revision", 1)))))
          .containsEntry("items", List.of("1"));
      assertThat(structured(h.query(null, request(Map.of("query", "count(jn:past($$))", "revision", 1)))))
          .containsEntry("items", List.of("0"));
    }
    assertThat(head()).isEqualTo(2);
  }

  @Test
  void historicalContextUsesTheHistoricalIndexAndDoesNotContaminateLaterQueries() throws Exception {
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"));
        final var session = db.beginResourceSession("public");
        final var writer = session.beginNodeTrx()) {
      session.getWtxIndexController(writer.getRevisionNumber())
             .createIndexes(
                 Set.of(IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/age", JSON)), 0, DbType.JSON)),
                 writer);
      writer.commit(null, FIRST.plusSeconds(5));
    }
    modify();
    final var h = handlers(true, true, 100, 4096);
    final String query = "for $doc in " + DOC + " where $doc.age = 1 return $doc.name";
    final var historical = h.query(null, request(Map.of("query", query, "revision", 2)));
    assertThat(historical.isError()).as(text(historical)).isFalse();
    assertThat(text(historical)).contains("Alice");
    final var current = h.query(null, request(Map.of("query", query)));
    assertThat(current.isError()).as(text(current)).isFalse();
    assertThat(structured(current)).containsEntry("items", List.of());
  }

  @Test
  void rejectsInvalidConflictingAndPrunedSelectors() throws Exception {
    modify();
    final var h = handlers(false, false, 100, 4096);
    for (final Object revision : List.of(0, -1, 3, 1.5, 4294967297L)) {
      assertThat(h.query(null, request(Map.of("query", "1", "revision", revision))).isError()).isTrue();
      assertThat(h.createSnapshot(null, request(Map.of("name", "bad", "revision", revision))).isError()).isTrue();
    }
    assertThat(
        h.query(null, request(Map.of("query", "1", "revision", 1, "timestamp", FIRST.toString()))).isError()).isTrue();
    assertThat(h.query(null, request(Map.of("query", "1", "snapshot", "missing"))).isError()).isTrue();
    assertThat(h.query(null, request(Map.of("query", "1", "timestamp", "bad"))).isError()).isTrue();
    assertThat(
        h.query(null, request(Map.of("query", "1", "timestamp", FIRST.minusSeconds(1).toString()))).isError()).isTrue();
    assertThat(h.revert(null, request(Map.of("revision", 1, "snapshot", "baseline"))).isError()).isTrue();
    assertThat(h.createSnapshot(null, request(Map.of("name", "pruned", "revision", 1))).isError()).isFalse();
    final ByteBuffer record = ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE);
    IOStorage.writePrunedRevisionRecord(record, FIRST.toEpochMilli());
    record.flip();
    final Path revisions = directory.resolve("db/resources/public/data").resolve(IOStorage.REVISIONS_FILENAME);
    try (final FileChannel channel = FileChannel.open(revisions, StandardOpenOption.WRITE)) {
      long offset = IOStorage.revisionsFileOffset(1);
      while (record.hasRemaining())
        offset += channel.write(record, offset);
      channel.force(false);
    }
    // Every handler above closes its database; invalidate the explicitly patched revision index.
    Databases.clearGlobalCaches();
    for (final var result : List.of(h.query(null, request(Map.of("query", "1", "revision", 1))),
        h.query(null, request(Map.of("query", "1", "snapshot", "pruned"))),
        h.query(null, request(Map.of("query", "1", "timestamp", FIRST.toString()))),
        h.createSnapshot(null, request(Map.of("name", "bad", "revision", 1))),
        h.diff(null, request(Map.of("from_revision", 1, "to_revision", 2))),
        h.revert(null, request(Map.of("revision", 1))))) {
      assertThat(result.isError()).as(text(result)).isTrue();
      assertThat(structured(result)).containsEntry("code", "revision_pruned");
    }
    assertThat(head()).isEqualTo(2);
  }

  @Test
  void rejectsSameAndReversedDiffPairsEvenWithOneRevision() throws Exception {
    final var h = handlers(true, false, 100, 4096);
    assertThat(h.diff(null, request(Map.of("from_revision", 1, "to_revision", 1))).isError()).isTrue();
    modify();
    for (final var pair : List.of(Map.of("from_revision", 2, "to_revision", 1),
        Map.of("from_revision", 2, "to_revision", 2), Map.of("from_revision", 0, "to_revision", 2),
        Map.of("from_revision", 1, "to_revision", 3))) {
      assertThat(h.diff(null, request(new LinkedHashMap<>(pair))).isError()).isTrue();
    }
  }

  @Test
  void enforcesReadOnlyAndActualResourceAtExecution() throws Exception {
    final var readOnly = handlers(true, true, 100, 4096);
    for (final String query : List.of("sdb:commit(" + DOC + ")",
        "replace json value of " + DOC + ".name with 'Changed'", "jn:store('db','public','{}')",
        "jn:drop-database('db')")) {
      final var result = readOnly.query(null, request(Map.of("query", query)));
      assertThat(result.isError()).as(query + ": " + text(result)).isTrue();
      assertThat(head()).isEqualTo(1);
    }
    final Path xml = directory.resolve("xml");
    final String xmlStore = "xn:store('" + xml + "','res',<a/>)";
    assertThat(readOnly.query(null, request(Map.of("query", xmlStore))).isError()).isTrue();
    assertThat(Files.exists(xml)).isFalse();
    final var restricted = handlers(false, true, 100, 4096);
    for (final String query : List.of("jn:doc('db','secret')", "jn:exists-resource('db','secret')",
        "sdb:commit(jn:doc('db','secret'))", "jn:store('db','secret','{}')", "jn:drop-database('db')")) {
      final var result = restricted.query(null, request(Map.of("query", query)));
      assertThat(result.isError()).as(query + ": " + text(result)).isTrue();
    }
    final var noResource =
        new CallToolRequest("sirix_query", Map.of("database", "db", "query", "jn:doc('db','secret')"));
    assertThat(restricted.query(null, noResource).isError()).isTrue();
    assertThat(text(restricted.listResources(null, request(Map.of())))).contains("public").doesNotContain("secret");
    assertThat(
        restricted.query(null, request(Map.of("query", "sdb:commit(" + DOC + ")", "revision", 1))).isError()).isTrue();
    assertThat(head()).isEqualTo(1);
    final var unrestricted = handlers(false, false, 100, 4096);
    assertThat(
        unrestricted.query(null, request(Map.of("query", "jn:drop-database('db')", "revision", 1))).isError()).isTrue();
    assertThat(unrestricted.query(null, request(Map.of("query", "jn:store('newdb','res','{}')", "revision", 1)))
                           .isError()).isTrue();
    assertThat(Databases.existsDatabase(directory.resolve("newdb"))).isFalse();
    assertThat(restricted.query(null, request(Map.of("query", "sdb:commit(" + DOC + ")"))).isError()).isTrue();
    assertThat(head()).isEqualTo(1);
  }

  @Test
  void diffPreservesFittingFragmentsAndOmitsOversizedFragmentsWhole() throws Exception {
    final var h = handlers(true, false, 100, 512);
    for (final String resource : List.of("small-fragment", "large-fragment")) {
      final String value = resource.startsWith("small")
          ? "small"
          : "x".repeat(4096);
      try (final var db = Databases.openJsonDatabase(directory.resolve("db"))) {
        db.createResource(ResourceConfiguration.newBuilder(resource).build());
        try (final var session = db.beginResourceSession(resource); final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
          writer.moveToDocumentRoot();
          assertThat(writer.moveToFirstChild()).isTrue();
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"value\":\"" + value + "\"}"),
              JsonNodeTrx.Commit.NO);
          writer.commit();
        }
      }
      final var result = h.diff(null, request(Map.of("resource", resource, "from_revision", 1, "to_revision", 2)));
      assertThat(result.isError()).as(text(result)).isFalse();
      final var document = MAPPER.readTree(text(result));
      if (resource.startsWith("small")) {
        assertThat(document.get("truncated").asBoolean()).isFalse();
        final String data = document.get("diffs").get(0).get("insert").get("data").asText();
        assertThat(MAPPER.readTree(data).get("value").asText()).isEqualTo("small");
      } else {
        assertThat(document.get("truncated").asBoolean()).isTrue();
        assertThat(document.get("diffs").isEmpty()).isTrue();
        assertThat(document.get("continuation")).isNotNull();
      }
    }
  }




  @Test
  void updatingQueriesAreRejectedBeforeExecutionInBothModes() throws Exception {
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"))) {
      db.removeResource("secret");
    }
    final var before = resourceFiles(directory.resolve("db"));
    for (final boolean readOnly : new boolean[] {false, true}) {
      final var h = handlers(readOnly, true, 2, 128);
      for (final String query : List.of("jn:store('db','secret','{}')", "jn:store('db','public','{}')",
          "jn:store('newdb','res','{}')", "jn:store('db','public',('{}','{}'))", "jn:drop-database('db')",
          "sdb:commit(" + DOC + ")", "for $i in 1 to 5 return sdb:commit(" + DOC + ")",
          "replace json value of " + DOC + ".name with 'Changed'",
          "delete json sdb:select-item(" + DOC + ",2)",
          "replace json value of jn:doc('db','array')[0] with 99",
          "xn:store('" + directory.resolve("xml") + "','public',(<a/>,<b/>))",
          "jn:load('db','public','file:///missing-mcp-document.json')",
          "declare namespace j='" + JSONFun.JSON_NSURI + "'; j:store('db','secret','{}')",
          "declare function local:change() { jn:store('db','secret','{}') }; local:change()",
          "if (false()) then jn:store('db','secret','{}') else ()",
          "xn:serialize(<a/>,false(),'" + directory.resolve("unexpected.xml") + "')")) {
        final var result = h.query(null, request(Map.of("query", query)));
        assertThat(result.isError()).as(query + ": " + text(result)).isTrue();
        assertThat(structured(result)).as(query).containsEntry("code", "updating_query");
        assertThat(text(result)).contains("sirix_create_resource", "sirix_update");
        assertResourceFiles(directory.resolve("db"), before);
        assertThat(Files.exists(directory.resolve("newdb"))).isFalse();
        assertThat(Files.exists(directory.resolve("xml"))).isFalse();
        assertThat(Files.exists(directory.resolve("unexpected.xml"))).isFalse();
      }
      final var missing = h.query(null, request(Map.of("database", "missing", "resource", "absent", "query",
          "jn:store('db','secret','{}')")));
      assertThat(structured(missing)).containsEntry("code", "updating_query");
      assertThat(structured(h.query(null, request(Map.of("query", "(1,2,3,4)")))))
          .containsEntry("items", List.of("1", "2")).containsEntry("truncated", true);
      assertThat(structured(h.query(null, request(Map.of("query", "(1,2,3,4)", "offset", 2)))))
          .containsEntry("items", List.of("3", "4")).containsEntry("truncated", false);
    }
    assertThat(head()).isEqualTo(1);
  }

  @Test
  void creationAuthorizesBeforeChangesAndOverwritesOnlyTheRequestedResource() throws Exception {
    final var h = handlers(directory, false, Map.of("db", List.of("public", "fresh")), 100, 4096);
    final var before = resourceFiles(directory.resolve("db"));
    for (final String denied : List.of("secret", "denied-new")) {
      assertThat(h.createResource(null, request(Map.of("resource", denied, "data", "{}", "overwrite", true)))
          .isError()).isTrue();
      assertResourceFiles(directory.resolve("db"), before);
    }
    assertThat(handlers(true, false, 100, 4096)
        .createResource(null, request(Map.of("data", "{}", "overwrite", true))).isError()).isTrue();
    assertThat(structured(h.createResource(null, request(Map.of("data", "{}")))))
        .containsEntry("code", "resource_exists");
    for (final String malformed : List.of("", "{} {}", "{bad")) {
      assertThat(h.createResource(null, request(Map.of("data", malformed, "overwrite", true))).isError()).isTrue();
      assertResourceFiles(directory.resolve("db"), before);
    }
    final var created = h.createResource(null, request(Map.of("resource", "fresh", "data", "[1,2]")));
    assertThat(created.isError()).as(text(created)).isFalse();
    assertThat(structured(created)).containsEntry("revision", 1);
    final var replaced = h.createResource(null, request(Map.of("data", "{\"name\":\"Changed\"}", "overwrite", true)));
    assertThat(replaced.isError()).as(text(replaced)).isFalse();
    assertThat(structured(replaced)).containsEntry("revision", 2);
    assertStoredResource("public", 2, "{\"name\":\"Changed\"}");
    assertStoredResource("secret", 1, "{\"name\":\"Alice\",\"age\":1}");
    assertStoredResource("fresh", 1, "[1,2]");
    assertThat(text(h.query(null, request(Map.of("query", DOC + ".name", "revision", 1))))).contains("Alice");
    for (final var result : List.of(h.insert(null, request(Map.of("resource", "secret", "data", "{}"))),
        h.update(null, request(Map.of("resource", "secret", "nodeKey", 2, "value", "Denied"))),
        h.delete(null, request(Map.of("resource", "secret", "nodeKey", 2))),
        h.revert(null, request(Map.of("resource", "secret", "revision", 1))))) {
      assertThat(result.isError()).isTrue();
    }
    assertStoredResource("secret", 1, "{\"name\":\"Alice\",\"age\":1}");
    Databases.clearGlobalCaches();
    assertStoredResource("public", 2, "{\"name\":\"Changed\"}");
  }

  @Test
  void scalarJsonRootsCreateAndOverwriteWithoutEmptyResourcesOrUnrelatedChanges() throws Exception {
    final var writable = handlers(false, false, 100, 4096);
    final var restricted = handlers(false, true, 100, 4096);
    final var readOnly = handlers(true, false, 100, 4096);
    final List<String> roots = List.of("\"text with \\\"quotes\\\" and \\n\"", "42", "123456789012345678901234567890",
        "0.12345678901234567890123456789", "true", "false", "null");
    int revision = 1;
    String current = "{\"name\":\"Alice\",\"age\":1}";
    for (int index = 0; index < roots.size(); index++) {
      final String root = roots.get(index);
      final String resource = "scalar-" + index;
      final var before = resourceFiles(directory.resolve("db"));
      assertThat(restricted.createResource(null, request(Map.of("resource", resource, "data", root))).isError()).isTrue();
      assertThat(readOnly.createResource(null, request(Map.of("resource", resource, "data", root))).isError()).isTrue();
      assertResourceFiles(directory.resolve("db"), before);
      assertThat(Files.exists(directory.resolve("db/resources").resolve(resource))).isFalse();
      final var created = writable.createResource(null, request(Map.of("resource", resource, "data", root)));
      assertThat(created.isError()).as(root + ": " + text(created)).isFalse();
      assertThat(structured(created)).containsEntry("revision", 1);
      assertStoredResource(resource, 1, root);
      final var afterCreation = resourceFiles(directory.resolve("db"));
      assertThat(structured(writable.createResource(null, request(Map.of("resource", resource, "data", root)))))
          .containsEntry("code", "resource_exists");
      assertThat(structured(writable.createResource(null, request(Map.of("data", root)))))
          .containsEntry("code", "resource_exists");
      assertThat(readOnly.createResource(null, request(Map.of("data", root, "overwrite", true))).isError()).isTrue();
      assertResourceFiles(directory.resolve("db"), afterCreation);
      assertStoredResource("public", revision, current);
      final var replaced = writable.createResource(null, request(Map.of("data", root, "overwrite", true)));
      assertThat(replaced.isError()).as(root + ": " + text(replaced)).isFalse();
      assertThat(structured(replaced)).containsEntry("revision", ++revision);
      assertStoredResource("public", revision, root);
      current = root;
      assertStoredResource("secret", 1, "{\"name\":\"Alice\",\"age\":1}");
    }
    Databases.clearGlobalCaches();
    assertStoredResource("public", revision, "null");
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"));
        final var session = db.beginResourceSession("public");
        final var output = new StringWriter()) {
      JsonSerializer.newBuilder(session, output, 1).build().call();
      assertThat(MAPPER.readTree(output.toString())).isEqualTo(MAPPER.readTree("{\"name\":\"Alice\",\"age\":1}"));
    }
  }

  @Test
  void xmlCreationAndOverwritePreserveSiblingsAndHistory() throws Exception {
    final var h = handlers(false, false, 100, 4096);
    for (final var parameters : List.of(Map.of("resource", "public", "data", "<!--before--><a>one</a><!--after-->"),
        Map.of("resource", "sibling", "data", "<keep/>"))) {
      final var args = new LinkedHashMap<String, Object>(parameters);
      args.put("database", "xml");
      args.put("format", "xml");
      final var result = h.createResource(null, request(args));
      assertThat(result.isError()).as(text(result)).isFalse();
      assertThat(structured(result)).containsEntry("revision", 1);
    }
    final var restricted = handlers(directory, false, Map.of("xml", List.of("public")), 100, 4096);
    final var before = resourceFiles(directory.resolve("xml"));
    assertThat(restricted.createResource(null, request(Map.of("database", "xml", "resource", "sibling", "format",
        "xml", "data", "<denied/>", "overwrite", true))).isError()).isTrue();
    assertThat(structured(restricted.createResource(null, request(Map.of("database", "xml", "format", "xml", "data", "<b/>")))))
        .containsEntry("code", "resource_exists");
    assertResourceFiles(directory.resolve("xml"), before);
    final var replaced = restricted.createResource(null, request(Map.of("database", "xml", "format", "xml", "data",
        "<b>two</b>", "overwrite", true)));
    assertThat(replaced.isError()).as(text(replaced)).isFalse();
    assertThat(structured(replaced)).containsEntry("revision", 2);
    assertThat(xmlDocument("public", 2)).isEqualTo("<b>two</b>");
    assertThat(xmlDocument("public", 1)).contains("<a>one</a>");
    assertThat(xmlDocument("sibling", 1)).isEqualTo("<keep/>");
    for (final String invalid : List.of("<a>", "<!DOCTYPE a [<!ENTITY x SYSTEM 'file:///missing'>]><a>&x;</a>")) {
      assertThat(h.createResource(null, request(Map.of("database", "invalid-xml", "format", "xml", "data", invalid)))
          .isError()).isTrue();
      assertThat(Files.exists(directory.resolve("invalid-xml"))).isFalse();
    }
  }

  private String xmlDocument(final String resource, final int revision) throws Exception {
    try (final var db = Databases.openXmlDatabase(directory.resolve("xml"));
        final var session = db.beginResourceSession(resource);
        final var output = new ByteArrayOutputStream()) {
      XmlSerializer.newBuilder(session, output, revision).build().call();
      return output.toString(StandardCharsets.UTF_8);
    }
  }

  private void assertStoredResource(final String resource, final int revision, final String expected) throws Exception {
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"));
        final var session = db.beginResourceSession(resource);
        final var output = new StringWriter()) {
      assertThat(session.getMostRecentRevisionNumber()).isEqualTo(revision);
      JsonSerializer.newBuilder(session, output).build().call();
      assertThat(MAPPER.readTree(output.toString())).isEqualTo(MAPPER.readTree(expected));
      if (MAPPER.readTree(expected).isNumber())
        assertThat(new BigDecimal(output.toString())).isEqualByComparingTo(new BigDecimal(expected));
    }
  }

  @Test
  void returnsWholeValuesAndContinuationWithSanitizationDisabled() throws Exception {
    final var h = handlers(true, false, 2, 128);
    final var first = h.query(null, request(Map.of("query", "(1,2,3,4)")));
    assertThat(first.isError()).as(text(first)).isFalse();
    assertThat(structured(first)).containsEntry("items", List.of("1", "2")).containsEntry("truncated", true);
    final var second = h.query(null, request(Map.of("query", "(1,2,3,4)", "offset", 2)));
    assertThat(structured(second)).containsEntry("items", List.of("3", "4")).containsEntry("truncated", false);
    final var oversized = h.query(null, request(Map.of("query", "{\"large\":\"" + "x".repeat(512) + "\"}")));
    assertThat(oversized.isError()).as(text(oversized)).isFalse();
    assertThat(structured(oversized)).containsEntry("items", List.of()).containsEntry("truncated", true);
    assertThat(MAPPER.readTree(text(oversized)).get("continuation")).isNotNull();
    modify();
    final var small = handlers(true, false, 1, 4096);
    final var diff = small.diff(null, request(Map.of("from_revision", 1, "to_revision", 2)));
    assertThat(diff.isError()).as(text(diff)).isFalse();
    assertThat(structured(diff)).containsEntry("truncated", true);
    assertThat(MAPPER.readTree(text(diff)).get("diffs").size()).isEqualTo(1);
    final var next = small.diff(null, request(Map.of("from_revision", 1, "to_revision", 2, "offset", 1)));
    assertThat(structured(next)).containsEntry("truncated", false);
    assertThat(MAPPER.readTree(text(next)).get("diffs").size()).isEqualTo(1);
  }



  @Test
  void deniedJsonReplacementAndDropLeaveEveryResourceUnchanged() throws Exception {
    final Path path = directory.resolve("db");
    final String permitted;
    try (final var database = Databases.openJsonDatabase(path)) {
      permitted = database.listResources().getFirst().getFileName().toString();
    }
    final var h = handlers(directory, false, Map.of("db", List.of(permitted)), 100, 4096);
    final var before = resourceFiles(path);
    for (final String query : List.of("jn:store('db','" + permitted + "','{}')",
        "jn:store('db','" + permitted + "',('{}','{}'))", "jn:drop-database('db')")) {
      final var result = h.query(null, request(Map.of("resource", permitted, "query", query)));
      assertThat(result.isError()).as(query + ": " + text(result)).isTrue();
      assertResourceFiles(path, before);
      assertThat(text(handlers(true, false, 100, 4096)
          .query(null, request(Map.of("query", "jn:doc('db','secret').name"))))).contains("Alice");
    }
  }


  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void queriesUseCanonicalIdentityUnderASymlinkedDataDirectory() throws Exception {
    final Path real = Files.createDirectory(directory.resolve("real"));
    Files.move(directory.resolve("db"), real.resolve("db"));
    final Path link = directory.resolve("link");
    Files.createSymbolicLink(link, real);
    final Path root = link.resolve(".");
    final var h = handlers(root, false, Map.of("db", List.of("public")), 100, 4096);
    final var current = h.query(null, request(Map.of("query", DOC + ".name")));
    assertThat(current.isError()).as(text(current)).isFalse();
    assertThat(text(current)).contains("Alice");
    assertThat(h.query(null, request(Map.of("query", "sdb:commit(" + DOC + ")"))).isError()).isTrue();
    final var historical = h.query(null, request(Map.of("query", DOC + ".name", "revision", 1)));
    assertThat(historical.isError()).as(text(historical)).isFalse();
    assertThat(text(historical)).contains("Alice");
    assertThat(h.query(null, request(Map.of("query", "jn:doc('db','secret')"))).isError()).isTrue();
    final var json = h.createResource(null, request(Map.of("database", "newdb", "resource", "res", "data", "{}")));
    assertThat(json.isError()).as(text(json)).isFalse();
    assertThat(Databases.existsDatabase(real.resolve("newdb"))).isTrue();
    final var xml = h.createResource(null, request(Map.of("database", "xml", "resource", "res", "format", "xml", "data", "<a/>")));
    assertThat(xml.isError()).as(text(xml)).isFalse();
    assertThat(Databases.existsDatabase(real.resolve("xml"))).isTrue();
    Files.createSymbolicLink(real.resolve("escape"), directory.resolve("db"));
    final var outside = h.createResource(null, request(Map.of("database", "escape", "resource", "res", "data", "{}")));
    assertThat(outside.isError()).isTrue();
    assertThat(Files.exists(directory.resolve("db"))).isFalse();
    Files.createSymbolicLink(real.resolve("loop-a"), Path.of("loop-b"));
    Files.createSymbolicLink(real.resolve("loop-b"), Path.of("loop-a"));
    final var loop = h.createResource(null, request(Map.of("database", "loop-a", "resource", "res", "data", "{}")));
    assertThat(loop.isError()).isTrue();
    assertThat(Files.isSymbolicLink(real.resolve("loop-a"))).isTrue();
    assertThat(Files.isSymbolicLink(real.resolve("loop-b"))).isTrue();
  }

  private static Map<Path, byte[]> resourceFiles(final Path database) throws IOException {
    final var contents = new LinkedHashMap<Path, byte[]>();
    final Path resources = database.resolve("resources");
    try (final var paths = Files.walk(resources)) {
      for (final Path path : paths.filter(Files::isRegularFile).toList()) {
        contents.put(resources.relativize(path), Files.readAllBytes(path));
      }
    }
    return contents;
  }

  private static void assertResourceFiles(final Path database, final Map<Path, byte[]> expected) throws IOException {
    final var actual = resourceFiles(database);
    assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
    for (final var file : expected.entrySet()) {
      assertThat(actual.get(file.getKey())).as(file.getKey().toString()).isEqualTo(file.getValue());
    }
  }
}
