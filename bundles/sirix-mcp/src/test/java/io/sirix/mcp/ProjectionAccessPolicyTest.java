package io.sirix.mcp;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.sirix.access.ResourceAccessScope;
import io.sirix.access.Databases;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated
class ProjectionAccessPolicyTest {
  private static final String REVISION_TWO = "sum(for $r in jn:doc('db','data',2)[] return $r.n)";
  private static final String REVISION_THREE = "sum(for $r in jn:doc('db','data',3)[] return $r.n)";
  private static final String LATEST = "sum(for $r in jn:doc('db','data')[] return $r.n)";
  @TempDir
  Path directory;

  @BeforeEach
  void createRevisionedProjection() {
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var ctx = SirixQueryContext.createWithJsonStore(store)) {
      store.create("db", "data", "[{\"n\":1},{\"n\":2},{\"n\":3}]");
      new Query(chain, """
          let $doc := jn:doc('db','data')
          let $index := jn:create-projection-index($doc, '/[]', ('/[]/n'), ('long'))
          return sdb:commit($doc)
          """).evaluate(ctx);
    }
    try (final var db = Databases.openJsonDatabase(directory.resolve("db"));
        final var session = db.beginResourceSession("data");
        final var writer = session.beginNodeTrx()) {
      assertThat(session.getMostRecentRevisionNumber()).isEqualTo(2);
      writer.moveToDocumentRoot();
      assertThat(writer.moveToFirstChild()).isTrue();
      assertThat(writer.moveToFirstChild()).isTrue();
      assertThat(writer.moveToFirstChild()).isTrue();
      writer.setNumberValue(10);
      writer.commit();
      assertThat(session.getMostRecentRevisionNumber()).isEqualTo(3);
    }
    clearProjectionCaches();
  }

  @AfterEach
  void clearProjectionCaches() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
  }

  @Test
  void coldAndWarmProjectionsCannotEscapeTheSelectedRevisionInEitherMode() throws Exception {
    for (final boolean readOnly : new boolean[] {false, true}) {
      final var h = handlers(config(readOnly));
      for (final boolean warm : new boolean[] {false, true}) {
        clearProjectionCaches();
        if (warm) {
          final long served = SirixVectorizedExecutor.projectionAggregatesServed();
          assertAnswer(h, REVISION_TWO, null, "6");
          assertAnswer(h, REVISION_TWO, null, "6");
          assertThat(SirixVectorizedExecutor.projectionAggregatesServed()).isGreaterThan(served);
          assertThat(ProjectionIndexCatalog.dataCacheSize()).isPositive();
        }
        assertDeniedWithoutServing(h, REVISION_TWO, 3);
        assertAnswer(h, REVISION_TWO, 2, "6");
        assertDeniedWithoutServing(h, REVISION_TWO, 3);
        assertDeniedWithoutServing(h, REVISION_THREE, 2);
        assertAnswer(h, REVISION_THREE, 3, "15");
        assertAnswer(h, LATEST, 2, "6");
        assertAnswer(h, LATEST, 3, "15");
        assertAnswer(h, LATEST, null, "15");
        assertAnswer(h, REVISION_TWO, null, "6");
        assertAnswer(h, REVISION_THREE, null, "15");
      }
    }
  }

  @Test
  void sessionBoundFixedAndLatestExecutorsReauthorizeBeforeCacheReuse() throws Exception {
    final var access = new AccessControl(config(true));
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store)) {
      final var session = store.lookup("db").getDatabase().beginResourceSession("data");
      try (final var fixed = SirixCompileChain.createWithNodeAndJsonStore(null, store, session, 2)) {
        assertThat(answer(new Query(fixed, REVISION_TWO), ctx)).isEqualTo("6");
        final var selectedThree = new QueryAccessPolicy(directory, access, "db", "data", 3);
        assertThatThrownBy(() -> ResourceAccessScope.where(
            selectedThree).call(() -> answer(new Query(fixed, REVISION_TWO), ctx))).isInstanceOf(
                AccessControl.AccessDeniedException.class);
        final var selectedTwo = new QueryAccessPolicy(directory, access, "db", "data", 2);
        assertThat(
            ResourceAccessScope.where(selectedTwo).call(() -> answer(new Query(fixed, REVISION_TWO), ctx))).isEqualTo(
                "6");
      }
    }
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var ctx = SirixQueryContext.createWithJsonStore(store)) {
      final var session = store.lookup("db").getDatabase().beginResourceSession("data");
      try (final var latest = SirixCompileChain.createWithJsonStore(store, session)) {
        assertThat(answer(new Query(latest, LATEST), ctx)).isEqualTo("15");
        for (final int revision : new int[] {2, 3, 2, 3}) {
          final var selected = new QueryAccessPolicy(directory, access, "db", "data", revision);
          assertThat(ResourceAccessScope.where(selected).call(() -> answer(new Query(latest, LATEST), ctx))).isEqualTo(
              revision == 2
                  ? "6"
                  : "15");
        }
        assertThat(answer(new Query(latest, LATEST), ctx)).isEqualTo("15");
      }
    }
  }

  @Test
  void reusedStoreExecutorsReauthorizePrimarySecondaryAndBoundDocumentSources() {
    final var access = new AccessControl(config(true));
    final var selectedThree = new QueryAccessPolicy(directory, access, "db", "data", 3);
    final var selectedTwo = new QueryAccessPolicy(directory, access, "db", "data", 2);
    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build();
        final var chain = SirixCompileChain.createWithJsonStore(store);
        final var ctx = SirixQueryContext.createWithJsonStore(store)) {
      final Query primary = new Query(chain, REVISION_TWO);
      assertThat(answer(primary, ctx)).isEqualTo("6");
      assertThatThrownBy(() -> ResourceAccessScope.where(selectedThree).call(() -> answer(primary, ctx))).isInstanceOf(
          AccessControl.AccessDeniedException.class);
      assertThat(ResourceAccessScope.where(selectedTwo).call(() -> answer(primary, ctx))).isEqualTo("6");
      final Query secondary = new Query(chain, "(" + REVISION_THREE + "," + REVISION_TWO + ")");
      assertThat(answer(secondary, ctx)).isEqualTo("15 6");
      assertThatThrownBy(
          () -> ResourceAccessScope.where(selectedThree).call(() -> answer(secondary, ctx))).isInstanceOf(
              AccessControl.AccessDeniedException.class);
      ctx.bind(new QNm("doc"), store.lookup("db").getDocument("data", 2));
      final Query bound = new Query(chain, "declare variable $doc external; sum(for $r in $doc[] return $r.n)");
      assertThat(answer(bound, ctx)).isEqualTo("6");
      assertThatThrownBy(() -> ResourceAccessScope.where(selectedThree).call(() -> answer(bound, ctx))).isInstanceOf(
          AccessControl.AccessDeniedException.class);
      assertThat(ResourceAccessScope.where(selectedTwo).call(() -> answer(bound, ctx))).isEqualTo("6");
    }
  }

  private static void assertDeniedWithoutServing(final ToolHandlers h, final String query, final int revision) {
    final long served = SirixVectorizedExecutor.projectionAggregatesServed();
    final long cached = ProjectionIndexCatalog.dataCacheSize();
    final var result = h.query(null, request(query, revision));
    assertThat(result.isError()).as(text(result)).isTrue();
    final var content = (Map<?, ?>) result.structuredContent();
    assertThat(content).isNotNull();
    assertThat(content.get("code")).isEqualTo("access_denied");
    assertThat(content.get("message").toString()).contains("outside its historical context");
    assertThat(content.containsKey("items")).isFalse();
    assertThat(content.containsKey("revision")).isFalse();
    assertThat(SirixVectorizedExecutor.projectionAggregatesServed()).isEqualTo(served);
    assertThat(ProjectionIndexCatalog.dataCacheSize()).isEqualTo(cached);
  }

  private static void assertAnswer(final ToolHandlers h, final String query, final Integer revision,
      final String expected) {
    final var result = h.query(null, request(query, revision));
    assertThat(result.isError()).as(text(result)).isFalse();
    final var content = (Map<?, ?>) result.structuredContent();
    assertThat(content.get("items")).isEqualTo(List.of(expected));
    if (revision == null)
      assertThat(content.containsKey("revision")).isFalse();
    else
      assertThat(content.get("revision")).isEqualTo(revision);
  }

  private McpServerConfig config(final boolean readOnly) {
    return new McpServerConfig("test", "1", "stdio", directory.toString(), readOnly, List.of(), List.of(), Map.of(),
        100, 4096, false, false, false, null);
  }

  private ToolHandlers handlers(final McpServerConfig config) throws Exception {
    final var access = new AccessControl(config);
    return new ToolHandlers(config, access, new OutputSanitizer(config), new SnapshotRegistry(access),
        AuditLog.create(config));
  }

  private static CallToolRequest request(final String query, final Integer revision) {
    final var args = new LinkedHashMap<String, Object>();
    args.put("database", "db");
    args.put("query", query);
    if (revision != null) {
      args.put("resource", "data");
      args.put("revision", revision);
    }
    return new CallToolRequest("test", args);
  }

  private static String text(final CallToolResult result) {
    return ((TextContent) result.content().getFirst()).text();
  }

  private static String answer(final Query query, final SirixQueryContext ctx) {
    final var buffer = new StringWriter();
    try (final var out = new PrintWriter(buffer)) {
      query.serialize(ctx, out);
    }
    return buffer.toString();
  }
}
