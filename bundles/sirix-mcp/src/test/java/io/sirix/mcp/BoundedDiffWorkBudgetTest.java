package io.sirix.mcp;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@Isolated
class BoundedDiffWorkBudgetTest {
  private static final int LENGTH = 2048;
  private static final int CHANGES = 20;
  private static final int PAGE_SIZE = 10;
  private static final JsonMapper MAPPER = JsonMapper.builder().build();

  @TempDir
  Path directory;

  @Test
  void initialAndContinuationPagesShareRevisionReadersAndArrayPositions() throws Exception {
    final Path path = directory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (final var database = Databases.openJsonDatabase(path)) {
      database.createResource(
          ResourceConfiguration.newBuilder("array").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (final var session = database.beginResourceSession("array")) {
        final StringBuilder array = new StringBuilder(LENGTH * 5).append('[');
        for (int index = 0; index < LENGTH; index++) {
          if (index != 0)
            array.append(',');
          array.append(index);
        }
        array.append(']');
        try (final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(array.toString()), JsonNodeTrx.Commit.NO);
          writer.commit();
          assertThat(writer.moveToDocumentRoot()).isTrue();
          assertThat(writer.moveToFirstChild()).isTrue();
          assertThat(writer.moveToFirstChild()).isTrue();
          for (int index = 0; index < LENGTH; index++) {
            if (index >= LENGTH - CHANGES)
              writer.setNumberValue(-index);
            if (index + 1 < LENGTH)
              assertThat(writer.moveToRightSibling()).isTrue();
          }
          writer.commit();
        }
        final var work = new ReaderWork();
        final JsonResourceSession counted = work.session(session);
        final var config = new McpServerConfig("test", "1", "stdio", directory.toString(), true, List.of(), List.of(),
            Map.of(), PAGE_SIZE, 4096, false, false, false, null);
        for (final int offset : new int[] {0, PAGE_SIZE}) {
          final var page = WorkCapture.of(work.siblings, work.readers, work.closedReaders, work.summaries)
                                      .call(() -> BoundedDiff.generate("db", counted, 1, 2, offset, config));
          final var document = MAPPER.readTree(MAPPER.writeValueAsString(page.result()));
          assertThat(document.get("diffs").size()).isEqualTo(PAGE_SIZE);
          assertThat(document.get("truncated").asBoolean()).isEqualTo(offset == 0);
          if (offset == 0)
            assertThat(document.get("continuation").get("offset").asInt()).isEqualTo(PAGE_SIZE);
          for (int index = 0; index < PAGE_SIZE; index++) {
            final int ordinal = LENGTH - CHANGES + offset + index;
            final var update = document.get("diffs").get(index).get("update");
            assertThat(update.get("path").asText()).isEqualTo("/[" + ordinal + "]");
            assertThat(update.get("type").asText()).isEqualTo("number");
            assertThat(update.get("value").asInt()).isEqualTo(-ordinal);
          }
          page.work()
              .assertBetween(work.siblings, LENGTH - CHANGES, 3L * LENGTH + 4L * CHANGES,
                  "each operation rewalking the same array prefix")
              .assertExactly(work.readers, 4, "opening revision readers per operation rather than per page")
              .assertExactly(work.closedReaders, 4, "leaking a page's revision readers")
              .assertExactly(work.summaries, 1, "reopening the new revision path summary for every operation");
        }
      }
    }
  }

  private static final class ReaderWork {
    private long siblingMoves;
    private long readerOpens;
    private long readerCloses;
    private long summaryOpens;
    private final WorkCounter siblings = WorkCounter.alwaysOn("boundedDiffSiblingMoves",
        "one sibling cursor move while generating a bounded diff page", () -> siblingMoves);
    private final WorkCounter readers = WorkCounter.alwaysOn("boundedDiffReaderOpens",
        "one revision reader opened for a bounded diff page", () -> readerOpens);
    private final WorkCounter closedReaders = WorkCounter.alwaysOn("boundedDiffReaderCloses",
        "one revision reader closed after a bounded diff page", () -> readerCloses);
    private final WorkCounter summaries = WorkCounter.alwaysOn("boundedDiffPathSummaryOpens",
        "one path summary opened for a bounded diff page", () -> summaryOpens);

    private JsonResourceSession session(final JsonResourceSession delegate) {
      return (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
          new Class<?>[] {JsonResourceSession.class}, (proxy, method, arguments) -> {
            final Object result = invoke(delegate, method, arguments);
            if (method.getName().equals("openPathSummary"))
              summaryOpens++;
            if (result instanceof JsonNodeReadOnlyTrx reader) {
              readerOpens++;
              return transaction(reader);
            }
            return result;
          });
    }

    private JsonNodeReadOnlyTrx transaction(final JsonNodeReadOnlyTrx delegate) {
      return (JsonNodeReadOnlyTrx) Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
          new Class<?>[] {JsonNodeReadOnlyTrx.class}, (proxy, method, arguments) -> {
            if (method.getName().equals("moveToLeftSibling") || method.getName().equals("moveToRightSibling"))
              siblingMoves++;
            if (method.getName().equals("close"))
              readerCloses++;
            return invoke(delegate, method, arguments);
          });
    }
  }

  private static Object invoke(final Object delegate, final Method method, final Object[] arguments) throws Throwable {
    try {
      return method.invoke(delegate, arguments);
    } catch (final InvocationTargetException exception) {
      throw exception.getCause();
    }
  }
}
