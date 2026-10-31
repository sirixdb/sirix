package io.sirix.mcp;

import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.io.StorageType;
import io.sirix.query.json.AbstractJsonDBArray;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.JsonDBCollectionImpl;
import io.sirix.service.json.shredder.JsonShredder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import static org.assertj.core.api.Assertions.assertThat;

@Isolated
class BoundedQueryResultWorkBudgetTest {
  private static final int CHARACTER_LIMIT = 64;

  @TempDir
  Path directory;

  @Test
  void oversizedStoredArraysStopWithinTheCharacterBudgetWithoutMaterializingMembers() throws Exception {
    final Path path = directory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final var config = new McpServerConfig("test", "1", "stdio", directory.toString(), true, List.of(), List.of(),
        Map.of(), 2, CHARACTER_LIMIT, false, false, false, null);
    try (final var database = Databases.openJsonDatabase(path)) {
      final var collection = new JsonDBCollectionImpl("db", database);
      for (final int length : new int[] {256, 4096}) {
        final String resource = "array" + length;
        database.createResource(
            ResourceConfiguration.newBuilder(resource).storageType(StorageType.FILE_CHANNEL).build());
        try (final var session = database.beginResourceSession(resource)) {
          final StringBuilder json = new StringBuilder(length * 5).append('[');
          for (int index = 0; index < length; index++) {
            if (index != 0)
              json.append(',');
            json.append(index);
          }
          json.append(']');
          try (final var writer = session.beginNodeTrx()) {
            writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
            writer.commit();
          }
          for (final boolean multiple : new boolean[] {false, true}) {
            try (final var reader = session.beginNodeReadOnlyTrx()) {
              reader.moveToFirstChild();
              final var work = new ReaderWork();
              final var array = new JsonDBArray(work.reader(reader), collection);
              final Sequence sequence = multiple
                  ? new ItemSequence(new Int32(7), array, new Int32(9))
                  : array;
              final var page =
                  WorkCapture.of(work.values, work.siblings, work.materializations)
                             .call(
                                 () -> BoundedQueryResult.serialize(sequence, 0, config, new OutputSanitizer(config)));
              assertThat(page.result()).containsEntry("items", multiple
                  ? List.of("7")
                  : List.of()).containsEntry("truncated", true);
              final var continuation = (Map<?, ?>) page.result().get("continuation");
              assertThat(continuation.get("offset")).isEqualTo(multiple
                  ? 1
                  : 0);
              assertThat(continuation.get("hint").toString()).contains("Project a smaller value");
              page.work()
                  .assertBetween(work.values, 1, CHARACTER_LIMIT + 1,
                      "reading array members beyond the bounded serialization prefix")
                  .assertBetween(work.siblings, 1, 2L * CHARACTER_LIMIT + 2,
                      "walking the whole array before applying the serialization limit")
                  .assertExactly(work.materializations, 0, "building the array's complete member list");
              final var skipped =
                  WorkCapture.of(work.values, work.siblings, work.materializations)
                             .call(() -> BoundedQueryResult.serialize(array, 1, config, new OutputSanitizer(config)));
              assertThat(skipped.result()).containsEntry("items", List.of()).containsEntry("truncated", false);
              skipped.work()
                     .assertExactly(work.values, 0, "serializing an array skipped by the item offset")
                     .assertExactly(work.siblings, 0, "walking an array skipped by the item offset")
                     .assertExactly(work.materializations, 0, "materializing an array skipped by the item offset");
            }
          }
          try (final var reader = session.beginNodeReadOnlyTrx()) {
            reader.moveToFirstChild();
            final var work = new ReaderWork();
            final var array = new JsonDBArray(work.reader(reader), collection);
            final var eager = WorkCapture.of(work.values, work.materializations).call(array::values);
            assertThat(eager.result()).hasSize(length);
            eager.work()
                 .assertExactly(work.values, length, "the positive control must read every numeric member")
                 .assertExactly(work.materializations, 1, "the positive control must build a complete member list");
          }
        }
      }
    }
  }

  private static final class ReaderWork {
    private long valueReads;
    private long siblingMoves;
    private final WorkCounter values = WorkCounter.alwaysOn("boundedQueryArrayValueReads",
        "one stored numeric member read while rendering a bounded query value", () -> valueReads);
    private final WorkCounter siblings = WorkCounter.alwaysOn("boundedQueryArraySiblingMoves",
        "one sibling cursor move while rendering a bounded query value", () -> siblingMoves);
    private final WorkCounter materializations = WorkCounter.alwaysOn("boundedQueryArrayMaterializations",
        "one complete array member list allocation", AbstractJsonDBArray::materializations);

    private JsonNodeReadOnlyTrx reader(final JsonNodeReadOnlyTrx delegate) {
      return (JsonNodeReadOnlyTrx) Proxy.newProxyInstance(JsonNodeReadOnlyTrx.class.getClassLoader(),
          new Class<?>[] {JsonNodeReadOnlyTrx.class}, (proxy, method, arguments) -> {
            if (method.getName().equals("getNumberValue"))
              valueReads++;
            if (method.getName().equals("moveToLeftSibling") || method.getName().equals("moveToRightSibling"))
              siblingMoves++;
            try {
              return method.invoke(delegate, arguments);
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          });
    }
  }
}
