package io.sirix.index.changelog;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.changelog.ChangeLogDirectory.Grouping;
import io.sirix.index.changelog.ChangeLogSlab.Entry;
import io.sirix.index.changelog.ChangeLogSlab.Row;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class ChangeLogGroupReaderTest {
  private static final byte[] KINDS = {ChangeLogSlab.LONG, ChangeLogSlab.STRING};
  private static final List<Grouping> GROUPINGS = List.of(new Grouping(7, List.of(0)));

  @TempDir
  File directory;

  @Test
  void readsOneGroupAndBothSidesOfMovesWithoutReadingUnrelatedPayloads() {
    final List<Entry> entries = new ArrayList<>();
    for (int record = 0; record < 600; record++) {
      entries.add(new Entry(record, new long[] {3}, row(record % 30, "old"), row(record % 30, "new")));
    }
    entries.add(new Entry(600, new long[] {1}, row(0, "moved"), row(40, "moved")));
    entries.add(new Entry(601, new long[] {3}, row(0, "removed"), null));
    entries.add(new Entry(602, new long[] {3}, null, row(41, "inserted")));
    final ChangeLogSlab slab = new ChangeLogSlab(0, 1, 0, KINDS, entries, GROUPINGS);
    withSlab(slab, source -> {
      assertArrayEquals(slab.encode(), ChangeLogStorage.read(source, 0, 1).encode());
      try (var reader = new ChangeLogGroupReader(source, 0, 1, new ChangeLogGroupReader.Budget())) {
        final ChangeLogDirectory.Groups groups = Objects.requireNonNull(reader.groups(7));
        assertTrue(groups.supported());
        final List<Entry> selected = new ArrayList<>();
        final long before = ChangeLogStorage.chunksRead();
        reader.visit(groups, key(0), selected::add);
        assertEquals(22, selected.size());
        assertTrue(ChangeLogStorage.chunksRead() - before <= 2);
        assertTrue(selected.stream().allMatch(entry -> Objects.requireNonNull(entry.before()).numbers[0] == 0));
        selected.clear();
        reader.visit(groups, key(40), selected::add);
        assertEquals(List.of(600L), selected.stream().map(Entry::record).toList());
        selected.clear();
        reader.visit(groups, key(41), selected::add);
        assertEquals(List.of(602L), selected.stream().map(Entry::record).toList());
        final long cached = ChangeLogStorage.chunksRead();
        reader.visit(groups, key(0), ignored -> {
        });
        reader.visit(groups, key(99), ignored -> {
        });
        assertEquals(cached, ChangeLogStorage.chunksRead(), "Cached and missing groups read no payload chunks");
      }
    });
  }

  @Test
  void oversizedGroupDeclinesBeforeReadingItsPayload() {
    final List<Entry> entries = new ArrayList<>();
    for (int record = 0; record < 1000; record++) {
      entries.add(new Entry(record, new long[] {3}, row(1, "old"), row(1, "new")));
    }
    withSlab(new ChangeLogSlab(0, 1, 0, KINDS, entries, GROUPINGS), source -> {
      final long before = ChangeLogStorage.chunksRead();
      try (var reader = new ChangeLogGroupReader(source, 0, 1, new ChangeLogGroupReader.Budget())) {
        assertThrows(ChangeLogGroupReader.LimitExceeded.class,
            () -> reader.visit(Objects.requireNonNull(reader.groups(7)), key(1), ignored -> {
              throw new AssertionError("No partial fold may escape a declined group");
            }));
      }
      assertEquals(before, ChangeLogStorage.chunksRead());
    });
  }

  @Test
  void externalMetadataIsBoundedBeforeAllocatingTheDictionary() {
    final List<Entry> entries = new ArrayList<>();
    for (int record = 0; record < 1000; record++) {
      entries.add(new Entry(record, new long[] {3}, null, row(record, record + "x".repeat(100))));
    }
    withSlab(new ChangeLogSlab(0, 1, 0, KINDS, entries, GROUPINGS), source -> {
      final long before = ChangeLogStorage.chunksRead();
      assertThrows(ChangeLogGroupReader.LimitExceeded.class,
          () -> new ChangeLogGroupReader(source, 0, 1, new ChangeLogGroupReader.Budget()));
      assertEquals(1, ChangeLogStorage.chunksRead() - before, "Only the first section-offset chunk is needed");
    });
  }

  @Test
  void externalDirectoryCannotExceedTheColdGroupChunkBound() {
    final List<Entry> entries = new ArrayList<>();
    for (int record = 0; record < 300; record++) {
      entries.add(new Entry(record, new long[] {3}, null, row(record, "value")));
    }
    withSlab(new ChangeLogSlab(0, 1, 0, KINDS, entries, GROUPINGS), source -> {
      final long before = ChangeLogStorage.chunksRead();
      assertThrows(ChangeLogGroupReader.LimitExceeded.class,
          () -> new ChangeLogGroupReader(source, 0, 1, new ChangeLogGroupReader.Budget()));
      assertEquals(1, ChangeLogStorage.chunksRead() - before,
          "Decline from section offsets before reading an oversized external directory");
    });
  }

  @Test
  void externalMetadataAndPayloadShareTheTwoChunkBound() {
    final String value = "x".repeat(2100);
    withSlab(new ChangeLogSlab(0, 1, 0, KINDS, List.of(new Entry(0, new long[] {3}, null, row(1, value))), GROUPINGS),
        source -> {
          final long before = ChangeLogStorage.chunksRead();
          try (var reader = new ChangeLogGroupReader(source, 0, 1, new ChangeLogGroupReader.Budget())) {
            final List<Entry> selected = new ArrayList<>();
            reader.visit(Objects.requireNonNull(reader.groups(7)), key(1), selected::add);
            assertEquals(value, Objects.requireNonNull(selected.getFirst().after()).text[1]);
          }
          assertEquals(2, ChangeLogStorage.chunksRead() - before);
        });
  }

  @Test
  void sharedQueryEntryBudgetStopsFurtherReads() {
    final List<Entry> entries = new ArrayList<>();
    for (int record = 0; record < 30; record++) {
      entries.add(new Entry(record, new long[] {3}, null, row(record, "value")));
    }
    withSlab(new ChangeLogSlab(0, 1, 0, KINDS, entries, GROUPINGS), source -> {
      final ChangeLogGroupReader.Budget budget = new ChangeLogGroupReader.Budget();
      try (var reader = new ChangeLogGroupReader(source, 0, 1, budget)) {
        final ChangeLogDirectory.Groups groups = Objects.requireNonNull(reader.groups(7));
        final List<Entry> selected = new ArrayList<>();
        reader.visit(groups, key(29), selected::add);
        assertEquals(29, selected.getFirst().record());
        for (int read = 1; read < 16_384; read++) {
          reader.visit(groups, key(29), ignored -> {
          });
        }
        final long before = ChangeLogStorage.chunksRead();
        assertThrows(ChangeLogGroupReader.LimitExceeded.class, () -> reader.visit(groups, key(29), ignored -> {
        }));
        assertEquals(before, ChangeLogStorage.chunksRead());
      }
    });
  }

  private static Row row(final long group, final String text) {
    final Row row = new Row(true, 2);
    row.flags[0] = ChangeLogSlab.PRESENT;
    row.flags[1] = ChangeLogSlab.PRESENT;
    row.numbers[0] = group;
    row.text[1] = text;
    return row;
  }

  private static byte[] key(final long group) {
    return Objects.requireNonNull(ChangeLogGroupKey.encode(row(group, ""), KINDS, List.of(0)));
  }

  private void withSlab(final ChangeLogSlab slab, final Consumer<StorageEngineReader> test) {
    final var path = directory.toPath().resolve("log");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(
          ResourceConfiguration.newBuilder("records").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("records")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          try (ChangeLogStorage storage = new ChangeLogStorage(writer.getStorageEngineWriter(), 0)) {
            storage.initialize(1, slab.kinds());
            storage.write(slab);
          }
          writer.commit();
        }
        try (StorageEngineReader source = session.createStorageEngineReader()) {
          test.accept(source);
        }
      }
    }
  }
}
