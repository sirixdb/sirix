package io.sirix.index.interval;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class HotOrderedStoreRangeTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void normalizedChunksPreserveUnsignedChunkIdsAndHistoricalReferences(final VersioningType versioning) {
    final long[] refs = {0, 65535, 65536, (1L << 47) + 3, (1L << 48) - 1};
    final IntervalDomain domain = new IntervalDomain(8, 0);
    final Path path = directory.resolve("chunks");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path)) {
      db.createResource(ResourceConfiguration.newBuilder("rows").versioningApproach(versioning).build());
      try (JsonResourceSession session = db.beginResourceSession("rows"); JsonNodeTrx wtx = session.beginNodeTrx()) {
        final RelationalIntervalTree tree =
            ValidTimeIntervalIndexFactory.createWriterTree(wtx.getStorageEngineWriter(), 0, domain);
        for (final long ref : refs) {
          tree.insert(ref, 10, 20);
        }
        tree.insert(42, 30, 40);
        assertRange(tree, refs);
        wtx.commit();
        final RelationalIntervalTree changed =
            ValidTimeIntervalIndexFactory.createWriterTree(wtx.getStorageEngineWriter(), 0, domain);
        changed.delete(refs[3], 10, 20);
        wtx.commit();
      }
    }
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path);
        JsonResourceSession session = db.beginResourceSession("rows")) {
      try (var reader = session.beginNodeReadOnlyTrx(1)) {
        assertRange(ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), 0, domain), refs);
      }
      try (var reader = session.beginNodeReadOnlyTrx(2)) {
        final LongOpenHashSet actual = new LongOpenHashSet();
        ValidTimeIntervalIndexFactory.createReaderTree(reader.getStorageEngineReader(), 0, domain)
                                     .intersect(15, 25, actual::add);
        final LongOpenHashSet expected = new LongOpenHashSet(refs);
        expected.remove(refs[3]);
        assertEquals(expected, actual);
      }
    }
  }

  private static void assertRange(final RelationalIntervalTree tree, final long[] refs) {
    final LongArrayList actual = new LongArrayList();
    tree.intersect(15, 25, actual::add);
    assertEquals(new LongOpenHashSet(refs), new LongOpenHashSet(actual));
    assertEquals(refs.length, actual.size(), "each registration is streamed once");
    actual.clear();
    tree.stabHalfOpen(15, actual::add);
    assertEquals(new LongOpenHashSet(refs), new LongOpenHashSet(actual));
    actual.clear();
    tree.endingAt(20, actual::add);
    assertEquals(new LongOpenHashSet(refs), new LongOpenHashSet(actual));
  }
}
