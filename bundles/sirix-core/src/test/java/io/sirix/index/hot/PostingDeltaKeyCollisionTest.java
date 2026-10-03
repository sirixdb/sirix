package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Delta slots must not alias a longer logical CAS key's ordinary high-numbered chunk. */
final class PostingDeltaKeyCollisionTest {
  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void deltaAndLongerCasKeyStayDistinct(final VersioningType versioningType) {
    final CASValue shortKey = new CASValue(new Str("hot"), Type.STR, 1);
    final CASValue longKey = new CASValue(new Str("hot\0\0\0\0"), Type.STR, 1);
    final long highNodeKey = (1L << 47) | 4000;
    final Path path = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(
          ResourceConfiguration.newBuilder("resource").versioningApproach(versioningType).storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx trx = session.beginNodeTrx()) {
        final HOTIndexWriter<CASValue> writer =
            HOTIndexWriter.create(trx.getStorageEngineWriter(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
        final HOTBulkIndexLoader<CASValue> loader = writer.createBulkLoader();
        for (int i = 0; i < 400; i++) {
          loader.add(shortKey, i * 2L);
        }
        loader.flush();
        final long writes = HOTIndexWriter.postingDeltaWrites();
        writer.indexNodeKey(shortKey, 2000);
        assertEquals(writes + 1, HOTIndexWriter.postingDeltaWrites());
        writer.indexNodeKey(longKey, highNodeKey);
        trx.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource");
        JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      final HOTIndexReader<CASValue> reader =
          HOTIndexReader.create(trx.getStorageEngineReader(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
      assertArrayEquals(new long[] {highNodeKey}, reader.get(longKey, SearchMode.EQUAL).toSortedArray());
      assertEquals(401, reader.get(shortKey, SearchMode.EQUAL).getNodeKeys().getLongCardinality());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void nulExtendedRangesKeepAllFortyEightNodeKeyBits(final VersioningType versioningType) {
    final String[] values = {"", "hot", "hot\0", "hot\0\0\0\0", "hot\0\0\0\0\0", "hot\1", "hotx"};
    final long[] boundaries = {(1L << 47) - 1, 1L << 47, (1L << 47) + 1, (1L << 48) - 1};
    final List<long[]> snapshots = new ArrayList<>();
    final TreeSet<Long> expected = new TreeSet<>();
    final Path path = temporaryDirectory.resolve("ranges");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= 3; revision++) {
          try (JsonNodeTrx trx = session.beginNodeTrx()) {
            final HOTIndexWriter<CASValue> writer =
                HOTIndexWriter.create(trx.getStorageEngineWriter(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
            if (revision == 1) {
              final HOTBulkIndexLoader<CASValue> loader = writer.createBulkLoader();
              for (final String value : values) {
                final CASValue key = key(value);
                for (int i = 0; i < 400; i++) {
                  loader.add(key, 2L * i);
                  expected.add(2L * i);
                }
                for (final long nodeKey : boundaries) {
                  loader.add(key, nodeKey);
                  expected.add(nodeKey);
                }
              }
              loader.flush();
            } else {
              for (final String value : values) {
                final CASValue key = key(value);
                for (int i = 0; i < 150; i++) {
                  if (revision == 2) {
                    writer.indexNodeKey(key, 1000L + i);
                  } else {
                    assertTrue(writer.remove(key, 1000L + i));
                  }
                }
              }
              for (int i = 0; i < 150; i++) {
                if (revision == 2) {
                  expected.add(1000L + i);
                } else {
                  expected.remove(1000L + i);
                }
              }
            }
            final long[] keys = expected.stream().mapToLong(Long::longValue).toArray();
            for (final String value : values) {
              assertArrayEquals(keys, writer.get(key(value), SearchMode.EQUAL).toSortedArray());
            }
            snapshots.add(keys);
            trx.commit();
          }
          assertRanges(session, revision, values, snapshots.get(revision - 1));
        }
        for (int revision = 1; revision <= snapshots.size(); revision++) {
          assertRanges(session, revision, values, snapshots.get(revision - 1));
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= snapshots.size(); revision++) {
        assertRanges(session, revision, values, snapshots.get(revision - 1));
      }
    }
  }

  private static CASValue key(final String value) {
    return new CASValue(new Str(value), Type.STR, 1);
  }

  private static void assertRanges(final JsonResourceSession session, final int revision, final String[] values,
      final long[] expected) {
    try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
      final HOTIndexReader<CASValue> reader =
          HOTIndexReader.create(trx.getStorageEngineReader(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
      assertEntries(reader.iterator(), values, 0, values.length, expected);
      for (int i = 0; i < values.length; i++) {
        final CASValue key = key(values[i]);
        assertArrayEquals(expected, reader.get(key, SearchMode.EQUAL).toSortedArray());
        assertEntries(reader.iteratorFrom(key, true), values, i, values.length, expected);
        assertEntries(reader.iteratorFrom(key, false), values, i + 1, values.length, expected);
        assertEntries(reader.iteratorTo(key, true), values, 0, i + 1, expected);
        assertEntries(reader.iteratorTo(key, false), values, 0, i, expected);
        assertEntries(reader.range(key, key), values, i, i + 1, expected);
      }
    }
  }

  private static void assertEntries(final Iterator<Map.Entry<CASValue, NodeReferences>> entries, final String[] values,
      final int from, final int to, final long[] expected) {
    int i = from;
    while (entries.hasNext()) {
      assertTrue(i < to, "unexpected extra logical group");
      final Map.Entry<CASValue, NodeReferences> entry = entries.next();
      assertEquals(values[i++], entry.getKey().getAtomicValue().stringValue());
      assertArrayEquals(expected, entry.getValue().toSortedArray());
    }
    assertEquals(to, i, "missing logical groups");
  }

}
