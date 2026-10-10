package io.sirix.budget;

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTKeySerializer;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.index.hot.PostingDeltas;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Posting fragments must fill missing slots with one search, preserving newer tombstones. */
@Isolated
final class HOTPostingFragmentMergeWorkBudgetTest {
  private static final int OLDER_KEYS = 500;
  private static final WorkCounter SUFFIX_READS = EngineWorkCounters.HOT_SUFFIX_PROBE_READS;
  private static final WorkCapture CAPTURE = WorkCapture.of(SUFFIX_READS);

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void postingFragmentsUseOneSearchPerOlderKey(final VersioningType versioning) throws Exception {
    for (final IndexType type : List.of(IndexType.CAS, IndexType.VALIDTIME)) {
      final byte[][] keys = new byte[OLDER_KEYS][];
      final byte[][] values = new byte[OLDER_KEYS][];
      final byte[] tombstone = NodeReferencesSerializer.serialize(new NodeReferences());
      try (HOTLeafPage newest = new HOTLeafPage(17, 2, type); HOTLeafPage older = new HOTLeafPage(17, 1, type)) {
        // Its first fork byte differs from every older key: a search inspects one native word
        // per step, regardless of the remaining posting key and delta suffix bytes.
        assertTrue(newest.put(key(Long.MIN_VALUE), tombstone));
        assertTrue(newest.put(key(OLDER_KEYS / 2), tombstone));
        for (int i = 0; i < OLDER_KEYS; i++) {
          keys[i] = key(i);
          values[i] = i % 3 == 0
              ? tombstone
              : NodeReferencesSerializer.serialize(NodeReferences.copyOfSortedUnchecked(new long[] {i}));
          assertTrue(older.put(keys[i], values[i]));
        }
        older.setCompleteDump(true);
        final WorkCapture.Captured<HOTLeafPage> capture = CAPTURE.call(
            () -> versioning.combineHOTLeafPages(List.of(newest, older), 3, mock(StorageEngineReader.class)));
        final HOTLeafPage merged = capture.result();
        try {
          assertEquals(versioning == VersioningType.FULL
              ? 2
              : OLDER_KEYS + 1, merged.getEntryCount());
          assertArrayEquals(tombstone, merged.getValue(merged.findEntry(key(OLDER_KEYS / 2))));
          if (versioning != VersioningType.FULL) {
            for (int i = 0; i < OLDER_KEYS; i++) {
              assertArrayEquals(i == OLDER_KEYS / 2
                  ? tombstone
                  : values[i], merged.getValue(merged.findEntry(keys[i])));
            }
            // At most ten steps for each of 500 older keys; a second search per fill exceeds
            // this bound. The capture excludes fixture construction and result verification.
            capture.work()
                   .assertBetween(SUFFIX_READS, 1, 5_000,
                       "CAS/VALIDTIME reconstruction must not search again to insert an absent key");
          } else {
            capture.work().assertZero(SUFFIX_READS, "FULL reads only the newest fragment");
          }
        } finally {
          if (merged != newest) {
            merged.close();
          }
        }
      }
    }
  }

  private static byte[] key(final long fork) {
    final byte[] composite = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(new ValidTimeKey(ValidTimeKey.STORE_LOWER, fork, 7), 0,
        composite, 0);
    final byte[] delta = Arrays.copyOf(composite, composite.length + PostingDeltas.SUFFIX_BYTES);
    HOTKeySerializer.writeChunkIdxBE(delta, composite.length, PostingDeltas.suffix(0, false));
    return delta;
  }
}
