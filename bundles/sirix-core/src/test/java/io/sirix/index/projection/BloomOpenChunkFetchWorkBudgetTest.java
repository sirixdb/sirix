/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budget for the open Bloom chunk's fetch: pruning a column whose open-chunk tails are
 * REFERENCED costs one ranged fetch, which is one read transaction, however many tails the chunk
 * holds.
 *
 * <p>
 * A sealed chunk is one contiguous block, so its payload arrives in one ranged fetch. The open
 * chunk is one blob per row group instead, and paginating those by the block window
 * ({@code FETCH_WINDOW_CHUNKS}, default 16) made one chunk cost {@code ceil(tails / 16)} fetches —
 * and {@link ProjectionIndexCatalog} opens a read transaction per
 * {@code fetchRange}, so that is a transaction open per window. Measured on this fixture's 100
 * referenced tails: 7 ranged fetches with the block window, 1 with the chunk-wide window. The same
 * pagination is what made the range holding the open chunk far heavier than its siblings when
 * {@link ProjectionColumnStore#applyBloomPruneMany} splits the walk.
 *
 * <p>
 * Every way of getting this wrong returns the same keep mask, so only the counters tell them apart.
 * The fetcher is the seam the prune already takes as an argument, so a counting decorator sees
 * every transaction the walk opens without any process-wide state. A decorator reads zero when the
 * route stops going through it, so the requested-offset count carries the floor: all 100 tails must
 * be referenced and asked for exactly once, and the mask must really have been pruned.
 */
@Isolated
final class BloomOpenChunkFetchWorkBudgetTest {

  private static final String RESOURCE = "resource";
  private static final int INDEX_NUMBER = 0;
  private static final byte[] COLUMN_KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  /**
   * Open-chunk row groups. Below {@link ProjectionBloomChunks#CHUNK_LEAVES}, so nothing is sealed and
   * the evidence spans exactly one chunk — which also keeps
   * {@link ProjectionColumnStore#applyBloomPruneMany} serial on every machine, since it only splits
   * from 32 chunks upwards.
   */
  private static final int OPEN_TAILS = 100;

  /**
   * Distinct strings per row group. At ~10 bits per value the fingerprint is a 8 192-bit filter, so
   * each tail is just over 1 KiB — past {@code INLINE_SEGMENT_MAX_BYTES} (512), which is what makes
   * the tails referenced side pages rather than inline bytes carried in the locator. Inline tails
   * need no fetch at all and would make this budget vacuous.
   */
  private static final int DISTINCT_VALUES = 500;

  /** One ranged fetch for the whole chunk, whatever the block window is set to. */
  private static final int MAX_RANGED_FETCHES = 1;

  @TempDir
  private Path directory;

  @BeforeEach
  void createResource() {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory)) {
      // Name the backend: a budget must not depend on which one a platform defaults to.
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build()));
    }
  }

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void anOpenChunkOfReferencedTailsIsFetchedInOneRangedFetch() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup fat = fatRowGroup();
    final byte[] fingerprint = bloomSegment(fat);
    assertTrue(fingerprint.length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES,
        "the fixture's tails must exceed the inline threshold or no fetch happens at all; got " + fingerprint.length
            + " bytes");
    final long rejected = hashRejectedBy(fingerprint);
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= OPEN_TAILS; rowGroupId++) {
          writer.append(fat, rowGroupId, storage);
        }
        writer.finishChunks(storage, OPEN_TAILS, COLUMN_KINDS);
        writer.publishManifests(storage, OPEN_TAILS);
        wtx.commit();
      }
      // Cold: a warm buffer cache answers whatever route the prune takes.
      Databases.clearGlobalCaches();
      ProjectionIndexCatalog.clearCache();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, OPEN_TAILS);
        assertNotNull(evidence, "the fixture must publish usable evidence");
        assertEquals(1, evidence[0].chunkCount(), "the fixture must span exactly the one open chunk");
        final CountingFetcher fetcher =
            new CountingFetcher(ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));
        final long[] keep = new long[(OPEN_TAILS + 63) >>> 6];
        Arrays.fill(keep, -1L);
        final int dropped = evidence[0].prune(rejected, keep, OPEN_TAILS, fetcher);

        assertEquals(OPEN_TAILS, dropped,
            "non-vacuity: every tail must have been read and must have rejected the absent hash");
        for (int leaf = 0; leaf < OPEN_TAILS; leaf++) {
          assertFalse((keep[leaf >>> 6] & 1L << (leaf & 63)) != 0, "leaf " + leaf + " must be pruned");
        }
        assertEquals(MAX_RANGED_FETCHES, fetcher.rangedFetches(),
            "the open chunk's tails must arrive in ONE ranged fetch, i.e. one read transaction");
        assertEquals(0, fetcher.wholeColumnFetches(),
            "the prune must stay on the ranged path and never fall back to a whole-column fetch");
        assertEquals(OPEN_TAILS, fetcher.offsetsRequested(),
            "every tail is a referenced side page and must be asked for exactly once");
      }
    } finally {
      writer.release();
    }
  }

  /**
   * Counts what the walk asks of its fetcher; owns no process-wide state and changes no behaviour.
   */
  private static final class CountingFetcher implements ProjectionColumnStore.ColumnSegmentFetcher {

    private final ProjectionColumnStore.ColumnSegmentFetcher delegate;
    private int rangedFetches;
    private int wholeColumnFetches;
    private int offsetsRequested;

    private CountingFetcher(final ProjectionColumnStore.ColumnSegmentFetcher delegate) {
      this.delegate = delegate;
    }

    private int rangedFetches() {
      return rangedFetches;
    }

    private int wholeColumnFetches() {
      return wholeColumnFetches;
    }

    /** Referenced payloads actually requested: the side-page reads the walk pays for. */
    private int offsetsRequested() {
      return offsetsRequested;
    }

    @Override
    public byte @Nullable [] @Nullable [] fetchAll(final long[] offsets) {
      wholeColumnFetches++;
      return delegate.fetchAll(offsets);
    }

    @Override
    public void fetchRange(final long[] offsets, final int from, final int to, final byte[][] out) {
      rangedFetches++;
      for (int i = from; i < to; i++) {
        if (offsets[i] != Constants.NULL_ID_LONG) {
          offsetsRequested++;
        }
      }
      delegate.fetchRange(offsets, from, to, out);
    }

    @Override
    public boolean rangedFetchIsConcurrent() {
      return delegate.rangedFetchIsConcurrent();
    }

    @Override
    public byte[] @Nullable [] fetchNumericProofs(final int indexNumber, final int column, final long[] slots) {
      return delegate.fetchNumericProofs(indexNumber, column, slots);
    }

    @Override
    public void fetchSlotRange(final int indexNumber, final long[] slotKeys, final int from, final int to,
        final byte[][] out) {
      delegate.fetchSlotRange(indexNumber, slotKeys, from, to, out);
    }
  }

  /** One row group whose per-leaf fingerprint is large enough to be stored as a referenced page. */
  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup fatRowGroup() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone());
    for (int row = 0; row < DISTINCT_VALUES; row++) {
      page.appendRow(row + 1L, new long[] {0L}, new boolean[] {false}, new String[] {"value-" + row},
          new boolean[] {true}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false});
    }
    return Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
  }

  private static byte[] bloomSegment(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    final int bloomId = ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(0);
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      if (encoded.columnSegmentIds()[i] == bloomId) {
        return encoded.segments()[i];
      }
    }
    throw new AssertionError("encoded string row group carries no Bloom segment");
  }

  private static long hashRejectedBy(final byte[] bloomSegment) {
    for (int i = 0; i < 100_000; i++) {
      final long hash = ProjectionIndexColumnSegmentCodec.bloomHash(("absent-" + i).getBytes(StandardCharsets.UTF_8));
      if (!ProjectionIndexColumnSegmentCodec.bloomMayContainHash(bloomSegment, hash)) {
        return hash;
      }
    }
    throw new AssertionError("implausible Bloom filter: admitted every absent probe");
  }
}
