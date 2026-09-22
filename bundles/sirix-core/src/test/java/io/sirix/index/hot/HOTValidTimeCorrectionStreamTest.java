/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stream of valid-time registrations that stopped a load of 100,000 records, replayed against
 * the index writer with the publications it was committed in.
 *
 * <p>
 * A first publication registers every record under one interval spanning the whole horizon. Each
 * further publication corrects a twentieth of the records: a span of a record's validity is
 * replaced or retracted, which shortens or removes the intervals it overlaps and registers what
 * remains. The stream is a fixed function of a seed, so it is identical on every run, and it is
 * replayed as the interval index issues it — one posting removed or added at a time, lower store
 * before upper — with a commit and a fresh writer per publication. That matters: the writer's
 * consolidation cadence restarts with each transaction, and replayed in a single one the same
 * registrations build a different trie.
 * </p>
 *
 * <p>
 * On the way the stream meets each structural case that needed a fix, and that it still does is
 * asserted through the counter incremented at the source of each: a fold at a bit the parent's mask
 * already holds whose upper half would land past a sibling (the first publication of corrections —
 * where the load used to stop), a complete-frontier join that has to split a side a bit of its
 * block cuts through, and the decomposition of a full node one of whose halves would break the trie
 * condition against its own child (the twentieth — published unseen, committed, and fatal three
 * publications later). {@code HOTStraddlingLeafSpliceTest} pins the first two with a few
 * registrations each; the third needs the trie this stream builds.
 * </p>
 *
 * <p>
 * Surviving is not enough. The committed trie must satisfy every structural invariant, including
 * that each stored key still routes to the leaf that holds it, and the postings of every interval
 * must equal what the stream says they are.
 * </p>
 *
 * <p>
 * Runs in about 7 to 8 seconds on a development machine, so it belongs in the default lane; a suite
 * that grows past 60 seconds belongs behind {@code @Tag("heavy")}, which the advisory cross-platform
 * CI lanes exclude ({@code bundles/sirix-core/build.gradle}).
 * </p>
 */
final class HOTValidTimeCorrectionStreamTest {

  private static final String RESOURCE = "valid-time-correction-stream";
  private static final int INDEX_NUMBER = 0;

  private static final int RECORDS = 100_000;

  /** The first publication and the twenty-four of corrections the load consisted of. */
  private static final int PUBLICATIONS = 25;
  private static final int HORIZON_DAYS = 366;
  private static final Instant FIRST_DAY = Instant.parse("2024-01-01T00:00:00Z");

  /** One record is nine nodes apart from the next, as in an array of eight-field objects. */
  private static final long FIRST_RECORD_KEY = 2L;
  private static final long RECORD_KEY_STRIDE = 9L;

  private static final IntervalDomain DOMAIN = new IntervalDomain();

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("every publication of corrections leaves a sound trie holding exactly the stream's postings")
  void correctionStreamStaysSoundAndExact() {
    final long foldsDeclinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();
    final long joinSplitsBefore = AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get();
    final long nodeSplitsDeclinedBefore = AbstractHOTIndexWriter.FULL_NODE_SPLIT_BREAKS_TRIE_CONDITION.get();
    final CorrectionStream stream = new CorrectionStream();

    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        for (int publication = 0; publication < PUBLICATIONS; publication++) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            stream.publish(publication, into(HOTIndexWriter.create(wtx.getStorageEngineWriter(),
                ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER)));
            wtx.commit();
          }
        }
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_NUMBER).assertOk();
        final HOTIndexReader<ValidTimeKey> reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
        for (final Map.Entry<ValidTimeKey, LongArrayList> expected : stream.expectedPostings().entrySet()) {
          final NodeReferences postings = reader.get(expected.getKey(), SearchMode.EQUAL);
          assertNotNull(postings, "postings of " + expected.getKey());
          final long[] nodeKeys = expected.getValue().toLongArray();
          Arrays.sort(nodeKeys);
          assertArrayEquals(nodeKeys, postings.toSortedArray(), "postings of " + expected.getKey());
        }
      }

      // Last, so that a broken writer is reported as the defect it is and not as a stream that no longer
      // reaches it.
      assertTrue(HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get() > foldsDeclinedBefore,
          "the stream must reach a fold whose upper half would not land beside its slot");
      assertTrue(AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get() > joinSplitsBefore,
          "the stream must reach a complete-frontier join that has to split a side");
      assertTrue(AbstractHOTIndexWriter.FULL_NODE_SPLIT_BREAKS_TRIE_CONDITION.get() > nodeSplitsDeclinedBefore,
          "the stream must reach the decomposition of a full node one of whose halves would break the trie "
              + "condition against its own child; without one it no longer covers that split");
    }
  }

  /** Where the stream's registrations go: a posting added to, or removed from, one interval key. */
  interface Registrations {
    void add(ValidTimeKey key, long nodeKey);

    void remove(ValidTimeKey key, long nodeKey);
  }

  private static Registrations into(final HOTIndexWriter<ValidTimeKey> writer) {
    return new Registrations() {
      @Override
      public void add(final ValidTimeKey key, final long nodeKey) {
        writer.indexNodeKey(key, nodeKey);
      }

      @Override
      public void remove(final ValidTimeKey key, final long nodeKey) {
        assertTrue(writer.remove(key, nodeKey), "the posting to remove must be registered: " + key + " / " + nodeKey);
      }
    };
  }

  /** One span of validity of a record, registered under the node that carries it. */
  private static final class Segment {
    private final int fromDay;
    private final int toDay;
    /**
     * Compared when adjacent spans coalesce: two spans of one record merge only if they say the same.
     */
    private final long payload;
    private long nodeKey = -1L;

    private Segment(final int fromDay, final int toDay, final long payload) {
      this.fromDay = fromDay;
      this.toDay = toDay;
      this.payload = payload;
    }

    private Segment withBounds(final int from, final int to) {
      final Segment copy = new Segment(from, to, payload);
      if (from == fromDay) {
        copy.nodeKey = nodeKey; // a span that keeps its start keeps its node; its end is updated in place
      }
      return copy;
    }
  }

  /**
   * The registrations, publication by publication, and the state they leave. A correction of a record
   * keeps the node of every span whose start survives, removes the node of every span whose start
   * does not, and appends a node for every span that starts anew — in that order, which is the order
   * the interval index sees them in.
   */
  static final class CorrectionStream {
    private static final byte[] SEED = "20260920|contracts|".getBytes(StandardCharsets.UTF_8);

    private final MessageDigest digest;
    @SuppressWarnings("unchecked")
    private final List<Segment>[] segments = new List[RECORDS + 1];
    private long nextNodeKey = FIRST_RECORD_KEY + RECORD_KEY_STRIDE * RECORDS;

    CorrectionStream() {
      try {
        digest = MessageDigest.getInstance("SHA-256");
      } catch (final NoSuchAlgorithmException e) {
        throw new AssertionError(e);
      }
    }

    void publish(final int publication, final Registrations sink) {
      if (publication == 0) {
        for (int record = 1; record <= RECORDS; record++) {
          final Segment whole = new Segment(0, HORIZON_DAYS, payload(0, record));
          whole.nodeKey = FIRST_RECORD_KEY + RECORD_KEY_STRIDE * (record - 1);
          segments[record] = new ArrayList<>(List.of(whole));
          register(sink, whole);
        }
        return;
      }
      // The first record is corrected on fixed publications, so that one record sees every case.
      switch (publication) {
        case 6 -> correct(sink, 1, 90, 210, true, payload(0, 1) + (100L << 32));
        case 12 -> correct(sink, 1, 150, 180, true, payload(0, 1) + (200L << 32));
        case 18 -> correct(sink, 1, 160, 170, false, 0L);
        case 20 -> correct(sink, 1, 164, 168, true, payload(0, 1) + (300L << 32));
        default -> {
        }
      }
      for (final int record : corrected(publication)) {
        final int from = draw(publication, record, "direction", 5) == 0
            ? Math.min(365, 15 * publication + 1 + draw(publication, record, "lag", 30))
            : Math.max(0, 15 * publication - 1 - draw(publication, record, "lag", 120));
        final int to = Math.min(HORIZON_DAYS, from + 1 + draw(publication, record, "length", 90));
        final boolean replaces = draw(publication, record, "op", 5) != 0;
        correct(sink, record, from, to, replaces, replaces
            ? payload(publication, record)
            : 0L);
      }
    }

    /** Replace or retract {@code [from, to)} of a record and reconcile its registrations. */
    private void correct(final Registrations sink, final int record, final int from, final int to,
        final boolean replaces, final long payload) {
      final List<Segment> old = segments[record];
      final List<Segment> next = new ArrayList<>(old.size() + 2);
      for (final Segment segment : old) {
        if (segment.toDay <= from || to <= segment.fromDay) {
          next.add(segment.withBounds(segment.fromDay, segment.toDay));
          continue;
        }
        if (segment.fromDay < from) {
          next.add(segment.withBounds(segment.fromDay, from));
        }
        if (to < segment.toDay) {
          next.add(segment.withBounds(to, segment.toDay));
        }
      }
      if (replaces) {
        next.add(new Segment(from, to, payload));
      }
      next.sort(Comparator.comparingInt(segment -> segment.fromDay));
      coalesce(next);

      for (final Segment target : next) {
        final Segment kept = startingOn(old, target.fromDay);
        if (kept != null) {
          target.nodeKey = kept.nodeKey;
          if (kept.toDay != target.toDay) {
            unregister(sink, kept);
            register(sink, target);
          }
        }
      }
      for (final Segment prior : old) {
        if (startingOn(next, prior.fromDay) == null) {
          unregister(sink, prior);
        }
      }
      for (final Segment target : next) {
        if (target.nodeKey < 0) {
          target.nodeKey = nextNodeKey;
          nextNodeKey += RECORD_KEY_STRIDE;
          register(sink, target);
        }
      }
      segments[record] = next;
    }

    private static void coalesce(final List<Segment> spans) {
      int write = 0;
      for (int read = 0; read < spans.size(); read++) {
        final Segment current = spans.get(read);
        if (write > 0) {
          final Segment previous = spans.get(write - 1);
          if (previous.toDay == current.fromDay && previous.payload == current.payload) {
            spans.set(write - 1, previous.withBounds(previous.fromDay, current.toDay));
            continue;
          }
        }
        spans.set(write++, current);
      }
      spans.subList(write, spans.size()).clear();
    }

    private static Segment startingOn(final List<Segment> spans, final int fromDay) {
      for (final Segment segment : spans) {
        if (segment.fromDay == fromDay) {
          return segment;
        }
      }
      return null;
    }

    private static void register(final Registrations sink, final Segment segment) {
      sink.add(key(ValidTimeKey.STORE_LOWER, segment), segment.nodeKey);
      sink.add(key(ValidTimeKey.STORE_UPPER, segment), segment.nodeKey);
    }

    private static void unregister(final Registrations sink, final Segment segment) {
      sink.remove(key(ValidTimeKey.STORE_LOWER, segment), segment.nodeKey);
      sink.remove(key(ValidTimeKey.STORE_UPPER, segment), segment.nodeKey);
    }

    /** What the index has to hold once every publication is in: each interval's nodes, per store. */
    private Map<ValidTimeKey, LongArrayList> expectedPostings() {
      final Map<ValidTimeKey, LongArrayList> postings = new HashMap<>(1 << 16);
      for (int record = 1; record <= RECORDS; record++) {
        for (final Segment segment : segments[record]) {
          postings.computeIfAbsent(key(ValidTimeKey.STORE_LOWER, segment), ignored -> new LongArrayList())
                  .add(segment.nodeKey);
          postings.computeIfAbsent(key(ValidTimeKey.STORE_UPPER, segment), ignored -> new LongArrayList())
                  .add(segment.nodeKey);
        }
      }
      return postings;
    }

    /** The twentieth of the records a publication corrects: those with the smallest pick, in order. */
    private int[] corrected(final int publication) {
      final int count = (RECORDS + 19) / 20;
      final long[][] picks = new long[RECORDS - 1][2];
      for (int record = 2; record <= RECORDS; record++) {
        picks[record - 2][0] = hashBits(publication, record, "pick");
        picks[record - 2][1] = record;
      }
      Arrays.sort(picks, (a, b) -> {
        final int byPick = Long.compareUnsigned(a[0], b[0]);
        return byPick != 0
            ? byPick
            : Long.compare(a[1], b[1]);
      });
      final int[] records = new int[count];
      for (int i = 0; i < count; i++) {
        records[i] = (int) picks[i][1];
      }
      Arrays.sort(records);
      return records;
    }

    /**
     * Two replaced spans of one record say the same only if these agree; what they are is immaterial.
     */
    private long payload(final int publication, final int record) {
      return ((long) (10_000 + draw(publication, record, "cost", 90_001)) << 32)
          | (1 + draw(publication, record, "qty", 1_000));
    }

    /** A draw in {@code [0, bound)}: the unsigned remainder of the whole 64-bit hash. */
    private int draw(final int publication, final int record, final String purpose, final int bound) {
      return (int) Long.remainderUnsigned(hashBits(publication, record, purpose), bound);
    }

    private long hashBits(final int publication, final int record, final String purpose) {
      digest.reset();
      digest.update(SEED);
      digest.update((publication + "|" + record + '|' + purpose).getBytes(StandardCharsets.UTF_8));
      return ByteBuffer.wrap(digest.digest(), 0, Long.BYTES).getLong();
    }
  }

  /** The relational-interval-tree key of a span in one store; spans are closed on both ends. */
  private static ValidTimeKey key(final byte store, final Segment segment) {
    final long lo = DOMAIN.lowerBound(FIRST_DAY.plus(segment.fromDay, ChronoUnit.DAYS));
    final long hi = DOMAIN.upperBound(FIRST_DAY.plus(segment.toDay, ChronoUnit.DAYS));
    return new ValidTimeKey(store, RelationalIntervalTree.forkNode(lo, hi), store == ValidTimeKey.STORE_LOWER
        ? lo
        : hi);
  }
}
