/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.reader.HOTRangeCursor;
import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.TreeSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Differential oracle for {@link HOTTrieReader#lowerBound}, {@link HOTTrieReader#upperBound} and
 * {@link HOTTrieReader#range}: every result on a production-built trie is compared with a binary
 * search over the sorted key list (HOT index specification §4.3.1, LB1 to LB5).
 *
 * <p>
 * One seed derives the key sets, the leaf geometry and the probes, so a failure is reproduced by
 * its seed alone. {@link HOTTrieReaderBoundPropertyTest} replays fixed seeds;
 * {@code io.sirix.property.HotBoundSeekOracleRandomTest} explores fresh ones.
 * </p>
 *
 * <p>
 * Stored key sets follow the key model the contract is stated for: a stored key that extends
 * another stored key continues with a 1 bit, as a CAS or VALIDTIME delta slot does (§2.5). Search
 * keys are not restricted: they include stored keys with one bit flipped (a bit that is not
 * discriminative on the route changes the sort position without changing the route), zero-padded
 * and truncated forms, numeric neighbours and oversized buffers with stale tails.
 * </p>
 */
public final class HOTTrieReaderBoundOracle {

  private static final HexFormat HEX = HexFormat.of();
  private static final String RESOURCE = "hot-bound-oracle";
  private static final int INDEX_NUMBER = 0;

  private HOTTrieReaderBoundOracle() {
    throw new AssertionError("no instances");
  }

  /** How the stored keys of one trie are distributed. */
  enum Shape {
    /** Consecutive 8-byte integers: every low bit is discriminative. The control shape. */
    DENSE_FIXED,
    /** Uniformly random 8-byte keys: the high bits discriminate. The second control shape. */
    UNIFORM_FIXED,
    /** Clusters that share 40 high bits and are sparse below: most bits are not discriminative. */
    CLUSTERED_FIXED,
    /** One cluster, like the values of a single indexed path. */
    ONE_CLUSTER_FIXED,
    /** One- to three-byte keys over a small alphabet: every probe class occurs within a few keys. */
    SMALL_UNIVERSE,
    /** An 8-byte path prefix, framed text, a 4-byte chunk index and delta-suffixed extensions. */
    FRAMED_TEXT_WITH_DELTAS,
    /** Random variable-length keys rich in 0x00, 0x7f, 0x80 and 0xff bytes. */
    RANDOM_VARIABLE
  }

  /**
   * How often each probe geometry was checked. A property run that never reaches a geometry proves
   * nothing about it, so the fixed-seed test asserts these counts.
   */
  public static final class Coverage {
    long tries;
    long probes;
    long exact;
    long insideLeaf;
    /** Below the routed leaf, and the bound is that leaf's first key. */
    long belowAtRoutedLeaf;
    /** Below the routed leaf, and the bound lies in an earlier leaf: the geometry of issue 1427. */
    long belowInEarlierLeaf;
    /** Below the routed leaf, whose first key extends the probe. */
    long belowExtendedByFirstKey;
    /** Above the routed leaf, and the bound is the first key of the next leaf. */
    long aboveAtNextLeaf;
    /** Above the routed leaf, and the bound lies past the next leaf or past the trie. */
    long abovePastNextLeaf;
    /** Above the routed leaf, as its first key followed only by zero bytes. */
    long aboveZeroContinuation;
    long rangesChecked;
    long rangeKeysCompared;

    void add(final Coverage other) {
      tries += other.tries;
      probes += other.probes;
      exact += other.exact;
      insideLeaf += other.insideLeaf;
      belowAtRoutedLeaf += other.belowAtRoutedLeaf;
      belowInEarlierLeaf += other.belowInEarlierLeaf;
      belowExtendedByFirstKey += other.belowExtendedByFirstKey;
      aboveAtNextLeaf += other.aboveAtNextLeaf;
      abovePastNextLeaf += other.abovePastNextLeaf;
      aboveZeroContinuation += other.aboveZeroContinuation;
      rangesChecked += other.rangesChecked;
      rangeKeysCompared += other.rangeKeysCompared;
    }

    @Override
    public String toString() {
      return "tries=" + tries + " probes=" + probes + " exact=" + exact + " insideLeaf=" + insideLeaf
          + " belowAtRoutedLeaf=" + belowAtRoutedLeaf + " belowInEarlierLeaf=" + belowInEarlierLeaf
          + " belowExtendedByFirstKey=" + belowExtendedByFirstKey + " aboveAtNextLeaf=" + aboveAtNextLeaf
          + " abovePastNextLeaf=" + abovePastNextLeaf + " aboveZeroContinuation=" + aboveZeroContinuation
          + " rangesChecked=" + rangesChecked + " rangeKeysCompared=" + rangeKeysCompared;
    }
  }

  // ======================================================================
  // Bulk-built tries, in memory.
  // ======================================================================

  /**
   * Build one trie per {@link Shape} with {@link HOTBulkBuilder} and compare every probe with the
   * oracle. Value sizes vary per key so that leaves hold anything from one entry to several hundred.
   */
  public static Coverage runBulkBuiltTries(final long seed) {
    final Coverage total = new Coverage();
    for (final Shape shape : Shape.values()) {
      final Random random = new Random(seed * 31 + shape.ordinal());
      final List<byte[]> oracle = keys(shape, random);
      final String context = "seed=" + seed + " shape=" + shape;
      final long[] nextPageKey = {1L};
      final List<HOTBulkBuilder.Entry> entries = new ArrayList<>(oracle.size());
      final int geometry = random.nextInt(4);
      for (final byte[] key : oracle) {
        entries.add(new HOTBulkBuilder.Entry(key, value(random, geometry)));
      }
      final HOTBulkBuilder.BuildResult built = HOTBulkBuilder.build(entries, 1, IndexType.CAS, () -> nextPageKey[0]++);
      final List<HOTLeafPage> leaves = new ArrayList<>();
      try {
        collectLeaves(built.rootPage(), leaves);
        final PageReference root = built.rootReference();
        if (!HOTMalformedSubtreeDetector.detect(root, PageReference::getPage).isEmpty()
            || !HOTInvariantValidator.validate(root, mock(StorageEngineReader.class)).violations().isEmpty()) {
          throw new AssertionError(context + ": the bulk builder produced a malformed trie");
        }
        final StorageEngineReader storage = mock(StorageEngineReader.class);
        // Alternate between the memoized and the descending first-key resolution.
        when(storage.hasTrxIntentLog()).thenReturn((seed + shape.ordinal()) % 2 == 0);
        try (HOTTrieReader reader = new HOTTrieReader(storage)) {
          total.add(check(reader, root, oracle, probes(oracle, shape, random), random, context));
        }
      } finally {
        for (final HOTLeafPage leaf : leaves) {
          if (!leaf.isClosed()) {
            leaf.close();
          }
        }
      }
    }
    return total;
  }

  /** The builder copies value bytes into its leaves, so every entry of one size shares an array. */
  private static final byte[] VALUE_ONE_PER_LEAF = filled(40_000);
  private static final byte[] VALUE_THREE_PER_LEAF = filled(20_000);
  private static final byte[] VALUE_DOZENS_PER_LEAF = filled(2_500);
  private static final byte[] VALUE_HUNDRED_PER_LEAF = filled(600);

  private static byte[] filled(final int size) {
    final byte[] value = new byte[size];
    Arrays.fill(value, (byte) 0x11);
    return value;
  }

  /** A value whose size puts one, three, some dozen or about a hundred entries into a 64 KiB leaf. */
  private static byte[] value(final Random random, final int geometry) {
    return switch (geometry) {
      case 0 -> VALUE_HUNDRED_PER_LEAF;
      case 1 -> VALUE_DOZENS_PER_LEAF;
      case 2 -> random.nextInt(4) == 0
          ? VALUE_ONE_PER_LEAF
          : VALUE_THREE_PER_LEAF;
      default -> switch (random.nextInt(4)) {
        case 0 -> VALUE_ONE_PER_LEAF;
        case 1 -> VALUE_THREE_PER_LEAF;
        case 2 -> VALUE_DOZENS_PER_LEAF;
        default -> VALUE_HUNDRED_PER_LEAF;
      };
    };
  }

  private static void collectLeaves(final Page page, final List<HOTLeafPage> leaves) {
    if (page instanceof HOTLeafPage leaf) {
      leaves.add(leaf);
    } else if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        collectLeaves(indirect.getChildReference(i).getPage(), leaves);
      }
    }
  }

  // ======================================================================
  // A trie built by the production index writer, committed and reopened cold.
  // ======================================================================

  /**
   * Insert clustered fixed-width logical keys in random order through {@link HOTIndexWriter}, over
   * several commits, and compare every probe with the oracle of physical keys after a cold reopen.
   * Incremental inserts produce shapes the bulk builder does not.
   *
   * <p>
   * The writer's leaves hold up to 512 of these small entries, so the keys form one to four large
   * clusters: each spans several leaves that share their high bits, which is what routes a probe
   * below or above a whole run of leaves.
   * </p>
   */
  public static Coverage runWriterBuiltTrie(final long seed, final Path databasePath) {
    final Random random = new Random(seed);
    final List<byte[]> logicalKeys = clusteredKeys(random, 1 + (int) Math.floorMod(seed, 4L), 2_400);
    final List<byte[]> insertionOrder = new ArrayList<>(logicalKeys);
    Collections.shuffle(insertionOrder, random);
    final TreeSet<byte[]> physical = new TreeSet<>(Arrays::compareUnsigned);
    final String context = "seed=" + seed + " writer-built";

    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      }
      final int commits = 3;
      for (int commit = 0; commit < commits; commit++) {
        try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
            JsonResourceSession session = database.beginResourceSession(RESOURCE);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          final HOTIndexWriter<ByteKey> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
              ByteKeySerializer.INSTANCE, IndexType.CAS, INDEX_NUMBER);
          final int from = insertionOrder.size() * commit / commits;
          final int to = insertionOrder.size() * (commit + 1) / commits;
          for (int i = from; i < to; i++) {
            final byte[] logicalKey = insertionOrder.get(i);
            // Most postings live in chunk 0; some keys have no chunk 0, some have two chunks.
            final int firstChunk = random.nextInt(8) == 0
                ? 1 + random.nextInt(3)
                : 0;
            index(writer, physical, logicalKey, firstChunk, i);
            if (random.nextInt(16) == 0) {
              index(writer, physical, logicalKey, firstChunk + 1 + random.nextInt(200), i);
            }
          }
          wtx.commit();
        }
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();
          HOTTrieReader reader = new HOTTrieReader(rtx.getStorageEngineReader())) {
        final StorageEngineReader storage = rtx.getStorageEngineReader();
        final HOTInvariantValidator.Result validation =
            HOTInvariantValidator.validateIndex(storage, IndexType.CAS, INDEX_NUMBER);
        if (!validation.violations().isEmpty()) {
          throw new AssertionError(
              context + ": the index writer produced a malformed trie: " + validation.violations());
        }
        final PageReference root = HOTInvariantValidator.resolveRootRef(storage, IndexType.CAS, INDEX_NUMBER);
        Objects.requireNonNull(root, "the committed HOT root must be addressable");
        final List<byte[]> oracle = new ArrayList<>(physical);
        if (validation.storedKeyCount() != oracle.size()) {
          throw new AssertionError(
              context + ": the trie stores " + validation.storedKeyCount() + " keys, expected " + oracle.size());
        }
        return check(reader, root, oracle, probes(oracle, Shape.CLUSTERED_FIXED, random), random, context);
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static void index(final HOTIndexWriter<ByteKey> writer, final TreeSet<byte[]> physical,
      final byte[] logicalKey, final int chunk, final int ordinal) {
    writer.indexNodeKey(new ByteKey(logicalKey), ((long) chunk << 16) | (ordinal & 0xFFFF));
    final byte[] composite = Arrays.copyOf(logicalKey, logicalKey.length + HOTKeySerializer.CHUNK_IDX_BYTES);
    HOTKeySerializer.writeChunkIdxBE(composite, logicalKey.length, chunk);
    physical.add(composite);
  }

  // ======================================================================
  // Stored keys.
  // ======================================================================

  /**
   * {@code size} sorted 8-byte keys in {@code clusters} clusters. A cluster shares its 40 high bits
   * and is sparse in the 24 below: multiples of a small stride, so most low bits do not discriminate
   * either.
   */
  private static List<byte[]> clusteredKeys(final Random random, final int clusters, final int size) {
    final long[] bases = new long[clusters];
    final int[] strides = new int[clusters];
    for (int cluster = 0; cluster < clusters; cluster++) {
      bases[cluster] = random.nextLong() & 0xFFFF_FFFF_FF00_0000L;
      strides[cluster] = 1 + random.nextInt(13);
    }
    final TreeSet<byte[]> keys = new TreeSet<>(Arrays::compareUnsigned);
    while (keys.size() < size) {
      final int cluster = random.nextInt(clusters);
      keys.add(be8(bases[cluster] | (((long) random.nextInt(1 << 20) * strides[cluster]) & 0xFF_FFFFL)));
    }
    return new ArrayList<>(keys);
  }

  /** Sorted, distinct keys of {@code shape}. */
  private static List<byte[]> keys(final Shape shape, final Random random) {
    final TreeSet<byte[]> keys = new TreeSet<>(Arrays::compareUnsigned);
    final int n = 160 + random.nextInt(240);
    switch (shape) {
      case DENSE_FIXED -> {
        final long base = random.nextLong() & 0xFFFF_FFFF_FFFF_0000L;
        for (int i = 0; i < n; i++) {
          keys.add(be8(base + i));
        }
      }
      case UNIFORM_FIXED -> {
        while (keys.size() < n) {
          keys.add(be8(random.nextLong()));
        }
      }
      case CLUSTERED_FIXED -> {
        while (keys.size() < n) {
          final long base = random.nextLong() & 0xFFFF_FFFF_FF00_0000L;
          final int stride = 1 + random.nextInt(13);
          for (int i = 0; i < 48 && keys.size() < n; i++) {
            keys.add(be8(base | ((long) random.nextInt(4_096) * stride)));
          }
        }
      }
      case ONE_CLUSTER_FIXED -> keys.addAll(clusteredKeys(random, 1, n));
      case SMALL_UNIVERSE -> {
        final int[] alphabet = {0x00, 0x01, 0x10, 0x7F, 0x80, 0x81, 0xC0, 0xFF};
        final int target = 24 + random.nextInt(40);
        for (int attempt = 0; attempt < target * 6 && keys.size() < target; attempt++) {
          final byte[] key = new byte[1 + random.nextInt(3)];
          for (int i = 0; i < key.length; i++) {
            key[i] = (byte) alphabet[random.nextInt(alphabet.length)];
          }
          addIfAdmissible(keys, key);
        }
      }
      case FRAMED_TEXT_WITH_DELTAS -> {
        final byte[] path = be8(0x8000_0000_0000_0000L | random.nextInt(4));
        final String letters = "abcdefghijklmnopqrstuvwxyz0123456789";
        while (keys.size() < n) {
          final StringBuilder text = new StringBuilder(8).append((char) ('a' + random.nextInt(3)));
          final int length = random.nextInt(5);
          for (int i = 0; i < length; i++) {
            text.append(letters.charAt(random.nextInt(letters.length())));
          }
          final byte[] utf8 = text.toString().getBytes(StandardCharsets.US_ASCII);
          // path, text, a two-byte terminator that makes the logical keys prefix-free, chunk index.
          final byte[] base = new byte[path.length + utf8.length + 2 + HOTKeySerializer.CHUNK_IDX_BYTES];
          System.arraycopy(path, 0, base, 0, path.length);
          System.arraycopy(utf8, 0, base, path.length, utf8.length);
          if (random.nextInt(6) == 0) {
            HOTKeySerializer.writeChunkIdxBE(base, base.length - HOTKeySerializer.CHUNK_IDX_BYTES,
                1 + random.nextInt(3));
          }
          addIfAdmissible(keys, base);
          // Delta slots extend their base key with a suffix whose top bit is set.
          for (int delta = random.nextInt(4); delta > 0; delta--) {
            final byte[] slot = Arrays.copyOf(base, base.length + PostingDeltas.SUFFIX_BYTES);
            HOTKeySerializer.writeChunkIdxBE(slot, base.length,
                PostingDeltas.suffix(random.nextInt(64), random.nextBoolean()));
            addIfAdmissible(keys, slot);
          }
        }
      }
      case RANDOM_VARIABLE -> {
        final int[] special = {0x00, 0x00, 0x7F, 0x80, 0xFF};
        for (int attempt = 0; attempt < n * 4 && keys.size() < n; attempt++) {
          final byte[] key = new byte[1 + random.nextInt(9)];
          // A shared head makes long common prefixes, hence deep tries with few discriminative bits.
          key[0] = (byte) (0x40 | random.nextInt(2));
          for (int i = 1; i < key.length; i++) {
            key[i] = (byte) (random.nextInt(3) == 0
                ? random.nextInt(256)
                : special[random.nextInt(special.length)]);
          }
          addIfAdmissible(keys, key);
        }
      }
      default -> throw new AssertionError(shape);
    }
    return new ArrayList<>(keys);
  }

  /**
   * Add {@code key} unless the set would then hold a key that extends another one with a 0 bit, in
   * either direction. That rule also excludes two keys no bit separates.
   */
  private static void addIfAdmissible(final TreeSet<byte[]> keys, final byte[] key) {
    if (keys.contains(key)) {
      return;
    }
    for (int prefixLength = 1; prefixLength < key.length; prefixLength++) {
      if ((key[prefixLength] & 0x80) == 0 && keys.contains(Arrays.copyOf(key, prefixLength))) {
        return;
      }
    }
    // Stored keys that extend the new key sort directly after it.
    for (final byte[] larger : keys.tailSet(key, false)) {
      if (larger.length <= key.length || !Arrays.equals(larger, 0, key.length, key, 0, key.length)) {
        break;
      }
      if ((larger[key.length] & 0x80) == 0) {
        return;
      }
    }
    keys.add(key);
  }

  private static byte[] be8(final long value) {
    final byte[] key = new byte[Long.BYTES];
    for (int i = 0; i < Long.BYTES; i++) {
      key[i] = (byte) (value >>> (56 - 8 * i));
    }
    return key;
  }

  // ======================================================================
  // Probes.
  // ======================================================================

  private static List<byte[]> probes(final List<byte[]> oracle, final Shape shape, final Random random) {
    final List<byte[]> probes = new ArrayList<>(1 << 15);
    probes.add(new byte[0]);
    probes.add(new byte[] {0x00});
    probes.add(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    final int stride = Math.max(1, oracle.size() / 96);
    for (int index = 0; index < oracle.size(); index++) {
      final byte[] key = oracle.get(index);
      probes.add(key);
      // Zero-padded and truncated forms of every stored key.
      probes.add(Arrays.copyOf(key, key.length + 1));
      probes.add(Arrays.copyOf(key, key.length + 3));
      probes.add(Arrays.copyOf(key, key.length - 1));
      probes.add(append(key, 0x00, 0x01));
      probes.add(append(key, 0x7F));
      probes.add(append(key, 0x80));
      probes.add(append(key, 0xFF));
      probes.add(neighbour(key, 1));
      probes.add(neighbour(key, -1));
      if (index % stride != random.nextInt(stride)) {
        continue;
      }
      // Every prefix, and every single-bit flip, of a sample of the stored keys.
      for (int length = 0; length < key.length; length++) {
        probes.add(Arrays.copyOf(key, length));
      }
      for (int bit = 0; bit < key.length * 8; bit++) {
        final byte[] flipped = key.clone();
        flipped[bit >>> 3] ^= (byte) (0x80 >>> (bit & 7));
        probes.add(flipped);
      }
    }
    for (int i = 0; i < 256; i++) {
      final byte[] base = oracle.get(random.nextInt(oracle.size()));
      final byte[] mixed = Arrays.copyOf(base, Math.max(1, base.length - random.nextInt(3) + random.nextInt(3)));
      for (int change = 1 + random.nextInt(3); change > 0; change--) {
        mixed[random.nextInt(mixed.length)] = (byte) random.nextInt(256);
      }
      probes.add(mixed);
    }
    if (shape == Shape.SMALL_UNIVERSE) {
      for (int first = 0; first < 256; first++) {
        probes.add(new byte[] {(byte) first});
        for (final int second : new int[] {0x00, 0x01, 0x10, 0x7F, 0x80, 0x81, 0xC0, 0xFF}) {
          probes.add(new byte[] {(byte) first, (byte) second});
          probes.add(new byte[] {(byte) first, (byte) second, 0x00});
          probes.add(new byte[] {(byte) first, (byte) second, (byte) 0x80});
        }
      }
    }
    return probes;
  }

  private static byte[] append(final byte[] key, final int... tail) {
    final byte[] result = Arrays.copyOf(key, key.length + tail.length);
    for (int i = 0; i < tail.length; i++) {
      result[key.length + i] = (byte) tail[i];
    }
    return result;
  }

  /** {@code key} plus or minus one as an unsigned big-endian integer of the same width. */
  private static byte[] neighbour(final byte[] key, final int delta) {
    final byte[] result = key.clone();
    for (int i = result.length - 1; i >= 0; i--) {
      final int sum = (result[i] & 0xFF) + delta;
      result[i] = (byte) sum;
      if (sum >= 0 && sum <= 0xFF) {
        break;
      }
    }
    return result;
  }

  // ======================================================================
  // The comparison.
  // ======================================================================

  private static Coverage check(final HOTTrieReader reader, final PageReference root, final List<byte[]> oracle,
      final List<byte[]> probes, final Random random, final String context) {
    final Coverage coverage = new Coverage();
    coverage.tries = 1;
    final byte[] buffer = new byte[64];
    int probeNumber = 0;
    for (final byte[] probe : probes) {
      probeNumber++;
      final int lowerAt = lowerIndex(oracle, probe, false);
      final int upperAt = lowerIndex(oracle, probe, true);
      final byte[] expectedLower = keyAt(oracle, lowerAt);
      final byte[] expectedUpper = keyAt(oracle, upperAt);
      classify(reader, root, oracle, probe, lowerAt, lowerAt != upperAt, coverage);

      final byte[] searchKey;
      final int searchKeyLength = probe.length;
      if (probeNumber % 3 == 0 && probe.length < buffer.length) {
        // The valid-length overloads: a reusable buffer whose tail holds bytes of an earlier key.
        Arrays.fill(buffer, (byte) random.nextInt(256));
        System.arraycopy(probe, 0, buffer, 0, probe.length);
        searchKey = buffer;
      } else {
        searchKey = probe;
      }
      final byte[] actualLower;
      final byte[] actualUpper;
      try {
        actualLower = seek(reader, root, searchKey, searchKeyLength, false);
        actualUpper = seek(reader, root, searchKey, searchKeyLength, true);
      } catch (final RuntimeException failure) {
        throw new AssertionError(context + ": the seek for " + hex(probe) + " threw " + failure, failure);
      }
      if (!Arrays.equals(expectedLower, actualLower)) {
        throw mismatch(context, "lowerBound", probe, expectedLower, actualLower);
      }
      if (!Arrays.equals(expectedUpper, actualUpper)) {
        throw mismatch(context, "upperBound", probe, expectedUpper, actualUpper);
      }

      if (probeNumber % 23 == 0) {
        // An open-ended range from the probe, and one bounded by another probe.
        checkRange(reader, root, oracle, probe, null, context, coverage);
        checkRange(reader, root, oracle, probe, probes.get(random.nextInt(probes.size())), context, coverage);
      }
    }
    coverage.probes = probes.size();
    checkRange(reader, root, oracle, null, null, context, coverage);
    return coverage;
  }

  /** Record which geometry the probe has relative to the leaf PEXT routes it to. */
  private static void classify(final HOTTrieReader reader, final PageReference root, final List<byte[]> oracle,
      final byte[] probe, final int lowerAt, final boolean stored, final Coverage coverage) {
    if (stored) {
      coverage.exact++;
      return;
    }
    byte[] first = null;
    byte[] last = null;
    for (int attempt = 0; attempt < HOTTrieReader.MAX_STAMP_RETRIES && first == null; attempt++) {
      final HOTLeafPage routed = reader.navigateToLeaf(root, probe);
      try {
        final byte[] firstKey = routed.getKey(0);
        final byte[] lastKey = routed.getKey(routed.getEntryCount() - 1);
        if (reader.validateCurrentLeaf()) {
          first = firstKey;
          last = lastKey;
        }
      } catch (final RuntimeException torn) {
        if (reader.validateCurrentLeaf()) {
          throw torn;
        }
      }
    }
    if (first == null) {
      throw new AssertionError("the routed leaf never stabilised for " + hex(probe));
    }
    final byte[] expected = keyAt(oracle, lowerAt);
    if (Arrays.compareUnsigned(probe, first) < 0) {
      if (Arrays.equals(expected, first)) {
        coverage.belowAtRoutedLeaf++;
      } else {
        coverage.belowInEarlierLeaf++;
      }
      if (first.length > probe.length && Arrays.equals(first, 0, probe.length, probe, 0, probe.length)) {
        coverage.belowExtendedByFirstKey++;
      }
    } else if (Arrays.compareUnsigned(probe, last) > 0) {
      // The key after the routed leaf's last key is the first key of the next leaf.
      final int afterLeaf = lowerIndex(oracle, last, true);
      if (lowerAt == afterLeaf) {
        coverage.aboveAtNextLeaf++;
      } else {
        coverage.abovePastNextLeaf++;
      }
      if (probe.length > first.length && Arrays.equals(probe, 0, first.length, first, 0, first.length)
          && isZero(probe, first.length)) {
        coverage.aboveZeroContinuation++;
      }
    } else {
      coverage.insideLeaf++;
    }
  }

  private static boolean isZero(final byte[] key, final int from) {
    for (int i = from; i < key.length; i++) {
      if (key[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private static byte[] seek(final HOTTrieReader reader, final PageReference root, final byte[] searchKey,
      final int searchKeyLength, final boolean upper) {
    for (int attempt = 0; attempt < HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
      final HOTTrieReader.LowerBoundResult result = upper
          ? reader.upperBound(root, searchKey, searchKeyLength)
          : reader.lowerBound(root, searchKey, searchKeyLength);
      if (result.leaf == null) {
        return null;
      }
      final byte[] key;
      try {
        key = result.leaf.getKey(result.indexInLeaf);
      } catch (final RuntimeException torn) {
        if (reader.validateCurrentLeaf()) {
          throw torn;
        }
        continue;
      }
      if (reader.validateCurrentLeaf()) {
        return key;
      }
    }
    throw new AssertionError(
        "the seek never produced a stamp-stable key for " + hex(Arrays.copyOf(searchKey, searchKeyLength)));
  }

  /** Compare {@code range(from, to)} key by key with the oracle slice, without collecting it. */
  private static void checkRange(final HOTTrieReader reader, final PageReference root, final List<byte[]> oracle,
      final byte[] from, final byte[] to, final String context, final Coverage coverage) {
    final int first = from == null
        ? 0
        : lowerIndex(oracle, from, false);
    final int afterLast = Math.max(first, to == null
        ? oracle.size()
        : lowerIndex(oracle, to, true));
    int next = first;
    try (HOTRangeCursor cursor = reader.range(root, from, to)) {
      while (cursor.hasNext()) {
        final byte[] actual = cursor.next().keyBytes();
        if (next >= afterLast || !Arrays.equals(oracle.get(next), actual)) {
          throw new AssertionError(context + ": range(" + hex(from) + ", " + hex(to) + ") returned " + hex(actual)
              + " at position " + (next - first) + ", expected " + (next >= afterLast
                  ? "the end of the range"
                  : hex(oracle.get(next))));
        }
        next++;
      }
    }
    if (next != afterLast) {
      throw new AssertionError(context + ": range(" + hex(from) + ", " + hex(to) + ") returned " + (next - first)
          + " keys, expected " + (afterLast - first) + "; the first missing key is " + hex(oracle.get(next)));
    }
    coverage.rangesChecked++;
    coverage.rangeKeysCompared += afterLast - first;
  }

  private static AssertionError mismatch(final String context, final String operation, final byte[] probe,
      final byte[] expected, final byte[] actual) {
    return new AssertionError(context + ": " + operation + "(" + hex(probe) + ") returned " + hex(actual)
        + ", the sorted reference says " + hex(expected));
  }

  /** Index of the first oracle key {@code >= probe}, or {@code > probe} when {@code strict}. */
  private static int lowerIndex(final List<byte[]> oracle, final byte[] probe, final boolean strict) {
    int low = 0;
    int high = oracle.size();
    while (low < high) {
      final int middle = (low + high) >>> 1;
      final int comparison = Arrays.compareUnsigned(oracle.get(middle), probe);
      if (comparison < 0 || strict && comparison == 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static byte[] keyAt(final List<byte[]> oracle, final int index) {
    return index == oracle.size()
        ? null
        : oracle.get(index);
  }

  private static String hex(final byte[] key) {
    return key == null
        ? "END"
        : key.length == 0
            ? "<empty>"
            : HEX.formatHex(key);
  }

  // ======================================================================
  // A raw-bytes logical key for the production index writer.
  // ======================================================================

  private static final class ByteKey implements Comparable<ByteKey> {
    private final byte[] bytes;

    private ByteKey(final byte[] bytes) {
      this.bytes = bytes.clone();
    }

    @Override
    public int compareTo(final ByteKey other) {
      return Arrays.compareUnsigned(bytes, other.bytes);
    }
  }

  private enum ByteKeySerializer implements HOTKeySerializer<ByteKey> {
    INSTANCE;

    @Override
    public int serialize(final ByteKey key, final byte[] destination, final int offset) {
      Objects.checkFromIndexSize(offset, key.bytes.length, destination.length);
      System.arraycopy(key.bytes, 0, destination, offset, key.bytes.length);
      return key.bytes.length;
    }

    @Override
    public int maxSerializedLength(final ByteKey key) {
      return key.bytes.length;
    }

    @Override
    public ByteKey deserialize(final byte[] bytes, final int offset, final int length) {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      return new ByteKey(Arrays.copyOfRange(bytes, offset, offset + length));
    }
  }
}
