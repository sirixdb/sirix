/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.utils;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The FSST encoder's two contracts: decode(encode(x)) == x for arbitrary bytes, and a real
 * compression ratio on the text the string region actually holds.
 *
 * <p>
 * The ratio floor is the load-bearing assertion. The previous encoder round-tripped perfectly and
 * compressed English text by about one percent — single-byte symbols were never candidates, so
 * under the escape-doubles-a-byte scheme almost every position expanded, and the benefit gate
 * silently vetoed FSST for entire workloads. Nothing failed; the feature just never engaged. A
 * floor turns that silent regression into a red test.
 */
@DisplayName("FSST encoder quality")
public final class FsstCompressorQualityTest {

  /** Region-shaped corpus: distinct values, heavy shared structure — URLs, titles, prose. */
  private static List<byte[]> corpus() {
    final List<byte[]> samples = new ArrayList<>(600);
    final Random rnd = new Random(0xF557);
    final String[] cities = {"Springfield", "Riverton", "Lakewood", "Fairview", "Greenville"};
    for (int i = 0; i < 200; i++) {
      samples.add(("https://media.example.org/catalog/items/product-" + rnd.nextInt(100_000)
          + "/images/main.jpg?size=large&format=webp").getBytes(StandardCharsets.UTF_8));
      samples.add(("A restless drama about a family in "
          + cities[i % cities.length] + " confronting the past, directed with patience and a fine eye for the small "
          + "moments that make a life.").getBytes(StandardCharsets.UTF_8));
      samples.add(("item-" + i + "/rev=" + rnd.nextInt(9) + "/state=published").getBytes(StandardCharsets.UTF_8));
    }
    return samples;
  }

  /** Fixed corpus whose pre-workspace table bytes are a wire/determinism golden. */
  private static List<byte[]> goldenCorpus() {
    final List<byte[]> samples = new ArrayList<>(128);
    for (int i = 0; i < 128; i++) {
      samples.add(("https://sirix.example/api/items/tenant-" + (i % 17) + "/entity-" + i + "/shared-tail").getBytes(
          StandardCharsets.UTF_8));
    }
    return samples;
  }

  private static List<byte[]> alternateCorpus() {
    final List<byte[]> samples = new ArrayList<>(128);
    for (int i = 0; i < 128; i++) {
      samples.add(
          ("zzzzzz/alternate/cluster-" + (i % 11) + "/payload-yyyyyyyy/value-" + i).getBytes(StandardCharsets.UTF_8));
    }
    return samples;
  }

  private record FlatCorpus(byte[] backing, int[] offsets, int[] lengths) {
  }

  private static FlatCorpus flatten(final List<byte[]> samples) {
    int total = 0;
    for (final byte[] sample : samples) {
      total += sample.length;
    }
    final byte[] backing = new byte[total];
    final int[] offsets = new int[samples.size()];
    final int[] lengths = new int[samples.size()];
    int position = 0;
    for (int i = 0; i < samples.size(); i++) {
      final byte[] sample = samples.get(i);
      offsets[i] = position;
      lengths[i] = sample.length;
      System.arraycopy(sample, 0, backing, position, sample.length);
      position += sample.length;
    }
    return new FlatCorpus(backing, offsets, lengths);
  }

  /**
   * Independent copy of the pre-primitive training algorithm. It intentionally retains the old
   * immutable byte-array keys and fastutil map so randomized differential tests cover hash slots,
   * rehash order, stable gain ties, length ties, and final wire codes rather than merely round-trip.
   */
  private static byte[] legacySymbolTable(final List<byte[]> samples) {
    int eligibleSamples = 0;
    long totalBytes = 0;
    for (final byte[] sample : samples) {
      if (sample != null && sample.length >= FSSTCompressor.MIN_COMPRESSION_SIZE) {
        eligibleSamples++;
        totalBytes += sample.length;
      }
    }
    if (samples.size() < FSSTCompressor.MIN_SAMPLES_FOR_TABLE || eligibleSamples < FSSTCompressor.MIN_SAMPLES_FOR_TABLE
        || totalBytes < FSSTCompressor.MIN_TOTAL_BYTES_FOR_TABLE) {
      return new byte[0];
    }

    final List<byte[]> corpus = new ArrayList<>(FSSTCompressor.MAX_SAMPLES_TO_ANALYZE);
    for (final byte[] sample : samples) {
      if (sample != null && sample.length >= FSSTCompressor.MIN_COMPRESSION_SIZE) {
        corpus.add(sample);
        if (corpus.size() == FSSTCompressor.MAX_SAMPLES_TO_ANALYZE) {
          break;
        }
      }
    }

    final int[] byteCounts = new int[256];
    for (final byte[] sample : corpus) {
      for (final byte value : sample) {
        byteCounts[value & 0xFF]++;
      }
    }
    Object2IntOpenHashMap<LegacySequence> gains = new Object2IntOpenHashMap<>();
    for (int value = 0; value < byteCounts.length; value++) {
      if (byteCounts[value] > 0) {
        gains.put(new LegacySequence(new byte[] {(byte) value}, 0, 1), byteCounts[value]);
      }
    }
    List<LegacySequence> table = legacyTopByGain(gains);

    for (int iteration = 0; iteration < 5; iteration++) {
      gains = new Object2IntOpenHashMap<>();
      for (final byte[] sample : corpus) {
        int position = 0;
        int previousStart = -1;
        int previousLength = 0;
        while (position < sample.length) {
          final int length = legacyMatchLength(table, sample, position);
          gains.addTo(new LegacySequence(sample, position, length), 2 * length - 1);
          if (previousStart >= 0) {
            final int concatenatedLength = Math.min(previousLength + length, FSSTCompressor.MAX_SYMBOL_LENGTH);
            if (concatenatedLength > previousLength && previousStart + concatenatedLength <= sample.length) {
              gains.addTo(new LegacySequence(sample, previousStart, concatenatedLength), 2 * concatenatedLength - 1);
            }
          }
          previousStart = position;
          previousLength = length;
          position += length;
        }
      }
      table = legacyTopByGain(gains);
    }

    int tableLength = 1 + table.size();
    for (final LegacySequence symbol : table) {
      tableLength += symbol.data.length;
    }
    final byte[] serialized = new byte[tableLength];
    int position = 0;
    serialized[position++] = (byte) table.size();
    for (final LegacySequence symbol : table) {
      serialized[position++] = (byte) symbol.data.length;
    }
    for (final LegacySequence symbol : table) {
      System.arraycopy(symbol.data, 0, serialized, position, symbol.data.length);
      position += symbol.data.length;
    }
    return serialized;
  }

  private static List<LegacySequence> legacyTopByGain(final Object2IntOpenHashMap<LegacySequence> gains) {
    final List<Object2IntMap.Entry<LegacySequence>> entries = new ArrayList<>(gains.object2IntEntrySet());
    entries.sort((first, second) -> Integer.compare(second.getIntValue(), first.getIntValue()));
    final List<LegacySequence> selected = new ArrayList<>(Math.min(entries.size(), FSSTCompressor.MAX_SYMBOLS));
    for (final Object2IntMap.Entry<LegacySequence> entry : entries) {
      if (selected.size() == FSSTCompressor.MAX_SYMBOLS) {
        break;
      }
      final int length = entry.getKey().data.length;
      final int minimumGain = length == 1
          ? 2
          : 2 * (2 * length - 1);
      if (entry.getIntValue() >= minimumGain) {
        selected.add(entry.getKey());
      }
    }
    selected.sort(Comparator.comparingInt((LegacySequence value) -> value.data.length).reversed());
    return selected;
  }

  private static int legacyMatchLength(final List<LegacySequence> table, final byte[] sample, final int position) {
    for (final LegacySequence symbol : table) {
      if (position + symbol.data.length <= sample.length
          && Arrays.equals(sample, position, position + symbol.data.length, symbol.data, 0, symbol.data.length)) {
        return symbol.data.length;
      }
    }
    return 1;
  }

  private static final class LegacySequence {
    private final byte[] data;

    private LegacySequence(final byte[] source, final int offset, final int length) {
      data = Arrays.copyOfRange(source, offset, offset + length);
    }

    @Override
    public boolean equals(final Object object) {
      return this == object || object instanceof LegacySequence other && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }

  @Test
  @DisplayName("workspace training preserves golden bytes and fully resets A/B/A")
  void workspaceTrainingPreservesGoldenBytesAndResetsDirtyState() throws Exception {
    final List<byte[]> a = goldenCorpus();
    final List<byte[]> b = alternateCorpus();
    final byte[] statelessA = FSSTCompressor.buildSymbolTable(a);
    final FSSTCompressor.Workspace workspace = new FSSTCompressor.Workspace();
    try {
      final byte[] firstA = FSSTCompressor.buildSymbolTable(a, workspace);
      assertArrayEquals(statelessA, firstA, "workspace training changed deterministic table bytes");
      assertEquals("353ca0a56a0aab438e3c9a8c775cd6c013222de55a7a8431ecaa1c6662f6c72b",
          HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(firstA)),
          "FSST table wire bytes changed from the pre-workspace golden");
      final byte[][] firstSymbols = FSSTCompressor.parseSymbolTable(firstA);
      final byte[] firstEncoded = FSSTCompressor.encode(a.get(37), firstSymbols, workspace);

      final byte[] tableB = FSSTCompressor.buildSymbolTable(b, workspace);
      assertTrue(tableB.length > 0);
      final byte[][] symbolsB = FSSTCompressor.parseSymbolTable(tableB);
      assertArrayEquals(b.get(19),
          FSSTCompressor.decode(FSSTCompressor.encode(b.get(19), symbolsB, workspace), symbolsB));

      final byte[] secondA = FSSTCompressor.buildSymbolTable(a, workspace);
      final byte[][] secondSymbols = FSSTCompressor.parseSymbolTable(secondA);
      assertArrayEquals(firstA, secondA, "B left stale counts or cursors in the rebuilt A table");
      assertArrayEquals(firstEncoded, FSSTCompressor.encode(a.get(37), secondSymbols, workspace),
          "B left stale matcher buckets in the rebuilt A encoding");
    } finally {
      workspace.clear();
    }
  }

  @Test
  @DisplayName("flat ranges preserve legacy training, trial and encode bytes across A/B/A and failure")
  void flatRangesPreserveLegacyWireAcrossReuseAndFailure() {
    final List<byte[]> a = goldenCorpus();
    final List<byte[]> b = alternateCorpus();
    final FlatCorpus flatA = flatten(a);
    final FlatCorpus flatB = flatten(b);
    final byte[] expectedTableA = legacySymbolTable(a);
    final FSSTCompressor.Workspace workspace = new FSSTCompressor.Workspace();
    try {
      final byte[] firstA =
          FSSTCompressor.buildSymbolTable(flatA.backing(), flatA.offsets(), flatA.lengths(), a.size(), workspace);
      assertArrayEquals(expectedTableA, firstA, "flat training changed symbol codes or stable tie order");
      final byte[][] symbolsA = FSSTCompressor.parseSymbolTable(firstA);
      assertEquals(FSSTCompressor.isCompressionBeneficial(a, symbolsA, workspace),
          FSSTCompressor.isCompressionBeneficial(flatA.backing(), flatA.offsets(), flatA.lengths(), a.size(), symbolsA,
              workspace));
      final int entry = 37;
      final byte[] encoded =
          FSSTCompressor.encode(flatA.backing(), flatA.offsets()[entry], flatA.lengths()[entry], symbolsA, workspace);
      assertArrayEquals(FSSTCompressor.encode(a.get(entry), symbolsA, workspace), encoded);
      final byte[] encodedSnapshot = encoded.clone();
      flatA.backing()[flatA.offsets()[entry]] ^= 0x5A;
      assertArrayEquals(encodedSnapshot, encoded, "encoded output must not alias the page-owned flat backing");
      flatA.backing()[flatA.offsets()[entry]] ^= 0x5A;

      assertTrue(FSSTCompressor.buildSymbolTable(flatB.backing(), flatB.offsets(), flatB.lengths(), b.size(),
          workspace).length > 0);
      assertThrows(IllegalArgumentException.class, () -> FSSTCompressor.buildSymbolTable(flatA.backing(),
          new int[] {flatA.backing().length}, new int[] {1}, 1, workspace));
      assertArrayEquals(expectedTableA,
          FSSTCompressor.buildSymbolTable(flatA.backing(), flatA.offsets(), flatA.lengths(), a.size(), workspace),
          "B or a rejected range left dirty candidate/matcher state in the rebuilt A table");
    } finally {
      workspace.clear();
    }
  }

  @Test
  @DisplayName("primitive training is wire-identical to the legacy map on randomized tie-heavy corpora")
  void primitiveTrainingMatchesLegacyAcrossRandomizedCorpora() {
    final Random random = new Random(0xC4AD1DA7EL);
    final FSSTCompressor.Workspace workspace = new FSSTCompressor.Workspace();
    try {
      for (int round = 0; round < 6; round++) {
        final List<byte[]> samples = new ArrayList<>(64);
        final byte[] alphabet = round % 2 == 0
            ? "abcdefghijklmno/".getBytes(StandardCharsets.UTF_8)
            : null;
        for (int sampleIndex = 0; sampleIndex < 64; sampleIndex++) {
          final byte[] sample = new byte[64 + random.nextInt(33)];
          if (alphabet == null) {
            random.nextBytes(sample);
          } else {
            for (int i = 0; i < sample.length; i++) {
              sample[i] = alphabet[random.nextInt(alphabet.length)];
            }
          }
          // Repeated and equal-frequency islands exercise both multi-byte choices and stable ties.
          final byte[] motif =
              ("/round-" + round + "/bucket-" + (sampleIndex % 8) + "/").getBytes(StandardCharsets.UTF_8);
          System.arraycopy(motif, 0, sample, 0, Math.min(motif.length, sample.length));
          samples.add(sample);
        }

        final byte[] expected = legacySymbolTable(samples);
        assertArrayEquals(expected, FSSTCompressor.buildSymbolTable(samples, workspace),
            "primitive candidate layout diverged from legacy training in round " + round);
        assertArrayEquals(expected, FSSTCompressor.buildSymbolTable(samples),
            "compatibility-pool training diverged from legacy training in round " + round);
      }
    } finally {
      workspace.clear();
    }
  }

  @Test
  @DisplayName("distinct training workspaces are isolated under concurrent use")
  void distinctTrainingWorkspacesAreIsolatedUnderConcurrency() throws Exception {
    final List<byte[]> firstCorpus = goldenCorpus();
    final List<byte[]> secondCorpus = alternateCorpus();
    final FlatCorpus flatFirst = flatten(firstCorpus);
    final FlatCorpus flatSecond = flatten(secondCorpus);
    final byte[] expectedFirst = legacySymbolTable(firstCorpus);
    final byte[] expectedSecond = legacySymbolTable(secondCorpus);
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<byte[]> first = executor.submit(() -> {
        final FSSTCompressor.Workspace workspace = new FSSTCompressor.Workspace();
        try {
          start.await();
          for (int i = 0; i < 8; i++) {
            final FlatCorpus corpus = i % 2 == 0
                ? flatFirst
                : flatSecond;
            FSSTCompressor.buildSymbolTable(corpus.backing(), corpus.offsets(), corpus.lengths(),
                corpus.lengths().length, workspace);
          }
          return FSSTCompressor.buildSymbolTable(flatFirst.backing(), flatFirst.offsets(), flatFirst.lengths(),
              flatFirst.lengths().length, workspace);
        } finally {
          workspace.clear();
        }
      });
      final Future<byte[]> second = executor.submit(() -> {
        final FSSTCompressor.Workspace workspace = new FSSTCompressor.Workspace();
        try {
          start.await();
          for (int i = 0; i < 8; i++) {
            final FlatCorpus corpus = i % 2 == 0
                ? flatSecond
                : flatFirst;
            FSSTCompressor.buildSymbolTable(corpus.backing(), corpus.offsets(), corpus.lengths(),
                corpus.lengths().length, workspace);
          }
          return FSSTCompressor.buildSymbolTable(flatSecond.backing(), flatSecond.offsets(), flatSecond.lengths(),
              flatSecond.lengths().length, workspace);
        } finally {
          workspace.clear();
        }
      });
      start.countDown();
      assertArrayEquals(expectedFirst, first.get(30, TimeUnit.SECONDS));
      assertArrayEquals(expectedSecond, second.get(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  @DisplayName("workspace remains reusable after a failed matcher reset")
  void workspaceRemainsReusableAfterFailedReset() {
    final FSSTCompressor.Workspace workspace = new FSSTCompressor.Workspace();
    try {
      assertTrue(FSSTCompressor.buildSymbolTable(alternateCorpus(), workspace).length > 0,
          "precondition: dirty both candidate banks before the injected matcher failure");
      final byte[][] malformed = {"valid-prefix".getBytes(StandardCharsets.UTF_8), null};
      assertThrows(IllegalArgumentException.class,
          () -> FSSTCompressor.encode(goldenCorpus().get(0), malformed, workspace));

      final List<byte[]> samples = goldenCorpus();
      final byte[] table = FSSTCompressor.buildSymbolTable(samples, workspace);
      assertArrayEquals(legacySymbolTable(samples), table,
          "failed matcher reset or dirty primitive candidate slots changed the rebuilt table");
      final byte[][] symbols = FSSTCompressor.parseSymbolTable(table);
      final byte[] encoded = FSSTCompressor.encode(samples.get(0), symbols, workspace);
      assertArrayEquals(samples.get(0), FSSTCompressor.decode(encoded, symbols));
    } finally {
      workspace.clear();
    }
  }

  @Test
  @DisplayName("candidate ceiling fails closed and the workspace remains reusable")
  void candidateCeilingFailsClosedWithoutPoisoningWorkspace() {
    // Every possible two-byte value appears within an explicit adjacent pair. Together with the
    // 256 single-byte candidates this exceeds the hard 65,536 distinct-key ceiling deterministically.
    final byte[] allPairs = new byte[2 * 65_536];
    int output = 0;
    for (int first = 0; first < 256; first++) {
      for (int second = 0; second < 256; second++) {
        allPairs[output++] = (byte) first;
        allPairs[output++] = (byte) second;
      }
    }
    final List<byte[]> adversarial = new ArrayList<>(64);
    adversarial.add(allPairs);
    while (adversarial.size() < 64) {
      adversarial.add(Arrays.copyOf(allPairs, 64));
    }

    final FSSTCompressor.Workspace workspace = new FSSTCompressor.Workspace();
    try {
      assertEquals(0, FSSTCompressor.buildSymbolTable(adversarial, workspace).length,
          "an over-ceiling training set must fall back to raw instead of unbounded growth");
      assertArrayEquals(legacySymbolTable(goldenCorpus()), FSSTCompressor.buildSymbolTable(goldenCorpus(), workspace),
          "candidate-ceiling fallback poisoned retained map or sort state");
    } finally {
      workspace.clear();
    }
  }

  @Test
  @DisplayName("every sample round-trips byte-for-byte")
  void corpusRoundTrips() {
    final List<byte[]> samples = corpus();
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    assertTrue(table.length > 0, "no table built from a corpus designed to build one");
    final byte[][] symbols = FSSTCompressor.parseSymbolTable(table);
    for (final byte[] sample : samples) {
      final byte[] encoded = FSSTCompressor.encode(sample, symbols);
      assertArrayEquals(sample, FSSTCompressor.decode(encoded, symbols),
          "round-trip mismatch on: " + new String(sample, StandardCharsets.UTF_8));
    }
  }

  /**
   * Arbitrary bytes — including the escape byte itself, symbol-prefix collisions, and empty-ish
   * inputs — must survive. Compression is irrelevant here; only fidelity is.
   */
  @Test
  @DisplayName("random bytes round-trip under a text-trained table")
  void randomBytesRoundTrip() {
    final byte[] table = FSSTCompressor.buildSymbolTable(corpus());
    final byte[][] symbols = FSSTCompressor.parseSymbolTable(table);
    final Random rnd = new Random(0xBEEF);
    for (int i = 0; i < 500; i++) {
      final byte[] sample = new byte[1 + rnd.nextInt(300)];
      rnd.nextBytes(sample);
      final byte[] encoded = FSSTCompressor.encode(sample, symbols);
      assertArrayEquals(sample, FSSTCompressor.decode(encoded, symbols), "fuzz round-trip mismatch at iteration " + i);
    }
  }

  /**
   * The encoder has two match sources — a linear scan for one-off short inputs and the
   * two-byte-bucket matcher for batches — and the decoder cannot tell which produced the bytes, so
   * identical output is a wire-format requirement, not an optimization detail. The long concatenated
   * samples force multi-byte bucket hits and the beyond-prefix tail compare on symbols longer than
   * four bytes, which the short-sample tests never reach.
   */
  @Test
  @DisplayName("linear and matcher encode paths emit identical bytes")
  void linearAndMatcherPathsEmitIdenticalBytes() {
    final List<byte[]> samples = corpus();
    final StringBuilder longSample = new StringBuilder(1200);
    for (int i = 0; i < 8; i++) {
      longSample.append(new String(samples.get(i), StandardCharsets.UTF_8));
    }
    samples.add(longSample.toString().getBytes(StandardCharsets.UTF_8));
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    final byte[][] matcherIdentity = FSSTCompressor.parseSymbolTable(table);
    for (final byte[] sample : samples) {
      // A FRESH parse per sample keeps the linear side on the linear path: streak promotion
      // counts consecutive encodes against one array identity, so reusing an identity across
      // the loop would silently build a matcher after a few samples and compare the matcher
      // path against itself. A new identity resets the streak every time.
      final byte[][] linearIdentity = FSSTCompressor.parseSymbolTable(table);
      final byte[] viaLinear = FSSTCompressor.encode(sample, linearIdentity);
      byte[] viaMatcher = null;
      for (int warm = 0; warm < 8; warm++) {
        viaMatcher = FSSTCompressor.encode(sample, matcherIdentity);
      }
      assertArrayEquals(viaLinear, viaMatcher,
          "linear and matcher paths diverged on: " + new String(sample, StandardCharsets.UTF_8));
      assertArrayEquals(sample, FSSTCompressor.decode(viaMatcher, matcherIdentity),
          "matcher-path round-trip mismatch on: " + new String(sample, StandardCharsets.UTF_8));
    }
  }

  @Test
  @DisplayName("region-shaped text compresses by at least a third")
  void textCompressesSubstantially() {
    final List<byte[]> samples = corpus();
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    final byte[][] symbols = FSSTCompressor.parseSymbolTable(table);
    long raw = 0;
    long encoded = 0;
    for (final byte[] sample : samples) {
      raw += sample.length;
      encoded += FSSTCompressor.encode(sample, symbols).length;
    }
    final double savings = 1.0 - (double) encoded / raw;
    assertTrue(savings >= 0.35,
        String.format(Locale.ROOT, "savings %.3f below the 0.35 floor (raw=%d encoded=%d) — the encoder has regressed "
            + "toward the state where FSST silently never engages", savings, raw, encoded));
  }
}
