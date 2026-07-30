/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The FSST encoder's two contracts: decode(encode(x)) == x for arbitrary bytes, and a real
 * compression ratio on the text the string region actually holds.
 *
 * <p>The ratio floor is the load-bearing assertion. The previous encoder round-tripped perfectly
 * and compressed English text by about one percent — single-byte symbols were never candidates,
 * so under the escape-doubles-a-byte scheme almost every position expanded, and the benefit gate
 * silently vetoed FSST for entire workloads. Nothing failed; the feature just never engaged. A
 * floor turns that silent regression into a red test.
 */
@DisplayName("FSST encoder quality")
public final class FsstCompressorQualityTest {

  /** Region-shaped corpus: distinct values, heavy shared structure — URLs, titles, prose. */
  private static List<byte[]> corpus() {
    final List<byte[]> samples = new ArrayList<>(600);
    final Random rnd = new Random(0xF557);
    final String[] cities = { "Springfield", "Riverton", "Lakewood", "Fairview", "Greenville" };
    for (int i = 0; i < 200; i++) {
      samples.add(("https://media.example.org/catalog/items/product-" + rnd.nextInt(100_000)
          + "/images/main.jpg?size=large&format=webp").getBytes(StandardCharsets.UTF_8));
      samples.add(("A restless drama about a family in " + cities[i % cities.length]
          + " confronting the past, directed with patience and a fine eye for the small "
          + "moments that make a life.").getBytes(StandardCharsets.UTF_8));
      samples.add(("item-" + i + "/rev=" + rnd.nextInt(9) + "/state=published")
          .getBytes(StandardCharsets.UTF_8));
    }
    return samples;
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
      assertArrayEquals(sample, FSSTCompressor.decode(encoded, symbols),
          "fuzz round-trip mismatch at iteration " + i);
    }
  }

  /**
   * The encoder has two match sources — a linear scan for one-off short inputs and the
   * two-byte-bucket matcher for batches — and the decoder cannot tell which produced the
   * bytes, so identical output is a wire-format requirement, not an optimization detail. The
   * long concatenated samples force multi-byte bucket hits and the beyond-prefix tail compare
   * on symbols longer than four bytes, which the short-sample tests never reach.
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
        String.format(Locale.ROOT,
            "savings %.3f below the 0.35 floor (raw=%d encoded=%d) — the encoder has regressed "
                + "toward the state where FSST silently never engages", savings, raw, encoded));
  }
}
