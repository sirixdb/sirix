package io.sirix.node;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fuzz tests for {@link DeweyIDEncoder}.
 *
 * <p>Uses {@link SirixDeweyID#toBytes()} as a reference oracle.
 * Every random DeweyID is encoded via both implementations and compared byte-by-byte.
 *
 * <p>On failure the seed is printed for reproducibility.
 */
class DeweyIDEncoderFuzzTest {

  private static final HexFormat HEX = HexFormat.of();

  // Tier boundaries (from DeweyIDEncoder)
  private static final int TIER_0_MAX = 127;
  private static final int TIER_1_MAX = 16511;
  private static final int TIER_2_MAX = 2113663;
  private static final int TIER_3_MAX = 270549119;

  // ==================== ORACLE FUZZ: DeweyIDEncoder vs SirixDeweyID ====================

  @RepeatedTest(500)
  void fuzzEncoderMatchesOracle() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int depth = rng.nextInt(8) + 1; // 1..8 levels
    final int[] divisions = new int[depth + 1];
    divisions[0] = 1; // root
    final StringBuilder sb = new StringBuilder("1");

    for (int i = 1; i <= depth; i++) {
      divisions[i] = randomDivisionValue(rng);
      sb.append('.').append(divisions[i]);
    }

    final String deweyStr = sb.toString();

    try {
      // Oracle: SirixDeweyID.toBytes()
      final SirixDeweyID id = new SirixDeweyID(deweyStr);
      final byte[] oracle = id.toBytes();

      // Under test: DeweyIDEncoder.encode()
      final byte[] encoded = DeweyIDEncoder.encode(divisions);

      if (!Arrays.equals(oracle, encoded)) {
        fail("Encoding mismatch for '" + deweyStr + "' [seed=" + seed + "]"
            + "\n  divisions: " + Arrays.toString(divisions)
            + "\n  oracle:    " + HEX.formatHex(oracle)
            + "\n  encoded:   " + HEX.formatHex(encoded));
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception for '" + deweyStr + "' [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  @RepeatedTest(200)
  void fuzzEncodeToBufferMatchesEncode() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int depth = rng.nextInt(8) + 1;
    final int[] divisions = new int[depth + 1];
    divisions[0] = 1;
    for (int i = 1; i <= depth; i++) {
      divisions[i] = randomDivisionValue(rng);
    }

    try {
      final byte[] encoded = DeweyIDEncoder.encode(divisions);

      // Verify encodeToBuffer produces the same
      final byte[] buffer = new byte[128];
      final int length = DeweyIDEncoder.encodeToBuffer(divisions, buffer, 0);

      assertEquals(encoded.length, length,
          "encodeToBuffer length mismatch [seed=" + seed + "]");
      assertArrayEquals(encoded, Arrays.copyOf(buffer, length),
          "encodeToBuffer content mismatch [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== ROUNDTRIP FUZZ: encode → SirixDeweyID(bytes) ====================

  @RepeatedTest(500)
  void fuzzEncodeDecodeRoundtrip() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int depth = rng.nextInt(8) + 1;
    final int[] divisions = new int[depth + 1];
    divisions[0] = 1;
    final StringBuilder sb = new StringBuilder("1");
    for (int i = 1; i <= depth; i++) {
      divisions[i] = randomDivisionValue(rng);
      sb.append('.').append(divisions[i]);
    }

    final String original = sb.toString();

    try {
      final byte[] encoded = DeweyIDEncoder.encode(divisions);

      if (encoded.length == 0) {
        // Root-only DeweyID encodes to 0 bytes — this is expected for "1"
        return;
      }

      // Decode back via SirixDeweyID(byte[])
      final SirixDeweyID decoded = new SirixDeweyID(encoded);
      final int[] decodedDivisions = decoded.getDivisionValues();

      if (!Arrays.equals(divisions, decodedDivisions)) {
        fail("Roundtrip division mismatch for '" + original + "' [seed=" + seed + "]"
            + "\n  original: " + Arrays.toString(divisions)
            + "\n  decoded:  " + Arrays.toString(decodedDivisions)
            + "\n  bytes:    " + HEX.formatHex(encoded));
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception for '" + original + "' [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== TIER BOUNDARY STRESS ====================

  @RepeatedTest(200)
  void fuzzTierBoundaryValues() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    // Generate a DeweyID with values near tier boundaries
    final int depth = rng.nextInt(5) + 1;
    final int[] divisions = new int[depth + 1];
    divisions[0] = 1;
    final StringBuilder sb = new StringBuilder("1");

    for (int i = 1; i <= depth; i++) {
      divisions[i] = randomBoundaryValue(rng);
      sb.append('.').append(divisions[i]);
    }

    final String deweyStr = sb.toString();

    try {
      final SirixDeweyID id = new SirixDeweyID(deweyStr);
      final byte[] oracle = id.toBytes();
      final byte[] encoded = DeweyIDEncoder.encode(divisions);

      if (!Arrays.equals(oracle, encoded)) {
        fail("Tier boundary mismatch for '" + deweyStr + "' [seed=" + seed + "]"
            + "\n  oracle:  " + HEX.formatHex(oracle)
            + "\n  encoded: " + HEX.formatHex(encoded));
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception for '" + deweyStr + "' [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== BITS CALCULATION CONSISTENCY ====================

  @RepeatedTest(200)
  void fuzzCalculateTotalBitsMatchesActualEncoding() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int depth = rng.nextInt(10) + 1;
    final int[] divisions = new int[depth + 1];
    divisions[0] = 1;
    for (int i = 1; i <= depth; i++) {
      divisions[i] = randomDivisionValue(rng);
    }

    try {
      final int calculatedBits = DeweyIDEncoder.calculateTotalBits(divisions);
      final byte[] encoded = DeweyIDEncoder.encode(divisions);

      // Encoded length should be ceil(calculatedBits / 8)
      final int expectedBytes = (calculatedBits + 7) >>> 3;
      assertEquals(expectedBytes, encoded.length,
          "calculateTotalBits inconsistent with encode length [seed=" + seed + "]"
              + "\n  bits=" + calculatedBits + ", bytes=" + encoded.length
              + "\n  divisions=" + Arrays.toString(divisions));
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== getTier / getBitsForValue CONSISTENCY ====================

  @RepeatedTest(200)
  void fuzzGetTierVsGetBitsConsistency() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int value = randomDivisionValue(rng);

    final int tier = DeweyIDEncoder.getTier(value);
    final int bits = DeweyIDEncoder.getBitsForValue(value);

    // Tier↔bits mapping
    final int expectedBits = switch (tier) {
      case 0 -> 8;
      case 1 -> 16;
      case 2 -> 24;
      case 3 -> 32;
      case 4 -> 35;
      default -> -1;
    };

    assertEquals(expectedBits, bits,
        "getTier/getBitsForValue inconsistency [seed=" + seed + ", value=" + value
            + ", tier=" + tier + "]");
    assertTrue(tier >= 0 && tier <= 4,
        "Invalid tier [seed=" + seed + ", value=" + value + "]");
  }

  // ==================== DEEP HIERARCHY FUZZ ====================

  @RepeatedTest(50)
  void fuzzDeepHierarchy() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    final int depth = rng.nextInt(30) + 10; // 10..39 levels
    final int[] divisions = new int[depth + 1];
    divisions[0] = 1;
    final StringBuilder sb = new StringBuilder("1");
    for (int i = 1; i <= depth; i++) {
      // Keep values small for deep hierarchies (typical DeweyID pattern)
      divisions[i] = rng.nextInt(200);
      sb.append('.').append(divisions[i]);
    }

    final String deweyStr = sb.toString();

    try {
      final SirixDeweyID id = new SirixDeweyID(deweyStr);
      final byte[] oracle = id.toBytes();
      final byte[] encoded = DeweyIDEncoder.encode(divisions);

      assertArrayEquals(oracle, encoded,
          "Deep hierarchy mismatch for depth=" + depth + " [seed=" + seed + "]");
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception for depth=" + depth + " [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== NEGATIVE VALUE REJECTION ====================

  @Test
  void negativeValueRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> DeweyIDEncoder.encode(new int[]{1, -1}));
    assertThrows(IllegalArgumentException.class,
        () -> DeweyIDEncoder.encode(new int[]{1, 5, -100}));
    assertThrows(IllegalArgumentException.class,
        () -> DeweyIDEncoder.encode(new int[]{1, Integer.MIN_VALUE}));
  }

  // ==================== UNALIGNED ENCODING STRESS ====================

  @RepeatedTest(200)
  void fuzzMixedTierUnalignedEncoding() {
    final long seed = System.nanoTime();
    final Random rng = new Random(seed);

    // Force mixed tiers to stress unaligned bit operations
    final int depth = rng.nextInt(6) + 2;
    final int[] divisions = new int[depth + 1];
    divisions[0] = 1;
    final StringBuilder sb = new StringBuilder("1");

    for (int i = 1; i <= depth; i++) {
      // Alternate tiers to maximize unaligned boundary cases
      final int tier = (i - 1) % 5;
      divisions[i] = switch (tier) {
        case 0 -> rng.nextInt(TIER_0_MAX); // 8 bits
        case 1 -> TIER_0_MAX + rng.nextInt(TIER_1_MAX - TIER_0_MAX); // 16 bits
        case 2 -> TIER_1_MAX + rng.nextInt(Math.min(TIER_2_MAX - TIER_1_MAX, 100000)); // 24 bits
        case 3 -> TIER_2_MAX + rng.nextInt(Math.min(TIER_3_MAX - TIER_2_MAX, 1000000)); // 32 bits
        case 4 -> TIER_3_MAX + rng.nextInt(1000000); // 35 bits
        default -> rng.nextInt(1000);
      };
      sb.append('.').append(divisions[i]);
    }

    final String deweyStr = sb.toString();

    try {
      final SirixDeweyID id = new SirixDeweyID(deweyStr);
      final byte[] oracle = id.toBytes();
      final byte[] encoded = DeweyIDEncoder.encode(divisions);

      if (!Arrays.equals(oracle, encoded)) {
        fail("Mixed-tier encoding mismatch [seed=" + seed + "]"
            + "\n  id:      " + deweyStr
            + "\n  oracle:  " + HEX.formatHex(oracle)
            + "\n  encoded: " + HEX.formatHex(encoded));
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]: " + e.getMessage(), e);
    }
  }

  // ==================== HELPERS ====================

  private static int randomDivisionValue(final Random rng) {
    // Weighted distribution across tiers
    final int r = rng.nextInt(10);
    if (r < 5) {
      return rng.nextInt(TIER_0_MAX); // Tier 0 (most common)
    } else if (r < 7) {
      return TIER_0_MAX + rng.nextInt(TIER_1_MAX - TIER_0_MAX); // Tier 1
    } else if (r < 9) {
      return TIER_1_MAX + rng.nextInt(Math.min(TIER_2_MAX - TIER_1_MAX, 200000)); // Tier 2
    } else {
      return TIER_2_MAX + rng.nextInt(Math.min(TIER_3_MAX - TIER_2_MAX, 2000000)); // Tier 3
    }
  }

  private static int randomBoundaryValue(final Random rng) {
    // Values near tier boundaries
    final int[] boundaries = {
        0, 1, TIER_0_MAX - 1, TIER_0_MAX, TIER_0_MAX + 1,
        TIER_1_MAX - 1, TIER_1_MAX, TIER_1_MAX + 1,
        TIER_2_MAX - 1, TIER_2_MAX, TIER_2_MAX + 1,
        TIER_3_MAX - 1, TIER_3_MAX, TIER_3_MAX + 1
    };
    final int base = boundaries[rng.nextInt(boundaries.length)];
    // Add small random perturbation
    final int perturb = rng.nextInt(5) - 2; // [-2, 2]
    return Math.max(0, base + perturb);
  }
}
