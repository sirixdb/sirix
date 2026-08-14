/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Date;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Flt;
import io.brackit.query.atomic.Int;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.atomic.Time;
import io.brackit.query.jdm.Type;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Edge cases of the CAS key encoding, at the unit level.
 *
 * <p>
 * The invariant every test here serves: the two sides of a CAS index must agree. {@code
 * CASIndexBuilder} stores every indexed value as a {@link Str} built from the node's lexical form,
 * while {@code ScanCASIndex} casts the query's argument to the index's declared content type — so
 * the STORED side arrives as a string and the PROBE side as a typed atomic. Any encoder behaviour
 * that differs between those two shapes splits one logical value across two keys (the query finds
 * nothing) or merges two logical values onto one key (the query returns rows it should not).
 * </p>
 *
 * <p>
 * Every case below is expressed as a comparison between two encodings rather than against a golden
 * byte string, deliberately: a golden vector pins the CURRENT encoding, which is not the property
 * that matters and would have to be rewritten the next time the layout changes. Agreement and
 * disagreement between the two sides is the property that matters, and it survives a re-layout.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class CASKeySerializerEdgeCaseTest {

  private static final long PCR = 42L;

  /** Header is 8 bytes of sign-flipped pathNodeKey plus a 2-byte type id. */
  private static final int HEADER_BYTES = 10;

  /**
   * The encoded key for {@code value} under {@code type}, header included — the header is identical
   * across every comparison here (same PCR, same type), so including it costs nothing and keeps the
   * helper honest about what the index actually stores.
   */
  private static byte[] key(final Atomic value, final Type type) {
    final byte[] buffer = new byte[256];
    final int length = CASKeySerializer.INSTANCE.serialize(new CASValue(value, type, PCR), buffer, 0);
    return Arrays.copyOf(buffer, length);
  }

  @Nested
  @DisplayName("boolean")
  final class Booleans {

    @Test
    @DisplayName("the stored lexical form and the typed probe agree, for both values")
    void bothSidesAgree() {
      // The defect this pins: the encoder used Atomic#booleanValue(), XQuery's EFFECTIVE boolean
      // value, which on a non-empty Str is true regardless of what the string says. So the stored
      // "false" encoded as true and no probe of false() could ever reach it.
      assertArrayEquals(key(new Bool(false), Type.BOOL), key(new Str("false"), Type.BOOL),
          "stored \"false\" must encode like a probe of false()");
      assertArrayEquals(key(new Bool(true), Type.BOOL), key(new Str("true"), Type.BOOL),
          "stored \"true\" must encode like a probe of true()");
    }

    @Test
    @DisplayName("true and false do not collide")
    void theTwoValuesAreDistinct() {
      // The control. An encoder mapping everything to byte 0 would satisfy the agreement test above
      // while merging both values onto one key — which is the original defect with the sign flipped.
      assertFalse(Arrays.equals(key(new Str("true"), Type.BOOL), key(new Str("false"), Type.BOOL)),
          "true and false must not share a key");
    }

    @Test
    @DisplayName("the numeric lexical forms of xs:boolean are honoured")
    void theNumericLexicalFormsWork() {
      // "1" and "0" are as much a part of the xs:boolean lexical space as "true"/"false", and a
      // document may spell them either way.
      assertArrayEquals(key(new Bool(true), Type.BOOL), key(new Str("1"), Type.BOOL));
      assertArrayEquals(key(new Bool(false), Type.BOOL), key(new Str("0"), Type.BOOL));
    }

    @Test
    @DisplayName("surrounding whitespace does not change the value")
    void whitespaceIsIgnored() {
      assertArrayEquals(key(new Bool(true), Type.BOOL), key(new Str("  true  "), Type.BOOL));
    }
  }

  @Nested
  @DisplayName("float")
  final class Floats {

    @Test
    @DisplayName("the stored lexical form and the typed probe agree on a non-dyadic value")
    void bothSidesAgree() {
      // 1.1 is the case that mattered: the stored Str took Double.parseDouble("1.1") = 1.1d while the
      // Flt probe gave (double) 1.1f = 1.100000023841858, so the keys differed in the low bits and
      // `eq 1.1` found nothing at all. Rounding both through float makes them meet.
      assertArrayEquals(key(new Flt(1.1f), Type.FLO), key(new Str("1.1"), Type.FLO));
    }

    @Test
    @DisplayName("a dyadic value agrees too, which it always did")
    void aDyadicValueAgrees() {
      // The control that shows the test above is about float/double disagreement and not about
      // encoding being broken outright: 2.5 is exact in both widths and never disagreed.
      assertArrayEquals(key(new Flt(2.5f), Type.FLO), key(new Str("2.5"), Type.FLO));
    }

    @Test
    @DisplayName("distinct float values keep distinct keys")
    void distinctValuesStayDistinct() {
      assertFalse(Arrays.equals(key(new Str("1.1"), Type.FLO), key(new Str("1.2"), Type.FLO)));
    }

    @Test
    @DisplayName("a value that overflows float does not collide with NaN's key")
    void anOverflowingValueKeepsItsOwnKey() {
      // The float rounding happens BEFORE the NaN canonicalization for this reason: 1e40 is finite as
      // a double but infinite as a float, and NaN is canonicalized onto Double.MAX_VALUE's key. If
      // the order were reversed the infinity would be indistinguishable from a real value.
      assertFalse(Arrays.equals(key(new Str("1e40"), Type.FLO), key(new Dbl(Double.NaN), Type.FLO)),
          "a float overflow must not land on the NaN key");
    }
  }

  @Nested
  @DisplayName("integer")
  final class Integers {

    /** Beyond {@code Long.MAX_VALUE}, so {@code Long.parseLong} refuses it. */
    private static final String ABOVE_LONG_RANGE = "92233720368547758070";

    private static final String BELOW_LONG_RANGE = "-92233720368547758070";

    @Test
    @DisplayName("a value beyond long range saturates instead of landing on zero's key")
    void outOfRangeSaturates() {
      // The defect: the parse fallback returned 0, putting an enormous value in the MIDDLE of the key
      // space. `eq 0` returned it, and every range query placed it on the wrong side of every bound.
      assertFalse(Arrays.equals(key(new Str(ABOVE_LONG_RANGE), Type.INR), key(new Int32(0), Type.INR)),
          "an out-of-range integer must not share zero's key");
      assertArrayEquals(key(new Int64(Long.MAX_VALUE), Type.INR), key(new Str(ABOVE_LONG_RANGE), Type.INR),
          "it saturates to the end of the key space it belongs to");
    }

    @Test
    @DisplayName("saturation is signed, so the two ends do not meet")
    void saturationRespectsSign() {
      assertArrayEquals(key(new Int64(Long.MIN_VALUE), Type.INR), key(new Str(BELOW_LONG_RANGE), Type.INR));
      assertFalse(Arrays.equals(key(new Str(ABOVE_LONG_RANGE), Type.INR), key(new Str(BELOW_LONG_RANGE), Type.INR)));
    }

    @Test
    @DisplayName("in-range values keep byte order matching numeric order across the sign boundary")
    void byteOrderMatchesNumericOrder() {
      // The sign flip exists for this. Unsigned byte comparison over two's complement puts negatives
      // above positives without it, which would invert every range query crossing zero.
      final byte[] negative = key(new Int64(-1L), Type.INR);
      final byte[] zero = key(new Int64(0L), Type.INR);
      final byte[] positive = key(new Int64(1L), Type.INR);
      assertTrue(Arrays.compareUnsigned(negative, zero) < 0, "-1 must sort below 0");
      assertTrue(Arrays.compareUnsigned(zero, positive) < 0, "0 must sort below 1");
    }

    @Test
    @DisplayName("the stored lexical form and the typed probe agree")
    void bothSidesAgree() {
      assertArrayEquals(key(new Int64(1234567890123L), Type.INR), key(new Str("1234567890123"), Type.INR));
    }

    @Test
    @DisplayName("both sides agree BEYOND long range, where one saturated and the other wrapped")
    void bothSidesAgreeOutOfRange() {
      // The regression that saturating only ONE side introduced, and it was worse than the defect it
      // replaced. CASIndexBuilder stores a Str, so the stored side took the lexical branch and
      // saturated to Long.MAX_VALUE; ScanCASIndex casts the probe, which for a magnitude past long
      // range is brackit's arbitrary-precision Int, whose longValue() is BigDecimal#longValue() and
      // WRAPS — 2^63 to Long.MIN_VALUE, 2^70 to 0. The two sides landed at OPPOSITE ends of the key
      // space, so an equality query that used to match (both collapsing onto zero) began missing
      // outright. Cast.cast is not invoked here; Int is constructed directly, which is what it yields.
      final Atomic probe = new Int(new BigDecimal(ABOVE_LONG_RANGE));
      assertArrayEquals(key(new Str(ABOVE_LONG_RANGE), Type.INR), key(probe, Type.INR),
          "the stored value and the probe must land on the same key");
    }

    @Test
    @DisplayName("a wrapping probe does not land below zero")
    void aWrappingProbeDoesNotInvert() {
      // The sharpest form: 2^63 wraps to Long.MIN_VALUE, the very BOTTOM of the key space, so before
      // the fix `>= 2^63` matched the entire index and `eq 2^63` matched nothing.
      final Atomic probe = new Int(new BigDecimal("9223372036854775808"));
      assertTrue(Arrays.compareUnsigned(key(probe, Type.INR), key(new Int64(0L), Type.INR)) > 0,
          "a value above long range must sort ABOVE zero, not below it");
    }
  }

  @Nested
  @DisplayName("losesInformation")
  final class LossReporting {

    @Test
    @DisplayName("a null bound loses nothing")
    void nullIsLossless() {
      assertFalse(CASKeySerializer.losesInformation(null, Type.STR));
      assertFalse(CASKeySerializer.truncates(null, Type.STR));
    }

    @Test
    @DisplayName("every decimal is reported lossy, including the ones a double represents exactly")
    void everyDecimalIsLossy() {
      // 0.5 IS exactly a double, and the round-trip test that used to stand here therefore reported
      // it lossless and switched the re-check off — while a stored 0.5000000000000000001 encodes to
      // the same double. The predicate cannot see stored values, so probe-only reasoning is unsound
      // for a narrowing encoder and the only sound answer is "always".
      assertTrue(CASKeySerializer.losesInformation(new Dec(new BigDecimal("0.5")), Type.DEC),
          "a dyadic decimal is still a collision risk");
      assertTrue(CASKeySerializer.losesInformation(new Dec(new BigDecimal("19.99")), Type.DEC));
    }

    @Test
    @DisplayName("an ordinary double or float is NOT reported lossy")
    void doublesAreLossless() {
      // The carve-out that keeps these indexes fast: for xs:double the encoder IS the double, so
      // equality on the index is double equality and the key round-trips by construction. Reporting
      // these lossy sent every such query through a per-candidate document re-read.
      assertFalse(CASKeySerializer.losesInformation(new Dbl(0.1), Type.DBL));
      assertFalse(CASKeySerializer.losesInformation(new Flt(0.1f), Type.FLO));
    }

    @Test
    @DisplayName("NaN is reported lossy, because it is canonicalized onto a real value's key")
    void nanIsLossy() {
      assertTrue(CASKeySerializer.losesInformation(new Dbl(Double.NaN), Type.DBL));
    }

    @Test
    @DisplayName("an in-range integer is lossless and an out-of-range one is not")
    void integersReportByRange() {
      assertFalse(CASKeySerializer.losesInformation(new Int64(1234567890123L), Type.INR),
          "an ordinary integer round-trips exactly and pays nothing");
      assertTrue(CASKeySerializer.losesInformation(new Dec(new BigDecimal("92233720368547758070")), Type.INR),
          "beyond long range two values share the saturated key");
      assertTrue(CASKeySerializer.losesInformation(new Int64(Long.MAX_VALUE), Type.INR),
          "and the sentinel itself is shared with them, so it needs the re-check too");
    }
  }

  @Nested
  @DisplayName("truncates")
  final class Truncation {

    @Test
    @DisplayName("the boundary is at the cap, not past it")
    void theBoundaryIsInclusive() {
      // >= and not >, and the difference is a real over-match rather than a rounding preference. A
      // value measuring EXACTLY the cap is stored losslessly, but every LONGER value is capped to the
      // same 246 bytes — so a 250-byte value sharing the prefix produces a byte-identical key of
      // identical length, and nothing downstream separates them. At exactly the cap the seek is
      // therefore guaranteed to over-match, which is precisely where a `>` test switched the caller's
      // re-check off.
      assertFalse(CASKeySerializer.truncates(new Str("A".repeat(245)), Type.STR), "one below the cap is safe");
      assertTrue(CASKeySerializer.truncates(new Str("A".repeat(246)), Type.STR), "exactly at the cap collides");
      assertTrue(CASKeySerializer.truncates(new Str("A".repeat(247)), Type.STR));
    }

    @Test
    @DisplayName("the cap is measured in UTF-8 bytes, not characters")
    void multiByteCharactersCountTheirBytes() {
      // 82 three-byte characters measure exactly 246 bytes, so this collides while its character
      // count (82) is nowhere near the cap. Measuring characters would report it safe.
      assertTrue(CASKeySerializer.truncates(new Str("中".repeat(82)), Type.STR));
      assertFalse(CASKeySerializer.truncates(new Str("中".repeat(81)), Type.STR));
    }

    @Test
    @DisplayName("a type with no id truncates exactly as a string does")
    void unrecognizedTypesTruncateToo() {
      // The rule is "does this reach the truncating branch", NOT "is this xs:string". Every type
      // getTypeId does not recognize falls into the encoder's default case and is capped identically;
      // testing for xs:string alone left that whole family answering with an unchecked superset.
      assertTrue(CASKeySerializer.truncates(new Str("A".repeat(300)), Type.AURI));
      assertFalse(CASKeySerializer.truncates(new Str("short"), Type.AURI));
    }

    @Test
    @DisplayName("a numeric bound never reports truncation, however lossy it is")
    void numericBoundsDoNotTruncate() {
      // The split that keeps range queries on the bounded cursor. Numeric narrowing is monotone, so
      // the cursor still places every stored key correctly against the bound; only truncation breaks
      // the ordering. A range caller consulting losesInformation instead paid an O(index) scan to
      // reach the identical answer.
      assertFalse(CASKeySerializer.truncates(new Dec(new BigDecimal("19.99")), Type.DEC));
      assertTrue(CASKeySerializer.losesInformation(new Dec(new BigDecimal("19.99")), Type.DEC),
          "the same bound IS lossy for equality, which is why the two predicates are separate");
    }

    @Test
    @DisplayName("the empty string is not truncation")
    void theEmptyStringIsFine() {
      assertFalse(CASKeySerializer.truncates(new Str(""), Type.STR));
    }
  }

  @Nested
  @DisplayName("type predicates")
  final class TypePredicates {

    @Test
    @DisplayName("the numeric family is exactly the four narrowing encoders")
    void numericFamilyMembership() {
      assertTrue(CASKeySerializer.isNumericFamily(Type.INR));
      assertTrue(CASKeySerializer.isNumericFamily(Type.DEC));
      assertTrue(CASKeySerializer.isNumericFamily(Type.DBL));
      assertTrue(CASKeySerializer.isNumericFamily(Type.FLO));
      assertFalse(CASKeySerializer.isNumericFamily(Type.STR));
      assertFalse(CASKeySerializer.isNumericFamily(Type.BOOL));
    }

    @Test
    @DisplayName("the families the encoder orders deliberately ARE byte-order-preserving")
    void orderedFamiliesQualify() {
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.STR));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.INR));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.DEC));
      assertTrue(CASKeySerializer.isByteOrderPreserving(Type.DBL));
    }
  }

  /**
   * The instant family, whose encoding is the one place where two comments in the serializer disagree
   * with the code and with each other.
   *
   * <p>
   * {@code isByteOrderPreserving} RETURNS true for these types, and the encoder routes them through
   * {@code InstantKeyCodec}'s binary form — but that predicate's own javadoc says it returns false
   * for them, and a block comment at the top of the class says the binary codec "was wrong and is
   * reverted to the lexical form". Both statements are false about the shipped code. Rather than
   * trust any of the three, these assert the two properties that actually matter, so whichever
   * comment survives has to match measured behaviour.
   * </p>
   *
   * <p>
   * INJECTIVITY is the load-bearing one. Two distinct values sharing a key merge their posting lists,
   * which corrupts equality lookups and deletes, not merely ranges — so a collision is far worse than
   * a mis-ordered range. The cases below are exactly the ones the stale comment claimed collided.
   * </p>
   */
  @Nested
  @DisplayName("instants")
  final class Instants {

    @Test
    @DisplayName("a timezoned date does not collide with the previous day in UTC")
    void datesWithOffsetsStayDistinct() {
      // The stale comment's first claim: canonicalizing to UTC moves the offset into a time-of-day
      // that xs:date cannot hold, so 2020-01-01+02:00 was said to encode identically to 2019-12-31Z
      // — a collision, and the reason the codec was supposedly reverted.
      assertFalse(Arrays.equals(key(new Date("2020-01-01+02:00"), Type.DATE), key(new Date("2019-12-31Z"), Type.DATE)),
          "distinct dates must not share a key");
    }

    @Test
    @DisplayName("a timezoned time orders the same way brackit compares it")
    void timesOrderChronologically() {
      // The stale comment's second claim: the reference-date comparison carries a rollover xs:time
      // cannot hold, so byte order came out INVERTED against compareTo for a non-UTC offset.
      final Time earlier = new Time("08:00:00Z");
      final Time later = new Time("10:00:00Z");
      final int byValue = earlier.compareTo(later);
      final int byBytes = Arrays.compareUnsigned(key(earlier, Type.TIME), key(later, Type.TIME));
      assertTrue(byValue < 0 && byBytes < 0 || byValue > 0 && byBytes > 0,
          "byte order must agree with brackit's own comparison, not invert it");
    }

    @Test
    @DisplayName("dateTimes order chronologically in byte order")
    void dateTimesOrderChronologically() {
      // This is what isByteOrderPreserving == true actually promises, and what puts these types on
      // the bounded cursor. If it does not hold, a range query silently drops records.
      final byte[] earlier = key(new DateTime("2020-01-01T00:00:00Z"), Type.DATI);
      final byte[] later = key(new DateTime("2020-06-01T00:00:00Z"), Type.DATI);
      assertTrue(Arrays.compareUnsigned(earlier, later) < 0, "an earlier instant must sort first");
    }

    @Test
    @DisplayName("the stored lexical form and the typed probe agree")
    void bothSidesAgree() {
      // The same two-sides invariant as every other family: the builder stores a Str, the scan casts
      // the probe. InstantKeyCodec documents that it coerces a raw Str, and this is what pins it.
      assertArrayEquals(key(new DateTime("2020-01-01T12:00:00Z"), Type.DATI),
          key(new Str("2020-01-01T12:00:00Z"), Type.DATI));
    }
  }

  @Test
  @DisplayName("the empty string encodes to a bare header and sorts below every non-empty value")
  void theEmptyStringIsABareHeader() {
    final byte[] empty = key(new Str(""), Type.STR);
    final byte[] nonEmpty = key(new Str("a"), Type.STR);
    assertTrue(empty.length == HEADER_BYTES, "no value bytes at all");
    assertTrue(Arrays.compareUnsigned(empty, nonEmpty) < 0, "and it sorts first, which is correct");
  }
}
