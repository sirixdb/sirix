/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.property;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Flt;
import io.brackit.query.atomic.Int;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.Tuple.Tuple2;
import net.jqwik.api.Tuple.Tuple3;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three properties a CAS key encoding has to have, checked over generated values rather than
 * chosen ones.
 *
 * <p>
 * These exist because the example-based tests next door did not find the bugs they were written
 * after. Three encodings shipped returning wrong answers — {@code xs:boolean} mapped every value
 * onto one key, {@code xs:float} put the stored and probe sides on different keys, and an
 * out-of-range {@code xs:integer} saturated on one side while wrapping on the other — and every one
 * of them is a counterexample to {@link #storedAndProbeSidesAgree} that a generator stumbles into
 * immediately. The integer one in particular needs a magnitude past {@code Long.MAX_VALUE}, which
 * is a value nobody writes down by hand and every {@code BigInteger} generator produces.
 * </p>
 *
 * <p>
 * On failure jqwik shrinks to a minimal counterexample and records the seed, so a found defect
 * stays found — which is the property the hand-written cases lacked.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
final class CASKeySerializerPropertyTest {

  /** Arbitrary, fixed: these properties are about the VALUE region, which follows the PCR prefix. */
  private static final long PCR = 42L;

  /** The serializer's string cap. Past it two values share a key BY DESIGN, so order and */
  private static final int MAX_STRING_VALUE_BYTES = 246;

  private static byte[] key(final Atomic value, final Type type) {
    final byte[] buffer = new byte[256];
    final int length = CASKeySerializer.INSTANCE.serialize(new CASValue(value, type, PCR), buffer, 0);
    return Arrays.copyOf(buffer, length);
  }

  /**
   * The strongest property here, and the one that catches all three shipped defects.
   *
   * <p>
   * {@code CASIndexBuilder} stores every indexed value as a {@link Str} built from the node's lexical
   * form, while {@code ScanCASIndex} casts the query's argument to the index's content type. So the
   * two sides of every CAS index reach the serializer in DIFFERENT shapes and must still land on one
   * key. Anything in the encoder that behaves differently for a {@code Str} than for the typed atomic
   * splits them apart, and the index silently stops matching.
   * </p>
   */
  @Property
  void storedAndProbeSidesAgree(@ForAll("typedValues") final Tuple3<Atomic, String, Type> sample) {
    final Atomic probe = sample.get1();
    final Type type = sample.get3();
    // The lexical form CASIndexBuilder would have stored, which is String.valueOf of the node's
    // primitive -- NOT the atomic's stringValue(). The distinction is load-bearing: brackit's
    // Flt#stringValue() truncates the exponent to a single digit, so 1.0E10 comes back as "1.0E1",
    // and building the stored side from it would test a value no document ever holds.
    final Atomic stored = new Str(sample.get2());

    assertArrayEquals(key(stored, type), key(probe, type),
        () -> "stored \"" + sample.get2() + "\" and probe " + probe + " of type " + type + " must share a key");
  }

  /**
   * Byte order over the encoded value is value order.
   *
   * <p>
   * Stated as MONOTONICITY rather than a strict isomorphism, because two of these encoders narrow on
   * purpose: {@code xs:decimal} funnels through {@code double} and {@code xs:float} through
   * {@code float}, so distinct values can share a key. Narrowing is order-preserving even where it is
   * not injective, and that is exactly the guarantee a bounded range cursor rests on — it may return
   * a value it cannot distinguish from the bound, but it must never place one on the wrong side.
   * </p>
   */
  @Property
  void byteOrderIsValueOrder(@ForAll("orderedPairs") final Tuple2<Tuple2<Atomic, Atomic>, Type> pair) {
    final Atomic a = pair.get1().get1();
    final Atomic b = pair.get1().get2();
    final Type type = pair.get2();

    final int valueOrder = Integer.signum(a.compareTo(b));
    final int byteOrder = Integer.signum(Arrays.compareUnsigned(key(a, type), key(b, type)));

    if (valueOrder < 0) {
      assertTrue(byteOrder <= 0,
          () -> a + " < " + b + " of type " + type + ", so its key must not sort above the other's");
    } else if (valueOrder > 0) {
      assertTrue(byteOrder >= 0,
          () -> a + " > " + b + " of type " + type + ", so its key must not sort below the other's");
    } else {
      assertTrue(byteOrder == 0, () -> a + " equals " + b + " of type " + type + ", so the keys must be equal");
    }
  }

  /**
   * Distinct values keep distinct keys, for the families that encode losslessly.
   *
   * <p>
   * The property that matters MOST, per the instant-codec history in {@code CASKeySerializer}: two
   * values sharing one key merge their posting lists, which corrupts equality lookups and deletes,
   * not merely ranges. A mis-ordered range costs the range; a collision costs the data.
   * </p>
   *
   * <p>
   * Restricted to the lossless families on purpose. {@code xs:float} narrows by design and
   * {@code xs:string} truncates past {@link #MAX_STRING_VALUE_BYTES}, so asserting injectivity there
   * would be asserting something false — which is why the equality path re-checks those candidates
   * against the real values instead.
   *
   * <p>
   * {@code xs:decimal} was in that excluded set until the key began carrying the exact value after
   * the order-preserving double. It is included now, and that inclusion IS the proof the encoding
   * change worked: the property could not even be stated for decimals before, because two values
   * differing past double precision genuinely shared a key.
   * </p>
   */
  @Property
  void distinctValuesKeepDistinctKeys(
      @ForAll("distinctLosslessPairs") final Tuple2<Tuple2<Atomic, Atomic>, Type> pair) {
    final Atomic a = pair.get1().get1();
    final Atomic b = pair.get1().get2();
    final Type type = pair.get2();

    assertNotEquals(Arrays.toString(key(a, type)), Arrays.toString(key(b, type)),
        () -> a + " and " + b + " are distinct " + type + " values and must not share a key");
  }

  // ===== generators =====

  /**
   * One value per encoder branch, typed as the probe side would arrive.
   *
   * <p>
   * The integer arbitrary deliberately reaches past {@code Long} range: that is where the saturating
   * and wrapping conversions disagreed, and it is not a region hand-written cases visit.
   * </p>
   */
  @Provide
  Arbitrary<Tuple3<Atomic, String, Type>> typedValues() {
    return Arbitraries.oneOf(
        Arbitraries.of(true, false).map(v -> Tuple.of((Atomic) new Bool(v), String.valueOf(v), Type.BOOL)),
        Arbitraries.strings()
                   .ofMaxLength(MAX_STRING_VALUE_BYTES + 40)
                   .map(v -> Tuple.of((Atomic) new Str(v), v, Type.STR)),
        Arbitraries.longs().map(v -> Tuple.of((Atomic) new Int64(v), String.valueOf(v), Type.INR)),
        bigIntegerValues().map(v -> Tuple.of((Atomic) new Int(new BigDecimal(v)), v.toString(), Type.INR)),
        decimalValues().map(v -> Tuple.of((Atomic) new Dec(v), v.toString(), Type.DEC)),
        Arbitraries.oneOf(Arbitraries.doubles(), Arbitraries.of(-0.0d, 0.0d))
                   .map(v -> Tuple.of((Atomic) new Dbl(v), String.valueOf(v), Type.DBL)),
        Arbitraries.floats().map(v -> Tuple.of((Atomic) new Flt(v), String.valueOf(v), Type.FLO)));
  }

  /**
   * Pairs over the byte-order-preserving families, strings kept inside the truncation cap.
   *
   * <p>
   * {@code xs:boolean} is EXCLUDED, and not because the encoding is wrong. brackit's
   * {@code Bool#compareTo} answers that {@code false} is GREATER than {@code true} — inverted against
   * XQuery, where {@code false() lt true()}. The encoder is right by the spec (false to byte 0, true
   * to byte 1) and the comparator is not, so asserting agreement between them here would be asserting
   * brackit's inversion. It is recorded rather than encoded: the canonical index's byte ordering and
   * {@code CASFilterRange#inRange}'s Brackit comparator disagree on boolean range order.
   * </p>
   */
  @Provide
  Arbitrary<Tuple2<Tuple2<Atomic, Atomic>, Type>> orderedPairs() {
    return Arbitraries.oneOf(pairsOf(shortStrings(), Type.STR), pairsOf(longs(), Type.INR),
        pairsOf(bigIntegers(), Type.INR), pairsOf(decimals(), Type.DEC), pairsOf(finiteDoubles(), Type.DBL),
        // The pairs that share a double, NEGATIVES INCLUDED. Their absence here is what let an
        // ordering inversion through: the colliding pairs were added to the injectivity generator
        // only, so the ordering property never saw a case where the suffix decides. Within one
        // double the suffix IS the comparison, and for negatives a naive byte compare inverts it.
        collidingDecimalPairs());
  }

  /** Pairs over the families whose encoding is injective, so a collision is a real defect. */
  @Provide
  Arbitrary<Tuple2<Tuple2<Atomic, Atomic>, Type>> distinctLosslessPairs() {
    return Arbitraries.oneOf(pairsOf(shortStrings(), Type.STR), pairsOf(longs(), Type.INR),
        pairsOf(finiteDoubles(), Type.DBL), pairsOf(booleans(), Type.BOOL), pairsOf(decimals(), Type.DEC),
        collidingDecimalPairs()).filter(p -> areDistinctValues(p.get1().get1(), p.get1().get2()));
  }

  /**
   * Whether two atomics are distinct VALUES, which is not the same question as
   * {@code compareTo != 0}.
   *
   * <p>
   * {@code Double.compare} is a TOTAL order and separates {@code -0.0} from {@code 0.0}; XQuery's
   * {@code eq} does not — {@code -0.0 eq 0.0} is true. Injectivity is a statement about equality, so
   * the zero pair must be excluded here, and in fact the encoder is required to give it ONE key:
   * canonicalizing {@code -0.0} is what stops it landing on the minimum key, below {@code -Infinity}.
   * The sibling ordering property uses {@code compareTo} precisely because ordering IS the total
   * order. The two properties disagree about this pair on purpose.
   * </p>
   */
  private static boolean areDistinctValues(final Atomic a, final Atomic b) {
    if (a instanceof final Dbl left && b instanceof final Dbl right) {
      return left.doubleValue() != right.doubleValue();
    }
    return a.compareTo(b) != 0;
  }


  /**
   * Decimal pairs that share a {@code double}, which is the ONLY interesting case for decimal
   * injectivity — and one a random generator will not produce.
   *
   * <p>
   * Without these the property is a decoration: {@code decimals()} generates at scale 6 within
   * +/-1e12, and no two such values collide, so the assertion passed just as happily over the old
   * bare-double key that could not tell them apart. Deleting the exact suffix from the encoder left
   * the whole property green until these pairs were added. That is the second time in this file a
   * generator gap hid a real defect — the first was {@code -0.0} — so the lesson is worth stating
   * where the next person will read it: a property is only as strong as the values it is fed.
   * </p>
   *
   * <p>
   * The pairs come in two SHAPES, and the second shape is here because the first alone was not
   * enough. In an EXTENSION pair one value is a textual prefix of the other, so their suffixes first
   * differ where the shorter one has ended — the comparison lands on the terminator, and only the
   * terminator's side of the encoding is under test. Every original pair was of that shape, which
   * left the digit comparison itself unexercised: perturbing the suffix bytes with an order-inverting
   * but bijective transform kept this property green, because the transform never reached a position
   * where two DIGITS met. The DIVERGENT pairs collide on the same double and differ at a digit
   * instead, in both signs, so the complement now has to be right for digits as well as for lengths.
   * </p>
   */
  private static Arbitrary<Tuple2<Tuple2<Atomic, Atomic>, Type>> collidingDecimalPairs() {
    return Arbitraries.of(
        // Extension: the shorter value's terminator meets the longer value's next digit.
        Tuple.of("0.5", "0.5000000000000000001"), Tuple.of("19.99", "19.990000000000000001"),
        Tuple.of("0.1", "0.1000000000000000001"), Tuple.of("-3.25", "-3.250000000000000001"),
        // Divergent: equal length, differing at a digit past double precision. '0' against '1' on
        // purpose — adjacent codes, so a transform that merely permutes bytes locally shows up.
        Tuple.of("0.5000000000000000001", "0.5000000000000000011"),
        Tuple.of("19.990000000000000001", "19.990000000000000011"),
        Tuple.of("-3.250000000000000001", "-3.250000000000000011"),
        Tuple.of("-0.1000000000000000011", "-0.1000000000000000001"))
                      .map(pair -> Tuple.of(Tuple.of((Atomic) new Dec(new BigDecimal(pair.get1())),
                          (Atomic) new Dec(new BigDecimal(pair.get2()))), Type.DEC));
  }

  private static <A extends Atomic> Arbitrary<Tuple2<Tuple2<Atomic, Atomic>, Type>> pairsOf(final Arbitrary<A> values,
      final Type type) {
    return Combinators.combine(values, values).as((a, b) -> Tuple.of(Tuple.of((Atomic) a, (Atomic) b), type));
  }

  private static Arbitrary<Bool> booleans() {
    return Arbitraries.of(true, false).map(Bool::new);
  }

  private static Arbitrary<Str> strings() {
    // Past the cap as well as inside it: truncation is legal, but the two SIDES must still agree,
    // which is what storedAndProbeSidesAgree asks of these.
    return Arbitraries.strings().ofMaxLength(MAX_STRING_VALUE_BYTES + 40).map(Str::new);
  }

  private static Arbitrary<Str> shortStrings() {
    // ASCII and comfortably inside the cap, so ordering and injectivity are genuine expectations.
    // A multi-byte char could push the UTF-8 length past the cap while the char count stays under it.
    return Arbitraries.strings().ascii().ofMaxLength(MAX_STRING_VALUE_BYTES / 4).map(Str::new);
  }

  private static Arbitrary<Int64> longs() {
    return Arbitraries.longs().map(Int64::new);
  }

  private static Arbitrary<BigInteger> bigIntegerValues() {
    return Arbitraries.bigIntegers()
                      .between(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(4)),
                          BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(4)));
  }

  private static Arbitrary<BigDecimal> decimalValues() {
    return Arbitraries.bigDecimals().ofScale(6).between(BigDecimal.valueOf(-1e12), BigDecimal.valueOf(1e12));
  }

  private static Arbitrary<Int> bigIntegers() {
    // Straddles Long range on purpose — the saturate-versus-wrap split lived entirely out here.
    return Arbitraries.bigIntegers()
                      .between(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(4)),
                          BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(4)))
                      .map(v -> new Int(new BigDecimal(v)));
  }

  private static Arbitrary<Dec> decimals() {
    return Arbitraries.bigDecimals()
                      .ofScale(6)
                      .between(BigDecimal.valueOf(-1e12), BigDecimal.valueOf(1e12))
                      .map(Dec::new);
  }

  private static Arbitrary<Dbl> doubles() {
    return Arbitraries.doubles().map(Dbl::new);
  }

  /**
   * Doubles with the two ZEROS forced in.
   *
   * <p>
   * {@code Arbitraries.doubles()} does not reliably produce {@code -0.0}, and its absence is exactly
   * why these properties passed over an encoder that mapped it to the minimum key — below
   * {@code -Infinity}. A generator that never emits the interesting value makes a property a
   * decoration, which is the failure mode jqwik's {@code Statistics.collect} exists to expose.
   * </p>
   */
  private static Arbitrary<Dbl> doublesWithZeros() {
    return Arbitraries.oneOf(Arbitraries.doubles(), Arbitraries.of(-0.0d, 0.0d)).map(Dbl::new);
  }

  private static Arbitrary<Dbl> finiteDoubles() {
    // NaN is canonicalized onto Double.MAX_VALUE's key by design, so it is neither ordered nor
    // injective and is excluded from those two properties rather than asserted about wrongly.
    return Arbitraries.oneOf(Arbitraries.doubles(), Arbitraries.of(-0.0d, 0.0d))
                      .filter(d -> !Double.isNaN(d))
                      .map(Dbl::new);
  }

  private static Arbitrary<Flt> floats() {
    return Arbitraries.floats().map(Flt::new);
  }
}
