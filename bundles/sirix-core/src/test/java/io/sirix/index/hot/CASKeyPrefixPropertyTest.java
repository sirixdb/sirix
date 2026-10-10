package io.sirix.index.hot;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Date;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Flt;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.atomic.Time;
import io.brackit.query.jdm.Type;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

final class CASKeyPrefixPropertyTest {
  @Provide
  Arbitrary<String> strings() {
    return Arbitraries.strings().withChars('\0', '\1', 'a', 'b', 'z', '\u00e9', '\uffff').ofMaxLength(60);
  }

  @Property(tries = 1000, seed = "31032026")
  void stringsIncludingTrailingNulsStayOrderedAndPrefixFree(@ForAll("strings") final String a,
      @ForAll("strings") final String b) {
    assertOrder(new Str(a), new Str(b), Type.STR);
    assertOrder(new Str(a), new Str(a + "\0"), Type.STR);
    assertOrder(new Str(a), new Str(a + "\0\0\0\0"), Type.STR);
  }

  @Property(tries = 1000, seed = "31032027")
  void allBinaryAtomicFamiliesKeepTheirOrder(@ForAll @IntRange(min = -10000, max = 10000) final int a,
      @ForAll @IntRange(min = -10000, max = 10000) final int b) {
    assertOrder(new Int64(a), new Int64(b), Type.LON);
    assertOrder(new Dbl(a / 4.0), new Dbl(b / 4.0), Type.DBL);
    assertOrder(new Flt(a / 4.0f), new Flt(b / 4.0f), Type.FLO);
    assertOrder(new Dec(a + ".25"), new Dec(b + ".25"), Type.DEC);
    assertOrder(a > 0
        ? Bool.TRUE
        : Bool.FALSE,
        b > 0
            ? Bool.TRUE
            : Bool.FALSE,
        Type.BOOL);
    final String dayA = "2000-01-" + String.format("%02d", 1 + Math.floorMod(a, 28));
    final String dayB = "2000-01-" + String.format("%02d", 1 + Math.floorMod(b, 28));
    assertOrder(new Date(dayA + "Z"), new Date(dayB + "Z"), Type.DATE);
    assertOrder(new DateTime(dayA + "T00:00:00Z"), new DateTime(dayB + "T00:00:00Z"), Type.DATI);
    assertOrder(new Time(String.format("%02d:00:00Z", Math.floorMod(a, 24))),
        new Time(String.format("%02d:00:00Z", Math.floorMod(b, 24))), Type.TIME);
  }

  private static void assertOrder(final Atomic a, final Atomic b, final Type type) {
    final CASValue keyA = new CASValue(a, type, 7);
    final CASValue keyB = new CASValue(b, type, 7);
    final byte[] encodedA = encode(keyA);
    final byte[] encodedB = encode(keyB);
    // Boolean value order is false < true; do not use Brackit's inverted Bool.compareTo here.
    final int order = Type.BOOL.equals(type)
        ? Boolean.compare(a.booleanValue(), b.booleanValue())
        : Integer.signum(keyA.compareTo(keyB));
    assertEquals(order, Integer.signum(Arrays.compareUnsigned(encodedA, encodedB)), type.toString());
    if (order != 0) {
      assertFalse(Arrays.equals(encodedA, 0, Math.min(encodedA.length, encodedB.length), encodedB, 0,
          Math.min(encodedA.length, encodedB.length)), "distinct logical keys cannot prefix one another");
    }
    assertEquals(0, keyA.compareTo(CASKeySerializer.INSTANCE.deserialize(encodedA, 0, encodedA.length)));
  }

  private static byte[] encode(final CASValue key) {
    final byte[] buffer = new byte[CASKeySerializer.INSTANCE.maxSerializedLength(key)];
    return Arrays.copyOf(buffer, CASKeySerializer.INSTANCE.serialize(key, buffer, 0));
  }
}
