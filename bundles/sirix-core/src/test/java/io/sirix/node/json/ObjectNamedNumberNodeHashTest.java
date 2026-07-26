package io.sirix.node.json;

import io.sirix.node.Bytes;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Regression: a fused OBJECT_NAMED_NUMBER record must hash its value TYPE/PRECISION-faithfully. The
 * old hash collapsed every value to {@code doubleValue()}, so the optimized HASHED diff (the
 * production {@code GET /diff} path for ROLLING hashes) silently missed real numeric changes.
 */
public final class ObjectNamedNumberNodeHashTest {

  private static final long NULL = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final LongHashFunction HF = LongHashFunction.xx();

  private static long hash(final Number value) {
    final var node =
        new ObjectNamedNumberNode(2L, 1L, NULL, NULL, 0, 0L, 0, 1, 0L, value, HF, (byte[]) null);
    return node.computeHash(Bytes.threadLocalHashBuffer());
  }

  @Test
  public void integerAndDoubleOfSameNumericValueHashDifferently() {
    // 10 (Integer) vs 10.0 (Double): collapsed to doubleValue() -> identical hash -> missed change.
    assertNotEquals(hash(Integer.valueOf(10)), hash(Double.valueOf(10.0)));
  }

  @Test
  public void distinctLongsSharingADoubleHashDifferently() {
    // 2^53 and 2^53+1 are distinct longs that round to the same double.
    assertNotEquals(hash(Long.valueOf(9007199254740992L)), hash(Long.valueOf(9007199254740993L)));
  }

  @Test
  public void equalValuesHashEqually() {
    assertEquals(hash(Integer.valueOf(42)), hash(Integer.valueOf(42)));
    assertEquals(hash(Double.valueOf(3.5)), hash(Double.valueOf(3.5)));
  }
}
