package org.sirix.index.art;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class BinaryComparableUtilsTest {

  /*
    -128 to 127 available signed range
    but we want to interpret it as unsigned (since Java does not have unsigned types)
    so that we can order the bytes right.
    in the unsigned form, bytes with MSB set would come later and be "greater"
    than ones with MSB unset.
    Therefore we need to treat all bytes as unsigned.
    And for that, we need to map "1111 1111" as the largest byte.
    But since we have signed bytes, we'd need to map "1111 1111" to 127
    (since that's the highest positive value we can store)
    "1111 1111" in the 2s complement (signed form) is -1.
    But we don't care about the interpretation.
    We want the mapping right.

    0000 0000 (0) = -128 (least possible byte value that can be assigned)
    0000 0001 = -127
    ....
    0111 1111 (127) = -1
    1000 0000 (-128) = 0
    1000 0001 (-127) = 1
    ...
    1111 1110 = 126
    1111 1111 (-1) = 127
   */
  @Test
  public void testInterpretUnsigned() {
    // forget the byte being supplied to unsigned method
    // think of the lexicographic bit representation
    assertEquals((byte) 0, BinaryComparableUtils.unsigned((byte) -128)); // 1000 0000
    assertEquals((byte) 127, BinaryComparableUtils.unsigned((byte) -1)); // 1111 1111
    assertEquals((byte) -128, BinaryComparableUtils.unsigned((byte) 0)); // 0000 0000
    assertEquals((byte) -127, BinaryComparableUtils.unsigned((byte) 1)); // 0000 0001
    assertEquals((byte) -1, BinaryComparableUtils.unsigned((byte) 127)); // 0111 1111
    assertEquals((byte) 1, BinaryComparableUtils.unsigned((byte) -127)); // 1000 0001
  }
}
