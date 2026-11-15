package io.sirix;

import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class BinaryEncodingVersionStrictTest {
  @Test
  public void everyByteExceptZeroRefusesAndPublicValuesCannotChangeLookup() {
    assertSame(BinaryEncodingVersion.V0, BinaryEncodingVersion.fromByte((byte) 0));
    final BinaryEncodingVersion[] exposed = BinaryEncodingVersion.values();
    exposed[0] = null;
    assertSame(BinaryEncodingVersion.V0, BinaryEncodingVersion.fromByte((byte) 0));
    for (int id = 1; id < 256; id++) {
      final byte version = (byte) id;
      assertTrue(
          assertThrows(IllegalStateException.class, () -> BinaryEncodingVersion.fromByte(version)).getMessage()
                                                                                                  .contains(
                                                                                                      Byte.toString(
                                                                                                          version)));
    }
  }
}
