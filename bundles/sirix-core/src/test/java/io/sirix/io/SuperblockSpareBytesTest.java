package io.sirix.io;

import io.sirix.exception.SirixIOException;
import net.openhft.hashing.LongHashFunction;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class SuperblockSpareBytesTest {
  private static final long MSB = 0x0011223344556677L;
  private static final long LSB = 0x8899AABBCCDDEEFFL;

  @Test
  public void checksummedUnknownSpareBytesAreNotSilentlyIgnored() {
    for (int offset = 13; offset < 16; offset++) {
      final ByteBuffer bytes = Superblock.build(Superblock.ROLE_DATA, MSB, LSB).order(ByteOrder.LITTLE_ENDIAN);
      bytes.put(offset, (byte) 1);
      final byte[] prefix = new byte[56];
      bytes.duplicate().get(prefix);
      bytes.putLong(56, LongHashFunction.xx3().hashBytes(prefix));
      final SirixIOException error =
          assertThrows(SirixIOException.class, () -> Superblock.validate(bytes, Superblock.ROLE_DATA, "fixture"));
      assertTrue(error.getMessage(), error.getMessage().contains("spare bytes"));
    }
  }
}
