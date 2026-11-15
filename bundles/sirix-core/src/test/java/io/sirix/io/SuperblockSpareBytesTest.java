package io.sirix.io;

import io.sirix.exception.SirixIOException;
import net.openhft.hashing.LongHashFunction;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class SuperblockSpareBytesTest {
  @Test
  public void checksummedUnknownSpareBytesAreNotSilentlyIgnored() {
    for (int offset = 13; offset < 16; offset++) {
      final ByteBuffer bytes = Superblock.build(Superblock.ROLE_DATA).order(ByteOrder.LITTLE_ENDIAN);
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
