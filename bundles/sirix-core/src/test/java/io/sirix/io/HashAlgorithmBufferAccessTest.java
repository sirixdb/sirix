package io.sirix.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Every access path of {@link HashAlgorithm#XXH3} must produce the identical value for the same
 * bytes: persisted page checksums are computed over one representation (a heap array or a native
 * segment on the write side) and verified over another (a FileChannel reader's borrowed direct
 * buffer). The direct-buffer path also must not depend on a module the JVM does not export — see
 * {@code io.sirix.access.trx.page.PinnedTrieProjectionSpillColdReopenTest}, whose child JVM runs
 * with only {@code --add-modules}/{@code --enable-native-access} and is the process-level witness.
 */
final class HashAlgorithmBufferAccessTest {

  private static final int[] LENGTHS = {0, 1, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 240, 241,
      1023, 1024, 4095, 4096, 65_536, 65_537};

  @Test
  @DisplayName("byte[], heap buffer, sliced heap buffer, direct buffer and native segment hash identically")
  void everyAccessPathAgrees() {
    final Random random = new Random(0x5EEDL);
    for (final int length : LENGTHS) {
      final byte[] bytes = new byte[length];
      random.nextBytes(bytes);
      final long expected = HashAlgorithm.XXH3.computeHashLong(bytes);

      assertEquals(expected, HashAlgorithm.XXH3.computeHashLong(ByteBuffer.wrap(bytes)),
          "heap buffer, length " + length);

      // A heap buffer whose array does not start at the payload: position and arrayOffset both apply.
      final byte[] padded = new byte[length + 37];
      System.arraycopy(bytes, 0, padded, 21, length);
      final ByteBuffer sliced = ByteBuffer.wrap(padded, 5, length + 30).slice();
      sliced.position(16).limit(16 + length);
      assertEquals(expected, HashAlgorithm.XXH3.computeHashLong(sliced), "sliced heap buffer, length " + length);

      assertEquals(expected, HashAlgorithm.XXH3.computeHashLong(ByteBuffer.wrap(bytes).asReadOnlyBuffer()),
          "read-only heap buffer, length " + length);

      // The FileChannel reader shape: a pooled little-endian direct buffer, payload after a header.
      final ByteBuffer direct = ByteBuffer.allocateDirect(length + 64).order(ByteOrder.LITTLE_ENDIAN);
      direct.position(48);
      direct.put(bytes);
      direct.position(48).limit(48 + length);
      assertEquals(expected, HashAlgorithm.XXH3.computeHashLong(direct), "direct buffer, length " + length);
      assertEquals(48, direct.position(), "hashing must not move the buffer's position");
      assertEquals(48 + length, direct.limit(), "hashing must not move the buffer's limit");

      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment segment = arena.allocate(Math.max(1, length));
        MemorySegment.copy(bytes, 0, segment, java.lang.foreign.ValueLayout.JAVA_BYTE, 0L, length);
        assertEquals(expected, HashAlgorithm.XXH3.computeHashLong(segment.asSlice(0, length)),
            "native segment, length " + length);
      }
    }
  }

  @Test
  @DisplayName("verifyLong over a direct buffer matches the persisted array checksum")
  void verifyAgreesAcrossRepresentations() {
    final byte[] bytes = new byte[8192];
    new Random(42L).nextBytes(bytes);
    final long persisted = HashAlgorithm.XXH3.computeHashLong(bytes);
    final ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
    direct.put(bytes).flip();
    assertEquals(persisted, HashAlgorithm.XXH3.computeHashLong(direct));
    // A single flipped bit must be detected through the direct-buffer path as well.
    direct.put(4096, (byte) (direct.get(4096) ^ 0x10));
    assertEquals(false, persisted == HashAlgorithm.XXH3.computeHashLong(direct));
  }
}
