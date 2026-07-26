/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 *
 * Utility test — run manually to dump a canonical 32 KiB-page encoded frame
 * to /tmp/sirix-lz77-page.bin for use by the standalone C bench.
 */
package io.sirix.page;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;

final class SirixLZ77DumpEncodedTest {

  @Test
  @Disabled("manual utility — used to dump a canonical encoded frame for the standalone C bench")
  void dumpEncodedFrameForNativeBench() throws Exception {
    final Path out = Path.of("/tmp/claude-1000/sirix-lz77-page.bin");
    try (Arena arena = Arena.ofConfined()) {
      final int n = 32_768;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0x5EED);
      for (int i = 0; i < n; i++) {
        final int r = rng.nextInt(100);
        if (r < 50) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        } else if (r < 70) {
          in.set(ValueLayout.JAVA_BYTE, i, (byte) (rng.nextInt(127) + 1));
        } else if (i + 4 <= n) {
          in.set(ValueLayout.JAVA_BYTE, i,     (byte) 0x01);
          in.set(ValueLayout.JAVA_BYTE, i + 1, (byte) 0x42);
          in.set(ValueLayout.JAVA_BYTE, i + 2, (byte) 0x00);
          in.set(ValueLayout.JAVA_BYTE, i + 3, (byte) 0x00);
          i += 3;
        }
      }
      final byte[] buf = new byte[SirixLZ77Codec.maxEncodedSize(n)];
      final int encoded = SirixLZ77Codec.encode(in, 0, n, buf, 0);
      final byte[] outBytes = new byte[encoded];
      System.arraycopy(buf, 0, outBytes, 0, encoded);
      Files.createDirectories(out.getParent());
      Files.write(out, outBytes);
      System.out.println("wrote " + encoded + " bytes to " + out);
    }
  }
}
