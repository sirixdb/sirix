/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.bytepipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Clean-fork allocation gate: once a thread's scratch holds a block, decoding allocates nothing.
 *
 * <p>
 * Runs in the separate allocation JVM, because a sibling suite mocks this decoder with an inline
 * mock, and the interception prologue of a retransformed class allocates.
 */
@DisplayName("JavaLz4BlockDecoder decodes on reused scratch")
final class JavaLz4BlockDecoderAllocationTest {
  private static final int ITERATIONS = 4096;
  private static volatile long checksum;

  /**
   * Decoded sizes. 4,088 and the limit make the output scratch exactly as long as the decoded block
   * plus its word slack, the case in which an off-by-one in the reuse test would reallocate every
   * call.
   */
  @ParameterizedTest(name = "{0} decoded bytes")
  @ValueSource(ints = {64, 2000, 4088, 20_000, JavaLz4BlockDecoder.HEAP_DECODE_LIMIT})
  @DisplayName("repeated decodes of one block allocate no bytes")
  void repeatedDecodesAllocateNothing(final int decodedSize) throws InterruptedException {
    assertNoAllocation(block(decodedSize), decodedSize);
  }

  @Test
  @DisplayName("a block that fills its input scratch exactly allocates no bytes")
  void anExactlyFullInputScratchIsReused() throws InterruptedException {
    // One token, sixteen length-chain bytes and 4,071 literals: 4,088 bytes, which with the word
    // slack is exactly the 4 KiB input scratch.
    final ByteArrayOutputStream block = new ByteArrayOutputStream();
    block.write(0xf0);
    writeChain(block, 4071);
    for (int i = 0; i < 4071; i++) {
      block.write(i * 13);
    }
    assertEquals(4088, block.size());
    assertEquals(4096, JavaLz4BlockDecoder.scratchCapacity(block.size()));
    assertNoAllocation(block.toByteArray(), 4071);
  }

  /**
   * Measures on a thread that has never decoded. The scratch is thread-owned and only grows, so on a
   * reused thread an earlier, larger block would hide a scratch that is reallocated when it fits
   * exactly.
   */
  private static void assertNoAllocation(final byte[] block, final int decodedSize) throws InterruptedException {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    final long[] allocated = {Long.MAX_VALUE};
    final Throwable[] failure = new Throwable[1];
    final Thread fresh = new Thread(() -> {
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment source = arena.allocate(block.length);
        MemorySegment.copy(block, 0, source, ValueLayout.JAVA_BYTE, 0, block.length);
        final MemorySegment destination = arena.allocate(decodedSize);
        final long thread = Thread.currentThread().threadId();
        // Same call site and enough invocations to compile it before the measured passes.
        for (int pass = 0; pass < 14; pass++) {
          final long before = bean.getThreadAllocatedBytes(thread);
          long sum = 0;
          for (int i = 0; i < ITERATIONS; i++) {
            sum += JavaLz4BlockDecoder.decompressSafe(source, 0, block.length, destination, 0, decodedSize);
          }
          final long bytes = bean.getThreadAllocatedBytes(thread) - before;
          checksum += sum;
          assertEquals((long) ITERATIONS * decodedSize, sum, "every decode returns the whole block");
          if (pass >= 11) {
            allocated[0] = Math.min(allocated[0], bytes);
          }
        }
      } catch (final Throwable thrown) {
        failure[0] = thrown;
      }
    }, "lz4-fallback-allocation");
    fresh.start();
    fresh.join();
    if (failure[0] != null) {
      throw new AssertionError("decoding on a fresh thread failed", failure[0]);
    }
    System.out.printf("lz4FallbackAllocation decoded=%d block=%d bytes/%d decodes=%d%n", decodedSize, block.length,
        ITERATIONS, allocated[0]);
    assertEquals(0, allocated[0], "decoding on a sized scratch must allocate nothing");
  }

  /**
   * A block that decodes to exactly {@code decodedSize} bytes through every copy the heap loop has:
   * short and long literal runs, word-sized matches, a long non-overlapping match, a fill and a
   * short-period repeat. The compressed length is whatever that takes; only 4,088 and the limit need
   * the decoded side to sit on a scratch boundary.
   */
  private static byte[] block(final int decodedSize) {
    final ByteArrayOutputStream block = new ByteArrayOutputStream();
    int produced = 0;
    int shape = 0;
    while (decodedSize - produced > 600) {
      final int literals = switch (shape % 4) {
        case 0 -> 3;
        case 1 -> 12;
        case 2 -> 40;
        default -> 0;
      };
      final int distance = produced + literals == 0
          ? 0
          : switch (shape % 5) {
            case 0 -> 1;
            case 1 -> Math.min(3, produced + literals);
            case 2 -> Math.min(9, produced + literals);
            case 3 -> Math.min(16, produced + literals);
            default -> Math.min(200, produced + literals);
          };
      final int match = switch (shape % 3) {
        case 0 -> 6;
        case 1 -> 14;
        default -> 120;
      };
      shape++;
      if (distance == 0) {
        continue;
      }
      block.write(Math.min(literals, 15) << 4 | Math.min(match - 4, 15));
      writeChain(block, literals);
      for (int i = 0; i < literals; i++) {
        block.write(produced * 31 + i);
      }
      block.write(distance & 0xff);
      block.write(distance >>> 8);
      writeChain(block, match - 4);
      produced += literals + match;
    }
    final int tail = decodedSize - produced;
    block.write(Math.min(tail, 15) << 4);
    writeChain(block, tail);
    for (int i = 0; i < tail; i++) {
      block.write(i * 7);
    }
    return block.toByteArray();
  }

  private static void writeChain(final ByteArrayOutputStream block, final int length) {
    if (length < 15) {
      return;
    }
    int rest = length - 15;
    while (rest >= 255) {
      block.write(255);
      rest -= 255;
    }
    block.write(rest);
  }
}
