/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.exception.SirixCorruptionException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.io.memorymapped.MMFileReader;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A batched read that fails part way must not strand the allocator frames of the members it had
 * already decoded: the failure propagates unchanged and every decoded member is released, for the
 * file-channel backend (one coalesced run and members read individually), the memory-mapped backend
 * and the interface's default batch loop. A reader that hands out the instances it stores keeps them:
 * its failed batch leaves every page it returned open, also when reached through a forwarding reader.
 */
final class BatchReadFailureReleaseTest {

  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("batch-failure-release").byteHandlerPipeline(new ByteHandlerPipeline()).build();

  private static final String BORROW_BATCH_INPUT = "sirix.filechannel.borrowBatchInput";

  private static final byte[] KEY = {5, 7, 11};

  private static FrameSlotAllocator allocator;

  enum Backend {
    FILE_CHANNEL, MEMORY_MAPPED
  }

  @BeforeAll
  static void initializeAllocator() {
    allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    allocator.init(8L * 1024 * 1024 * 1024);
  }

  @ParameterizedTest(name = "{0}: stride={1}, failing member={2}, borrowBatchInput={3}")
  @CsvSource({"FILE_CHANNEL, 4096, 1, true", "FILE_CHANNEL, 4096, 2, true", "FILE_CHANNEL, 4096, 1, false",
      "FILE_CHANNEL, 4096, 2, false", "FILE_CHANNEL, 131072, 1, true", "FILE_CHANNEL, 131072, 2, true",
      "MEMORY_MAPPED, 4096, 1, true", "MEMORY_MAPPED, 4096, 2, true"})
  @ResourceLock(Resources.SYSTEM_PROPERTIES)
  void aFailingMemberReleasesEveryPageDecodedBeforeIt(final Backend backend, final int stride,
      final int failingMember, final boolean borrowBatchInput) throws IOException {
    final String previous = System.getProperty(BORROW_BATCH_INPUT);
    System.setProperty(BORROW_BATCH_INPUT, Boolean.toString(borrowBatchInput));
    try (Arena arena = Arena.ofConfined()) {
      final int members = 3;
      final long[] offsets = new long[members];
      final byte[][] values = new byte[members][];
      final byte[][] frames = new byte[members][];
      for (int i = 0; i < members; i++) {
        offsets[i] = 64L + (long) i * stride;
        values[i] = value(97 + 31 * i, i);
        try (HOTLeafPage leaf = new HOTLeafPage(40L + i, 1, IndexType.PROJECTION)) {
          assertTrue(leaf.put(KEY, values[i]));
          frames[i] = frame(leaf);
        }
      }
      final byte[] file = new byte[Math.toIntExact(offsets[members - 1] + frames[members - 1].length)];
      final PageReference[] references = new PageReference[members];
      for (int i = 0; i < members; i++) {
        System.arraycopy(frames[i], 0, file, Math.toIntExact(offsets[i]), frames[i].length);
        references[i] = new PageReference();
        references[i].setKey(offsets[i]);
        references[i].setHash(PageHasher.computeLong(Arrays.copyOfRange(frames[i], Integer.BYTES, frames[i].length),
            CONFIG.hashAlgorithm));
      }
      final MemorySegment mapped = arena.allocate(file.length);
      MemorySegment.copy(file, 0, mapped, ValueLayout.JAVA_BYTE, 0L, file.length);

      try (Reader reader = backend == Backend.FILE_CHANNEL
          ? new FileChannelReader(channel(file), mock(FileChannel.class), new ByteHandlerPipeline(),
              SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build())
          : new MMFileReader(mapped, arena.allocate(Long.BYTES), new ByteHandlerPipeline(), SerializationType.DATA,
              new PagePersister(), Caffeine.newBuilder().build(), null, null)) {
        // An intact batch first: its pages hold allocator frames while they are open, which is what
        // makes the baseline comparison below meaningful.
        final Page[] intact = reader.read(references, CONFIG);
        final long activeWhileOpen = allocator.getActiveMemoryBytes();
        try {
          for (int i = 0; i < members; i++) {
            final HOTLeafPage leaf = assertInstanceOf(HOTLeafPage.class, intact[i]);
            assertArrayEquals(values[i], leaf.copyStoredValue(leaf.findEntry(KEY)));
          }
        } finally {
          for (final Page page : intact) {
            page.close();
          }
        }
        final long baseline = allocator.getActiveMemoryBytes();
        assertTrue(activeWhileOpen > baseline, "decoded HOT pages must own allocator frames while open");

        final int corrupted = Math.toIntExact(offsets[failingMember] + frames[failingMember].length - 1);
        file[corrupted] ^= 1;
        mapped.set(ValueLayout.JAVA_BYTE, corrupted, file[corrupted]);
        final long allocationsBefore = totalAllocations();

        final SirixCorruptionException failure =
            assertThrows(SirixCorruptionException.class, () -> reader.read(references, CONFIG));

        assertEquals(0, failure.getSuppressed().length, "releasing the decoded members must not fail");
        assertTrue(totalAllocations() > allocationsBefore,
            "the members before the failing one must have been decoded into frames");
        assertEquals(baseline, allocator.getActiveMemoryBytes(),
            "every frame decoded before the failure must be returned to the allocator");
      }
    } finally {
      if (previous == null) {
        System.clearProperty(BORROW_BATCH_INPUT);
      } else {
        System.setProperty(BORROW_BATCH_INPUT, previous);
      }
    }
  }

  @Test
  void theDefaultBatchLoopReleasesThePagesReadBeforeTheFailingReference() {
    final HOTLeafPage first = new HOTLeafPage(1L, 1, IndexType.PROJECTION);
    final HOTLeafPage second = new HOTLeafPage(2L, 1, IndexType.PROJECTION);
    final SirixIOException injected = new SirixIOException("injected read failure of the last reference");
    final Reader reader = mock(Reader.class, CALLS_REAL_METHODS);
    doAnswer(invocation -> {
      final long key = invocation.<PageReference>getArgument(0).getKey();
      if (key == 10L) {
        return first;
      }
      if (key == 20L) {
        return second;
      }
      throw injected;
    }).when(reader).read(any(PageReference.class), any(ResourceConfiguration.class));
    final PageReference[] references = new PageReference[4];
    references[0] = new PageReference().setKey(10L);
    references[2] = new PageReference().setKey(20L);
    references[3] = new PageReference().setKey(30L);
    try {
      assertSame(injected, assertThrows(SirixIOException.class, () -> reader.read(references, CONFIG)));

      assertTrue(first.isClosed(), "a page read before the failure must be released");
      assertTrue(second.isClosed(), "a page read before the failure must be released");
      assertEquals(0, injected.getSuppressed().length, "releasing the read pages must not fail");
    } finally {
      if (!first.isClosed()) {
        first.close();
      }
      if (!second.isClosed()) {
        second.close();
      }
    }
  }

  @Test
  void theDefaultBatchLoopLeavesThePagesOfASharedPageReaderOpen() {
    final Reader reader = mock(Reader.class, CALLS_REAL_METHODS);
    doReturn(true).when(reader).returnsSharedPages();

    assertStoredPageSurvivesAFailedBatch(reader, reader);
  }

  @Test
  void aForwardingReaderKeepsTheSharedPagesOfItsDelegate() {
    final Reader delegate = mock(Reader.class);
    when(delegate.returnsSharedPages()).thenReturn(true);
    final Reader forwarding = new AbstractForwardingReader() {
      @Override
      protected Reader delegate() {
        return delegate;
      }

      @Override
      public void close() {}
    };

    assertTrue(forwarding.returnsSharedPages(), "a forwarding reader hands out its delegate's pages");
    assertStoredPageSurvivesAFailedBatch(delegate, forwarding);
  }

  /**
   * Stubs {@code backend} to return one stored page and then fail, and checks that a batch read
   * through {@code batchReader} propagates the failure and leaves the stored page open and readable.
   */
  private static void assertStoredPageSurvivesAFailedBatch(final Reader backend, final Reader batchReader) {
    final HOTLeafPage stored = new HOTLeafPage(1L, 1, IndexType.PROJECTION);
    final byte[] storedValue = value(41, 3);
    final SirixIOException injected = new SirixIOException("injected read failure of the second reference");
    try {
      assertTrue(stored.put(KEY, storedValue));
      doAnswer(invocation -> {
        if (invocation.<PageReference>getArgument(0).getKey() == 10L) {
          return stored;
        }
        throw injected;
      }).when(backend).read(any(PageReference.class), any(ResourceConfiguration.class));
      final PageReference[] references = {new PageReference().setKey(10L), new PageReference().setKey(20L)};

      assertSame(injected, assertThrows(SirixIOException.class, () -> batchReader.read(references, CONFIG)));

      assertFalse(stored.isClosed(), "a page the reader still hands out must survive the failed batch");
      assertArrayEquals(storedValue, stored.copyStoredValue(stored.findEntry(KEY)));
    } finally {
      stored.close();
    }
  }

  private static long totalAllocations() {
    long total = 0L;
    for (int sizeClass = 0; sizeClass < FrameSlotAllocator.SIZE_CLASSES.length; sizeClass++) {
      total += allocator.allocateCount(sizeClass);
    }
    return total;
  }

  private static FileChannel channel(final byte[] contents) throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    when(channel.size()).thenReturn((long) contents.length);
    when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer target = invocation.getArgument(0);
      final long offset = invocation.getArgument(1);
      if (offset >= contents.length) {
        return -1;
      }
      final int from = Math.toIntExact(offset);
      final int count = Math.min(target.remaining(), contents.length - from);
      target.put(contents, from, count);
      return count;
    });
    return channel;
  }

  private static byte[] frame(final Page page) throws IOException {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(1024)) {
      new PagePersister().serializePage(CONFIG, out, page, SerializationType.DATA);
      final byte[] body = out.toByteArray();
      return ByteBuffer.allocate(Integer.BYTES + body.length)
                       .order(ByteOrder.LITTLE_ENDIAN)
                       .putInt(body.length)
                       .put(body)
                       .array();
    }
  }

  private static byte[] value(final int length, final int seed) {
    final byte[] value = new byte[length];
    for (int i = 0; i < length; i++) {
      value[i] = (byte) (31 * i + seed);
    }
    return value;
  }
}
