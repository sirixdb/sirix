package io.sirix.io.filechannel;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.PooledBytesOut;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

final class FileChannelWriterShortWriteTest {

  @Test
  void positionalWriteDrainsEveryShortWriteAtTheNextExactOffset() throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    final byte[] expected = {3, 1, 4, 1, 5, 9, 2, 6, 5, 3};
    final byte[] writtenBytes = new byte[expected.length];
    final long initialOffset = 37L;
    final AtomicInteger calls = new AtomicInteger();

    when(channel.write(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer source = invocation.getArgument(0);
      final long fileOffset = invocation.getArgument(1);
      final int count = Math.min(3, source.remaining());
      source.get(writtenBytes, Math.toIntExact(fileOffset - initialOffset), count);
      calls.incrementAndGet();
      return count;
    });

    final ByteBuffer source = ByteBuffer.wrap(expected.clone());
    FileChannelWriter.writeFully(channel, source, initialOffset);

    assertArrayEquals(expected, writtenBytes);
    assertEquals(expected.length, source.position());
    assertEquals(4, calls.get());
    verify(channel, times(4)).write(any(ByteBuffer.class), anyLong());
  }

  @Test
  void zeroProgressFailsInsteadOfPublishingATornTailOrSpinning() throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    final ByteBuffer source = ByteBuffer.wrap(new byte[] {8, 9, 7});
    when(channel.write(any(ByteBuffer.class), anyLong())).thenReturn(0);

    final IOException failure = assertThrows(IOException.class,
        () -> FileChannelWriter.writeFully(channel, source, 91L));

    assertEquals(0, source.position());
    assertTrue(failure.getMessage().contains("made no progress"));
  }

  @Test
  void positionalReadDrainsEveryShortReadAtTheNextExactOffset() throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    final byte[] expected = {2, 3, 5, 7, 11, 13, 17};
    final long initialOffset = 211L;
    final AtomicInteger calls = new AtomicInteger();

    when(channel.read(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer target = invocation.getArgument(0);
      final long fileOffset = invocation.getArgument(1);
      final int sourceOffset = Math.toIntExact(fileOffset - initialOffset);
      final int count = Math.min(2, target.remaining());
      target.put(expected, sourceOffset, count);
      calls.incrementAndGet();
      return count;
    });

    final ByteBuffer target = ByteBuffer.allocate(expected.length);
    FileChannelWriter.readFully(channel, target, initialOffset);

    assertArrayEquals(expected, target.array());
    assertEquals(expected.length, target.position());
    assertEquals(4, calls.get());
  }

  @Test
  void zeroProgressReadFailsInsteadOfSpinningDuringRecovery() throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    final ByteBuffer target = ByteBuffer.allocate(4);
    when(channel.read(any(ByteBuffer.class), anyLong())).thenReturn(0);

    final IOException failure = assertThrows(IOException.class,
        () -> FileChannelWriter.readFully(channel, target, 307L));

    assertEquals(0, target.position());
    assertTrue(failure.getMessage().contains("made no progress"));
  }

  @Test
  void reusableConcreteViewDrainsOnlyItsExactPrefixAcrossPartialWrites() throws IOException {
    final FileChannel channel = mock(FileChannel.class);
    final byte[] expected = {2, 7, 1, 8, 2, 8};
    final byte[] writtenBytes = new byte[expected.length];
    final long initialOffset = 113L;

    when(channel.write(any(ByteBuffer.class), anyLong())).thenAnswer(invocation -> {
      final ByteBuffer source = invocation.getArgument(0);
      final long fileOffset = invocation.getArgument(1);
      final int count = Math.min(2, source.remaining());
      source.get(writtenBytes, Math.toIntExact(fileOffset - initialOffset), count);
      return count;
    });

    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(64)) {
      out.write(expected);
      final ByteBuffer source = FileChannelWriter.readableByteBuffer(out);
      assertEquals(expected.length, source.limit());
      assertTrue(source.capacity() > source.limit(), "spare backing capacity must stay outside the write range");
      assertSame(source, FileChannelWriter.readableByteBuffer(out));

      FileChannelWriter.writeFully(channel, source, initialOffset);

      assertArrayEquals(expected, writtenBytes);
      assertEquals(expected.length, source.position());
      assertEquals(expected.length, source.limit());
      verify(channel, times(3)).write(any(ByteBuffer.class), anyLong());
    }
  }

  @Test
  void genericBytesOutFallbackHonoursLogicalLength() {
    final byte[] expected = {3, 1, 4, 1};
    try (PooledBytesOut out = new PooledBytesOut(32)) {
      out.write(expected);

      final ByteBuffer readable = FileChannelWriter.readableByteBuffer(out);

      assertEquals(0, readable.position());
      assertEquals(expected.length, readable.limit());
      assertTrue(readable.capacity() > readable.limit());
      final byte[] actual = new byte[readable.remaining()];
      readable.get(actual);
      assertArrayEquals(expected, actual);
    }
  }
}
