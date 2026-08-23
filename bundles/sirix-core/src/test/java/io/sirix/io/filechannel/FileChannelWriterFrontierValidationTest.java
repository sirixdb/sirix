/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.filechannel;

import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.PageHasher;
import io.sirix.io.RevisionFileData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Instant;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FileChannelWriterFrontierValidationTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void validatesARevisionRootLargerThanTheHashWindow() throws Exception {
    final byte[] payload = payload(64 * 1024 + 257);
    try (FileChannel channel = frame(payload.length, payload)) {
      final RevisionFileData identity = identity(payload, IOStorage.DATA_REGION_START);

      final FileChannelWriter.ValidatedFrame frame = FileChannelWriter.validateRevisionRootFrame(channel, identity, 7,
          channel.size(), channel.size(), "warm frontier");

      assertEquals(channel.size(), frame.frameEnd());
      assertEquals(payload.length, frame.dataLength());
    }
  }

  @Test
  void rejectsZeroOversizedAndHashMismatchedFrames() throws Exception {
    try (FileChannel zero = frame(0, new byte[0])) {
      assertThrows(SirixIOException.class, () -> FileChannelWriter.validateRevisionRootFrame(zero,
          new RevisionFileData(IOStorage.DATA_REGION_START, Instant.EPOCH, 1L), 1, zero.size(), -1L, "zero"));
    }

    try (FileChannel oversized = frame(Integer.MAX_VALUE, new byte[] {1})) {
      assertThrows(SirixIOException.class, () -> FileChannelWriter.validateRevisionRootFrame(oversized,
          new RevisionFileData(IOStorage.DATA_REGION_START, Instant.EPOCH, 1L), 1, oversized.size(), -1L, "oversized"));
    }

    final byte[] payload = payload(4096);
    try (FileChannel mismatch = frame(payload.length, payload)) {
      assertThrows(SirixIOException.class,
          () -> FileChannelWriter.validateRevisionRootFrame(mismatch,
              new RevisionFileData(IOStorage.DATA_REGION_START, Instant.EPOCH, 1L), 1, mismatch.size(), -1L,
              "hash mismatch"));
      assertThrows(SirixIOException.class,
          () -> FileChannelWriter.validateRevisionRootFrame(mismatch, identity(payload, IOStorage.DATA_REGION_START), 1,
              mismatch.size(), mismatch.size() - 1L, "warm-cache mismatch"));
    }
  }

  @Test
  void rejectsFrameEndOverflowBeforeAnyDestructiveAction() {
    assertThrows(SirixIOException.class,
        () -> FileChannelWriter.checkedFrameEnd(Long.MAX_VALUE - IOStorage.OTHER_BEACON, 1, "overflow", 3));
  }

  private FileChannel frame(final int declaredLength, final byte[] payload) throws Exception {
    final FileChannel channel = FileChannel.open(temporaryDirectory.resolve("frame-" + System.nanoTime()), CREATE,
        TRUNCATE_EXISTING, READ, WRITE);
    channel.position(IOStorage.DATA_REGION_START);
    final ByteBuffer header = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    header.putInt(declaredLength).flip();
    while (header.hasRemaining()) {
      channel.write(header);
    }
    final ByteBuffer body = ByteBuffer.wrap(payload);
    while (body.hasRemaining()) {
      channel.write(body);
    }
    return channel;
  }

  private static RevisionFileData identity(final byte[] payload, final long offset) {
    return new RevisionFileData(offset, Instant.EPOCH,
        IOStorage.normalizeRevisionRootPageHash(PageHasher.computeLong(payload)));
  }

  private static byte[] payload(final int length) {
    final byte[] bytes = new byte[length];
    for (int index = 0; index < bytes.length; index++) {
      bytes[index] = (byte) (index * 31 + 7);
    }
    return bytes;
  }
}
