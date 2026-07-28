/*
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.service.json.shredder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Presents one {@link JsonPartitioner.Partition} of a file as a single well-formed JSON value,
 * streaming and without materialising the partition.
 *
 * <p>Three transformations are applied on the fly, all of them synthetic — the partition's own
 * bytes are never rewritten:
 * <ul>
 *   <li>the byte range {@code [startOffset, endOffsetExclusive)} is read from the channel and
 *       nothing outside it is visible;</li>
 *   <li>with {@link JsonPartitioner.Partition#wrapInArray()} the range is bracketed by {@code [}
 *       and {@code ]} so a bare record sequence reads back as a JSON array;</li>
 *   <li>with {@link JsonPartitioner.Partition#spliceSeparators()} a comma is emitted in front of
 *       every top-level value after the first, turning concatenated records (LDJSON) into that
 *       array's elements.</li>
 * </ul>
 *
 * <p>Reads go through one reusable heap buffer and one reusable {@link ByteBuffer} view of it, so
 * steady-state streaming allocates nothing. The class owns the {@link FileChannel} it is handed and
 * closes it.
 *
 * <p>Instances are <strong>not</strong> thread-safe.
 *
 * @author Johannes Lichtenberger
 * @see JsonPartitioner
 */
final class JsonPartitionInputStream extends InputStream {

  /** Read granularity from the channel. Big enough to amortise syscalls, small enough to stay cached. */
  private static final int BUFFER_SIZE = 1 << 16;

  private final FileChannel channel;
  private final boolean wrapInArray;
  private final boolean spliceSeparators;
  private final long endOffsetExclusive;
  private final JsonStructureScanner scanner;

  private final byte[] buffer = new byte[BUFFER_SIZE];
  private final ByteBuffer view = ByteBuffer.wrap(buffer);

  /** Absolute file offset of the next byte to read from the channel. */
  private long position;

  /** Valid bytes in {@link #buffer}. */
  private int limit;

  /** Read cursor into {@link #buffer}. */
  private int cursor;

  /** Emission phase: the synthetic prefix, then the body, then the synthetic suffix. */
  private enum Phase {
    PREFIX, BODY, SUFFIX, DONE
  }

  private Phase phase;

  /** A body byte displaced by a spliced comma, to be emitted before any further input is consumed. */
  private byte heldByte;
  private boolean heldByteValid;

  /** Whether a top-level value has already been emitted (the first one takes no leading comma). */
  private boolean sawTopLevelValue;

  private boolean closed;

  /**
   * @param channel   the channel to read the partition from; owned and closed by this stream
   * @param partition the partition to present
   * @throws NullPointerException if an argument is {@code null}
   */
  JsonPartitionInputStream(final FileChannel channel, final JsonPartitioner.Partition partition) {
    this.channel = Objects.requireNonNull(channel, "channel");
    Objects.requireNonNull(partition, "partition");
    this.wrapInArray = partition.wrapInArray();
    this.spliceSeparators = partition.spliceSeparators();
    this.position = partition.startOffset();
    this.endOffsetExclusive = partition.endOffsetExclusive();
    this.scanner = spliceSeparators ? new JsonStructureScanner() : null;
    this.phase = wrapInArray ? Phase.PREFIX : Phase.BODY;
  }

  /** Scratch for the single-byte {@link #read()}, so it does not allocate per call. */
  private final byte[] singleByte = new byte[1];

  @Override
  public int read() throws IOException {
    final int read = read(singleByte, 0, 1);
    return read == -1 ? -1 : singleByte[0] & 0xFF;
  }

  @Override
  public int read(final byte[] destination, final int offset, final int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, destination.length);
    if (closed) {
      throw new IOException("stream is closed");
    }
    if (length == 0) {
      return 0;
    }

    int produced = 0;
    while (produced < length) {
      switch (phase) {
        case PREFIX -> {
          destination[offset + produced++] = '[';
          phase = Phase.BODY;
        }
        case BODY -> {
          final int written = fillFromBody(destination, offset + produced, length - produced);
          if (written == 0) {
            phase = wrapInArray ? Phase.SUFFIX : Phase.DONE;
          } else {
            produced += written;
          }
        }
        case SUFFIX -> {
          destination[offset + produced++] = ']';
          phase = Phase.DONE;
        }
        case DONE -> {
          return produced > 0 ? produced : -1;
        }
        default -> throw new AssertionError("unhandled phase: " + phase);
      }
    }
    return produced;
  }

  /**
   * Emit up to {@code length} body bytes, splicing separators when required.
   *
   * @return the number of bytes written; {@code 0} once the partition is exhausted
   */
  private int fillFromBody(final byte[] destination, final int offset, final int length) throws IOException {
    int produced = 0;
    while (produced < length) {
      if (heldByteValid) {
        destination[offset + produced++] = heldByte;
        heldByteValid = false;
        continue;
      }
      if (cursor == limit && !refill()) {
        return produced;
      }

      final byte b = buffer[cursor];
      if (!spliceSeparators) {
        // No transformation: copy the rest of the buffer in one bulk move.
        final int available = Math.min(limit - cursor, length - produced);
        System.arraycopy(buffer, cursor, destination, offset + produced, available);
        cursor += available;
        produced += available;
        continue;
      }

      scanner.step(b, 0L);
      cursor++;
      if (scanner.startedTopLevelValue()) {
        if (sawTopLevelValue) {
          // The record separator the source stream expressed as a newline (or nothing at all).
          destination[offset + produced++] = ',';
          heldByte = b;
          heldByteValid = true;
          continue;
        }
        sawTopLevelValue = true;
      }
      destination[offset + produced++] = b;
    }
    return produced;
  }

  /**
   * Refill {@link #buffer} from the channel, never reading past the partition's end.
   *
   * @return {@code false} once the partition is exhausted
   */
  private boolean refill() throws IOException {
    final long remaining = endOffsetExclusive - position;
    if (remaining <= 0L) {
      return false;
    }
    view.clear();
    view.limit((int) Math.min(remaining, BUFFER_SIZE));
    final int read = channel.read(view, position);
    if (read <= 0) {
      // The file was truncated or replaced underneath us. Ending the partition quietly here would
      // hand the shredder a prefix of the records it was promised and call that success — a short
      // shard, no error, nothing to notice. A non-positive read cannot make progress either, so
      // returning false would spin. Fail: the plan's offsets no longer describe this file.
      throw new IOException("partition ending at " + endOffsetExclusive + " runs past the end of the"
          + " file — " + remaining + " byte(s) short at offset " + position
          + ". The file changed after it was partitioned.");
    }
    position += read;
    cursor = 0;
    limit = read;
    return true;
  }

  @Override
  public int available() {
    if (closed) {
      return 0;
    }
    final long bodyRemaining = Math.max(0L, endOffsetExclusive - position) + (limit - cursor);
    final long total = bodyRemaining + (heldByteValid ? 1 : 0) + (phase == Phase.PREFIX ? 1 : 0)
        + (wrapInArray && phase != Phase.DONE ? 1 : 0);
    return (int) Math.min(Integer.MAX_VALUE, total);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    phase = Phase.DONE;
    try {
      channel.close();
    } finally {
      // Only now: a close that threw must leave the stream retryable rather than marked done with the
      // descriptor still open.
      closed = true;
    }
  }
}
