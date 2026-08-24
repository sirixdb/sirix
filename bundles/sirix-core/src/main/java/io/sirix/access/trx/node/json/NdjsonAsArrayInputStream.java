/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import java.io.IOException;
import java.io.InputStream;

/**
 * Presents an NDJSON stream (one JSON value per line) as a single top-level JSON ARRAY — the
 * shape the parallel bulk importer parallelizes. The transform is exact and 1:1 in the body:
 * JSON forbids raw control characters inside strings, so a literal {@code '\n'} can only separate
 * records and maps to {@code ','}; the only insertions are the opening {@code '['} and the
 * closing {@code ']'}. A trailing newline yields a trailing comma, which the importer's slicer
 * treats as separator noise.
 *
 * <p>
 * An optional record limit truncates the stream after N records — for bounded imports of a
 * prefix of a large corpus (the array closes cleanly at the cut).
 */
public final class NdjsonAsArrayInputStream extends InputStream {

  private static final int STATE_PREFIX = 0;
  private static final int STATE_BODY = 1;
  private static final int STATE_SUFFIX = 2;
  private static final int STATE_DONE = 3;

  private final InputStream in;
  private final long recordLimit;
  private long records;
  private int state = STATE_PREFIX;

  public NdjsonAsArrayInputStream(final InputStream in) {
    this(in, Long.MAX_VALUE);
  }

  public NdjsonAsArrayInputStream(final InputStream in, final long recordLimit) {
    this.in = in;
    this.recordLimit = recordLimit;
  }

  @Override
  public int read() throws IOException {
    final byte[] one = new byte[1];
    final int n = read(one, 0, 1);
    return n < 0
        ? -1
        : one[0] & 0xFF;
  }

  @Override
  public int read(final byte[] target, final int off, final int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    switch (state) {
      case STATE_PREFIX -> {
        target[off] = '[';
        state = STATE_BODY;
        return 1;
      }
      case STATE_BODY -> {
        final int n = in.read(target, off, len);
        if (n < 0) {
          state = STATE_SUFFIX;
          return read(target, off, len);
        }
        for (int i = off; i < off + n; i++) {
          if (target[i] == '\n') {
            if (++records >= recordLimit) {
              // Close the array at this record boundary and drop the rest of the block.
              target[i] = ']';
              state = STATE_DONE;
              return i - off + 1;
            }
            target[i] = ',';
          }
        }
        return n;
      }
      case STATE_SUFFIX -> {
        target[off] = ']';
        state = STATE_DONE;
        return 1;
      }
      default -> {
        return -1;
      }
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
