/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Streams a {@link Reader}'s characters as UTF-8 bytes — the bridge that lets character-based
 * callers (tests, in-process generators) use the byte-based parallel import pipeline. File and
 * network callers should hand the raw {@link InputStream} directly and skip the round trip.
 */
final class ReaderUtf8InputStream extends InputStream {

  private final Reader in;
  private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                                                               .onMalformedInput(CodingErrorAction.REPLACE)
                                                               .onUnmappableCharacter(CodingErrorAction.REPLACE);
  private final CharBuffer chars = CharBuffer.allocate(1 << 13);
  private final ByteBuffer bytes = ByteBuffer.allocate(1 << 14);
  private boolean endOfInput;

  ReaderUtf8InputStream(final Reader in) {
    this.in = in;
    chars.limit(0);
    bytes.limit(0);
  }

  @Override
  public int read() throws IOException {
    return refill()
        ? bytes.get() & 0xFF
        : -1;
  }

  @Override
  public int read(final byte[] target, final int off, final int len) throws IOException {
    if (!refill()) {
      return -1;
    }
    final int n = Math.min(len, bytes.remaining());
    bytes.get(target, off, n);
    return n;
  }

  private boolean refill() throws IOException {
    while (!bytes.hasRemaining()) {
      if (!chars.hasRemaining() && !endOfInput) {
        chars.clear();
        final int read = in.read(chars);
        if (read < 0) {
          endOfInput = true;
          chars.limit(0);
        } else {
          chars.flip();
        }
      }
      bytes.clear();
      final CoderResult result = encoder.encode(chars, bytes, endOfInput);
      bytes.flip();
      if (endOfInput && !bytes.hasRemaining() && result.isUnderflow()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
