package org.sirix.io.bytepipe;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * LZ4 compression/decompression.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class LZ4Compressor implements ByteHandler {

  @Override
  public OutputStream serialize(final OutputStream toSerialize) {
    try {
      return new LZ4FrameOutputStream(toSerialize);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) {
    try {
      return new LZ4FrameInputStream(toDeserialize);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public ByteHandler getInstance() {
    return new LZ4Compressor();
  }
}
