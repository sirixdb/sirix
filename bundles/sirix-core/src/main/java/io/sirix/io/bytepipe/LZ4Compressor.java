package io.sirix.io.bytepipe;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * LZ4 compression/decompression.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class LZ4Compressor implements ByteHandler {

  @Override
  public OutputStream serialize(final OutputStream toSerialize) {
    return new LZ4BlockOutputStream(toSerialize);
  }

  @Override
  public InputStream deserialize(final InputStream toDeserialize) {
    return new LZ4BlockInputStream(toDeserialize);
  }

  @Override
  public ByteHandler getInstance() {
    return new LZ4Compressor();
  }
}
