package org.sirix.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Compression/Decompression for text values or any other data.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class Compression {

  /** Buffer size. */
  public static final int BUFFER_SIZE = 1024;

  /** Compressor. */
  private static final Deflater mCompressor = new Deflater();

  /** Decompressor. */
  private static final Inflater mDecompressor = new Inflater();

  /** Private constructor to prevent from instantiation. */
  private Compression() {
    throw new AssertionError();
  }

  /**
   * Compress data based on the {@link Deflater}.
   *
   * @param toCompress input byte-array
   * @param level compression level (between -1 and 9 whereas 0 is the weakest and -1 is default)
   * @return compressed byte-array
   * @throws NullPointerException if {@code toCompress} is {@code null}
   */
  public static byte[] compress(final byte[] toCompress, final int level) {
    checkNotNull(toCompress);
    checkArgument(level >= -1 && level <= 9, "level must be between 0 and 9!");

    // Compressed result.
    byte[] compressed;

    // Set compression level.
    mCompressor.setLevel(level);

    // Give the compressor the data to compress.
    mCompressor.reset();
    mCompressor.setInput(toCompress);
    mCompressor.finish();

    /*
     * Create an expandable byte array to hold the compressed data. You cannot use an array that's the
     * same size as the orginal because there is no guarantee that the compressed data will be smaller
     * than the uncompressed data.
     */
    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(toCompress.length)) {
      // Compress the data.
      final byte[] buf = new byte[BUFFER_SIZE];
      while (!mCompressor.finished()) {
        final int count = mCompressor.deflate(buf);
        bos.write(buf, 0, count);
      }

      // Get the compressed data.
      compressed = bos.toByteArray();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    return compressed;
  }

  /**
   * Decompress data based on the {@link Inflater}.
   *
   * @param compressed input string
   * @return compressed byte-array
   * @throws NullPointerException if {@code pCompressed} is {@code null}
   */
  public static byte[] decompress(final byte[] compressed) {
    checkNotNull(compressed);

    // Reset the decompressor and give it the data to compress.
    mDecompressor.reset();
    mDecompressor.setInput(compressed);

    // Create an expandable byte array to hold the decompressed data.
    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(compressed.length)) {
      // Decompress the data.
      final byte[] buf = new byte[BUFFER_SIZE];
      while (!mDecompressor.finished()) {
        try {
          final int count = mDecompressor.inflate(buf);
          bos.write(buf, 0, count);
        } catch (final DataFormatException e) {
          throw new IllegalStateException(e);
        }
      }
      // Get the decompressed data.
      return bos.toByteArray();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
