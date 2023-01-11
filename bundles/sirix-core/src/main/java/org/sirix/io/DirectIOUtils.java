package org.sirix.io;

import net.openhft.chronicle.core.OS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class DirectIOUtils {
  public static final int BLOCK_SIZE = OS.pageSize();

  /**
   * Allocates a block aligned direct byte buffer. The size of the returned
   * buffer is the nearest multiple of BLOCK_SIZE to the requested size.
   *
   * @param size the requested size of a byte buffer.
   * @return aligned byte buffer.
   */
  public static ByteBuffer allocate(int size) {
    int n = (size + BLOCK_SIZE - 1) / BLOCK_SIZE + 1;
    return ByteBuffer.allocateDirect(n * BLOCK_SIZE).alignedSlice(BLOCK_SIZE);
  }

  /**
   * Reads a sequence of bytes from the file channel into the given byte buffer.
   * <p>
   * We rely on ByteBuffer.compact() method so that the caller of this method
   * can safely flip() the buffer for reading. Avoiding compact() improves
   * performance but requires callers of this method to avoid using flip() and
   * not rely on a position-0 invariant for chunk reader.
   *
   * @param channel  the channel to read from.
   * @param dst      the destination byte buffer.
   * @param position the position of the channel to start reading from.
   * @return the number of bytes read.
   * @throws IOException
   */
  public static int read(FileChannel channel, ByteBuffer dst, long position) throws IOException {
    int lim = dst.limit();
    int r = (int) (position & (BLOCK_SIZE - 1));
    int len = lim + r;
    dst.limit((len & (BLOCK_SIZE - 1)) == 0 ? len : (len & -BLOCK_SIZE) + BLOCK_SIZE);

    int n = channel.read(dst, position & -BLOCK_SIZE);
    n -= r;
    n = Math.min(n, lim);

    dst.position(r).limit(r + n);
    if (r != 0) {
      dst.compact();
    }
    dst.position(n).limit(lim);

    return n;
  }
}

