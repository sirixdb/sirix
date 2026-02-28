package io.sirix.access.trx.node.json.objectvalue;

import io.sirix.node.NodeKind;

/**
 * Mutable, reusable {@link ObjectRecordValue} for pre-encoded UTF-8 byte arrays.
 *
 * <p>Designed for hot-path reuse in shredders that encode string values directly
 * to {@code byte[]} without constructing a {@link String} intermediate.
 * A single instance can be {@link #set(byte[], int, int) rebound} per record
 * to avoid per-value wrapper allocation. Carries (buffer, offset, length) to
 * support reusable buffers larger than the actual data.
 */
public final class ByteStringValue implements ObjectRecordValue<byte[]> {

  private byte[] utf8;
  private int off;
  private int len;

  public void set(final byte[] utf8, final int off, final int len) {
    this.utf8 = utf8;
    this.off = off;
    this.len = len;
  }

  @Override
  public byte[] getValue() {
    return utf8;
  }

  public int getOffset() {
    return off;
  }

  public int getLength() {
    return len;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.STRING_VALUE;
  }
}
