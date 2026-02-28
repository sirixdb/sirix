package io.sirix.access.trx.node.json.objectvalue;

import io.sirix.node.NodeKind;

/**
 * Mutable, reusable {@link ObjectRecordValue} for pre-encoded UTF-8 byte arrays.
 *
 * <p>Designed for hot-path reuse in shredders that encode string values directly
 * to {@code byte[]} without constructing a {@link String} intermediate.
 * A single instance can be {@link #set(byte[]) rebound} per record to avoid
 * per-value wrapper allocation.
 */
public final class ByteStringValue implements ObjectRecordValue<byte[]> {

  private byte[] utf8;

  public void set(final byte[] utf8) {
    this.utf8 = utf8;
  }

  @Override
  public byte[] getValue() {
    return utf8;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.STRING_VALUE;
  }
}
