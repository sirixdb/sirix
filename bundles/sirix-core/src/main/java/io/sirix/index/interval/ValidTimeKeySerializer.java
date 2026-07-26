/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import io.sirix.index.hot.HOTKeySerializer;

import static java.util.Objects.requireNonNull;

/**
 * Order-preserving serializer for {@link ValidTimeKey}s.
 *
 * <p>The 17-byte encoding is {@code [store:1][signFlippedBE(forkNode):8][signFlippedBE(endpoint):8]}.
 * The {@code store} discriminator leads so the two RI-tree stores (lower / upper) occupy disjoint,
 * contiguous key ranges in the single HOT sub-tree; the fork node leads the endpoint so a fixed
 * {@code (store, forkNode)} endpoint sub-range is one contiguous range scan. Both longs are
 * sign-flipped big-endian (XOR with the sign bit) so unsigned byte comparison matches signed numeric
 * order — the exact technique {@code CASKeySerializer} uses for the path-node-key prefix.</p>
 *
 * <p>Stateless and thread-safe; all methods write into caller-provided buffers (zero allocation on
 * the hot path). The HOT chunked-bitmap layer appends a 4-byte {@code chunkIdx} trailer after this
 * prefix (see {@link HOTKeySerializer#serializeWithChunkIdx}); the trailing chunkIdx keeps all
 * chunks of one {@code (store, fork, endpoint)} key lex-clustered, so the endpoint range scan still
 * captures every chunk of every endpoint in range.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeKeySerializer implements HOTKeySerializer<ValidTimeKey> {

  /** Sign-flip constant for order-preserving encoding of signed longs. */
  private static final long SIGN_FLIP = 0x8000_0000_0000_0000L;

  /** Number of bytes the prefix occupies: 1 (store) + 8 (fork) + 8 (endpoint). */
  public static final int KEY_BYTES = 17;

  /** Singleton instance (stateless, thread-safe). */
  public static final ValidTimeKeySerializer INSTANCE = new ValidTimeKeySerializer();

  private ValidTimeKeySerializer() {
  }

  @Override
  public int serialize(final ValidTimeKey key, final byte[] dest, final int offset) {
    requireNonNull(key, "Key cannot be null");
    int o = offset;
    dest[o++] = key.store();
    o = putSignFlippedLong(key.forkNode(), dest, o);
    o = putSignFlippedLong(key.endpoint(), dest, o);
    return o - offset;
  }

  @Override
  public ValidTimeKey deserialize(final byte[] bytes, final int offset, final int length) {
    final byte store = bytes[offset];
    final long fork = getSignFlippedLong(bytes, offset + 1);
    final long endpoint = getSignFlippedLong(bytes, offset + 9);
    return new ValidTimeKey(store, fork, endpoint);
  }

  private static int putSignFlippedLong(final long value, final byte[] dest, final int offset) {
    final long bits = value ^ SIGN_FLIP;
    dest[offset] = (byte) (bits >>> 56);
    dest[offset + 1] = (byte) (bits >>> 48);
    dest[offset + 2] = (byte) (bits >>> 40);
    dest[offset + 3] = (byte) (bits >>> 32);
    dest[offset + 4] = (byte) (bits >>> 24);
    dest[offset + 5] = (byte) (bits >>> 16);
    dest[offset + 6] = (byte) (bits >>> 8);
    dest[offset + 7] = (byte) bits;
    return offset + 8;
  }

  private static long getSignFlippedLong(final byte[] bytes, final int offset) {
    final long bits = ((long) (bytes[offset] & 0xFF) << 56) | ((long) (bytes[offset + 1] & 0xFF) << 48)
        | ((long) (bytes[offset + 2] & 0xFF) << 40) | ((long) (bytes[offset + 3] & 0xFF) << 32)
        | ((long) (bytes[offset + 4] & 0xFF) << 24) | ((long) (bytes[offset + 5] & 0xFF) << 16)
        | ((long) (bytes[offset + 6] & 0xFF) << 8) | ((long) (bytes[offset + 7] & 0xFF));
    return bits ^ SIGN_FLIP;
  }
}
