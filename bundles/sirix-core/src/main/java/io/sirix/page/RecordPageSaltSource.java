package io.sirix.page;

import io.sirix.node.LE;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

final class RecordPageSaltSource {
  private static final ThreadLocal<RecordPageSaltSource> SOURCES = ThreadLocal.withInitial(RecordPageSaltSource::new);
  private final Cipher generator;
  private final byte[] input = new byte[2048];
  private final byte[] output = new byte[2048];
  private final MemorySegment salts = MemorySegment.ofArray(output);
  private int offset = output.length;

  private RecordPageSaltSource() {
    this(newSeed());
  }

  RecordPageSaltSource(final byte[] seed) {
    if (seed.length != 48) {
      throw new IllegalArgumentException("A salt source requires a 32-byte key and a 16-byte counter");
    }
    try {
      generator = Cipher.getInstance("AES/CTR/NoPadding");
      generator.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(seed, 0, 32, "AES"), new IvParameterSpec(seed, 32, 16));
    } catch (final GeneralSecurityException e) {
      throw new IllegalStateException("Cannot initialize the record-page salt generator", e);
    }
  }

  static RecordPageSaltSource current() {
    return SOURCES.get();
  }

  long nextSalt() {
    if (offset == output.length) {
      try {
        final int written = generator.update(input, 0, input.length, output, 0);
        if (written != output.length) {
          throw new IllegalStateException("Incomplete record-page salt batch");
        }
      } catch (final ShortBufferException e) {
        throw new IllegalStateException("Cannot fill the record-page salt batch", e);
      }
      offset = 0;
    }
    final long salt = salts.get(LE.LONG, offset);
    offset += Long.BYTES;
    return salt;
  }

  private static byte[] newSeed() {
    try {
      final SecureRandom random = SecureRandom.getInstance(System.getProperty("os.name").startsWith("Windows")
          ? "Windows-PRNG"
          : "NativePRNGNonBlocking");
      final byte[] seed = new byte[48];
      random.nextBytes(seed);
      return seed;
    } catch (final GeneralSecurityException e) {
      throw new IllegalStateException("Non-blocking randomness is required for record-page salts", e);
    }
  }
}
