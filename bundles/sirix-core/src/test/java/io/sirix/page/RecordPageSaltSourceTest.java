package io.sirix.page;

import io.sirix.node.LE;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class RecordPageSaltSourceTest {
  @Test
  void rejectsMissingOrMalformedSeedMaterial() {
    assertThrows(NullPointerException.class, () -> new RecordPageSaltSource(null));
    for (final int length : new int[] {0, 16, 32, 47, 49, 64}) {
      assertThrows(IllegalArgumentException.class, () -> new RecordPageSaltSource(new byte[length]));
    }
  }

  @Test
  void bufferedSaltsMatchNistAes256CtrAndContinueAcrossRefills() throws GeneralSecurityException {
    final HexFormat hex = HexFormat.of();
    final byte[] seed = hex.parseHex("603deb1015ca71be2b73aef0857d7781"
        + "1f352c073b6108d72d9810a30914dff4"
        + "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");
    final RecordPageSaltSource source = new RecordPageSaltSource(seed);
    final byte[] publishedPlaintext = hex.parseHex("6bc1bee22e409f96e93d7e117393172a");
    final byte[] publishedCiphertext = hex.parseHex("601ec313775789a5b7a7f504bbf3d228");
    final byte[] keystream = new byte[16];
    for (int i = 0; i < keystream.length; i++) {
      keystream[i] = (byte) (publishedPlaintext[i] ^ publishedCiphertext[i]);
    }
    final Cipher blockCipher = Cipher.getInstance("AES/ECB/NoPadding");
    blockCipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(seed, 0, 32, "AES"));
    BigInteger counter = new BigInteger(1, Arrays.copyOfRange(seed, 32, 48));
    for (int block = 0; block < 1024; block++) {
      final byte[] encodedCounter = counter.toByteArray();
      final byte[] input = Arrays.copyOfRange(encodedCounter, encodedCounter.length - 16, encodedCounter.length);
      final byte[] expected = blockCipher.doFinal(input);
      if (block == 0) {
        for (int i = 0; i < keystream.length; i++) {
          assertEquals(keystream[i], expected[i], "NIST SP 800-38A F.5.5");
        }
      }
      for (int offset = 0; offset < 16; offset += 8) {
        assertEquals(MemorySegment.ofArray(expected).get(LE.LONG, offset), source.nextSalt());
      }
      counter = counter.add(BigInteger.ONE);
    }
  }
}
