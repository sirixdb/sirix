package io.sirix.io;

import io.sirix.exception.SirixIOException;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The superblock's resource UUID cross-links the binary files to their resource-settings JSON:
 * a matching pair validates, a mismatched pair (files restored from a different resource's
 * backup) fails fast, and a zero UUID on either side (legacy file or legacy config) skips the
 * cross-check.
 */
public final class SuperblockResourceUuidTest {

  private static final long MSB = 0x0011223344556677L;
  private static final long LSB = 0x8899AABBCCDDEEFFL;

  @Test
  public void matchingUuidValidates() {
    final ByteBuffer sb = Superblock.build(Superblock.ROLE_DATA, MSB, LSB);
    Superblock.validate(sb, Superblock.ROLE_DATA, "sirix.data", MSB, LSB);
  }

  @Test
  public void mismatchedUuidFailsFast() {
    final ByteBuffer sb = Superblock.build(Superblock.ROLE_DATA, MSB, LSB);
    final SirixIOException e = assertThrows(SirixIOException.class,
        () -> Superblock.validate(sb, Superblock.ROLE_DATA, "sirix.data", MSB, LSB + 1));
    assertTrue(e.getMessage().contains("resource UUID mismatch"));
  }

  @Test
  public void zeroStoredUuidIsLegacyAndSkipsCheck() {
    final ByteBuffer sb = Superblock.build(Superblock.ROLE_DATA);
    Superblock.validate(sb, Superblock.ROLE_DATA, "sirix.data", MSB, LSB);
  }

  @Test
  public void zeroExpectedUuidIsLegacyAndSkipsCheck() {
    final ByteBuffer sb = Superblock.build(Superblock.ROLE_REVISIONS, MSB, LSB);
    Superblock.validate(sb, Superblock.ROLE_REVISIONS, "sirix.revisions", 0L, 0L);
  }

  @Test
  public void uuidIsCoveredByTheHeaderChecksum() {
    final ByteBuffer sb = Superblock.build(Superblock.ROLE_DATA, MSB, LSB);
    final ByteBuffer corrupted = ByteBuffer.allocate(Superblock.BYTES);
    corrupted.put(sb.duplicate());
    corrupted.put(40, (byte) (corrupted.get(40) ^ 0x01)); // flip a UUID bit
    corrupted.flip();
    final SirixIOException e = assertThrows(SirixIOException.class,
        () -> Superblock.validate(corrupted, Superblock.ROLE_DATA, "sirix.data", 0L, 0L));
    assertTrue(e.getMessage().contains("checksum"));
  }
}
