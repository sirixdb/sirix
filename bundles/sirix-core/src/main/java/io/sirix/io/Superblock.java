package io.sirix.io;

import io.sirix.exception.UnsupportedFormatVersionException;
import io.sirix.exception.SirixIOException;
import net.openhft.hashing.LongHashFunction;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.nio.ByteOrder;

/**
 * The 64-byte superblock at offset 0 of both {@code sirix.data} and {@code sirix.revisions}.
 *
 * <p>
 * Before the superblock existed, the binary files carried no magic, version, or endianness marker —
 * format identity lived only in the resource-settings JSON, so opening a file with the wrong
 * backend/version/architecture mis-read it silently. The superblock makes every such mismatch fail
 * fast. Spare bytes are not an optional extension point; see {@code docs/DISK_FORMAT.md} for
 * validation and format-evolution rules.
 *
 * <p>
 * Layout (all fields little-endian, see {@code docs/DISK_FORMAT.md}):
 *
 * <pre>
 *  0  8 B  magic "SIRIXDB!"
 *  8  4 B  layout version (= {@link #LAYOUT_VERSION})
 * 12  1 B  file role: 0 = data, 1 = revisions
 * 13  3 B  spare bytes (must be zero)
 * 16  4 B  endianness check pattern {@link #ENDIAN_CHECK} (mis-ordered on a foreign-endian host)
 * 20  4 B  slot size in bytes (data file: {@link IOStorage#BEACON_SLOT_BYTES} beacon slots;
 *                              revisions: {@link IOStorage#REVISIONS_FILE_RECORD_SIZE} record
 *                              stride — persisted so the stride is file geometry, not a compiled-in
 *                              assumption)
 * 24  8 B  primary beacon offset   (data file: {@link IOStorage#PRIMARY_BEACON_OFFSET}; revisions: 0)
 * 32  8 B  content start offset    (data: {@link IOStorage#DATA_REGION_START};
 *                                   revisions: {@link IOStorage#REVISIONS_RECORDS_START})
 * 40 16 B  resource UUID (msb u64, lsb u64) — cross-links the file to its resource-settings
 *          JSON; always nonzero (FMT-102)
 * 56  8 B  XXH3-64 of bytes [0, 56)
 * </pre>
 */
public final class Superblock {

  /** {@code "SIRIXDB!"} */
  public static final byte[] MAGIC = {'S', 'I', 'R', 'I', 'X', 'D', 'B', '!'};

  /**
   * File-layout version (independent of the per-page {@code BinaryEncodingVersion}). V0 is the FIRST
   * and only layout — there are no pre-superblock files in the wild, so the superblocked layout is
   * not a "v1" of anything.
   */
  public static final int LAYOUT_VERSION = 0;

  /** Fixed pattern whose byte order proves the writer's endianness to a reader. */
  public static final int ENDIAN_CHECK = 0x01020304;

  /** Size of the superblock itself. */
  public static final int BYTES = 64;

  public static final byte ROLE_DATA = 0;
  public static final byte ROLE_REVISIONS = 1;

  private static final int HASHED_PREFIX_BYTES = 56;

  private Superblock() {}

  /** Builds the 64-byte superblock for the given file role and resource UUID. */
  public static ByteBuffer build(final byte fileRole, final long uuidMsb, final long uuidLsb) {
    final ByteBuffer buf = ByteBuffer.allocate(BYTES).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(MAGIC);
    buf.putInt(LAYOUT_VERSION);
    buf.put(fileRole);
    buf.put((byte) 0);
    buf.putShort((short) 0);
    buf.putInt(ENDIAN_CHECK);
    if (fileRole == ROLE_DATA) {
      buf.putInt(IOStorage.BEACON_SLOT_BYTES);
      buf.putLong(IOStorage.PRIMARY_BEACON_OFFSET);
      buf.putLong(IOStorage.DATA_REGION_START);
    } else {
      buf.putInt(IOStorage.REVISIONS_FILE_RECORD_SIZE);
      buf.putLong(0L);
      buf.putLong(IOStorage.REVISIONS_RECORDS_START);
    }
    buf.putLong(uuidMsb);
    buf.putLong(uuidLsb);
    final long hash = LongHashFunction.xx3().hashBytes(buf.array(), 0, HASHED_PREFIX_BYTES);
    buf.putLong(hash);
    buf.flip();
    return buf;
  }

  /**
   * Validates a superblock whose resource identity is not yet known — recovery paths that run before
   * the configuration is loaded. The stored UUID must still be nonzero; only the cross-check against
   * the settings identity is left out.
   */
  public static void validate(final ByteBuffer raw, final byte expectedRole, final String fileLabel) {
    validate(raw, expectedRole, fileLabel, 0L, 0L);
  }

  /**
   * Validates a superblock read from offset 0. Throws {@link UnsupportedFormatVersionException} for a
   * foreign layout version and {@link SirixIOException} for other mismatches — wrong magic, foreign
   * endianness, wrong file role, geometry drift, checksum corruption, or a resource-UUID mismatch
   * (the file belongs to a different resource than the settings JSON). A zero stored UUID is refused
   * outright; a zero expected UUID means the caller does not know the resource identity yet and only
   * skips the cross-check.
   */
  public static void validate(final ByteBuffer raw, final byte expectedRole, final String fileLabel,
      final long expectedUuidMsb, final long expectedUuidLsb) {
    if (raw.remaining() < BYTES) {
      throw new SirixIOException(
          fileLabel + ": file too short for a superblock (" + raw.remaining() + " bytes) — not a sirix data file");
    }
    // An all-zero superblock is not a foreign file: it is what a crash during the very first
    // commit (or a lost first sector) leaves behind — "bad magic" would point at the wrong cause.
    if (isAllZero(raw)) {
      throw new SirixIOException(fileLabel + ": superblock is all zeros — interrupted first commit "
          + "or lost file header; the resource has no committed revision: delete and re-create it");
    }
    final ByteBuffer buf = raw.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    final byte[] magic = new byte[MAGIC.length];
    buf.get(magic);
    if (!Arrays.equals(magic, MAGIC)) {
      throw new SirixIOException(
          fileLabel + ": bad magic — not a sirix data file " + "(or written by an incompatible version)");
    }
    final int version = buf.getInt();
    if (version != LAYOUT_VERSION) {
      throw new UnsupportedFormatVersionException(
          fileLabel + ": unsupported format version " + version + " (expected " + LAYOUT_VERSION + ")");
    }
    final byte role = buf.get();
    final int spareBytes = (buf.get() & 0xFF) | (buf.getShort() & 0xFFFF);
    if (spareBytes != 0) {
      throw new SirixIOException(fileLabel + ": unsupported nonzero superblock spare bytes");
    }
    final int endian = buf.getInt();
    if (endian != ENDIAN_CHECK) {
      throw new SirixIOException(fileLabel + ": endianness mismatch (written on a foreign-endian host)");
    }
    if (role != expectedRole) {
      throw new SirixIOException(fileLabel + ": wrong file role " + role + " (expected " + expectedRole + ")");
    }
    final int slotSize = buf.getInt();
    if (role == ROLE_REVISIONS && slotSize != IOStorage.REVISIONS_FILE_RECORD_SIZE) {
      throw new SirixIOException(fileLabel + ": revisions record stride " + slotSize + " does not match this build's "
          + IOStorage.REVISIONS_FILE_RECORD_SIZE + "-byte records — file written by an incompatible version");
    }
    if (role == ROLE_DATA && slotSize != IOStorage.BEACON_SLOT_BYTES) {
      throw new SirixIOException(fileLabel + ": beacon slot size " + slotSize + " does not match this build's "
          + IOStorage.BEACON_SLOT_BYTES + "-byte slots — file written by an incompatible version");
    }
    final long storedUuidMsb = buf.getLong(40);
    final long storedUuidLsb = buf.getLong(48);
    if (storedUuidMsb == 0 && storedUuidLsb == 0) {
      throw new SirixIOException(
          fileLabel + ": zero resource UUID in the superblock — the file does not identify its resource");
    }
    // A zero EXPECTED UUID is not leniency: the caller (pre-configuration recovery) does not know
    // the resource identity yet, so there is nothing to compare against.
    final boolean expectedUuidPresent = expectedUuidMsb != 0 || expectedUuidLsb != 0;
    if (expectedUuidPresent && (storedUuidMsb != expectedUuidMsb || storedUuidLsb != expectedUuidLsb)) {
      throw new SirixIOException(fileLabel + ": resource UUID mismatch — the file belongs to a "
          + "different resource than the settings JSON (wrong-backup restore or copied files)");
    }
    final byte[] prefix = new byte[HASHED_PREFIX_BYTES];
    raw.duplicate().get(prefix);
    final long expectedHash = LongHashFunction.xx3().hashBytes(prefix);
    final long storedHash = buf.getLong(HASHED_PREFIX_BYTES);
    if (expectedHash != storedHash) {
      throw new SirixIOException(fileLabel + ": superblock checksum mismatch — header corrupted");
    }
  }

  private static boolean isAllZero(final ByteBuffer raw) {
    final ByteBuffer dup = raw.duplicate();
    for (int i = 0; i < BYTES; i++) {
      if (dup.get() != 0) {
        return false;
      }
    }
    return true;
  }
}
