package io.sirix.io;

import io.sirix.exception.SirixIOException;
import net.openhft.hashing.LongHashFunction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The 64-byte superblock at offset 0 of both {@code sirix.data} and {@code sirix.revisions}.
 *
 * <p>Before the superblock existed, the binary files carried no magic, version, or endianness
 * marker — format identity lived only in the resource-settings JSON, so opening a file with the
 * wrong backend/version/architecture mis-read it silently. The superblock makes every such
 * mismatch fail fast and reserves file-level extension space (flags + reserved region).
 *
 * <p>Layout (all fields little-endian, see {@code docs/DISK_FORMAT.md}):
 *
 * <pre>
 *  0  8 B  magic "SIRIXDB!"
 *  8  4 B  layout version (= {@link #LAYOUT_VERSION})
 * 12  1 B  file role: 0 = data, 1 = revisions
 * 13  1 B  reserved (0)
 * 14  2 B  flags (0)
 * 16  4 B  endianness check pattern {@link #ENDIAN_CHECK} (mis-ordered on a foreign-endian host)
 * 20  4 B  slot size in bytes (data file: {@link IOStorage#BEACON_SLOT_BYTES} beacon slots;
 *                              revisions: {@link IOStorage#REVISIONS_FILE_RECORD_SIZE} record
 *                              stride — persisted so the stride is file geometry, not a compiled-in
 *                              assumption; 0 in pre-stride dev files)
 * 24  8 B  primary beacon offset   (data file: {@link IOStorage#PRIMARY_BEACON_OFFSET}; revisions: 0)
 * 32  8 B  content start offset    (data: {@link IOStorage#DATA_REGION_START};
 *                                   revisions: {@link IOStorage#REVISIONS_RECORDS_START})
 * 40 16 B  reserved (future resource UUID)
 * 56  8 B  XXH3-64 of bytes [0, 56)
 * </pre>
 */
public final class Superblock {

  /** {@code "SIRIXDB!"} */
  public static final byte[] MAGIC = { 'S', 'I', 'R', 'I', 'X', 'D', 'B', '!' };

  /**
   * File-layout version (independent of the per-page {@code BinaryEncodingVersion}). V0 is the
   * FIRST and only layout — there are no pre-superblock files in the wild, so the superblocked
   * layout is not a "v1" of anything.
   */
  public static final int LAYOUT_VERSION = 0;

  /** Fixed pattern whose byte order proves the writer's endianness to a reader. */
  public static final int ENDIAN_CHECK = 0x01020304;

  /** Size of the superblock itself. */
  public static final int BYTES = 64;

  public static final byte ROLE_DATA = 0;
  public static final byte ROLE_REVISIONS = 1;

  private static final int HASHED_PREFIX_BYTES = 56;

  private Superblock() {
  }

  /** Builds the 64-byte superblock for the given file role. */
  public static ByteBuffer build(final byte fileRole) {
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
    buf.position(buf.position() + 16); // reserved
    final long hash = LongHashFunction.xx3().hashBytes(buf.array(), 0, HASHED_PREFIX_BYTES);
    buf.putLong(hash);
    buf.flip();
    return buf;
  }

  /**
   * Validates a superblock read from offset 0. Throws {@link SirixIOException} with an actionable
   * message on any mismatch — wrong magic (not a sirix file / pre-superblock layout), wrong
   * layout version, foreign endianness, wrong file role, or checksum corruption.
   */
  public static void validate(final ByteBuffer raw, final byte expectedRole, final String fileLabel) {
    if (raw.remaining() < BYTES) {
      throw new SirixIOException(fileLabel + ": file too short for a superblock (" + raw.remaining()
          + " bytes) — not a sirix data file");
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
    if (!java.util.Arrays.equals(magic, MAGIC)) {
      throw new SirixIOException(fileLabel + ": bad magic — not a sirix data file "
          + "(or written by an incompatible version)");
    }
    final int version = buf.getInt();
    if (version != LAYOUT_VERSION) {
      throw new SirixIOException(fileLabel + ": unsupported layout version " + version + " (expected "
          + LAYOUT_VERSION + ")");
    }
    final byte role = buf.get();
    buf.get();
    buf.getShort();
    final int endian = buf.getInt();
    if (endian != ENDIAN_CHECK) {
      throw new SirixIOException(fileLabel + ": endianness mismatch (written on a foreign-endian host)");
    }
    if (role != expectedRole) {
      throw new SirixIOException(fileLabel + ": wrong file role " + role + " (expected " + expectedRole + ")");
    }
    final int slotSize = buf.getInt();
    if (role == ROLE_REVISIONS && slotSize != IOStorage.REVISIONS_FILE_RECORD_SIZE && slotSize != 0) {
      // 0 = pre-stride dev files (the field was reserved-zero before it carried the stride).
      throw new SirixIOException(fileLabel + ": revisions record stride " + slotSize
          + " does not match this build's " + IOStorage.REVISIONS_FILE_RECORD_SIZE
          + "-byte records — file written by an incompatible version");
    }
    if (role == ROLE_DATA && slotSize != IOStorage.BEACON_SLOT_BYTES) {
      throw new SirixIOException(fileLabel + ": beacon slot size " + slotSize
          + " does not match this build's " + IOStorage.BEACON_SLOT_BYTES
          + "-byte slots — file written by an incompatible version");
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
