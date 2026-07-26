package io.sirix.crash;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test-only global write/force log shared by every {@link RecordingFileChannel} of one recording
 * session. A single monotonically increasing sequence number spans BOTH resource files
 * ({@code sirix.data} and {@code sirix.revisions}) so the cross-file ordering of the commit
 * protocol (data tail force → revisions force → secondary beacon → force → primary beacon →
 * commit-end forceAll) is captured faithfully.
 *
 * <p>Milestones mark "the API call acknowledged this revision to the client at sequence S" —
 * the verifier derives from them which revision MUST survive any crash at instant ≥ S.
 */
final class PowerLossRecorder {

  enum TargetFile {
    DATA, REVISIONS
  }

  enum OpKind {
    WRITE, FORCE, TRUNCATE
  }

  /**
   * Per-write durability from the channel's open options. {@code DSYNC} = content (and
   * retrieve-metadata per POSIX) durable at write-return; {@code SYNC} = content AND all
   * metadata (including size extension) durable at write-return; {@code NONE} = buffered,
   * durable only via a later force barrier.
   */
  enum WriteDurability {
    NONE, DSYNC, SYNC
  }

  /**
   * One recorded channel operation. {@code bytes} is only set for WRITE, {@code metaData} only
   * meaningful for FORCE, {@code newSize} only for TRUNCATE.
   */
  static final class Op {
    final long seq;
    final TargetFile file;
    final OpKind kind;
    final long offset;
    final byte[] bytes;
    final boolean metaData;
    final long newSize;
    final WriteDurability durability;

    private Op(final long seq, final TargetFile file, final OpKind kind, final long offset, final byte[] bytes,
        final boolean metaData, final long newSize, final WriteDurability durability) {
      this.seq = seq;
      this.file = file;
      this.kind = kind;
      this.offset = offset;
      this.bytes = bytes;
      this.metaData = metaData;
      this.newSize = newSize;
      this.durability = durability;
    }

    long end() {
      return offset + (bytes == null ? 0 : bytes.length);
    }

    @Override
    public String toString() {
      return switch (kind) {
        case WRITE -> "#" + seq + " WRITE " + file + " [" + offset + ", " + end() + ") len=" + bytes.length
            + (durability == WriteDurability.NONE ? "" : " " + durability);
        case FORCE -> "#" + seq + " FORCE " + file + " metaData=" + metaData;
        case TRUNCATE -> "#" + seq + " TRUNCATE " + file + " to " + newSize;
      };
    }
  }

  /** A point in the op stream at which an API call returned (revision acknowledged). */
  record Milestone(String name, long lastSeqInclusive, int acknowledgedRevision) {
  }

  private final List<Op> ops = new ArrayList<>();
  private final Map<String, Milestone> milestones = new LinkedHashMap<>();
  private long nextSeq;

  synchronized void recordWrite(final TargetFile file, final long offset, final byte[] bytes,
      final WriteDurability durability) {
    ops.add(new Op(nextSeq++, file, OpKind.WRITE, offset, bytes, false, -1, durability));
  }

  synchronized void recordForce(final TargetFile file, final boolean metaData) {
    ops.add(new Op(nextSeq++, file, OpKind.FORCE, -1, null, metaData, -1, WriteDurability.NONE));
  }

  synchronized void recordTruncate(final TargetFile file, final long newSize) {
    ops.add(new Op(nextSeq++, file, OpKind.TRUNCATE, -1, null, false, newSize, WriteDurability.NONE));
  }

  /** Marks "the API call that produced {@code acknowledgedRevision} has returned". */
  synchronized void mark(final String name, final int acknowledgedRevision) {
    milestones.put(name, new Milestone(name, nextSeq - 1, acknowledgedRevision));
  }

  synchronized List<Op> snapshotOps() {
    return List.copyOf(ops);
  }

  synchronized List<Milestone> snapshotMilestones() {
    return List.copyOf(milestones.values());
  }
}
