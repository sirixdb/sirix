package io.sirix.crash;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reconstructs a candidate post-power-loss image of one file from a recorded op stream.
 *
 * <p>Power-loss model (matches the documented {@code FileChannel.force} contract):
 * <ul>
 *   <li>every op up to (and including) the file's LAST COMPLETED {@code force()} at or before the
 *       crash instant is durable and fully applied;</li>
 *   <li>ops issued after that barrier but before the crash instant are "in flight": each may be
 *       lost, fully applied, or — for writes — torn (an arbitrary prefix applied), independently
 *       of one another and of issue order (the page cache flushes in any order);</li>
 *   <li>ops after the crash instant never happened.</li>
 * </ul>
 *
 * <p>Holes created by applying a later append without an earlier one read as zeros (newly
 * allocated blocks). The recorded original files are never touched — materialization builds a
 * fresh byte image.
 */
final class CrashStateMaterializer {

  private CrashStateMaterializer() {
  }

  /**
   * @param ops the full recorded op stream (both files)
   * @param file which file to materialize
   * @param crashSeq the crash instant: ops with {@code seq <= crashSeq} were issued (use -1 for
   *        "before anything")
   * @param appliedInFlight seqs of in-flight ops that made it to the platter
   * @param tornPrefixLength for in-flight WRITE seqs in this map, only the given byte prefix is
   *        applied (overrides membership in {@code appliedInFlight})
   * @return the materialized file content
   */
  static byte[] materialize(final List<PowerLossRecorder.Op> ops, final PowerLossRecorder.TargetFile file,
      final long crashSeq, final Set<Long> appliedInFlight, final Map<Long, Integer> tornPrefixLength) {
    return materialize(ops, file, crashSeq, appliedInFlight, tornPrefixLength, false);
  }

  /**
   * Like {@link #materialize(List, PowerLossRecorder.TargetFile, long, Set, Map)}, but when
   * {@code metadataSplit} is true the file LENGTH is additionally capped at the length the file
   * had after the last completed {@code force(true)} — modelling a platform where
   * {@code force(false)} makes data blocks durable but NOT the size-extension metadata. NOTE:
   * this is STRICTER than POSIX {@code fdatasync}, which must persist metadata required to
   * retrieve the data (i.e. the size) — findings under this mode are hardening observations, not
   * POSIX-reachable states.
   */
  static byte[] materialize(final List<PowerLossRecorder.Op> ops, final PowerLossRecorder.TargetFile file,
      final long crashSeq, final Set<Long> appliedInFlight, final Map<Long, Integer> tornPrefixLength,
      final boolean metadataSplit) {
    long lastBarrier = -1;
    for (final PowerLossRecorder.Op op : ops) {
      if (op.seq > crashSeq) {
        break;
      }
      if (op.file == file && op.kind == PowerLossRecorder.OpKind.FORCE) {
        lastBarrier = op.seq;
      }
    }

    long metadataDurableLength = -1;
    if (metadataSplit) {
      long lastMetaBarrier = -1;
      for (final PowerLossRecorder.Op op : ops) {
        if (op.seq > crashSeq) {
          break;
        }
        if (op.file == file && op.kind == PowerLossRecorder.OpKind.FORCE && op.metaData) {
          lastMetaBarrier = op.seq;
        }
      }
      final Image metaImage = new Image();
      for (final PowerLossRecorder.Op op : ops) {
        if (op.seq > crashSeq) {
          break;
        }
        if (op.file != file) {
          continue;
        }
        // Size-extension durability: covered by the last force(true) barrier, OR — for writes
        // through a SYNC channel that RETURNED before the crash instant — by O_SYNC's
        // all-metadata-per-write guarantee. DSYNC writes deliberately do NOT raise the cap
        // here (paranoid beyond POSIX): the protocol must not depend on fdatasync-class size
        // semantics for any file-extending write.
        final boolean syncReturned =
            op.kind == PowerLossRecorder.OpKind.WRITE && op.durability == PowerLossRecorder.WriteDurability.SYNC
                && op.seq < crashSeq;
        if (op.seq <= lastMetaBarrier || syncReturned) {
          if (op.kind == PowerLossRecorder.OpKind.WRITE) {
            metaImage.write(op.offset, op.bytes, op.bytes.length);
          } else if (op.kind == PowerLossRecorder.OpKind.TRUNCATE) {
            metaImage.truncate(op.newSize);
          }
        }
      }
      metadataDurableLength = metaImage.length();
    }

    final Image image = new Image();
    for (final PowerLossRecorder.Op op : ops) {
      if (op.seq > crashSeq) {
        break;
      }
      if (op.file != file) {
        continue;
      }
      switch (op.kind) {
        case FORCE -> {
          // No content effect.
        }
        case WRITE -> {
          // A write through a write-through (DSYNC/SYNC) channel that RETURNED before the crash
          // instant (a later op was issued ⇒ this one completed) is durable by definition. The
          // op exactly AT the crash boundary may still be mid-write — the torn/in-flight rules
          // below apply to it like to any other write.
          final boolean returnedDurably = op.durability != PowerLossRecorder.WriteDurability.NONE
              && op.seq < crashSeq;
          if (returnedDurably || op.seq <= lastBarrier) {
            image.write(op.offset, op.bytes, op.bytes.length);
          } else if (tornPrefixLength.containsKey(op.seq)) {
            image.write(op.offset, op.bytes, tornPrefixLength.get(op.seq));
          } else if (appliedInFlight.contains(op.seq)) {
            image.write(op.offset, op.bytes, op.bytes.length);
          }
        }
        case TRUNCATE -> {
          if (op.seq <= lastBarrier || appliedInFlight.contains(op.seq)) {
            image.truncate(op.newSize);
          }
        }
      }
    }
    if (metadataSplit && metadataDurableLength >= 0 && metadataDurableLength < image.length()) {
      image.truncate(metadataDurableLength);
    }
    return image.toBytes();
  }

  /** Growable zero-filled byte image. */
  private static final class Image {
    private byte[] bytes = new byte[16 * 1024];
    private long length;

    void write(final long offset, final byte[] data, final int count) {
      if (count <= 0) {
        return;
      }
      final long end = offset + count;
      if (end > Integer.MAX_VALUE) {
        throw new IllegalStateException("Materialized image too large: " + end);
      }
      ensureCapacity((int) end);
      System.arraycopy(data, 0, bytes, (int) offset, count);
      if (end > length) {
        length = end;
      }
    }

    void truncate(final long newSize) {
      if (newSize < length) {
        // Zero the dropped range so a later regrow reads zeros, like a real truncate.
        java.util.Arrays.fill(bytes, (int) newSize, (int) length, (byte) 0);
        length = newSize;
      }
    }

    long length() {
      return length;
    }

    private void ensureCapacity(final int end) {
      if (end > bytes.length) {
        int newCapacity = bytes.length;
        while (newCapacity < end) {
          newCapacity *= 2;
        }
        bytes = java.util.Arrays.copyOf(bytes, newCapacity);
      }
    }

    byte[] toBytes() {
      return java.util.Arrays.copyOf(bytes, (int) length);
    }
  }
}
