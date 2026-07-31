package io.sirix.io.filechannel;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Per-JVM watermark of the highest revision whose 32-byte revisions-file record is known DURABLE
 * (covered by a completed {@code force} on the revisions file), for the lazy-revision-record
 * commit profile ({@link FileChannelWriter}), plus the two writer-to-writer handoffs that profile
 * needs: the tail-log ring snapshot and the write-frontier snapshot.
 *
 * <p>Writers are per-transaction objects, so none of this can live in the writer: the whole point
 * is that writer B (this commit) may rely on work writer A (a previous commit or its
 * {@code close()}) already paid for. Entries are keyed by the revisions file path PLUS the
 * resource UUID — deleting a resource and recreating one at the same path mints a fresh UUID and
 * therefore a fresh entry, so a stale claim can never vouch for the new resource's records.
 *
 * <p>The watermark is an optimization, not a correctness source on its own: a fresh JVM starts at
 * {@code -1} and the first tail-log eviction simply pays one synchronous {@code force}. Claims are
 * only ever advanced immediately after a successful force; truncation, rollback and file
 * re-initialization drop the WHOLE entry via {@link #invalidateFor} (there is deliberately no
 * partial lowering — the ring snapshot and frontier cache would go stale with it).
 */
public final class RevisionRecordDurability {

  private static final ConcurrentHashMap<String, RevisionRecordDurability> REGISTRY = new ConcurrentHashMap<>();

  /** Highest revision whose record is known durable; {@code -1} = nothing proven. */
  private final AtomicLong highestDurableRevision = new AtomicLong(-1L);

  private RevisionRecordDurability() {
  }

  /**
   * The entry for the given revisions file / resource identity. UUID halves of {@code 0/0}
   * (legacy resources without a UUID) key by path alone.
   *
   * @param revisionsFile path of the {@code sirix.revisions} file
   * @param resourceUuidMsb most significant UUID half of the resource ({@code 0} = legacy)
   * @param resourceUuidLsb least significant UUID half of the resource ({@code 0} = legacy)
   * @return the shared instance
   */
  public static RevisionRecordDurability forFile(final Path revisionsFile, final long resourceUuidMsb,
      final long resourceUuidLsb) {
    final String key = requireNonNull(revisionsFile, "revisionsFile").toAbsolutePath().normalize()
        + "#" + resourceUuidMsb + "#" + resourceUuidLsb;
    return REGISTRY.computeIfAbsent(key, k -> new RevisionRecordDurability());
  }

  /**
   * Drops every entry in the JVM. Wired into {@code Databases.clearGlobalCaches()} so the
   * cold-process simulation the tests rely on covers this registry too — a warm frontier or ring
   * snapshot would otherwise mask exactly the out-of-band file damage those tests inject.
   */
  public static void clearAll() {
    REGISTRY.clear();
  }

  /**
   * Drops every entry registered under the given revisions file path (any resource UUID). Called
   * when a resource's files are re-initialized outside the writer (interrupted-first-commit
   * recovery, truncation), where record offsets are about to be reused.
   *
   * @param revisionsFile path of the {@code sirix.revisions} file being re-initialized
   */
  public static void invalidateFor(final Path revisionsFile) {
    final String prefix = requireNonNull(revisionsFile, "revisionsFile").toAbsolutePath().normalize() + "#";
    final Iterator<Map.Entry<String, RevisionRecordDurability>> iterator = REGISTRY.entrySet().iterator();
    while (iterator.hasNext()) {
      if (iterator.next().getKey().startsWith(prefix)) {
        iterator.remove();
      }
    }
  }

  /** Highest revision whose record is known durable, or {@code -1}. */
  public long highestDurable() {
    return highestDurableRevision.get();
  }

  /**
   * Records that a force completed AFTER the given revision's record was written — every record up
   * to and including it is now durable. Monotonic: never lowers the watermark.
   *
   * <p>Callers claim only COMPLETED commits (typically {@code revision - 1}): a commit still in
   * flight may fail and be retried, and the retry rewrites its record with different content that
   * the completed force never covered.
   *
   * @param revision the highest revision covered by the completed force
   */
  public void advance(final long revision) {
    highestDurableRevision.accumulateAndGet(revision, Math::max);
  }

  /**
   * Once-per-resource-per-JVM latch for the legacy profile's on-disk tail-log preservation scan
   * (see {@link FileChannelWriter}): writers are per-commit, so a per-writer flag re-ran the
   * two-slot scan on EVERY legacy commit.
   */
  private final AtomicBoolean legacyTailChecked = new AtomicBoolean();

  /** @return {@code true} exactly once per resource per JVM — the caller then runs the scan. */
  public boolean beginLegacyTailCheck() {
    return legacyTailChecked.compareAndSet(false, true);
  }

  // ===== Tail-log ring snapshot (writer-to-writer handoff) =====

  /** Defensive copy of the in-memory tail-log ring after the newest staged entry. */
  private byte[] tailLogSnapshot;

  /** Revision whose entry the snapshot was staged with; {@code -1} = no snapshot. */
  private long tailLogSnapshotRevision = -1L;

  /**
   * Adopts the predecessor writer's ring for a commit of {@code currentRevision}, or returns
   * {@code null} when no exact-predecessor snapshot exists (fresh JVM, rollback, gap). Without
   * this handoff every commit re-derived the ring from the on-disk beacon slots — two 4 KiB preads
   * plus up to {@code capacity} record verifications per commit. The returned array is a defensive
   * copy (the caller mutates it freely).
   *
   * @param currentRevision the revision about to be staged
   * @return a copy of the ring as of revision {@code currentRevision - 1}, or {@code null}
   */
  public synchronized byte[] adoptTailLogSnapshot(final int currentRevision) {
    if (tailLogSnapshot == null || tailLogSnapshotRevision != currentRevision - 1L) {
      return null;
    }
    return tailLogSnapshot.clone();
  }

  /**
   * Stores a defensive copy of the ring right after the given revision's entry was staged. The
   * copy (not the live array) is essential: the storing writer's background hardening phase may
   * still read its own array while the successor mutates a fresh adoption copy.
   *
   * @param tailLog the writer's in-memory ring
   * @param revision the revision whose entry was just staged
   */
  public synchronized void storeTailLogSnapshot(final byte[] tailLog, final int revision) {
    this.tailLogSnapshot = requireNonNull(tailLog, "tailLog").clone();
    this.tailLogSnapshotRevision = revision;
  }

  // ===== Write-frontier snapshot (writer-to-writer handoff, preallocated profile) =====

  /**
   * The three frontiers as ONE immutable snapshot. They are only meaningful together — the
   * adoption gate validates the logical end against the preallocation ends — so publishing them
   * as three independent volatiles would let a reader mix one commit's logical end with another's
   * preallocation end and adopt a frontier no writer ever held.
   *
   * @param dataLogicalEnd logical data-file write frontier; {@code -1} = unknown
   * @param dataPreallocEnd physical preallocation end of the data file
   * @param revisionsPreallocEnd physical preallocation end of the revisions file
   */
  private record Frontiers(long dataLogicalEnd, long dataPreallocEnd, long revisionsPreallocEnd) {
  }

  private static final Frontiers UNKNOWN_FRONTIERS = new Frontiers(-1L, -1L, -1L);

  private volatile Frontiers frontiers = UNKNOWN_FRONTIERS;

  /**
   * Stores the writer's current frontiers. Without this handoff every commit re-derived the
   * logical frontier from the durable revision graph — a beacon parse plus a revision-record read
   * plus a length-header pread per commit. Truncation and rollback invalidate the whole entry
   * ({@link #invalidateFor}), so a stale frontier can never survive a timeline change.
   *
   * @param dataLogicalEnd logical data-file write frontier
   * @param dataPreallocEnd physical preallocation end of the data file
   * @param revisionsPreallocEnd physical preallocation end of the revisions file
   */
  public void storeFrontiers(final long dataLogicalEnd, final long dataPreallocEnd,
      final long revisionsPreallocEnd) {
    frontiers = new Frontiers(dataLogicalEnd, dataPreallocEnd, revisionsPreallocEnd);
  }

  /**
   * The cached frontiers as one consistent triple. Callers MUST validate and adopt from this
   * single snapshot rather than re-reading, so the values they check are the values they use.
   *
   * @return {@code [dataLogicalEnd, dataPreallocEnd, revisionsPreallocEnd]}; a logical end of
   *         {@code -1} means "unknown, derive from disk"
   */
  public long[] cachedFrontiers() {
    final Frontiers snapshot = frontiers;
    return new long[] {snapshot.dataLogicalEnd(), snapshot.dataPreallocEnd(),
        snapshot.revisionsPreallocEnd()};
  }
}
