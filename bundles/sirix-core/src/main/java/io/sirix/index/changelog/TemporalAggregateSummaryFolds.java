package io.sirix.index.changelog;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.aggregate.TemporalAggregateDomain;
import io.sirix.index.aggregate.TemporalAggregateParts;
import io.sirix.index.hot.reader.HOTTrieReader;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/**
 * Maintainer-only binary folds of source summaries. Completed ranges are immutable in each derived
 * publication; retaining their children supports every historical (base, target] interval. A
 * covered interval needs at most two ranges per revision bit, regardless of its lag. The source
 * commit path never opens or writes this tree.
 */
public final class TemporalAggregateSummaryFolds {
  private static final byte[] FRONTIER = {10};
  private static final int HEADER_BYTES = 24;
  private static final TemporalAggregateCatchUpBudget.Budget COMPLETE =
      TemporalAggregateCatchUpBudget.Budget.complete();

  private TemporalAggregateSummaryFolds() {}

  private static byte[] key(final int index, final int end, final int level, final int chunk) {
    return ByteBuffer.allocate(14).put((byte) 8).putInt(index).putInt(end).put((byte) level).putInt(chunk).array();
  }

  /** A frontier from a truncated/reissued source history is never reused. */
  public static int frontier(final StorageEngineReader derived, final StorageEngineReader source) {
    try (Reader reader = new Reader(derived)) {
      final byte[] value = reader.get(FRONTIER);
      if (value == null) {
        return 0;
      }
      if (value.length != HEADER_BYTES) {
        throw new IllegalStateException("Invalid summary fold frontier");
      }
      final ByteBuffer in = ByteBuffer.wrap(value);
      final int revision = in.getInt();
      final int projection = in.getInt();
      if (revision < 1 || projection < -1) {
        throw new IllegalStateException("Invalid summary fold frontier identity");
      }
      if (revision > source.getRevisionNumber()) {
        return 0;
      }
      if (projection < 0) {
        return revision;
      }
      // A source revert changes its current log root, but retains the older publications.
      // Maintenance follows that publication history, whereas query ranges follow their pinned root.
      try (StorageEngineReader publication = source.getResourceSession().createStorageEngineReader(revision)) {
        return Arrays.equals(Arrays.copyOfRange(value, 8, HEADER_BYTES),
            ChangeLogStorage.identity(publication, projection, revision))
                ? revision
                : 0;
      }
    }
  }

  /** Folds at most eight publications; its caller atomically publishes data and frontier. */
  public static int advance(final StorageEngineWriter destination, final StorageEngineReader source, final int first,
      final int last) {
    if (first < 1 || last < first || (long) last - first >= 8 || last > source.getRevisionNumber()) {
      throw new IllegalArgumentException("Summary folding requires one through eight committed revisions");
    }
    final JsonResourceSession session = (JsonResourceSession) source.getResourceSession();
    int identityProjection = -1;
    try (ChangeLogStorage storage = new ChangeLogStorage(destination, 0)) {
      for (int offset = 0; offset <= last - first; offset++) {
        final int revision = first + offset;
        identityProjection = -1;
        for (final IndexDef definition : session.getRtxIndexController(revision).getIndexes().getIndexDefs()) {
          if (!definition.isTemporalAggregateIndex()) {
            continue;
          }
          final int projection = definition.getTemporalAggregateSpec().projectionId();
          identityProjection = identityProjection < 0
              ? projection
              : Math.min(identityProjection, projection);
          if ((revision & 1) == 0) {
            try (StorageEngineReader publication = session.createStorageEngineReader(revision)) {
              fold(storage, destination, publication, definition, revision);
            }
          }
        }
      }
      try (StorageEngineReader publication = session.createStorageEngineReader(last)) {
        final byte[] identity = identityProjection < 0
            ? new byte[16]
            : Objects.requireNonNull(ChangeLogStorage.identity(publication, identityProjection, last));
        storage.put(FRONTIER,
            ByteBuffer.allocate(HEADER_BYTES).putInt(last).putInt(identityProjection).put(identity).array());
      }
    }
    return last;
  }

  private static void fold(final ChangeLogStorage storage, final StorageEngineReader destination,
      final StorageEngineReader source, final IndexDef definition, final int end) {
    final int projection = definition.getTemporalAggregateSpec().projectionId();
    final byte[] identity = Objects.requireNonNull(ChangeLogStorage.identity(source, projection, end));
    final TemporalAggregateParts parts =
        new TemporalAggregateParts(definition.getTemporalAggregateSpec(), new TemporalAggregateDomain().height());
    final TreeMap<byte[], TemporalAggregateParts.SummaryBuilder> groups = new TreeMap<>(Arrays::compareUnsigned);
    final BiConsumer<byte[], byte[]> merge = (group,
        operand) -> groups.computeIfAbsent(group, ignored -> parts.summaryBuilder()).mergeFolded(operand, COMPLETE);
    boolean available = true;
    try (
        TemporalAggregateSummaryLog.Reader right =
            new TemporalAggregateSummaryLog.Reader(source, projection, end, definition.getID(), COMPLETE);
        TemporalAggregateSummaryLog.Reader left =
            new TemporalAggregateSummaryLog.Reader(source, projection, end - 1, definition.getID(), COMPLETE)) {
      left.visit(merge);
      right.visit(merge);
    } catch (final TemporalAggregateCatchUpBudget.LimitExceeded unsupported) {
      available = false;
    }
    for (int level = 1; level <= Integer.numberOfTrailingZeros(end); level++) {
      if (available && level > 1) {
        try (Reader reader = new Reader(destination)) {
          available = reader.visit(source, definition, end - (1 << (level - 1)), level - 1, merge);
        }
      }
      write(storage, definition.getID(), end, level, identity, available
          ? groups
          : null);
    }
  }

  private static void write(final ChangeLogStorage storage, final int index, final int end, final int level,
      final byte[] identity, final @Nullable Map<byte[], TemporalAggregateParts.SummaryBuilder> groups) {
    if (groups == null) {
      storage.put(key(index, end, level, -1),
          ByteBuffer.allocate(HEADER_BYTES).putInt(-1).putInt(0).put(identity).array());
      return;
    }
    final TreeMap<byte[], byte[]> encoded = new TreeMap<>(Arrays::compareUnsigned);
    int length = 0;
    for (final var group : groups.entrySet()) {
      final byte[] operand = group.getValue().encodeFolded();
      if (operand.length != 0) {
        encoded.put(group.getKey(), operand);
        length = Math.addExact(length, Math.addExact(8 + group.getKey().length, operand.length));
      }
    }
    final ByteBuffer payload = ByteBuffer.allocate(length);
    for (final var group : encoded.entrySet()) {
      payload.putInt(group.getKey().length).putInt(group.getValue().length).put(group.getKey()).put(group.getValue());
    }
    final byte[] bytes = payload.array();
    for (int offset = 0, chunk = 0; offset < length; chunk++) {
      final int next = (int) Math.min(length, (long) offset + ChangeLogStorage.CHUNK_BYTES);
      storage.put(key(index, end, level, chunk), Arrays.copyOfRange(bytes, offset, next));
      offset = next;
    }
    storage.put(key(index, end, level, -1),
        ByteBuffer.allocate(HEADER_BYTES).putInt(length).putInt(encoded.size()).put(identity).array());
  }

  /** A reader owns its trie and pins one committed derived publication. */
  public static final class Reader implements AutoCloseable {
    private final HOTTrieReader trie;
    private final @Nullable PageReference root;
    private final TemporalAggregateCatchUpBudget.Budget budget;

    public Reader(final StorageEngineReader derived) {
      this(derived, COMPLETE);
    }

    public Reader(final StorageEngineReader derived, final TemporalAggregateCatchUpBudget.Budget budget) {
      this.budget = Objects.requireNonNull(budget);
      trie = new HOTTrieReader(derived);
      root = derived.getChangeLogIndexPage(derived.getActualRevisionRootPage()).getIndexReference(0);
    }

    private byte @Nullable [] get(final byte[] key) {
      return root == null
          ? null
          : ChangeLogStorage.read(trie, root, key);
    }

    /** False means absent/unsupported coverage; no consumer is called in that case. */
    public boolean visit(final StorageEngineReader source, final IndexDef definition, final int end, final int level,
        final BiConsumer<byte[], byte[]> consumer) {
      if (level < 1 || level > 30 || end < (1 << level) || (end & ((1 << level) - 1)) != 0
          || end > source.getRevisionNumber()) {
        throw new IllegalArgumentException("Invalid folded summary range");
      }
      final byte[] header = get(key(definition.getID(), end, level, -1));
      if (header == null) {
        return false;
      }
      if (header.length != HEADER_BYTES) {
        throw new IllegalStateException("Invalid folded summary header");
      }
      final ByteBuffer in = ByteBuffer.wrap(header);
      final int length = in.getInt();
      final int count = in.getInt();
      if (length < -1 || count < 0 || (length == -1 && count != 0) || (length >= 0 && count > length / 10)) {
        throw new IllegalStateException("Invalid folded summary extent");
      }
      if (length == -1 || !Arrays.equals(Arrays.copyOfRange(header, 8, HEADER_BYTES),
          ChangeLogStorage.identity(source, definition.getTemporalAggregateSpec().projectionId(), end))) {
        return false;
      }
      final byte[] bytes = new byte[length];
      for (int offset = 0, chunk = 0; offset < length; chunk++) {
        budget.chunk();
        final byte[] value =
            Objects.requireNonNull(get(key(definition.getID(), end, level, chunk)), "Missing folded summary chunk");
        final int size = Math.min(ChangeLogStorage.CHUNK_BYTES, length - offset);
        if (value.length != size) {
          throw new IllegalStateException("Invalid folded summary chunk length");
        }
        ChangeLogStorage.recordChunkRead();
        System.arraycopy(value, 0, bytes, offset, size);
        offset += size;
      }
      final ByteBuffer payload = ByteBuffer.wrap(bytes);
      byte[] previous = null;
      for (int group = 0; group < count; group++) {
        if (payload.remaining() < 8) {
          throw new IllegalStateException("Truncated folded summary group");
        }
        final int keyLength = payload.getInt();
        final int operandLength = payload.getInt();
        if (keyLength < 1 || keyLength > ChangeLogStorage.CHUNK_BYTES || operandLength < 1
            || (long) keyLength + operandLength > payload.remaining()) {
          throw new IllegalStateException("Invalid folded summary group extent");
        }
        final byte[] groupKey = new byte[keyLength];
        final byte[] operand = new byte[operandLength];
        payload.get(groupKey).get(operand);
        if (previous != null && Arrays.compareUnsigned(previous, groupKey) >= 0) {
          throw new IllegalStateException("Unsorted folded summary groups");
        }
        consumer.accept(groupKey, operand);
        previous = groupKey;
      }
      if (payload.hasRemaining()) {
        throw new IllegalStateException("Trailing folded summary bytes");
      }
      return true;
    }

    @Override
    public void close() {
      trie.close();
    }
  }
}
