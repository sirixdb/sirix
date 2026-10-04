package io.sirix.query.function.jn.temporal;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.sequence.AbstractSequence;
import io.brackit.query.sequence.BaseIter;
import io.sirix.access.ValidTimeConfig;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.Evidence;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBObject;
import it.unimi.dsi.fastutil.longs.LongArrays;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.function.Predicate;

final class ValidTimeKeySequence extends AbstractSequence {
  private final JsonDBItem document;
  private final Instant instant;
  private final ValidTimeConfig config;
  private final boolean strictStart;
  private final boolean strictEnd;
  private final @Nullable Predicate<? super JsonDBObject> residual;
  private final int indexId;
  private final boolean exactPoint;
  private @Nullable Evidence evidence;
  private volatile long @Nullable [] candidates;

  ValidTimeKeySequence(final JsonDBItem document, final Instant instant, final ValidTimeConfig config,
      final boolean strictStart, final boolean strictEnd, final @Nullable Predicate<? super JsonDBObject> residual,
      final int indexId) {
    this.document = document;
    this.instant = instant;
    this.config = config;
    this.strictStart = strictStart;
    this.strictEnd = strictEnd;
    this.residual = residual;
    this.indexId = indexId;
    exactPoint = new IntervalDomain().isExact(instant);
  }

  private long[] candidates() {
    if (candidates == null) {
      final var closed = ValidTimeIntervalIndex.closedCandidates(document, instant, indexId);
      if (closed.isEmpty()) {
        candidates = LongArrays.EMPTY_ARRAY;
        return candidates;
      }
      evidence = ValidTimeIntervalIndex.readEvidence(document, indexId, closed);
      candidates =
          ValidTimeIntervalIndex.candidates(document, instant, strictStart, strictEnd, indexId, evidence, closed);
    }
    return candidates;
  }

  @SuppressWarnings("NullAway") // Only called for nonempty candidates, after evidence is loaded.
  private boolean needsVerification(final long key) {
    return !exactPoint || evidence.unverified().contains(key);
  }

  private boolean matches(final JsonDBObject object) {
    return ValidTimeIndexScan.isValidAtTime(object, instant, config.getNormalizedValidFromPath(),
        config.getNormalizedValidToPath(), residual == null && strictStart, residual == null && strictEnd)
        && (residual == null || residual.test(object));
  }

  private JsonDBObject item(final long key) {
    final var trx = document.getTrx();
    if (!trx.moveTo(key) || !trx.isObject()) {
      throw new IllegalStateException("Valid-time index object is not readable: " + key);
    }
    return new JsonDBObject(trx, document.getCollection());
  }

  private boolean matches(final long key) {
    if (!needsVerification(key)) {
      return true;
    }
    final var trx = document.getTrx();
    final long savedKey = trx.getNodeKey();
    try {
      return matches(item(key));
    } finally {
      if (trx.getNodeKey() != savedKey) {
        trx.moveTo(savedKey);
      }
    }
  }

  long[] matchingKeys() {
    final long[] keys = candidates();
    final long[] matches = new long[keys.length];
    int count = 0;
    for (final long key : keys) {
      if (matches(key)) {
        matches[count++] = key;
      }
    }
    return count == matches.length
        ? matches
        : Arrays.copyOf(matches, count);
  }

  @Override
  public boolean isRepeatable() {
    // Admission excludes mutable views; the built-in residual captures an immutable comparison point.
    // An arbitrary caller-supplied predicate may have side effects.
    return residual == null || residual instanceof ValidTimeResidual;
  }

  @Override
  public @Nullable IntNumeric knownSize() {
    if (!exactPoint || !isRepeatable()) {
      return null;
    }
    final long[] keys = candidates();
    // Only key-exact matches can supply cardinality without constructing or verifying objects.
    for (final long key : keys) {
      if (needsVerification(key)) {
        return null;
      }
    }
    return new Int64(keys.length);
  }

  @Override
  public IntNumeric size() {
    long count = 0;
    for (final long key : candidates()) {
      if (matches(key)) {
        count++;
      }
    }
    return new Int64(count);
  }

  @Override
  public boolean booleanValue() {
    try (final Iter iterator = iterate()) {
      final Item first = iterator.next();
      if (first == null) {
        return false;
      }
      if (iterator.next() != null) {
        throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE,
            "Effective boolean value is undefined for a sequence of JSON objects");
      }
      return first.booleanValue();
    }
  }

  @Override
  public @Nullable Item get(final IntNumeric position) {
    if (position.cmp(Int32.ONE) < 0) {
      return null;
    }
    final long[] keys = candidates();
    long remaining = position.cmp(new Int64(keys.length)) > 0
        ? (long) keys.length + 1
        : position.longValue();
    for (final long key : keys) {
      if (needsVerification(key)) {
        final JsonDBObject object = item(key);
        if (matches(object) && --remaining == 0) {
          return object;
        }
      } else if (--remaining == 0) {
        return item(key);
      }
    }
    return null;
  }

  @Override
  public Iter iterate() {
    return new BaseIter() {
      private int position;
      private boolean closed;

      @Override
      public @Nullable Item next() {
        if (closed) {
          return null;
        }
        final long[] keys = candidates();
        while (position < keys.length) {
          final long key = keys[position++];
          final JsonDBObject object = item(key);
          if (!needsVerification(key) || matches(object)) {
            return object;
          }
        }
        return null;
      }

      @Override
      public void close() {
        closed = true;
      }
    };
  }
}
