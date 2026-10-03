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
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBObject;

import java.time.Instant;
import java.util.function.Predicate;

/** Repeatable, revision-bound key sequence. Only requested items acquire JSON object wrappers. */
final class ValidTimeKeySequence extends AbstractSequence {
  private final JsonDBItem document;
  private final Instant instant;
  private final ValidTimeConfig config;
  private final boolean strictStart;
  private final boolean strictEnd;
  private final Predicate<JsonDBObject> residual;
  private long[] keys;

  ValidTimeKeySequence(final JsonDBItem document, final Instant instant, final ValidTimeConfig config,
      final boolean strictStart, final boolean strictEnd, final Predicate<JsonDBObject> residual) {
    this.document = document;
    this.instant = instant;
    this.config = config;
    this.strictStart = strictStart;
    this.strictEnd = strictEnd;
    this.residual = residual;
  }

  private long[] keys() {
    if (keys == null) {
      keys = ValidTimeIntervalIndex.keys(document, instant, config, strictStart, strictEnd, residual);
    }
    return keys;
  }

  private JsonDBObject item(final long key) {
    final var trx = document.getTrx();
    if (!trx.moveTo(key) || !trx.isObject()) {
      throw new IllegalStateException("Valid-time index object is not readable: " + key);
    }
    return new JsonDBObject(trx, document.getCollection());
  }

  @Override
  public IntNumeric size() {
    return new Int64(keys().length);
  }

  @Override
  public boolean booleanValue() {
    final long[] matches = keys();
    if (matches.length == 0) {
      return false;
    }
    if (matches.length > 1) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE,
          "Effective boolean value is undefined for a sequence of JSON objects");
    }
    return item(matches[0]).booleanValue();
  }

  @Override
  public Item get(final IntNumeric position) {
    final long index = position.longValue();
    final long[] matches = keys();
    return position.cmp(Int32.ONE) < 0 || position.cmp(new Int64(matches.length)) > 0
        ? null
        : item(matches[(int) index - 1]);
  }

  @Override
  public Iter iterate() {
    return new BaseIter() {
      private int position;
      private boolean closed;

      @Override
      public Item next() {
        if (closed) {
          return null;
        }
        final long[] matches = keys();
        return position == matches.length
            ? null
            : item(matches[position++]);
      }

      @Override
      public void close() {
        closed = true;
      }
    };
  }
}
