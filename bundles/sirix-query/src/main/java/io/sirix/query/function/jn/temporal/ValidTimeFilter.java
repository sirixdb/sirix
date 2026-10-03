package io.sirix.query.function.jn.temporal;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.QueryContext;
import io.brackit.query.module.StaticContext;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.access.ValidTimeConfig;
import io.sirix.query.json.JsonDBItem;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * Shared linear-scan ("fallback") implementation of the valid-time point-in-time predicate
 * {@code validFrom <= validTime <= validTo}, used by {@code jn:valid-at} /
 * {@code jn:open-bitemporal} and {@code jn:scan-valid-time-index} when no index applies.
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeFilter {

  private ValidTimeFilter() {}

  /** Exact fallback for the two original xs:dateTime comparisons, preserving conjunct order. */
  public static Sequence comparisonScanSequence(final JsonDBItem document, final Supplier<Sequence> point,
      final String from, final String to, final int mode, final StaticContext context,
      final QueryContext queryContext) {
    if (!(document instanceof Array array)) {
      throw new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE, "Expected an array for valid-time FLWOR");
    }
    final ValidTimeResidual lower = new ValidTimeResidual(context, queryContext, point, from, true, (mode & 1) != 0,
        (mode & 8) != 0, (mode & 32) == 0);
    final ValidTimeResidual upper = new ValidTimeResidual(context, queryContext, point, to, false, (mode & 2) != 0,
        (mode & 16) != 0, (mode & 64) != 0);
    final ValidTimeResidual first = (mode & 4) == 0
        ? lower
        : upper;
    final ValidTimeResidual second = (mode & 4) == 0
        ? upper
        : lower;
    return new LazySequence() {
      @Override
      public Iter iterate() {
        final Iter input = array.iterate();
        return new BaseIter() {
          @Override
          public @Nullable Item next() {
            Item item;
            while ((item = input.next()) != null) {
              if (first.test(item) && second.test(item)) {
                return item;
              }
            }
            return null;
          }

          @Override
          public void close() {
            input.close();
          }
        };
      }
    };
  }

  /**
   * A lazy sequence that yields the document item itself (if it satisfies the predicate) plus, when
   * the document is an array, its direct element children that satisfy the predicate.
   */
  public static Sequence linearScanSequence(final JsonDBItem document, final Instant validTime,
      final ValidTimeConfig validTimeConfig) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          private @Nullable Iter childIter;
          private boolean initialized;

          @Override
          public @Nullable Item next() {
            if (!initialized) {
              initialized = true;
              if (isValidAt(document)) {
                return document;
              }
              if (document instanceof Array array) {
                childIter = array.iterate();
              }
            }
            if (childIter != null) {
              Item item;
              while ((item = childIter.next()) != null) {
                if (item instanceof JsonDBItem jsonItem && isValidAt(jsonItem)) {
                  return item;
                }
              }
            }
            return null;
          }

          @Override
          public void close() {
            if (childIter != null) {
              childIter.close();
            }
          }
        };
      }

      private boolean isValidAt(final JsonDBItem item) {
        if (!(item instanceof Object obj)) {
          return false;
        }
        return ValidTimeIndexScan.isValidAtTime(obj, validTime, validTimeConfig.getNormalizedValidFromPath(),
            validTimeConfig.getNormalizedValidToPath());
      }
    };
  }
}
