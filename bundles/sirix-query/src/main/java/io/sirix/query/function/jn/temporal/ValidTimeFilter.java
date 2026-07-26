package io.sirix.query.function.jn.temporal;

import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.LazySequence;
import io.sirix.access.ValidTimeConfig;
import io.sirix.query.json.JsonDBItem;

import java.time.Instant;

/**
 * Shared linear-scan ("fallback") implementation of the valid-time point-in-time predicate
 * {@code validFrom <= validTime <= validTo}, used by {@code jn:valid-at} / {@code jn:open-bitemporal}
 * and {@code jn:scan-valid-time-index} when no index applies.
 *
 * <p>The predicate is exactly the one the interval-index and CAS-narrowing paths re-verify against
 * ({@link ValidTimeIndexScan#isValidAtTime}), so all three paths return the identical set.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeFilter {

  private ValidTimeFilter() {
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
          private Iter childIter;
          private boolean initialized;

          @Override
          public Item next() {
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
        if (!(item instanceof io.brackit.query.jdm.json.Object obj)) {
          return false;
        }
        return ValidTimeIndexScan.isValidAtTime(obj, validTime,
            validTimeConfig.getNormalizedValidFromPath(),
            validTimeConfig.getNormalizedValidToPath());
      }
    };
  }
}
