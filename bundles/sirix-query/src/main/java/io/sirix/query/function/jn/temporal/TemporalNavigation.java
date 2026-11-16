package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.temporal.FirstAxis;
import io.sirix.axis.temporal.LastAxis;
import io.sirix.axis.temporal.NextAxis;
import io.sirix.axis.temporal.PrefetchedAllTimeAxis;
import io.sirix.axis.temporal.PrefetchedFutureAxis;
import io.sirix.axis.temporal.PrefetchedPastAxis;
import io.sirix.axis.temporal.PreviousAxis;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.json.FieldNameJsonDBItem;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonItemFactory;
import io.sirix.query.json.TemporalJsonDBItem;
import io.sirix.query.stream.json.TemporalSirixJsonItemStream;

/**
 * The single dispatch point of the temporal navigation functions (spec:
 * {@code docs/TEMPORAL_FUNCTIONS_SPEC.md}).
 *
 * <p>
 * Every item read from a Sirix store has a node key that is stable across revisions, so the
 * temporal axes apply to all of them. Objects and arrays implement {@link TemporalJsonDBItem} and
 * keep their own result views (object, array, slice, key array). Every other stored item - numbers,
 * strings, booleans, {@code null} and object field names - is routed through its node key and
 * re-materialized per revision by {@link JsonItemFactory}, so a value comes back with the type and
 * value it has in that revision. Items not backed by a store node raise
 * {@link JNFun#ERR_NOT_A_STORED_ITEM}.
 */
final class TemporalNavigation {

  /** A navigation to at most one other revision. */
  enum Step {
    FIRST, LAST, PREVIOUS, NEXT
  }

  /** A navigation over a run of revisions. */
  enum Range {
    PAST, FUTURE, ALL_TIMES
  }

  private TemporalNavigation() {}

  /**
   * Validates the argument of a temporal function eagerly, before any lazy evaluation.
   *
   * @param function the function name, for the error message
   * @param argument the function argument
   * @return the argument, a {@link JsonDBItem} (objects and arrays are also
   *         {@link TemporalJsonDBItem}s)
   * @throws QueryException {@link JNFun#ERR_NOT_A_STORED_ITEM} if the argument is not a stored item
   */
  static Sequence requireStored(final QNm function, final Sequence argument) {
    if (argument instanceof JsonDBItem) {
      return argument;
    }
    final String type = argument instanceof Item item
        ? item.itemType().toString()
        : String.valueOf(argument);
    throw new QueryException(JNFun.ERR_NOT_A_STORED_ITEM,
        "%s requires an item read from a Sirix store (it navigates the item's node history), but got a computed %s",
        function, type);
  }

  /**
   * Navigates to the same node in at most one other revision.
   *
   * @param function the function name, for the error message
   * @param step which revision
   * @param argument the function argument
   * @return the node's item in that revision, or {@code null} (empty) if the node is absent there
   */
  static Sequence step(final QNm function, final Step step, final Sequence argument) {
    if (requireStored(function, argument) instanceof TemporalJsonDBItem<?> temporal) {
      return switch (step) {
        case FIRST -> temporal.getFirst();
        case LAST -> temporal.getLast();
        case PREVIOUS -> temporal.getPrevious();
        case NEXT -> temporal.getNext();
      };
    }
    final JsonDBItem item = (JsonDBItem) argument;
    final JsonNodeReadOnlyTrx rtx = positionedTrx(item);
    final JsonResourceSession session = rtx.getResourceSession();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = switch (step) {
      case FIRST -> new FirstAxis<>(session, rtx);
      case LAST -> new LastAxis<>(session, rtx);
      case PREVIOUS -> new PreviousAxis<>(session, rtx);
      case NEXT -> new NextAxis<>(session, rtx);
    };
    if (axis.hasNext()) {
      return JsonItemFactory.INSTANCE.getSequence(axis.next(), item.getCollection(), isFieldName(item));
    }
    return null;
  }

  /**
   * Navigates to the same node over a run of revisions.
   *
   * @param range which revisions
   * @param argument the function argument, already checked by {@link #requireStored}
   * @param includeSelf whether {@link Range#PAST}/{@link Range#FUTURE} include the item's own
   *        revision; ignored for {@link Range#ALL_TIMES}
   * @return a stream of the node's items, one per revision
   */
  // Producer covariance preserves each structural view without an adapter or unchecked cast.
  @SuppressWarnings("java:S1452")
  static Stream<? extends Item> range(final Range range, final Sequence argument, final boolean includeSelf) {
    if (argument instanceof TemporalJsonDBItem<?> temporal) {
      return switch (range) {
        case PAST -> temporal.getEarlier(includeSelf);
        case FUTURE -> temporal.getFuture(includeSelf);
        case ALL_TIMES -> temporal.getAllTimes();
      };
    }
    final JsonDBItem item = (JsonDBItem) argument;
    final JsonNodeReadOnlyTrx rtx = positionedTrx(item);
    final JsonResourceSession session = rtx.getResourceSession();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = switch (range) {
      case PAST -> new PrefetchedPastAxis<>(session, rtx, include);
      case FUTURE -> new PrefetchedFutureAxis<>(session, rtx, include);
      case ALL_TIMES -> new PrefetchedAllTimeAxis<>(session, rtx);
    };
    return new TemporalSirixJsonItemStream(axis, item.getCollection(), isFieldName(item));
  }

  /**
   * The item's cursor, positioned on its node: the axes read the node key and revision from it. A
   * value item's {@code getTrx()} moves the shared cursor back to the item's node.
   */
  private static JsonNodeReadOnlyTrx positionedTrx(final JsonDBItem item) {
    return item.getTrx();
  }

  private static boolean isFieldName(final JsonDBItem item) {
    return item instanceof FieldNameJsonDBItem;
  }
}
