package io.sirix.api;

import io.sirix.node.interfaces.DataRecord;

/**
 * Callback for {@link ResourceSession#scanValueRuns(long, RecordRunVisitor)}.
 *
 * <p>Invoked once per <i>run</i> of revisions in which a record holds the same value. A run spans
 * {@code [fromRevision, toRevision]} (both inclusive): the value was committed in
 * {@code fromRevision} and is unchanged up to and including {@code toRevision}. This lets a
 * consumer reconstruct the value at <i>every</i> revision while reading the record only once per
 * change — e.g. an aggregate over all versions is {@code value * (toRevision - fromRevision + 1)}.
 *
 * <p>The {@link DataRecord} is read under a storage reader that stays open only for the duration of
 * the callback and <b>must not be retained past it</b>.
 *
 * @author Johannes Lichtenberger
 */
@FunctionalInterface
public interface RecordRunVisitor {

  /**
   * Visit one run of revisions sharing the same record value.
   *
   * @param fromRevision the revision in which this value was committed (inclusive)
   * @param toRevision   the last revision in which this value still holds (inclusive)
   * @param record       the record value for the run; valid only during this call
   */
  void visit(int fromRevision, int toRevision, DataRecord record);
}
