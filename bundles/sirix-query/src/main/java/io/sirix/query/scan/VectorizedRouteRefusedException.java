package io.sirix.query.scan;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;

/**
 * Query-boundary refusal when an admitted legacy SPI route cannot preserve generic semantics. Never
 * thrown in a row/page kernel. Only this signal may trigger a generic retry; unrelated query and
 * storage failures must propagate.
 */
public final class VectorizedRouteRefusedException extends QueryException {
  private static final long serialVersionUID = 1L;

  public VectorizedRouteRefusedException(final String reason) {
    super(ErrorCode.BIT_DYN_INT_ERROR, "Vectorized route declined: %s", reason);
  }
}
