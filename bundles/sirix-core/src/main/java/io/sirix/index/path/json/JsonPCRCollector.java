package io.sirix.index.path.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.path.AbstractPCRCollector;
import io.sirix.index.path.PCRValue;
import io.sirix.index.path.summary.PathSummaryReader;

import java.util.Objects;
import java.util.Set;

public final class JsonPCRCollector extends AbstractPCRCollector {

  private final JsonNodeReadOnlyTrx rtx;

  public JsonPCRCollector(final JsonNodeReadOnlyTrx rtx) {
    this.rtx = Objects.requireNonNull(rtx, "The transaction must not be null.");
  }

  @Override
  public PCRValue getPCRsForPaths(Set<Path<QNm>> paths) {
    final PathSummaryReader reader = rtx instanceof JsonNodeTrx
        ? ((JsonNodeTrx) rtx).getPathSummary()
        : rtx.getResourceSession().openPathSummary(rtx.getRevisionNumber());
    try {
      return getPcrValue(paths, reader);
    } finally {
      if (!(rtx instanceof JsonNodeTrx)) {
        reader.close();
      }
    }
  }
}
