package org.sirix.index.path.json;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.index.path.AbstractPCRCollector;
import org.sirix.index.path.PCRValue;
import org.sirix.index.path.summary.PathSummaryReader;

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
        : rtx.getResourceManager().openPathSummary(rtx.getRevisionNumber());
    try {
      return getPcrValue(paths, reader);
    } finally {
      if (!(rtx instanceof JsonNodeTrx)) {
        reader.close();
      }
    }
  }
}
