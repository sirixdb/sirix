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

  private final JsonNodeReadOnlyTrx mRtx;

  public JsonPCRCollector(final JsonNodeReadOnlyTrx rtx) {
    mRtx = Objects.requireNonNull(rtx, "The transaction must not be null.");
  }

  @Override
  public PCRValue getPCRsForPaths(Set<Path<QNm>> paths) {
    final PathSummaryReader reader = mRtx instanceof JsonNodeTrx
        ? ((JsonNodeTrx) mRtx).getPathSummary()
        : mRtx.getResourceManager().openPathSummary(mRtx.getRevisionNumber());
    try {
      return getPcrValue(paths, reader);
    } finally {
      if (!(mRtx instanceof JsonNodeTrx)) {
        reader.close();
      }
    }
  }
}
