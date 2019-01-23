package org.sirix.index.path.json;

import java.util.Objects;
import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadWriteTrx;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PCRValue;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

public final class JsonPCRCollector implements PCRCollector {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(JsonPCRCollector.class));

  private final JsonNodeReadOnlyTrx mRtx;

  public JsonPCRCollector(final JsonNodeReadOnlyTrx rtx) {
    mRtx = Objects.requireNonNull(rtx, "The transaction must not be null.");
  }

  @Override
  public PCRValue getPCRsForPaths(Set<Path<QNm>> paths) {
    try (final PathSummaryReader reader = mRtx instanceof JsonNodeReadWriteTrx
        ? ((JsonNodeReadWriteTrx) mRtx).getPathSummary()
        : mRtx.getResourceManager().openPathSummary(mRtx.getRevisionNumber())) {
      final long maxPCR = reader.getMaxNodeKey();
      final Set<Long> pathClassRecords = reader.getPCRsForPaths(paths, true);
      return PCRValue.getInstance(maxPCR, pathClassRecords);
    } catch (final PathException e) {
      LOGGER.error(e.getMessage(), e);
    }

    return PCRValue.getEmptyInstance();
  }
}
