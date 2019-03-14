package org.sirix.index.path.xml;

import java.util.Objects;
import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.util.path.PathException;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.index.path.PCRCollector;
import org.sirix.index.path.PCRValue;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

public final class XmlPCRCollector implements PCRCollector {

  /** Logger. */
  private static final LogWrapper LOGGER =
      new LogWrapper(LoggerFactory.getLogger(XmlPCRCollector.class));

  private final NodeReadOnlyTrx mRtx;

  public XmlPCRCollector(final XmlNodeReadOnlyTrx rtx) {
    mRtx = Objects.requireNonNull(rtx, "The transaction must not be null.");
  }

  @Override
  public PCRValue getPCRsForPaths(Set<Path<QNm>> paths) {
    try (final PathSummaryReader reader = mRtx instanceof XmlNodeTrx
        ? ((XmlNodeTrx) mRtx).getPathSummary()
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
