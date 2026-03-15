package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.NodeKind;
import io.sirix.query.json.JsonDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Collects value samples from a JSON resource and builds a {@link Histogram}.
 *
 * <p>Analogous to SQL's ANALYZE command: walks nodes matching a given path,
 * reads their numeric values, and produces an equi-width histogram for
 * data-driven selectivity estimation.</p>
 *
 * <p>Sampling strategy: reads up to {@code sampleSize} values from nodes
 * matching the path. For resources smaller than the sample size, all values
 * are included (census). For larger resources, values are sampled uniformly
 * by stepping through matching nodes with a stride.</p>
 *
 * <p>The collector builds histograms for numeric values only. String fields
 * do not produce histograms (string selectivity relies on NDV estimates).</p>
 *
 * <p>Usage:
 * <pre>
 *   var collector = new HistogramCollector(jsonStore);
 *   Histogram hist = collector.collect("mydb", "resource1", "price", 10000);
 *   StatisticsCatalog.getInstance().put("mydb", "resource1", "price", hist);
 * </pre></p>
 */
public final class HistogramCollector {

  private static final Logger LOG = LoggerFactory.getLogger(HistogramCollector.class);

  /** Default sample size: 10,000 values is sufficient for 64-bucket histograms. */
  public static final int DEFAULT_SAMPLE_SIZE = 10_000;

  /** Default number of histogram buckets. Delegates to {@link Histogram#DEFAULT_BUCKET_COUNT}. */
  private static final int DEFAULT_BUCKET_COUNT = Histogram.DEFAULT_BUCKET_COUNT;

  private final JsonDBStore jsonStore;

  public HistogramCollector(JsonDBStore jsonStore) {
    this.jsonStore = Objects.requireNonNull(jsonStore, "jsonStore");
  }

  /**
   * Collect a histogram for a field at the given path.
   *
   * @param databaseName the database name
   * @param resourceName the resource name
   * @param fieldPath    the JSON field name (leaf field, e.g., "price")
   * @param sampleSize   maximum number of values to sample
   * @return the built histogram, or {@code null} if no numeric values found
   */
  public Histogram collect(String databaseName, String resourceName,
                           String fieldPath, int sampleSize) {
    return collect(databaseName, resourceName, fieldPath, sampleSize, DEFAULT_BUCKET_COUNT, -1);
  }

  /**
   * Collect a histogram for a field at the given path with full configuration.
   *
   * @param databaseName the database name
   * @param resourceName the resource name
   * @param fieldPath    the JSON field name (leaf field, e.g., "price")
   * @param sampleSize   maximum number of values to sample
   * @param bucketCount  number of histogram buckets
   * @param revision     the revision to sample from (-1 for most recent)
   * @return the built histogram, or {@code null} if no numeric values found
   */
  public Histogram collect(String databaseName, String resourceName,
                           String fieldPath, int sampleSize, int bucketCount, int revision) {
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(resourceName, "resourceName");
    Objects.requireNonNull(fieldPath, "fieldPath");
    if (sampleSize <= 0) {
      throw new IllegalArgumentException("sampleSize must be positive: " + sampleSize);
    }
    if (bucketCount <= 0) {
      throw new IllegalArgumentException("bucketCount must be positive: " + bucketCount);
    }

    try {
      final var jsonCollection = jsonStore.lookup(databaseName);
      final var database = jsonCollection.getDatabase();

      try (final ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> session =
               database.beginResourceSession(resourceName)) {
        return collectFromSession(session, fieldPath, sampleSize, bucketCount, revision);
      }
    } catch (Exception e) {
      LOG.warn("Failed to collect histogram for {}/{}/{}: {}",
          databaseName, resourceName, fieldPath, e.getMessage());
      return null;
    }
  }

  /**
   * Collect a histogram by walking PathSummary to find matching nodes,
   * then sampling their values via a read-only transaction.
   */
  private Histogram collectFromSession(
      ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> session,
      String fieldPath, int sampleSize, int bucketCount, int revision) {

    // Build a path for the field
    final Path<QNm> path = new Path<>();
    path.childObjectField(new QNm(fieldPath));

    // Quick check via PathSummary: if no PCRs match the path, skip traversal
    try (final var pathSummary = revision == -1
        ? session.openPathSummary()
        : session.openPathSummary(revision)) {

      final var pcrs = pathSummary.getPCRsForPath(path);
      if (pcrs.isEmpty()) {
        return null;
      }

      // Verify at least one PCR has references (nodes exist in this revision)
      long totalReferences = 0;
      final var iter = pcrs.iterator();
      while (iter.hasNext()) {
        final long pcr = iter.nextLong();
        final PathNode pathNode = pathSummary.getPathNodeForPathNodeKey(pcr);
        if (pathNode != null) {
          totalReferences += pathNode.getReferences();
        }
      }

      if (totalReferences == 0) {
        return null;
      }
    } catch (Exception e) {
      LOG.debug("PathSummary lookup failed for field '{}': {}", fieldPath, e.getMessage());
      return null;
    }

    // Sample values using a read-only transaction
    try (final var rtx = revision == -1
        ? session.beginNodeReadOnlyTrx()
        : session.beginNodeReadOnlyTrx(revision)) {

      return sampleValues(rtx, fieldPath, sampleSize, bucketCount);
    } catch (Exception e) {
      LOG.warn("Value sampling failed for field '{}': {}", fieldPath, e.getMessage());
      return null;
    }
  }

  /**
   * Sample numeric values by traversing the document and collecting values
   * from nodes whose name matches the field path.
   *
   * <p>Uses a simple document-order scan with stride-based sampling:
   * visits all nodes sequentially, collects numeric values from matching
   * field nodes, and stops after reaching the sample size.</p>
   */
  private Histogram sampleValues(JsonNodeReadOnlyTrx rtx, String fieldPath,
                                  int sampleSize, int bucketCount) {
    final var builder = new Histogram.Builder(bucketCount);
    final Set<Double> distinctValues = new HashSet<>();

    rtx.moveToDocumentRoot();
    final long totalNodes = rtx.getDescendantCount();

    // Compute stride: if total nodes >> sample size, skip nodes to sample uniformly
    final long stride = Math.max(1, totalNodes / (sampleSize * 2L));

    int collected = 0;
    long visited = 0;

    // Walk document in order, looking for value nodes
    if (rtx.moveToFirstChild()) {
      do {
        visited++;

        // Stride-based sampling: skip nodes when stride > 1
        if (stride > 1 && (visited % stride) != 0) {
          continue;
        }

        final Double value = tryExtractNumericValue(rtx, fieldPath);
        if (value != null) {
          builder.addValue(value);
          distinctValues.add(value);
          collected++;
          if (collected >= sampleSize) {
            break;
          }
        }
      } while (advanceToNextNode(rtx));
    }

    if (collected == 0) {
      return null;
    }

    builder.setDistinctCount(distinctValues.size());
    return builder.build();
  }

  /**
   * Try to extract a numeric value from the current node if it matches the field.
   *
   * <p>Checks if the current node is a number value node whose parent's name
   * matches the field path.</p>
   */
  private static Double tryExtractNumericValue(JsonNodeReadOnlyTrx rtx, String fieldPath) {
    final NodeKind kind = rtx.getKind();

    // Check if this is a numeric value node
    if (kind != NodeKind.NUMBER_VALUE && kind != NodeKind.OBJECT_NUMBER_VALUE) {
      return null;
    }

    // Check if the parent field name matches
    final long currentKey = rtx.getNodeKey();
    try {
      if (rtx.moveToParent()) {
        final String name = rtx.getName() != null ? rtx.getName().getLocalName() : null;
        if (fieldPath.equals(name)) {
          rtx.moveTo(currentKey);
          return rtx.getNumberValue().doubleValue();
        }
      }
    } catch (Exception e) {
      // Ignore parsing errors
    }
    rtx.moveTo(currentKey);
    return null;
  }

  /**
   * Advance to the next node in document order (pre-order traversal).
   *
   * @return true if moved to next node, false if traversal is complete
   */
  private static boolean advanceToNextNode(JsonNodeReadOnlyTrx rtx) {
    // Try first child
    if (rtx.moveToFirstChild()) {
      return true;
    }
    // Try right sibling
    if (rtx.moveToRightSibling()) {
      return true;
    }
    // Backtrack: move to parent and try its right sibling
    while (rtx.moveToParent()) {
      if (rtx.moveToRightSibling()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Convenience: collect with default sample size and register in the catalog.
   */
  public boolean collectAndRegister(String databaseName, String resourceName, String fieldPath) {
    final Histogram hist = collect(databaseName, resourceName, fieldPath, DEFAULT_SAMPLE_SIZE);
    if (hist != null) {
      StatisticsCatalog.getInstance().put(databaseName, resourceName, fieldPath, hist);
      return true;
    }
    return false;
  }
}
