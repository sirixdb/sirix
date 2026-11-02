package io.sirix.cache;

import org.jspecify.annotations.Nullable;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Startup allocation of disjoint retained-cache ceilings within half the runtime heap. The other
 * half remains available to query execution, output, GC and application/metadata objects. This is a
 * heap policy, not a second grant from the native persisted-page allocator or an RSS limit.
 */
public final class HeapCacheBudget {
  public static final String MAX_BYTES_PROPERTY = "sirix.heapCache.maxBytes";
  public static final String PROJECTION_FILL_BYTES_PROPERTY = "sirix.projection.eagerMaterializeBytes";

  /** Leaves of the allocation tree; their ceilings sum to no more than the envelope. */
  public enum Pool {
    PROJECTION("sirix.projection.cacheBytes"), DERIVED("sirix.derivedCache.heapReserveBytes"), DICTIONARY_RECORDS(
        "sirix.projection.globalDict.recordCacheBytes"), GLOBAL_VERDICTS(
            "sirix.projection.globalDict.verdictCacheBytes"), SEGMENT_VERDICTS(
                "sirix.projection.segmentDict.verdictCacheBytes"), HOT_IMAGES(
                    "sirix.hotHeapCache.maxBytes"), HOT_LOOKUPS("sirix.heapCache.hotLookupBytes"), HOT_MINI_PAGES(
                        "sirix.heapCache.hotMiniBytes"), PROJECTION_BLOBS("sirix.heapCache.projectionBlobBytes");

    private final String property;

    Pool(final String property) {
      this.property = property;
    }

    public String property() {
      return property;
    }
  }

  /** Configured=-1 denotes the default share; effective is always bounded by the share. */
  public record Limit(long configuredBytes, long ceilingBytes, long effectiveBytes) {
  }

  public record Metrics(long heapBytes, long configuredBytes, long envelopeBytes, long workspaceReserveBytes,
      Map<Pool, Limit> pools, Limit projectionFill) {
  }

  private static final class RuntimeBudget {
    private static final HeapCacheBudget INSTANCE =
        new HeapCacheBudget(Runtime.getRuntime().maxMemory(), System::getProperty);
  }

  private final Metrics metrics;

  public static HeapCacheBudget global() {
    return RuntimeBudget.INSTANCE;
  }

  /** Pure policy seam: property values are read once; no allocator or runtime sampling is needed. */
  public HeapCacheBudget(final long heapBytes, final Function<String, @Nullable String> properties) {
    if (heapBytes < 0) {
      throw new IllegalArgumentException("Negative runtime heap allowance");
    }
    Objects.requireNonNull(properties);
    final long configured = configured(properties, MAX_BYTES_PROPERTY);
    final long envelope = configured < 0
        ? heapBytes / 2
        : Math.min(configured, heapBytes / 2);
    final EnumMap<Pool, Limit> limits = new EnumMap<>(Pool.class);
    assign(limits, properties, Pool.PROJECTION, envelope / 2);
    assign(limits, properties, Pool.DERIVED, envelope / 4);
    final long dictionary = envelope / 8;
    assign(limits, properties, Pool.DICTIONARY_RECORDS, fraction(dictionary, 4, 7));
    assign(limits, properties, Pool.GLOBAL_VERDICTS, dictionary / 7);
    assign(limits, properties, Pool.SEGMENT_VERDICTS, fraction(dictionary, 2, 7));
    assign(limits, properties, Pool.HOT_IMAGES, envelope / 16);
    assign(limits, properties, Pool.HOT_LOOKUPS, envelope / 32);
    assign(limits, properties, Pool.HOT_MINI_PAGES, envelope / 64);
    assign(limits, properties, Pool.PROJECTION_BLOBS, envelope / 64);
    final long projection = limits.get(Pool.PROJECTION).effectiveBytes();
    // A windowed handle retains at most a fill plus a quarter-fill of payload windows. Price both
    // inside the projection share; a per-store override cannot grant more than the catalog owns.
    final long fillCeiling = fraction(projection, 4, 5);
    final long fillConfigured = configured(properties, PROJECTION_FILL_BYTES_PROPERTY);
    final long fill = fillConfigured < 0
        ? projection / 2
        : Math.min(fillConfigured, fillCeiling);
    metrics = new Metrics(heapBytes, configured, envelope, heapBytes - envelope, Map.copyOf(limits),
        new Limit(fillConfigured, fillCeiling, fill));
  }

  private static void assign(final EnumMap<Pool, Limit> limits, final Function<String, @Nullable String> properties,
      final Pool pool, final long ceiling) {
    final long requested = configured(properties, pool.property());
    limits.put(pool, new Limit(requested, ceiling, requested < 0
        ? ceiling
        : Math.min(requested, ceiling)));
  }

  private static long configured(final Function<String, @Nullable String> properties, final String property) {
    final String value = properties.apply(property);
    if (value == null) {
      return -1;
    }
    final long parsed;
    try {
      parsed = Long.parseLong(value.trim());
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException(property + " must be a non-negative decimal byte count", e);
    }
    if (parsed < 0) {
      throw new IllegalArgumentException(property + " must be a non-negative decimal byte count");
    }
    return parsed;
  }

  private static long fraction(final long value, final int numerator, final int denominator) {
    return value / denominator * numerator + value % denominator * numerator / denominator;
  }

  /** Integer weighers reserve one unit above the maximum for oversized-entry rejection. */
  public static long weightUnit(final long maximumBytes) {
    if (maximumBytes < 0) {
      throw new IllegalArgumentException("Negative cache ceiling");
    }
    final long units = Integer.MAX_VALUE - 1L;
    return Math.max(1L, maximumBytes / units + (maximumBytes % units == 0
        ? 0
        : 1));
  }

  /** Round capacity up, never wrap a long byte count into a cheap or negative cache entry. */
  public static int weight(final long bytes, final long unit) {
    if (bytes < 0 || unit <= 0) {
      throw new IllegalArgumentException("Invalid cache weight or unit");
    }
    return (int) Math.min(Integer.MAX_VALUE, bytes / unit + (bytes % unit == 0
        ? 0
        : 1));
  }

  public long bytes(final Pool pool) {
    return metrics.pools().get(Objects.requireNonNull(pool)).effectiveBytes();
  }

  public long projectionFillBytes() {
    return metrics.projectionFill().effectiveBytes();
  }

  /** Per-consumer planning ceiling within the reserved execution half; no free-heap declaration. */
  public long queryWorkspaceBytes() {
    return Math.min(metrics.heapBytes() / 8, metrics.workspaceReserveBytes() / 4);
  }

  public Metrics metrics() {
    return metrics;
  }
}
