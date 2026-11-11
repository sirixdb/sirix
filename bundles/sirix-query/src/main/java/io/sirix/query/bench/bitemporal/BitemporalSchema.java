package io.sirix.query.bench.bitemporal;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/** Constants and compact value types shared by the SH1 benchmark adapters. */
public final class BitemporalSchema {

  public static final String DATABASE = "bt";
  public static final String CONTRACTS = "contracts";
  public static final String PRODUCTS = "products";
  public static final String SUPPLIERS = "suppliers";
  public static final String EPOCHS = "epochs";
  public static final String DAYS = "days";
  public static final int PUBLICATIONS = 25;
  public static final int HORIZON_DAYS = 366;
  public static final long SEED = 20260920L;
  public static final Instant DAY_ZERO = Instant.parse("2024-01-01T00:00:00Z");

  private BitemporalSchema() {
    throw new AssertionError("no instances");
  }

  /** Supported, predeclared scale tiers. */
  public enum Tier {
    DEVELOPMENT("development", 2_000, 200, 100), T25K("t25k", 25_000, 2_500, 500), T100K("t100k", 100_000, 10_000,
        2_000);

    private final String label;
    private final int contracts;
    private final int products;
    private final int suppliers;

    Tier(final String label, final int contracts, final int products, final int suppliers) {
      this.label = label;
      this.contracts = contracts;
      this.products = products;
      this.suppliers = suppliers;
    }

    public String label() {
      return label;
    }

    public int contracts() {
      return contracts;
    }

    public int products() {
      return products;
    }

    public int suppliers() {
      return suppliers;
    }

    public static Tier parse(final String value) {
      final String normalized = value.trim().toLowerCase(Locale.ROOT);
      for (final Tier tier : values()) {
        if (tier.label.equals(normalized)) {
          return tier;
        }
      }
      throw new IllegalArgumentException("unknown tier '" + value + "' (development, t25k, or t100k)");
    }
  }

  public static Instant day(final int day) {
    if (day < 0 || day > HORIZON_DAYS) {
      throw new IllegalArgumentException("day out of range: " + day);
    }
    return DAY_ZERO.plus(day, ChronoUnit.DAYS);
  }

  public static Instant systemTime(final int epoch) {
    if (epoch < 0 || epoch >= PUBLICATIONS) {
      throw new IllegalArgumentException("epoch out of range: " + epoch);
    }
    return day(15 * epoch);
  }
}
