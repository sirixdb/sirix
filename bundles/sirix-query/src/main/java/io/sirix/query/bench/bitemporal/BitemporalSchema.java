package io.sirix.query.bench.bitemporal;

import com.google.gson.JsonObject;

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

  /** One canonical publication event. Payload slots are relation-specific primitive columns. */
  public record Event(int epoch, String table, boolean put, int id, int fromDay, int toDay, int value1, int value2,
      int value3, int value4, int value5) {

    public Event {
      if (epoch < 0 || epoch >= PUBLICATIONS) {
        throw new IllegalArgumentException("epoch out of range: " + epoch);
      }
      if (!CONTRACTS.equals(table) && !PRODUCTS.equals(table) && !SUPPLIERS.equals(table)) {
        throw new IllegalArgumentException("unknown table: " + table);
      }
      if (id < 1 || fromDay < 0 || fromDay >= toDay || toDay > HORIZON_DAYS) {
        throw new IllegalArgumentException("invalid event bounds/id");
      }
    }

    public static Event fromJson(final JsonObject json) {
      final String table = json.get("table").getAsString();
      final String operation = json.get("op").getAsString();
      if (!"put".equals(operation) && !"delete".equals(operation)) {
        throw new IllegalArgumentException("unknown operation: " + operation);
      }
      final boolean put = "put".equals(operation);
      final int epoch = json.get("epoch").getAsInt();
      final int id = json.get("id").getAsInt();
      final int from = json.get("a").getAsInt();
      final int to = json.get("b").getAsInt();
      if (!put) {
        return new Event(epoch, table, false, id, from, to, 0, 0, 0, 0, 0);
      }
      return switch (table) {
        case CONTRACTS ->
          new Event(epoch, table, true, id, from, to, json.get("pid").getAsInt(), json.get("sid").getAsInt(),
              json.get("cost").getAsInt(), json.get("qty").getAsInt(), json.get("grade").getAsInt());
        case PRODUCTS -> new Event(epoch, table, true, id, from, to, json.get("category").getAsInt(),
            json.get("retail").getAsInt(), 0, 0, 0);
        case SUPPLIERS -> new Event(epoch, table, true, id, from, to, json.get("region").getAsInt(),
            json.get("tier").getAsInt(), 0, 0, 0);
        default -> throw new AssertionError("validated table: " + table);
      };
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
