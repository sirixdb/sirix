package io.sirix.query.bench.clickbench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The ClickBench {@code hits} schema: the 105 columns of the canonical
 * <a href="https://github.com/ClickHouse/ClickBench">ClickBench</a> {@code create.sql}, in that
 * file's order, together with the JSON shape each of them takes in the SirixDB port.
 *
 * <h2>JSON encoding contract</h2>
 * The port stores one JSON object per hit with all 105 columns always present, in create.sql order,
 * and the whole dataset as a single JSON array. JSON has no date type and only one number type, so
 * the mapping is fixed as follows and every producer (the parquet export, {@link
 * ClickBenchHitsGenerator}) and every consumer (the queries, the DuckDB reference) has to agree on
 * it:
 * <ul>
 *   <li>{@code SMALLINT} / {@code INTEGER} / {@code BIGINT} become unquoted JSON numbers with exact
 *       {@code int64} digits — never quoted, never floats, never scientific notation. Quoting them
 *       is the failure mode that matters: a quoted {@code UserID} shreds as a string node and Q19,
 *       Q40 and Q41 then silently return nothing.</li>
 *   <li>{@code TEXT} / {@code VARCHAR(255)} / {@code CHAR} become JSON strings; the ClickBench data
 *       uses {@code ""} for absent text, never {@code null}.</li>
 *   <li>{@code DATE} becomes a {@code "YYYY-MM-DD"} string.</li>
 *   <li>{@code TIMESTAMP} becomes a {@code "YYYY-MM-DDTHH:MM:SS"} string (ISO-8601, {@code 'T'}
 *       separator, second resolution, no timezone). ISO-8601 orders lexicographically, so
 *       {@code ORDER BY EventTime} and the {@code EventDate} range predicates are plain string
 *       comparisons and {@code DATE_TRUNC('minute', EventTime)} is {@code substring(t, 1, 16)}.</li>
 * </ul>
 *
 * <h2>DuckDB reference side</h2>
 * {@link #duckdbType(String)} and {@link #duckdbColumnSpecJson()} hand out the create.sql type text
 * so the reference implementation can build a {@code read_json(..., columns = {...})} spec from this
 * class rather than from a second, hand-maintained copy of the schema. Reading the JSON with an
 * explicit column spec is what keeps DuckDB from inferring {@code DOUBLE} for a 64-bit id column,
 * which would round {@code UserID} and make the two engines disagree.
 */
public final class ClickBenchSchema {

  /** The SirixDB database the ClickBench dataset is loaded into. */
  public static final String DATABASE = "clickbench";

  /** The JSON resource inside {@link #DATABASE} that holds the hits array. */
  public static final String RESOURCE = "hits.jn";

  /** The JSON shape of a column under the encoding contract documented on this class. */
  public enum ColumnType {
    /** {@code SMALLINT} or {@code INTEGER}: an unquoted JSON number that fits {@code int32}. */
    INT,
    /** {@code BIGINT}: an unquoted JSON number carrying exact {@code int64} digits. */
    LONG,
    /** {@code TEXT}, {@code VARCHAR(255)} or {@code CHAR}: a JSON string, {@code ""} when absent. */
    STRING,
    /** {@code DATE}: a {@code "YYYY-MM-DD"} JSON string. */
    DATE,
    /** {@code TIMESTAMP}: a {@code "YYYY-MM-DDTHH:MM:SS"} JSON string. */
    DATETIME
  }

  // The create.sql type texts, as constants so the table below cannot drift by a typo and so the
  // switch in columnTypeOf can be exhaustive over them.
  private static final String BIGINT = "BIGINT";
  private static final String INTEGER = "INTEGER";
  private static final String SMALLINT = "SMALLINT";
  private static final String TEXT = "TEXT";
  private static final String VARCHAR_255 = "VARCHAR(255)";
  private static final String CHAR = "CHAR";
  private static final String TIMESTAMP = "TIMESTAMP";
  private static final String DATE = "DATE";

  /** How many columns create.sql declares; a guard against an accidental edit of the table. */
  private static final int EXPECTED_COLUMN_COUNT = 105;

  /**
   * {@code {name, DuckDB type}} pairs in create.sql order — the single source of truth from which
   * the column list, both type maps and the DuckDB column spec are derived.
   */
  private static final String[] COLUMN_TABLE = {
      "WatchID",               BIGINT,
      "JavaEnable",            SMALLINT,
      "Title",                 TEXT,
      "GoodEvent",             SMALLINT,
      "EventTime",             TIMESTAMP,
      "EventDate",             DATE,
      "CounterID",             INTEGER,
      "ClientIP",              INTEGER,
      "RegionID",              INTEGER,
      "UserID",                BIGINT,
      "CounterClass",          SMALLINT,
      "OS",                    SMALLINT,
      "UserAgent",             SMALLINT,
      "URL",                   TEXT,
      "Referer",               TEXT,
      "IsRefresh",             SMALLINT,
      "RefererCategoryID",     SMALLINT,
      "RefererRegionID",       INTEGER,
      "URLCategoryID",         SMALLINT,
      "URLRegionID",           INTEGER,
      "ResolutionWidth",       SMALLINT,
      "ResolutionHeight",      SMALLINT,
      "ResolutionDepth",       SMALLINT,
      "FlashMajor",            SMALLINT,
      "FlashMinor",            SMALLINT,
      "FlashMinor2",           TEXT,
      "NetMajor",              SMALLINT,
      "NetMinor",              SMALLINT,
      "UserAgentMajor",        SMALLINT,
      "UserAgentMinor",        VARCHAR_255,
      "CookieEnable",          SMALLINT,
      "JavascriptEnable",      SMALLINT,
      "IsMobile",              SMALLINT,
      "MobilePhone",           SMALLINT,
      "MobilePhoneModel",      TEXT,
      "Params",                TEXT,
      "IPNetworkID",           INTEGER,
      "TraficSourceID",        SMALLINT,
      "SearchEngineID",        SMALLINT,
      "SearchPhrase",          TEXT,
      "AdvEngineID",           SMALLINT,
      "IsArtifical",           SMALLINT,
      "WindowClientWidth",     SMALLINT,
      "WindowClientHeight",    SMALLINT,
      "ClientTimeZone",        SMALLINT,
      "ClientEventTime",       TIMESTAMP,
      "SilverlightVersion1",   SMALLINT,
      "SilverlightVersion2",   SMALLINT,
      "SilverlightVersion3",   INTEGER,
      "SilverlightVersion4",   SMALLINT,
      "PageCharset",           TEXT,
      "CodeVersion",           INTEGER,
      "IsLink",                SMALLINT,
      "IsDownload",            SMALLINT,
      "IsNotBounce",           SMALLINT,
      "FUniqID",               BIGINT,
      "OriginalURL",           TEXT,
      "HID",                   INTEGER,
      "IsOldCounter",          SMALLINT,
      "IsEvent",               SMALLINT,
      "IsParameter",           SMALLINT,
      "DontCountHits",         SMALLINT,
      "WithHash",              SMALLINT,
      "HitColor",              CHAR,
      "LocalEventTime",        TIMESTAMP,
      "Age",                   SMALLINT,
      "Sex",                   SMALLINT,
      "Income",                SMALLINT,
      "Interests",             SMALLINT,
      "Robotness",             SMALLINT,
      "RemoteIP",              INTEGER,
      "WindowName",            INTEGER,
      "OpenerName",            INTEGER,
      "HistoryLength",         SMALLINT,
      "BrowserLanguage",       TEXT,
      "BrowserCountry",        TEXT,
      "SocialNetwork",         TEXT,
      "SocialAction",          TEXT,
      "HTTPError",             SMALLINT,
      "SendTiming",            INTEGER,
      "DNSTiming",             INTEGER,
      "ConnectTiming",         INTEGER,
      "ResponseStartTiming",   INTEGER,
      "ResponseEndTiming",     INTEGER,
      "FetchTiming",           INTEGER,
      "SocialSourceNetworkID", SMALLINT,
      "SocialSourcePage",      TEXT,
      "ParamPrice",            BIGINT,
      "ParamOrderID",          TEXT,
      "ParamCurrency",         TEXT,
      "ParamCurrencyID",       SMALLINT,
      "OpenstatServiceName",   TEXT,
      "OpenstatCampaignID",    TEXT,
      "OpenstatAdID",          TEXT,
      "OpenstatSourceID",      TEXT,
      "UTMSource",             TEXT,
      "UTMMedium",             TEXT,
      "UTMCampaign",           TEXT,
      "UTMContent",            TEXT,
      "UTMTerm",               TEXT,
      "FromTag",               TEXT,
      "HasGCLID",              SMALLINT,
      "RefererHash",           BIGINT,
      "URLHash",               BIGINT,
      "CLID",                  INTEGER,
  };

  /** All 105 column names in create.sql order. */
  public static final List<String> COLUMNS;

  private static final Map<String, String> DUCKDB_TYPE_BY_COLUMN;
  private static final Map<String, ColumnType> TYPE_BY_COLUMN;
  private static final String DUCKDB_COLUMN_SPEC_JSON;

  static {
    if (COLUMN_TABLE.length % 2 != 0) {
      throw new IllegalStateException("COLUMN_TABLE must hold {name, type} pairs, got "
                                          + COLUMN_TABLE.length + " entries");
    }
    final int columnCount = COLUMN_TABLE.length / 2;
    final List<String> names = new ArrayList<>(columnCount);
    final Map<String, String> duckdbTypes = HashMap.newHashMap(columnCount);
    final Map<String, ColumnType> types = HashMap.newHashMap(columnCount);
    // No column name needs JSON escaping — they are all ASCII identifiers, which the loop asserts.
    final StringBuilder spec = new StringBuilder(4096).append('{');
    for (int i = 0; i < COLUMN_TABLE.length; i += 2) {
      final String name = COLUMN_TABLE[i];
      final String duckdbType = COLUMN_TABLE[i + 1];
      requireIdentifier(name);
      if (duckdbTypes.put(name, duckdbType) != null) {
        throw new IllegalStateException("duplicate ClickBench column: " + name);
      }
      types.put(name, columnTypeOf(name, duckdbType));
      names.add(name);
      if (i > 0) {
        spec.append(',');
      }
      spec.append('"').append(name).append("\":\"").append(duckdbType).append('"');
    }
    spec.append('}');
    if (names.size() != EXPECTED_COLUMN_COUNT) {
      throw new IllegalStateException("expected " + EXPECTED_COLUMN_COUNT + " ClickBench columns, got "
                                          + names.size());
    }
    COLUMNS = List.copyOf(names);
    DUCKDB_TYPE_BY_COLUMN = Collections.unmodifiableMap(duckdbTypes);
    TYPE_BY_COLUMN = Collections.unmodifiableMap(types);
    DUCKDB_COLUMN_SPEC_JSON = spec.toString();
  }

  private ClickBenchSchema() {
    throw new AssertionError("no instances");
  }

  /**
   * @param column a column name
   * @return the JSON shape that column takes under the encoding contract
   * @throws IllegalArgumentException if {@code column} is not a ClickBench column
   */
  public static ColumnType typeOf(final String column) {
    Objects.requireNonNull(column, "column");
    final ColumnType type = TYPE_BY_COLUMN.get(column);
    if (type == null) {
      throw new IllegalArgumentException("unknown ClickBench column: " + column);
    }
    return type;
  }

  /**
   * The column's SQL type exactly as create.sql spells it, for building a matching DuckDB
   * {@code read_json} column spec.
   *
   * @param column a column name
   * @return one of {@code BIGINT}, {@code INTEGER}, {@code SMALLINT}, {@code TEXT},
   *         {@code VARCHAR(255)}, {@code CHAR}, {@code TIMESTAMP}, {@code DATE}
   * @throws IllegalArgumentException if {@code column} is not a ClickBench column
   */
  public static String duckdbType(final String column) {
    Objects.requireNonNull(column, "column");
    final String type = DUCKDB_TYPE_BY_COLUMN.get(column);
    if (type == null) {
      throw new IllegalArgumentException("unknown ClickBench column: " + column);
    }
    return type;
  }

  /**
   * The whole type map as a JSON object literal — {@code {"WatchID":"BIGINT","JavaEnable":...}} — in
   * create.sql order, so the reference implementation can feed it straight into DuckDB's
   * {@code read_json(..., columns = ...)}.
   *
   * @return an immutable JSON object literal with one entry per column
   */
  public static String duckdbColumnSpecJson() {
    return DUCKDB_COLUMN_SPEC_JSON;
  }

  private static ColumnType columnTypeOf(final String column, final String duckdbType) {
    return switch (duckdbType) {
      case BIGINT -> ColumnType.LONG;
      case INTEGER, SMALLINT -> ColumnType.INT;
      case TEXT, VARCHAR_255, CHAR -> ColumnType.STRING;
      case DATE -> ColumnType.DATE;
      case TIMESTAMP -> ColumnType.DATETIME;
      default -> throw new IllegalStateException("column " + column + " has an unmapped SQL type: "
                                                     + duckdbType);
    };
  }

  /** Column names go into JSON keys unescaped, so reject anything that is not a plain identifier. */
  private static void requireIdentifier(final String name) {
    if (name.isEmpty()) {
      throw new IllegalStateException("empty column name");
    }
    for (int i = 0; i < name.length(); i++) {
      final char c = name.charAt(i);
      final boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
      if (!ok) {
        throw new IllegalStateException("column name is not a plain identifier: " + name);
      }
    }
  }
}
