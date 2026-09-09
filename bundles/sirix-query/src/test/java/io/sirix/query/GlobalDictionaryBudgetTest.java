package io.sirix.query;

import com.google.gson.stream.JsonReader;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.GlobalValueDictionaryWriter;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ProjectionSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The resource-wide value dictionary must be BOUNDED, and hitting the bound must be a decline.
 *
 * <p>
 * Defect (11): a 100M-row ClickBench load elected URL, Referer and Title — long, near-unique
 * strings — and their dictionaries outgrew a 16 GB heap. It never failed. The arena kept doubling
 * until the collector owned 3.4 cores and the load produced one megabyte a minute while still
 * looking alive, and it was killed after two hours. Two independent protections now exist and this
 * pins both:
 *
 * <ul>
 * <li>an ELECTION-TIME decline, which needs an expected-row-count hint — a streaming build learns
 * the real row count when the stream ends, thousands of leaves after the election — and which is
 * the only point where declining ONE column is cheap, because afterwards its ids are already in
 * every written leaf;</li>
 * <li>a RUNTIME cap in the writer, which needs no hint and is what an unhinted load falls back on.
 * Its outcome is coarser by necessity: the whole projection is abandoned, the load COMPLETES, and
 * queries take the generic pipeline.</li>
 * </ul>
 *
 * <p>
 * Non-vacuity is by construction throughout: every decline assertion is paired with the same corpus
 * under a budget that admits it, so a test that stopped exercising the mechanism would fail its
 * twin rather than pass quietly.
 */
@ResourceLock(value = Resources.SYSTEM_PROPERTIES, mode = ResourceAccessMode.READ_WRITE)
final class GlobalDictionaryBudgetTest {

  private static final String BUDGET_PROPERTY = "sirix.projection.globalDict.budgetBytes";
  private static final String MODE_PROPERTY = "sirix.projection.globalDict";
  private static final String ROOT_PATH = "/[]";
  private static final List<String> FIELD_PATHS = List.of("/[]/id", "/[]/url");
  private static final List<String> FIELD_TYPES = List.of("long", "string");
  private static final List<String> AGGREGATE_FIELD_PATHS =
      List.of("/[]/url", "/[]/low0", "/[]/low1", "/[]/low2", "/[]/low3", "/[]/low4", "/[]/low5");
  private static final List<String> AGGREGATE_FIELD_TYPES =
      List.of("string", "string", "string", "string", "string", "string", "string");

  /**
   * Enough records for several leaves AND enough distinct values to clear AUTO's minimum-entries
   * floor ({@code sirix.projection.globalDict.minEntries}, 4096) — below it nothing is ever elected
   * and every "control must elect" arm here would fail for a reason that has nothing to do with the
   * budget. Matched to the figure {@code GlobalValueDictionaryServingTest} already relies on.
   */
  private static final int RECORDS = 12_000;

  @TempDir
  private Path root;

  private String priorBudget;
  private String priorMode;

  @BeforeEach
  void configureAutoMode() {
    priorBudget = System.getProperty(BUDGET_PROPERTY);
    priorMode = System.getProperty(MODE_PROPERTY);
    System.clearProperty(BUDGET_PROPERTY);
    System.setProperty(MODE_PROPERTY, "auto");
  }

  @AfterEach
  void restoreProperties() {
    restoreProperty(BUDGET_PROPERTY, priorBudget);
    restoreProperty(MODE_PROPERTY, priorMode);
  }

  private static void restoreProperty(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  private static String dataset(final int records) {
    final StringBuilder sb = new StringBuilder(records * 96).append('[');
    for (int i = 0; i < records; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Distinct per row and long, so the per-leaf dictionary deduplicates nothing — exactly the
      // property the election reads as "a resource-wide dictionary would be better".
      sb.append("{\"id\":")
        .append(i)
        .append(",\"url\":\"http://example.com/a/rather/long/path/segment?id=")
        .append(i)
        .append("\"}");
    }
    return sb.append(']').toString();
  }

  /**
   * A corpus whose leading leaves elect a resource-wide dictionary and whose tail then offers a value
   * the V0 entry layout cannot hold.
   *
   * <p>
   * The trigger is the VALUE-LENGTH ceiling rather than the interner's distinct-entry ceiling,
   * because the streaming build flushes a dictionary generation at every drain and starts the next
   * one with an empty interner: the per-append entry limit therefore resets long before any realistic
   * corpus reaches it, and a dataset sized to cross it would exercise nothing. The length ceiling is
   * per VALUE, so it is reachable at any point in any generation — and it is genuinely a RUNTIME
   * refusal, because election samples only the leading leaves and this value is not among them.
   * </p>
   */
  private static String runtimeCapDataset() {
    final int sampledRows = 16 * ProjectionIndexRowGroupPage.MAX_ROWS;
    final int novelRows = 3 * ProjectionIndexRowGroupPage.MAX_ROWS;
    // One byte past the ceiling: the refusal must come from the bound itself, not from a value so
    // extreme that some earlier layer could have rejected it first.
    final int oversizedRow = sampledRows + ProjectionIndexRowGroupPage.MAX_ROWS;
    final String oversized = "x".repeat(GlobalValueDictionaryWriter.MAX_VALUE_BYTES + 1);
    final StringBuilder sb =
        new StringBuilder((sampledRows + novelRows) * 64 + GlobalValueDictionaryWriter.MAX_VALUE_BYTES).append('[');
    for (int i = 0; i < sampledRows + novelRows; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final String value;
      if (i == oversizedRow) {
        value = oversized;
      } else if (i < sampledRows) {
        value = "sample-" + (i % ProjectionIndexRowGroupPage.MAX_ROWS);
      } else {
        value = "novel-" + (i - sampledRows);
      }
      sb.append("{\"id\":").append(i).append(",\"url\":\"").append(value).append("\"}");
    }
    return sb.append(']').toString();
  }

  private static String nearStructuralCapDataset() {
    final int sampledRows = 16 * ProjectionIndexRowGroupPage.MAX_ROWS;
    final StringBuilder sb = new StringBuilder((sampledRows + 2) * 64).append('[');
    for (int i = 0; i < sampledRows + 2; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final int value = i == sampledRows - 1
          ? 0
          : i;
      sb.append("{\"id\":").append(i).append(",\"url\":\"near-cap-").append(value).append("\"}");
    }
    return sb.append(']').toString();
  }

  /** Seven declared string columns, but only {@code url} is a worthwhile AUTO candidate. */
  private static String aggregateBudgetDataset() {
    final StringBuilder sb = new StringBuilder(RECORDS * 160).append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"url\":\"http://example.com/a/rather/long/path/segment?id=")
        .append(i)
        .append("\",\"low0\":\"a\",\"low1\":\"b\",\"low2\":\"c\",\"low3\":\"d\"," + "\"low4\":\"e\",\"low5\":\"f\"}");
    }
    return sb.append(']').toString();
  }

  private int loadAndCountGlobalColumns(final String dbName, final long expectedRows) throws IOException {
    return loadAndCountGlobalColumns(dbName, expectedRows, RECORDS);
  }

  private int loadAndCountGlobalColumns(final String dbName, final long expectedRows, final int records)
      throws IOException {
    return loadAndCountGlobalColumns(dbName, expectedRows, dataset(records));
  }

  private int loadAndCountGlobalColumns(final String dbName, final long expectedRows, final String json)
      throws IOException {
    return loadAndCountGlobalColumns(dbName, expectedRows, json, FIELD_PATHS, FIELD_TYPES);
  }

  private int loadAndCountGlobalColumns(final String dbName, final long expectedRows, final String json,
      final List<String> fieldPaths, final List<String> fieldTypes) throws IOException {
    try (
        final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                       .location(root.resolve(dbName))
                                                       .numberOfNodesBeforeAutoCommit(4096)
                                                       .buildPathSummary(true)
                                                       .buildPathStatistics(false)
                                                       .build();
        final JsonReader reader = new JsonReader(new StringReader(json))) {
      store.create("coll", "res.jn", reader, new ProjectionSpec(ROOT_PATH, fieldPaths, fieldTypes, expectedRows));
    }
    return ProjectionIndexBuilder.globalDictionaryColumnsBuilt();
  }

  private ProjectionIndexMetadata projectionMetadata(final String dbName) {
    try (
        final Database<JsonResourceSession> database = Databases.openJsonDatabase(root.resolve(dbName).resolve("coll"));
        final JsonResourceSession session = database.beginResourceSession("res.jn");
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(session.getMostRecentRevisionNumber())) {
      final byte[] raw = ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), 0, 0L);
      return raw == null
          ? null
          : ProjectionIndexMetadata.parse(raw);
    }
  }

  @Test
  @DisplayName("With a row-count hint that makes the dictionary too big, the election declines the column")
  void oversizedProjectionDeclinesAtElection() throws IOException {
    // Control FIRST, so the decline below cannot be mistaken for a corpus that never elected at all:
    // the same data with a budget that admits it must promote exactly one column.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(128L << 20));
    assertEquals(1, loadAndCountGlobalColumns("admits", RECORDS),
        "the control must ELECT — otherwise the decline case proves nothing about the budget");

    // Same corpus, same hint, budget shrunk until the projection cannot fit. Nothing about the data
    // changed; only the bound did.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(256L << 10));
    assertEquals(0, loadAndCountGlobalColumns("declines", RECORDS),
        "a streaming dictionary whose combined four-times reservation exceeds the aggregate must remain "
            + "per-leaf DICT");
  }

  @Test
  @DisplayName("AUTO spends the aggregate on worthwhile candidates instead of slicing it across declared strings")
  void worthwhileCandidateIsNotDilutedByUnworthyStringColumns() throws IOException {
    // The 128 MiB combined envelope gives the generation writer and resident front 64 MiB each,
    // enough for the real 12k-value radix flush peak. The larger election hint makes url's
    // conservative combined reservation fit that aggregate while exceeding the former
    // 128 MiB / 7 columns / 2 uncertainty gate. Six unrelated low-cardinality declarations must not
    // decide the useful column's fate.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(128L << 20));

    assertEquals(1, loadAndCountGlobalColumns("aggregate-candidate", 120_000L, aggregateBudgetDataset(),
        AGGREGATE_FIELD_PATHS, AGGREGATE_FIELD_TYPES),
        "the one worthwhile column fits both disjoint component caps and must be elected");
    final ProjectionIndexMetadata metadata = projectionMetadata("aggregate-candidate");
    assertNotNull(metadata);
    final byte[] columnKinds = metadata.columnKinds();
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, columnKinds[0]);
    for (int column = 1; column < columnKinds.length; column++) {
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, columnKinds[column],
          "low-cardinality column " + column + " should remain local-dictionary encoded");
    }
  }

  @Test
  @DisplayName("A hint far larger than the corpus declines a column that would otherwise have fit")
  void theHintAndNotTheCorpusDrivesTheDecline() throws IOException {
    // The lever under test is the HINT: identical data and budget, and only the claimed row count
    // differs. This is what lets a 100M load decline up front on a sample of its first leaves.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(128L << 20));
    assertEquals(1, loadAndCountGlobalColumns("small-hint", RECORDS));
    assertEquals(0, loadAndCountGlobalColumns("huge-hint", 100_000_000L),
        "projected bytes scale with the hint, so a 100M-row claim must decline what 4k rows admitted");
  }

  @Test
  @DisplayName("Without a hint the election cannot judge, and the column is elected as before")
  void noHintKeepsTheHistoricalBehaviour() throws IOException {
    // The documented contract for hint-absent loads: the election-time check is skipped entirely
    // (it has no denominator) and the column is elected exactly as it was before this change.
    //
    // The budget here is the SAME generous one the control above uses, on purpose. A small budget
    // would prove nothing: the runtime cap would fire during the sample conversion and abandon the
    // projection, so a 0 could mean either "declined at election" or "abandoned at runtime" — two
    // different mechanisms with the same observable. Holding the budget fixed leaves the HINT as
    // the only variable, which is what this pins.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(128L << 20));
    assertEquals(1, loadAndCountGlobalColumns("no-hint", -1L),
        "an unhinted load must elect exactly as it always did — the election has no denominator to judge with");
  }

  @Test
  @DisplayName("AUTO reserves a full row group of headroom before converting a near-cap sample")
  void structuralEntryLimitDeclinesDuringElectionWithoutAbandoningTheProjection() throws IOException {
    System.setProperty(BUDGET_PROPERTY, String.valueOf(128L << 20));
    assertEquals(1, loadAndCountGlobalColumns("structural-control", -1L),
        "the smaller control must prove that AUTO elects this all-unique string shape");

    assertEquals(0, loadAndCountGlobalColumns("structural-decline", -1L, nearStructuralCapDataset()),
        "a 16,383-distinct sample must decline before two later novel values exhaust the interner");
    final ProjectionIndexMetadata metadata = projectionMetadata("structural-decline");
    assertNotNull(metadata, "a safe dictionary decline must retain the optional projection");
    assertFalse(metadata.isStale(), "the structurally bounded AUTO election abandoned the projection");
    assertEquals(17, metadata.rowGroupCount(),
        "every local-dictionary row group must remain visible after the decline");
  }

  @Test
  @DisplayName("An unhinted load whose dictionary blows its cap ABANDONS the projection and still completes")
  void runtimeCapAbandonsTheProjectionButNotTheLoad() throws IOException {
    // The first 16 leaves repeat the same 1,024 short values, so AUTO safely elects a global
    // dictionary and the election-time value-length check passes. A later row — past the sample,
    // and therefore invisible to the election — carries a value one byte above the safe V0 limit.
    // That is a genuine runtime structural refusal rather than an election-time decline.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(128L << 20));
    assertEquals(1, loadAndCountGlobalColumns("runtime-control", -1L),
        "the completed control must establish the last-successful-build diagnostic");
    assertEquals(1, loadAndCountGlobalColumns("abandoned", -1L, runtimeCapDataset()),
        "an abandoned builder must not overwrite the diagnostic from the latest completed build");

    final ProjectionIndexMetadata metadata = projectionMetadata("abandoned");
    assertTrue(metadata == null || metadata.isStale(),
        "a partially global-encoded projection became visible after runtime abandonment");
  }

  // The writer's own refusal is pinned next to the writer, in
  // io.sirix.index.projection.GlobalValueDictionaryWriterBudgetTest — its bounded constructor is
  // package-private, and reaching it from here would have meant opening production API for a test.
}
