package io.sirix.query;

import com.google.gson.stream.JsonReader;
import io.sirix.index.projection.GlobalValueDictionaryWriter;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ProjectionSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
final class GlobalDictionaryBudgetTest {

  private static final String BUDGET_PROPERTY = "sirix.projection.globalDict.budgetBytes";
  private static final String ROOT_PATH = "/[]";
  private static final List<String> FIELD_PATHS = List.of("/[]/id", "/[]/url");
  private static final List<String> FIELD_TYPES = List.of("long", "string");

  /**
   * Enough records for several leaves AND enough distinct values to clear AUTO's minimum-entries
   * floor ({@code sirix.projection.globalDict.minEntries}, 4096) — below it nothing is ever elected
   * and every "control must elect" arm here would fail for a reason that has nothing to do with the
   * budget. Matched to the figure {@code GlobalValueDictionaryServingTest} already relies on.
   */
  private static final int RECORDS = 12_000;

  @TempDir
  private Path root;

  @BeforeEach
  void clearBefore() {
    System.clearProperty(BUDGET_PROPERTY);
  }

  @AfterEach
  void clearAfter() {
    System.clearProperty(BUDGET_PROPERTY);
  }

  private static String dataset() {
    final StringBuilder sb = new StringBuilder(RECORDS * 96).append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Distinct per row and long, so the per-leaf dictionary deduplicates nothing — exactly the
      // property the election reads as "a resource-wide dictionary would be better".
      sb.append("{\"id\":").append(i).append(",\"url\":\"http://example.com/a/rather/long/path/segment?id=")
        .append(i).append("\"}");
    }
    return sb.append(']').toString();
  }

  private int loadAndCountGlobalColumns(final String dbName, final long expectedRows) throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(root.resolve(dbName))
                                                        .numberOfNodesBeforeAutoCommit(4096)
                                                        .buildPathSummary(true)
                                                        .buildPathStatistics(false)
                                                        .build();
        final JsonReader reader = new JsonReader(new StringReader(dataset()))) {
      store.create("coll", "res.jn", reader, new ProjectionSpec(ROOT_PATH, FIELD_PATHS, FIELD_TYPES, expectedRows));
    }
    return ProjectionIndexBuilder.globalDictionaryColumnsBuilt();
  }

  @Test
  @DisplayName("With a row-count hint that makes the dictionary too big, the election declines the column")
  void oversizedProjectionDeclinesAtElection() throws IOException {
    // Control FIRST, so the decline below cannot be mistaken for a corpus that never elected at all:
    // the same data with a budget that admits it must promote exactly one column.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(64L << 20));
    assertEquals(1, loadAndCountGlobalColumns("admits", RECORDS),
        "the control must ELECT — otherwise the decline case proves nothing about the budget");

    // Same corpus, same hint, budget shrunk until the projection cannot fit. Nothing about the data
    // changed; only the bound did.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(256L << 10));
    assertEquals(0, loadAndCountGlobalColumns("declines", RECORDS),
        "a dictionary projected past half the budget must leave its column as a per-leaf DICT");
  }

  @Test
  @DisplayName("A hint far larger than the corpus declines a column that would otherwise have fit")
  void theHintAndNotTheCorpusDrivesTheDecline() throws IOException {
    // The lever under test is the HINT: identical data and budget, and only the claimed row count
    // differs. This is what lets a 100M load decline up front on a sample of its first leaves.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(64L << 20));
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
    System.setProperty(BUDGET_PROPERTY, String.valueOf(64L << 20));
    assertEquals(1, loadAndCountGlobalColumns("no-hint", -1L),
        "an unhinted load must elect exactly as it always did — the election has no denominator to judge with");
  }

  @Test
  @DisplayName("An unhinted load whose dictionary blows its cap ABANDONS the projection and still completes")
  void runtimeCapAbandonsTheProjectionButNotTheLoad() throws IOException {
    // The fail-soft that defect (11) actually needed: no hint, a cap the corpus cannot respect, and
    // the load must still finish. Before the cap existed this shape did not throw — it degraded into
    // a collector loop that produced almost nothing and never terminated on its own.
    System.setProperty(BUDGET_PROPERTY, String.valueOf(GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES + (8L << 10)));
    // COMPLETION is the whole assertion, and it is not vacuous: this exact shape — no hint, a corpus
    // whose dictionary cannot fit — is what ran for two hours at 100M without finishing or failing.
    // Returning at all is the behaviour change.
    //
    // Deliberately NOT asserting the global-column count: when the cap fires during the sample
    // conversion the election never reaches its counter, so the static reads whatever a previous
    // test left there. The abandonment itself is pinned where it is observable — the writer's typed
    // refusal in GlobalValueDictionaryWriterBudgetTest, and the WARN the listener logs.
    loadAndCountGlobalColumns("abandoned", -1L);
  }

  // The writer's own refusal is pinned next to the writer, in
  // io.sirix.index.projection.GlobalValueDictionaryWriterBudgetTest — its bounded constructor is
  // package-private, and reaching it from here would have meant opening production API for a test.
}
