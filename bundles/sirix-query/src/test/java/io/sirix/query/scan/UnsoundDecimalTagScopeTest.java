package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One field the double column cannot carry exactly must not cost its NEIGHBOURS their columns.
 *
 * <p>A tag is refused when carrying it would mean storing a rounded double in place of a decimal —
 * the miscount {@link DecimalDoubleCollisionTest} pins. That refusal is right and stays; what is
 * measured here is its SCOPE. The region holds one tag per field, so a refusal that returns no
 * payload at all takes every other field on the page down with it, and a page with one awkward
 * price loses the column path for its rates, its years, everything.
 *
 * <h2>The trigger, without any mixed typing</h2>
 * {@code price} is scale-2 throughout except for one scale-14 record. An all-decimal tag is exact-
 * encoded at the maximum scale it holds, so that single value forces a {@code 10^12} lift on its
 * neighbours, and a scale-2 unscaled value past {@code Long.MAX_VALUE / 10^12} — a price over
 * {@code 92233.72}, which these are — overflows it. The tag falls out of the exact domain, cannot be
 * stored as an image either, and is refused. Only the ONE page holding the scale-14 record is
 * affected, which is why the fallback count below is an exact zero rather than a ratio.
 *
 * <p>{@code rate} is an ordinary scale-2 decimal on every record and has nothing wrong with it.
 */
@DisplayName("unsound decimal tag scope")
final class UnsoundDecimalTagScopeTest {

  private static final int N = 8_000;
  private static final String DB = "unsound-tag-db";
  private static final String RES = "records.jn";

  /** Scale 14, and inexact as a double — the value that drags its whole tag out of both domains. */
  private static final String AWKWARD_PRICE = "1000.25000000000001";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-unsound-tag-");
    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i).append(",\"price\":");
      if (i == 0) {
        sb.append(AWKWARD_PRICE);
      } else {
        // Scale 2, and large enough that lifting to scale 14 overflows a long.
        sb.append(100_000 + i % 500).append('.').append(i % 10).append(i % 10);
      }
      sb.append(",\"rate\":").append(1 + i % 90).append('.').append(i % 10).append(i % 10)
        .append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("the sound neighbour keeps its column on the page that holds the unsound field")
  void theSoundNeighbourKeepsItsColumn() throws Exception {
    final String predicate = "$u.rate gt 45.5";
    final long viaRecords = count(predicate, false);
    assertTrue(viaRecords > 0, "predicate matches nothing, so it proves nothing");

    // COLD, and load-bearing. A resident page is served by deriving whatever columns the predicate
    // asks for from the slotted page, so the price tag never reaches the encoder and this would
    // pass without ever exercising the refusal. The persisted region table — the thing the encoder
    // actually wrote — is only read when the page is not already in the cache.
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(viaRecords, count(predicate, true), "column path disagrees for: " + predicate);
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
               "no page was served from columns at all");
    assertEquals(0L, SirixVectorizedExecutor.regionOnlyPageFallbacks(),
                 "a page fell back to its records for a field with nothing wrong with it — the "
                     + "refusal of the unsound price tag took the whole double region with it "
                     + "instead of just that one field");
  }

  @Test
  @DisplayName("the unsound field itself still answers correctly, through the records")
  void theUnsoundFieldStillAnswersCorrectly() throws Exception {
    // Not accelerated, and not meant to be: refusing the tag is what keeps a rounded double from
    // standing in for a decimal. What must hold is that the answer is unchanged.
    for (final String predicate : new String[] { "$u.price gt 100250.55",
                                                 "$u.price lt 100100.00",
                                                 "$u.price gt 1000.25" }) {
      assertEquals(count(predicate, false), count(predicate, true),
                   "column path disagrees with the record path for: " + predicate);
    }
  }

  private long count(final String predicate, final boolean regionOnly) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec =
            new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          return ((Int64) new Query(chain,
                                    "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where "
                                        + predicate + " return $u)").evaluate(ctx)).longValue();
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }
}
