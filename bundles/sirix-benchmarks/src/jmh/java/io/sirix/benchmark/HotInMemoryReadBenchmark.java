/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.benchmark;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.Database;
import io.sirix.cache.HOTLookupCache;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.brackit.query.util.path.Path.parse;

/**
 * In-memory HOT read cost, measured the way it should be measured.
 *
 * <p>
 * This replaces a hand-rolled {@code System.nanoTime} best-of-N harness that produced misleading
 * numbers: running several timing loops over the same code inside one JVM polluted the JIT profile
 * badly enough that its own end-to-end figure came out 17x worse than a dedicated run of the same
 * code. Every stage below therefore gets its own {@link Fork}ed JVM, real warmup, and a
 * {@link Blackhole} so nothing is folded away.
 *
 * <p>
 * The stages isolate where the cost of a point lookup actually sits, so an optimization can be
 * attributed rather than guessed at:
 * </p>
 * <ul>
 * <li>{@link #serializeKey} — CAS key encoding alone.</li>
 * <li>{@link #descendToLeaf} — the PEXT-routed descent, the part directly comparable to a
 * main-memory trie lookup.</li>
 * <li>{@link #lowerBoundSeek} — PEXT descent plus the Binna search-stack walk to the affected
 * subtree for an absent key.</li>
 * <li>{@link #pointGet} — the whole public lookup, including chunk reassembly into
 * {@code NodeReferences}, which a single-value trie lookup does not have to do.</li>
 * <li>{@link #findEntryInLeaf} — the in-leaf binary search alone, on one fixed leaf, with no
 * descent, no key encoding and no chunk assembly in the frame.</li>
 * </ul>
 *
 * <p>
 * Everything is resident: the index is built in {@code @Setup} and every stage is driven over the
 * same shuffled key set, so no stage pays I/O.
 * </p>
 *
 * <p>
 * <b>Reading the numbers.</b> The composite stages are only trustworthy as a cross-run comparison
 * when {@link #serializeKey} — which no leaf-side change can touch — reproduces. It has been
 * observed to compile two different ways across forks (79 ns vs 190 ns for identical bytecode), and
 * a fork that lands in the slow mode drags {@link #pointGet} with it. Treat it as this harness's
 * control: if it moved, the run is not comparable and the narrow stages are the only evidence.
 * </p>
 *
 * <p>
 * <b>Reference point,</b> movies.json CAS index on {@code /[]/title}, 4 cores, JDK 25. Narrow
 * stages, interleaved A/B against the pre-optimization tree: descendToLeaf 72.6 -> 70.9 ns,
 * findEntryInLeaf 66.4 -> 54.1, serializeKey 125.9 -> 93.9. The descent stage is the figure
 * comparable to a main-memory trie lookup; the rest of a point lookup is key encoding, an in-page
 * binary search over ~180 entries, and posting-list reassembly, none of which a single-tuple trie
 * lookup performs.
 *
 * <p>
 * The composite stages depend on how the point-lookup cache is sized, so they are only meaningful
 * with {@code sirix.hotLookupCache.maxEntries} stated. Over a ~33K-key corpus:
 * </p>
 *
 * <pre>
 * maxEntries      pointGet          pointGetHotKeys
 * 0 (disabled)    551.9 ns          373.0 ns
 * 65536           240.2 ns           66.8 ns   (both hit)
 * 64              661.1 ns           66.1 ns   (pointGet misses; pointGetHotKeys still HITS)
 * </pre>
 *
 * <p>
 * Read that as: memoizing is worth 2.3x when the working set fits and 5.6x on a small hot set, and
 * costs about 20% when it never fits. The 64-entry row is the one worth keeping honest — sizing the
 * cache below the key count is the ONLY way this harness measures what a miss costs, since the
 * trial warm-up touches every probe and would otherwise leave even {@link #pointGet} a pure hit
 * workload.
 * </p>
 *
 * <p>
 * <b>The 64-entry row is a miss row for {@link #pointGet} ONLY</b>, and the second column is the
 * trap. {@code maxEntries=64} builds {@code highestOneBit(64/8) = 8} sets of {@link HOTLookupCache}
 * WAYS, i.e. 64 slots, and {@link #HOT_KEY_COUNT} is 32 — so the whole hot set stays resident and
 * 66.1 ns is a HIT measurement, statistically indistinguishable from the 66.8 ns above it, as the
 * numbers themselves show. Making {@code pointGetHotKeys} miss needs a table smaller than its
 * working set, which the constructor floors at one 8-way set; a separate row would have to state
 * both the size and the key count to mean anything. Do not read this column as the cost of a miss
 * on a hot key.
 * </p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
@State(Scope.Benchmark)
public class HotInMemoryReadBenchmark {

  private Path dbPath;
  private Database<JsonResourceSession> database;
  private JsonResourceSession session;
  private JsonNodeReadOnlyTrx rtx;
  private HOTIndexReader<CASValue> reader;
  private HOTTrieReader trieReader;
  private PageReference rootRef;

  private CASValue[] probes;
  private byte[][] seekKeys;
  private final byte[] scratch = new byte[512];
  private int cursor;

  /** One resident leaf, and composite keys that all land in it — see {@link #findEntryInLeaf}. */
  private HOTLeafPage leaf;
  private byte[][] leafKeys;
  private int leafCursor;

  /**
   * Working-set size for {@link #pointGetHotKeys}. Small enough that every key stays memoized, and
   * small enough that the whole set stays in cache, so the stage measures the hit path rather than
   * the memory system.
   */
  private static final int HOT_KEY_COUNT = 32;

  /** {@link #HOT_KEY_COUNT} clamped to the corpus, which {@code -Dsirix.bench.corpus} can shrink. */
  private int hotKeyCount;

  private int hotCursor;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    final Path corpus =
        Paths.get(System.getProperty("sirix.bench.corpus", "bundles/sirix-core/src/test/resources/json/movies.json"));
    dbPath = Files.createTempDirectory("hot-inmem-read");
    Databases.removeDatabase(dbPath);
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    database.createResource(ResourceConfiguration.newBuilder("r").build());

    final IndexDef casDef;
    try (final JsonResourceSession manager = database.beginResourceSession("r");
        final JsonNodeTrx trx = manager.beginNodeTrx()) {
      final var ic = manager.getWtxIndexController(trx.getRevisionNumber());
      casDef = IndexDefs.createCASIdxDef(false, Type.STR,
          Collections.singleton(parse("/[]/title", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
      new JsonShredder.Builder(trx, JsonShredder.createFileReader(corpus),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      ic.createIndexes(Set.of(casDef), trx);
      trx.commit();
    }

    session = database.beginResourceSession("r");
    rtx = session.beginNodeReadOnlyTrx();
    reader = HOTIndexReader.create(rtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, casDef.getType(),
        casDef.getID());

    final List<CASValue> keys = new ArrayList<>();
    for (final Iterator<Map.Entry<CASValue, ?>> it = cast(reader.iterator()); it.hasNext();) {
      keys.add(it.next().getKey());
    }
    // Fixed seed: a stable, cache-unfriendly access order that does not vary run to run.
    Collections.shuffle(keys, new Random(0x5EED));
    probes = keys.toArray(new CASValue[0]);

    seekKeys = new byte[probes.length][];
    for (int i = 0; i < probes.length; i++) {
      final int n = CASKeySerializer.INSTANCE.serialize(probes[i], scratch, 0);
      final byte[] composite = new byte[n + 4];
      System.arraycopy(scratch, 0, composite, 0, n);
      seekKeys[i] = composite;
    }

    rootRef = reader.getRootReference();
    trieReader = new HOTTrieReader(rtx.getStorageEngineReader());

    // Touch every entry once so the whole index is resident before measurement starts.
    for (final CASValue p : probes) {
      if (reader.get(p, SearchMode.EQUAL) == null) {
        throw new IllegalStateException("probe set does not round-trip: " + p);
      }
    }

    // Sample the leaf the median probe routes to, and take its probe keys from the leaf's own
    // entries: every one is guaranteed resident and a guaranteed hit, so findEntryInLeaf measures
    // the full search depth rather than a mix of hits and early-outs.
    leaf = trieReader.navigateToLeaf(rootRef, seekKeys[seekKeys.length / 2]);
    if (leaf == null) {
      throw new IllegalStateException("median probe does not route to a leaf");
    }
    hotKeyCount = Math.min(HOT_KEY_COUNT, probes.length);

    final int leafEntries = leaf.getEntryCount();
    final List<byte[]> resident = new ArrayList<>(leafEntries);
    for (int i = 0; i < leafEntries; i++) {
      final byte[] entryKey = leaf.getKey(i);
      if (entryKey != null) {
        resident.add(entryKey);
      }
    }
    if (resident.isEmpty()) {
      throw new IllegalStateException("sampled leaf exposes no keys");
    }
    Collections.shuffle(resident, new Random(0x5EED));
    leafKeys = resident.toArray(new byte[0][]);
    System.out.println("[setup] sampled leaf: entryCount=" + leafEntries + " probes=" + leafKeys.length);
  }

  @SuppressWarnings("unchecked")
  private static Iterator<Map.Entry<CASValue, ?>> cast(final Iterator<?> it) {
    return (Iterator<Map.Entry<CASValue, ?>>) it;
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (trieReader != null) {
      trieReader.close();
    }
    if (rtx != null) {
      rtx.close();
    }
    if (session != null) {
      session.close();
    }
    if (database != null) {
      database.close();
    }
  }

  private int next() {
    final int i = cursor;
    cursor = i + 1 == probes.length
        ? 0
        : i + 1;
    return i;
  }

  @Benchmark
  public void serializeKey(final Blackhole bh) {
    bh.consume(CASKeySerializer.INSTANCE.serialize(probes[next()], scratch, 0));
  }

  @Benchmark
  public void descendToLeaf(final Blackhole bh) {
    bh.consume(trieReader.navigateToLeaf(rootRef, seekKeys[next()]));
  }

  @Benchmark
  public void lowerBoundSeek(final Blackhole bh) {
    bh.consume(trieReader.lowerBound(rootRef, seekKeys[next()]));
  }

  @Benchmark
  public void pointGet(final Blackhole bh) {
    bh.consume(reader.get(probes[next()], SearchMode.EQUAL));
  }

  /**
   * The same lookup over a SMALL key set that fits the memoization cache, so nearly every call is a
   * cache hit.
   *
   * <p>
   * This is the stage the point-lookup cache exists for, and unlike {@link #pointGet} it is a
   * HIT-path measurement at EVERY cache size the table in the class javadoc lists — including the
   * 64-entry row, which is a miss row for {@code pointGet} alone. Its working set is
   * {@link #HOT_KEY_COUNT} keys against a table the constructor never builds smaller than one
   * {@code WAYS}-way set, so the whole set stays resident; turning this stage into a miss measurement
   * takes a table smaller than {@link #HOT_KEY_COUNT}, not merely one below the corpus size. See the
   * class javadoc for the sizing of all stages at once.
   * </p>
   */
  @Benchmark
  public void pointGetHotKeys(final Blackhole bh) {
    final int i = hotCursor;
    hotCursor = i + 1 == hotKeyCount
        ? 0
        : i + 1;
    bh.consume(reader.get(probes[i], SearchMode.EQUAL));
  }

  /**
   * The in-leaf search in isolation: one already-resolved leaf, keys that all belong to it, no
   * descent and no key encoding in the frame. This is the stage that can attribute a change to
   * {@code HOTLeafPage}'s comparators, which the composite stages cannot — their variance is larger
   * than the effect being measured.
   */
  @Benchmark
  public void findEntryInLeaf(final Blackhole bh) {
    final int i = leafCursor;
    leafCursor = i + 1 == leafKeys.length
        ? 0
        : i + 1;
    bh.consume(leaf.findEntry(leafKeys[i]));
  }
}
