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
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.Database;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.CASValue;
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
 * <li>{@link #lowerBoundSeek} — the descent plus the full lower-bound machinery (landing verify,
 * successor peek, lex re-descent on a miss).</li>
 * <li>{@link #pointGet} — the whole public lookup, including chunk reassembly into
 * {@code NodeReferences}, which a single-value trie lookup does not have to do.</li>
 * </ul>
 *
 * <p>
 * Everything is resident: the index is built in {@code @Setup} and every stage is driven over the
 * same shuffled key set, so no stage pays I/O.
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
  private io.sirix.api.json.JsonNodeReadOnlyTrx rtx;
  private HOTIndexReader<CASValue> reader;
  private HOTTrieReader trieReader;
  private PageReference rootRef;

  private CASValue[] probes;
  private byte[][] seekKeys;
  private final byte[] scratch = new byte[512];
  private int cursor;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    final Path corpus = Paths.get(System.getProperty("sirix.bench.corpus",
        "bundles/sirix-core/src/test/resources/json/movies.json"));
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
      new JsonShredder.Builder(trx, JsonShredder.createFileReader(corpus), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards().build().call();
      ic.createIndexes(Set.of(casDef), trx);
      trx.commit();
    }

    session = database.beginResourceSession("r");
    rtx = session.beginNodeReadOnlyTrx();
    reader = HOTIndexReader.create(rtx.getStorageEngineReader(), CASKeySerializer.INSTANCE, casDef.getType(), casDef.getID());

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
}
