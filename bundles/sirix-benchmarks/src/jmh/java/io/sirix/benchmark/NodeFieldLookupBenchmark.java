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

import io.sirix.page.NodeFieldLayout;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Resolving a record's field layout from its kind id — the lookup the page encoder does several
 * times per record, on every record, on every page.
 *
 * <p>{@link NodeFieldLayout} answers these from flat byte tables. It used to answer them from
 * {@code switch} statements over a sparse set of kind ids, which javac compiles to an indirect
 * jump; this benchmark is the evidence for the change and the guard against reverting it.
 *
 * <p>The {@code kindMix} parameter is the whole point. {@code SINGLE} is one kind repeated, which
 * is what a synthetic page tends to look like and where the indirect jump has exactly one target
 * and predicts perfectly — measure only that and the tables look like dead weight. {@code JSON} is
 * a container-and-leaves cycle, the shape a shredded JSON document actually puts on a page.
 * {@code RANDOM} draws uniformly from every kind in use and is the pessimistic bound. A branch
 * predictor separates these three by a wide margin; an array load does not.
 *
 * <p>Both arms call the same public API — the point is not to A/B two implementations here, it is
 * to measure what the encoder pays per record and to show how much of that cost depends on how
 * mixed the page is.
 *
 * <p>Run with:
 * <pre>./gradlew :sirix-benchmarks:jmh -Pjmh.includes=NodeFieldLookupBenchmark \
 *     -Pjmh.warmupIterations=5 -Pjmh.iterations=10 -Pjmh.fork=2</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class NodeFieldLookupBenchmark {

  /** One page worth of records. */
  private static final int RECORDS = 1024;

  /** Every kind id the JSON write path emits. */
  private static final int[] JSON_KINDS = { 24, 25, 27, 28, 29, 30, 48, 49, 50, 51 };

  /** Container, then the leaves hanging off it — one repeat of a shredded object. */
  private static final int[] JSON_CYCLE = { 24, 50, 49, 50, 48, 49, 25, 49, 49, 50, 51, 49 };

  @Param({ "JSON", "RANDOM", "SINGLE" })
  public String kindMix;

  private int[] kindIds;

  @Setup
  public void setUp() {
    kindIds = new int[RECORDS];
    final Random rnd = new Random(0x1A70C7);
    for (int i = 0; i < RECORDS; i++) {
      kindIds[i] = switch (kindMix) {
        case "JSON" -> JSON_CYCLE[i % JSON_CYCLE.length];
        case "RANDOM" -> JSON_KINDS[rnd.nextInt(JSON_KINDS.length)];
        case "SINGLE" -> 25;
        default -> throw new IllegalArgumentException("unknown kindMix " + kindMix);
      };
    }
  }

  /**
   * The five lookups the encoder's main per-slot scan performs, over a page of records.
   *
   * <p>Summed rather than blackholed per call so the loop stays the shape the encoder's loop is —
   * a dependent chain of cheap lookups feeding arithmetic, not an opaque barrier between each one.
   */
  @Benchmark
  public int encoderScanLookups() {
    final int[] ids = kindIds;
    int acc = 0;
    for (int i = 0; i < RECORDS; i++) {
      final int kindId = ids[i];
      acc += NodeFieldLayout.fieldCountForKind(kindId);
      acc += NodeFieldLayout.parentKeyFieldIndexForKind(kindId);
      acc += NodeFieldLayout.pathNodeKeyFieldIndexForKind(kindId);
      acc += NodeFieldLayout.hashFieldIndexForKind(kindId);
      acc += NodeFieldLayout.nameKeyFieldIndexForKind(kindId);
    }
    return acc;
  }

  /** Field count alone — the single most-called of the group, resolved three times per record. */
  @Benchmark
  public int fieldCountOnly() {
    final int[] ids = kindIds;
    int acc = 0;
    for (int i = 0; i < RECORDS; i++) {
      acc += NodeFieldLayout.fieldCountForKind(ids[i]);
    }
    return acc;
  }
}
