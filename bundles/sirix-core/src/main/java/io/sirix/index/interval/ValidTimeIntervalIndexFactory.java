/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTIndexWriter;

/**
 * Constructs the {@link RelationalIntervalTree} that backs a valid-time interval index over a single
 * HOT sub-tree.
 *
 * <p>Both RI-tree stores (lower/upper) are realised on ONE HOT sub-tree (one {@code indexNumber} =
 * {@code IndexDef#getID()}); a one-byte store discriminator keeps them in disjoint contiguous key
 * ranges (see {@link ValidTimeKey}). The writer factory wires a single
 * {@link HOTIndexWriter}{@code <ValidTimeKey>} into both stores; the reader factory wires a single
 * {@link HOTIndexReader}{@code <ValidTimeKey>}.</p>
 *
 * <p><b>Backend is always HOT, by construction.</b> The persistent {@link OrderedStore} the RI-tree
 * needs (an order-preserving range-scannable ordered map) only exists on the HOT trie, so this
 * factory unconditionally constructs HOT readers/writers — it does NOT consult the global
 * {@code sirix.index.useHOT} / {@code ResourceConfiguration.indexBackendType} setting that selects
 * the CAS/PATH/NAME backend. A resource may therefore freely mix an RBTree CAS/PATH/NAME index
 * (default backend) with a HOT VALIDTIME index in the same revision: they live in different
 * {@code RevisionRootPage} reference slots ({@code CASPage}/{@code PathPage}/{@code NamePage} vs
 * {@link io.sirix.page.ValidTimeIndexPage}) and never share state, so the VALIDTIME index builds,
 * maintains, and queries correctly under DEFAULT JVM settings with no {@code -D} flag.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeIntervalIndexFactory {

  private ValidTimeIntervalIndexFactory() {
    throw new AssertionError("May never be instantiated!");
  }

  /**
   * Create a writer-backed RI-tree for index {@code indexNumber}. The same {@link HOTIndexWriter}
   * drives both the lower and the upper store; for in-transaction scans (read-your-writes during a
   * full build) a {@link HOTIndexReader} over the same writer's storage engine is wired into the
   * stores too.
   */
  public static RelationalIntervalTree createWriterTree(final StorageEngineWriter storageEngineWriter,
      final int indexNumber, final IntervalDomain domain) {
    final HOTIndexWriter<ValidTimeKey> writer =
        HOTIndexWriter.create(storageEngineWriter, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, indexNumber);
    // A reader over the writer's storage engine sees the writer's uncommitted (TIL-backed) pages, so
    // delete-then-insert maintenance can find the previously-registered slot within the same trx.
    final HOTIndexReader<ValidTimeKey> reader =
        HOTIndexReader.create(storageEngineWriter, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, indexNumber);
    final OrderedStore lower = new HotOrderedStore(ValidTimeKey.STORE_LOWER, writer, reader);
    final OrderedStore upper = new HotOrderedStore(ValidTimeKey.STORE_UPPER, writer, reader);
    return new RelationalIntervalTree(domain.height(), lower, upper);
  }

  /**
   * Create a read-only RI-tree for index {@code indexNumber} (the query path). Only the lower/upper
   * stores' {@code scan} is exercised, so a single {@link HOTIndexReader} suffices; the stores have
   * no writer.
   */
  public static RelationalIntervalTree createReaderTree(final StorageEngineReader storageEngineReader,
      final int indexNumber, final IntervalDomain domain) {
    final HOTIndexReader<ValidTimeKey> reader =
        HOTIndexReader.create(storageEngineReader, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, indexNumber);
    final OrderedStore lower = new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader);
    final OrderedStore upper = new HotOrderedStore(ValidTimeKey.STORE_UPPER, null, reader);
    return new RelationalIntervalTree(domain.height(), lower, upper);
  }
}
