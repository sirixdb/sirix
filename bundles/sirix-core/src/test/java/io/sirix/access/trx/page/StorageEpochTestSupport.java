/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.api.NodeTrx;

/**
 * Test access to the plain-transaction storage-epoch bound and the epochs a transaction has
 * rotated, for tests outside this package ({@link NodeStorageEngineWriter} is package-private).
 */
public final class StorageEpochTestSupport {

  /** The async-flush import bound, the smallest bound a plain transaction accepts. */
  public static final int IMPORT_BOUND = NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT;

  private StorageEpochTestSupport() {}

  /**
   * A positive value bounds the live intent log of plain transactions at that many pages; {@code 0}
   * restores.
   */
  public static void setPlainTransactionBound(final int pages) {
    NodeStorageEngineWriter.setStorageEpochLogEntryCountForTesting(pages);
  }

  /** Live entries of the transaction's intent log. */
  public static int liveLogEntries(final NodeTrx trx) {
    return ((NodeStorageEngineWriter) trx.getStorageEngineWriter()).getLog().liveEntryCount();
  }

  /** Storage epochs the transaction has rotated so far (its intent-log generation). */
  public static int rotatedEpochs(final NodeTrx trx) {
    return ((NodeStorageEngineWriter) trx.getStorageEngineWriter()).getLog().getCurrentGeneration();
  }

  /** Reset the process-wide record-capture counters below. */
  public static void resetRecordCaptureCounters() {
    WriterRecordSnapshot.resetCounters();
    NodeStorageEngineWriter.DURABLE_DOCUMENT_RECORD_READS.reset();
  }

  /** Document records served from a capture after their page had been written out. */
  public static long capturedRecordHits() {
    return WriterRecordSnapshot.hitCount() + WriterRecordSnapshot.tombstoneHitCount();
  }

  /** Document record reads that went to a page the transaction had already written out. */
  public static long durableDocumentRecordReads() {
    return NodeStorageEngineWriter.DURABLE_DOCUMENT_RECORD_READS.sum();
  }

}
