/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

/**
 * The record-emission backend of {@link BulkJsonTreeAssembler}: everything the assembler's
 * prediction/container machinery needs from the storage layer, and nothing else. Three
 * implementations are planned — the transaction-backed sink (the original sequential behavior), a
 * counting sink (the parallel importer's Stage-A node counter, so count and build share ONE code
 * path), and a worker page builder emitting standalone {@code KeyValueLeafPage}s.
 *
 * <p>
 * CONTRACT — written for the hottest call path in a bulk load (~one call per node):
 * <ul>
 * <li>Primitives only: {@code long} keys/PCRs, {@code byte[]+length} string values. The lone object
 * parameters are the field-name {@link String} (canonical instance from the scanner's intern table)
 * and the heterogeneous {@link Number} a JSON number decodes to — both exactly what the underlying
 * encoders take.</li>
 * <li>Every creation returns the key the record MINTED AT; the assembler asserts it against its own
 * prediction, so a sink that mints out of sequence fails loudly, never silently.</li>
 * <li>A sink may bind returned state to reusable flyweights internally, so callers must not retain
 * anything a creation call produced beyond the NEXT sink call.</li>
 * <li>Notification PCRs ride the creation calls; a sink that does not notify ignores them.</li>
 * </ul>
 */
interface BulkRecordSink {

  // ==== containers (created with NULL child/right-sibling pointers; fixed once at close) =======

  long createObjectNode(long parentKey, long leftSibKey, long notifyPcr);

  long createArrayNode(long parentKey, long leftSibKey, long arrayPcr);

  long createObjectNamedObjectNode(long parentKey, long leftSibKey, long pathNodeKey, String name);

  long createObjectNamedArrayNode(long parentKey, long leftSibKey, long pathNodeKey, String name);

  // ==== leaves (created with FINAL pointers) ===================================================

  long createStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] utf8, int utf8Length, long notifyPcr);

  long createObjectNamedStringNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey, String name,
      byte[] utf8, int utf8Length);

  long createNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value, long notifyPcr);

  default long createNumberNode(long parentKey, long leftSibKey, long rightSibKey, int value, long notifyPcr) {
    return createNumberNode(parentKey, leftSibKey, rightSibKey, Integer.valueOf(value), notifyPcr);
  }

  default long createNumberNode(long parentKey, long leftSibKey, long rightSibKey, long value, long notifyPcr) {
    return createNumberNode(parentKey, leftSibKey, rightSibKey, Long.valueOf(value), notifyPcr);
  }

  long createObjectNamedNumberNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey, String name,
      Number value);

  default long createObjectNamedNumberNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, int value) {
    return createObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, Integer.valueOf(value));
  }

  default long createObjectNamedNumberNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long value) {
    return createObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, Long.valueOf(value));
  }

  long createBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean value, long notifyPcr);

  long createObjectNamedBooleanNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey, String name,
      boolean value);

  long createNullNode(long parentKey, long leftSibKey, long rightSibKey, long notifyPcr);

  long createObjectNamedNullNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey, String name);

  // ==== one-shot fixups ========================================================================

  /** Applies a container's close-time pointers in one touch; {@code rightSibKey} may be null-key. */
  void fixupContainer(long containerKey, long firstChildKey, long lastChildKey, long childCount, long rightSibKey);

  /** Applies the document node's pointers once, at the end of the run. */
  void fixupDocument(long documentKey, long firstChildKey, long lastChildKey, long childCount);

  // ==== epoch accounting =======================================================================

  /** Accounts {@code mutations} records at a safe boundary; the sink may rotate a flush epoch. */
  void accountRecords(int mutations);
}
