/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import org.jspecify.annotations.Nullable;

/**
 * Sparse exact locator for projection records whose stable node key is not part of the monotone
 * document-order routing backbone.
 *
 * <p>The key mapping is a bijection from Sirix's non-negative document node keys onto the negative
 * signed-long half: {@code Long.MIN_VALUE | recordKey}. Existing projection slot families are all
 * non-negative, so the namespaces cannot alias. The five-byte raw value is
 * {@code [formatVersion=0][physicalSlot int LE]}; it is stored directly in the HOT leaf and never
 * allocates a blob/overflow page.</p>
 */
final class ProjectionRecordLocator {

  private static final byte FORMAT_VERSION = 0;
  private static final int VALUE_BYTES = 1 + Integer.BYTES;

  private ProjectionRecordLocator() {
  }

  static long slotKey(final long recordKey) {
    if (recordKey < 0) {
      throw new IllegalArgumentException("projection record key must be non-negative: " + recordKey);
    }
    return Long.MIN_VALUE | recordKey;
  }

  static int read(final StorageEngineReader reader, final int indexNumber, final long recordKey) {
    if (reader == null) {
      throw new NullPointerException("reader is required");
    }
    return decode(ProjectionIndexHOTStorage.readRawSlot(reader, indexNumber, slotKey(recordKey)), recordKey);
  }

  static Accessor open(final ProjectionIndexHOTStorage storage) {
    return new Accessor(storage);
  }

  private static byte[] encode(final int physicalSlot) {
    checkPhysicalSlot(physicalSlot);
    final byte[] value = new byte[VALUE_BYTES];
    value[0] = FORMAT_VERSION;
    ProjectionIndexRowGroupCodec.putIntLEAt(value, 1, physicalSlot);
    return value;
  }

  private static int decode(final byte @Nullable [] value, final long recordKey) {
    if (value == null) {
      return 0;
    }
    if (value.length != VALUE_BYTES || value[0] != FORMAT_VERSION) {
      throw new IllegalStateException("projection record locator " + recordKey
          + " has malformed/future value bytes");
    }
    final int physicalSlot = ProjectionIndexRowGroupCodec.getIntLE(value, 1);
    checkPhysicalSlot(physicalSlot);
    return physicalSlot;
  }

  private static void checkPhysicalSlot(final int physicalSlot) {
    if (physicalSlot < 1 || physicalSlot > ProjectionIndexHOTStorage.MAX_ROW_GROUPS) {
      throw new IllegalStateException("projection locator physical slot out of range: " + physicalSlot);
    }
  }

  static final class Accessor {
    private final ProjectionIndexHOTStorage storage;
    private int slotsRead;
    private int slotsWritten;
    private long bytesRead;
    private long bytesWritten;

    private Accessor(final ProjectionIndexHOTStorage storage) {
      if (storage == null) {
        throw new NullPointerException("storage is required");
      }
      this.storage = storage;
    }

    int find(final long recordKey) {
      final byte[] value = storage.getRawSlot(slotKey(recordKey));
      if (value != null) {
        slotsRead++;
        bytesRead += value.length;
      }
      return decode(value, recordKey);
    }

    void put(final long recordKey, final int physicalSlot) {
      final byte[] next = encode(physicalSlot);
      final byte[] prior = storage.getRawSlot(slotKey(recordKey));
      if (prior != null) {
        slotsRead++;
        bytesRead += prior.length;
        if (decode(prior, recordKey) == physicalSlot) {
          return;
        }
      }
      storage.putRawSlot(slotKey(recordKey), next);
      slotsWritten++;
      bytesWritten += next.length;
    }

    void remove(final long recordKey) {
      final byte[] prior = storage.getRawSlot(slotKey(recordKey));
      if (prior == null) {
        return;
      }
      slotsRead++;
      bytesRead += prior.length;
      decode(prior, recordKey);
      storage.tombstoneRawSlot(slotKey(recordKey));
      slotsWritten++;
    }

    int slotsRead() {
      return slotsRead;
    }

    int slotsWritten() {
      return slotsWritten;
    }

    long bytesRead() {
      return bytesRead;
    }

    long bytesWritten() {
      return bytesWritten;
    }
  }
}
