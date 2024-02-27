package io.sirix.node.interfaces;

import io.sirix.api.PageReadOnlyTrx;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.ByteBuffer;

/**
 * Persisting a record (first byte of a record must be its type).
 *
 * @author Johannes Lichtenberger
 *
 */
public interface RecordSerializer {
  /**
   * Deserialize a record.
   *
   * @param source input source
   * @param recordID the unique recordID
   * @param deweyID optional deweyID of the record
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @return a {@link DataRecord} instance
   * @throws NullPointerException if one of the parameters is {@code null}
   */
  @NonNull
  DataRecord deserialize(BytesIn<?> source, @NonNegative long recordID, byte[] deweyID, PageReadOnlyTrx pageReadTrx);

  /**
   * Serialize a record.
   *
   * @param sink where the data should be serialized to
   * @param record the record to serialize
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @throws NullPointerException if one of the parameters is {@code null}
   */
  void serialize(BytesOut<?> sink, DataRecord record, PageReadOnlyTrx pageReadTrx);
}
