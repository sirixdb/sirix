package io.sirix.node.interfaces;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Persisting a record (first byte of a record must be its type).
 *
 * @author Johannes Lichtenberger
 */
public interface RecordSerializer {
  /**
   * Deserialize a record.
   *
   * @param source input source
   * @param recordID the unique recordID
   * @param deweyID optional deweyID of the record
   * @param resourceConfiguration the resource configuration
   * @return a {@link DataRecord} instance
   * @throws NullPointerException if one of the parameters is {@code null}
   */
  @NonNull
  DataRecord deserialize(BytesIn<?> source, @NonNegative long recordID, byte[] deweyID,
      ResourceConfiguration resourceConfiguration);

  /**
   * Serialize a record.
   *
   * @param sink where the data should be serialized to
   * @param record the record to serialize
   * @param resourceConfiguration the resource configuration
   * @throws NullPointerException if one of the parameters is {@code null}
   */
  void serialize(BytesOut<?> sink, DataRecord record, ResourceConfiguration resourceConfiguration);
}
