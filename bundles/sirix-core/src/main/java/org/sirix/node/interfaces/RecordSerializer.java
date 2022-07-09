package org.sirix.node.interfaces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.SirixDeweyID;

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
  DataRecord deserialize(Bytes<ByteBuffer> source, @NonNegative long recordID, byte[] deweyID, PageReadOnlyTrx pageReadTrx);

  /**
   * Serialize a record.
   *
   * @param sink where the data should be serialized to
   * @param record the record to serialize
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @throws NullPointerException if one of the parameters is {@code null}
   */
  void serialize(Bytes<ByteBuffer> sink, DataRecord record, PageReadOnlyTrx pageReadTrx);
}
