package org.sirix.node.interfaces;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.sirix.api.PageReadTrx;
import org.sirix.node.SirixDeweyID;

/**
 * Persistenting a record (first byte of a record must be its type).
 *
 * @author Johannes Lichtenberger
 *
 */
public interface RecordPersistenter {
  /**
   * Deserialize a record.
   *
   * @param source input source
   * @param recordID the unique recordID
   * @param deweyID optional deweyID of the record
   * @param pageReadTrx {@link PageReadTrx} instance
   * @return a {@link Record} instance
   * @throws NullPointerException if one of the parameters is {@code null}
   * @throws IOException if an I/O error occurs during deserialization
   */
  @Nonnull
  Record deserialize(DataInput source, @Nonnegative long recordID, Optional<SirixDeweyID> deweyID,
      PageReadTrx pageReadTrx) throws IOException;

  /**
   * Serialize a record.
   *
   * @param sink where the data should be serialized to
   * @param record the record to serialize
   * @param pageReadTrx {@link PageReadTrx} instance
   * @throws NullPointerException if one of the parameters is {@code null}
   * @throws IOException if an I/O error occurs during serialization
   */
  void serialize(DataOutput sink, Record record, PageReadTrx pageReadTrx) throws IOException;
}
