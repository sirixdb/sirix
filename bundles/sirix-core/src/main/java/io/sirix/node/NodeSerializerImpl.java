package io.sirix.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.DeweyIdSerializer;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;

/**
 * Serialize and deserialize nodes.
 *
 * @author Johannes Lichtenberger
 */
public final class NodeSerializerImpl implements DeweyIdSerializer {
  @Override
  public @NonNull DataRecord deserialize(final BytesIn<?> source, final @NonNegative long recordID,
      final byte[] deweyID, final ResourceConfiguration resourceConfiguration) {
    final byte id = source.readByte();
    final NodeKind enumKind = NodeKind.getKind(id);
    return enumKind.deserialize(source, recordID, deweyID, resourceConfiguration);
  }

  @Override
  public void serialize(final BytesOut<?> sink, final DataRecord record,
      final ResourceConfiguration resourceConfiguration) {
    final NodeKind nodeKind = (NodeKind) record.getKind();
    final byte id = nodeKind.getId();
    sink.writeByte(id);
    nodeKind.serialize(sink, record, resourceConfiguration);
  }

  /**
   * Deserialize a DeweyID using delta encoding.
   * 
   * <p>Optimized to avoid intermediate allocations:
   * - Single allocation for result array (instead of 3 allocations in original)
   * - Uses System.arraycopy instead of Arrays.copyOfRange + ByteBuffer
   * - Reads suffix directly into result array at correct offset
   * 
   * <p>For delta-encoded DeweyIDs: stores [cutOffSize][suffixSize][suffix bytes]
   * where the full ID = previousDeweyID[0..cutOffSize] + suffix
   */
  @Override
  public byte[] deserializeDeweyID(BytesIn<?> source, byte[] previousDeweyID, ResourceConfiguration resourceConfig) {
    if (!resourceConfig.areDeweyIDsStored) {
      return null;
    }
    
    if (previousDeweyID != null) {
      // Delta-encoded: combine prefix from previous + suffix from source
      final int cutOffSize = source.readByte() & 0xFF;
      final int suffixSize = source.readByte() & 0xFF;
      
      // Single allocation for result (becomes previousDeweyID for next iteration)
      final byte[] result = new byte[cutOffSize + suffixSize];
      
      // Copy prefix from previous DeweyID (no intermediate array)
      System.arraycopy(previousDeweyID, 0, result, 0, cutOffSize);
      
      // Read suffix directly into result at correct offset (no intermediate array)
      // MemorySegmentBytesIn.read() already uses MemorySegment.copy() internally
      source.read(result, cutOffSize, suffixSize);
      
      return result;
    } else {
      // First DeweyID: no delta encoding
      final int deweyIDLength = source.readByte() & 0xFF;
      final byte[] result = new byte[deweyIDLength];
      
      // Read directly into result array
      source.read(result, 0, deweyIDLength);
      
      return result;
    }
  }

  @Override
  public void serializeDeweyID(BytesOut<?> sink, byte[] deweyID, byte[] nextDeweyID,
      ResourceConfiguration resourceConfig) {
    if (resourceConfig.areDeweyIDsStored) {
      if (nextDeweyID != null) {

        //assert deweyIDBytes.length <= nextDeweyIDBytes.length;

        int i = 0;
        for (; i < deweyID.length && i < nextDeweyID.length; i++) {
          if (deweyID[i] != nextDeweyID[i]) {
            break;
          }
        }
        writeDeweyID(sink, nextDeweyID, i);
      } else {
        sink.writeByte((byte) deweyID.length);
        sink.write(deweyID);
      }
    }
  }

  private static void writeDeweyID(final BytesOut<?> sink, final byte[] deweyID, @NonNegative final int i) {
    sink.writeByte((byte) i);
    sink.writeByte((byte) (deweyID.length - i));
    final var bytes = Arrays.copyOfRange(deweyID, i, deweyID.length);
    sink.write(bytes);
  }
}
