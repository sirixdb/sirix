package org.sirix.node.interfaces;

import org.sirix.node.SirixDeweyID;

import java.math.BigInteger;

/**
 * Base interface for all records.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface DataRecord {

  BigInteger BIG_INT_31 = BigInteger.valueOf(31);

  /**
   * Get unique node key.
   * 
   * @return node key
   */
  long getNodeKey();

  /**
   * Get the DeweyID.
   *
   * @return the DeweyID if present, otherwise {@code null}
   */
  SirixDeweyID getDeweyID();

  byte[] getDeweyIDAsBytes();

  /**
   * Gets the kind of the node (element node, text node, attribute node....).
   * 
   * @return kind of node
   */
  RecordSerializer getKind();

  /**
   * Get the previous revision of this record.
   *
   * @return the previous revision
   */
  int getPreviousRevisionNumber();

  /**
   * Get the previous revision of this record.
   *
   * @return the previous revision
   */
  int getLastModifiedRevisionNumber();
}
