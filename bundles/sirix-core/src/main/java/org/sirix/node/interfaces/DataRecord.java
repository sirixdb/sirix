package org.sirix.node.interfaces;

import org.sirix.node.SirixDeweyID;

/**
 * Base interface for all records.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface DataRecord {
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

  /**
   * Gets the kind of the node (element node, text node, attribute node....).
   * 
   * @return kind of node
   */
  RecordSerializer getKind();

  /**
   * Get the revision this node has been inserted.
   * 
   * @return revision this node has been inserted
   */
  long getRevision();
}
