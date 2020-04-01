package org.sirix.node.interfaces;

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
   * Gets the kind of the node (element node, text node, attribute node....).
   * 
   * @return kind of node
   */
  RecordPersister getKind();

  /**
   * Get the revision this node has been inserted.
   * 
   * @return revision this node has been inserted
   */
  long getRevision();
}
