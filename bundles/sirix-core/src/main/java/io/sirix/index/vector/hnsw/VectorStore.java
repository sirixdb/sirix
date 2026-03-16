package io.sirix.index.vector.hnsw;

/**
 * Storage abstraction for HNSW graph data: vectors, neighbor lists, and graph metadata.
 *
 * <p>Implementations may store data in-memory (for testing) or backed by SirixDB pages
 * (for production). All methods must be safe for single-threaded use during graph construction.
 */
public interface VectorStore {

  /**
   * Retrieves the vector associated with a node.
   *
   * @param nodeKey the node key
   * @return the vector, never null
   * @throws IllegalArgumentException if the node key does not exist
   */
  float[] getVector(long nodeKey);

  /**
   * Returns the neighbor keys for a node at a given layer.
   *
   * @param nodeKey the node key
   * @param layer   the HNSW layer (0 = bottom)
   * @return array of neighbor keys; length may exceed actual neighbor count
   */
  long[] getNeighbors(long nodeKey, int layer);

  /**
   * Returns the number of actual neighbors for a node at a given layer.
   *
   * @param nodeKey the node key
   * @param layer   the HNSW layer
   * @return the neighbor count
   */
  int getNeighborCount(long nodeKey, int layer);

  /**
   * Returns the maximum layer this node participates in.
   *
   * @param nodeKey the node key
   * @return the max layer (0-based), or -1 if the node has no layers
   */
  int getMaxLayer(long nodeKey);

  /**
   * Sets the neighbor list for a node at a given layer.
   *
   * @param nodeKey   the node key
   * @param layer     the HNSW layer
   * @param neighbors array of neighbor keys
   * @param count     number of valid entries in the neighbors array
   */
  void setNeighbors(long nodeKey, int layer, long[] neighbors, int count);

  /**
   * Creates a new node in the store with the given vector and layer assignment.
   *
   * @param documentNodeKey the document-level node key for this vector
   * @param vector          the embedding vector
   * @param maxLayer        the maximum layer this node is assigned to
   * @return the node key of the newly created node
   */
  long createNode(long documentNodeKey, float[] vector, int maxLayer);

  /**
   * Updates the global entry point of the HNSW graph.
   *
   * @param entryPointKey the node key of the new entry point
   * @param maxLevel      the maximum level of the graph
   */
  void updateEntryPoint(long entryPointKey, int maxLevel);

  /**
   * Returns the current entry point node key, or -1 if the graph is empty.
   *
   * @return the entry point key
   */
  long getEntryPointKey();

  /**
   * Returns the current maximum level of the HNSW graph.
   *
   * @return the max level, or -1 if the graph is empty
   */
  int getMaxLevel();

  /**
   * Returns the dimensionality of vectors in this store.
   *
   * @return the vector dimension
   */
  int getDimension();

  /**
   * Marks a vector node as tombstone-deleted. The node remains in the graph
   * for neighbor traversal continuity but is excluded from search results.
   *
   * @param nodeKey the node key of the vector to mark as deleted
   * @throws UnsupportedOperationException if this store is read-only
   */
  void markDeleted(long nodeKey);

  /**
   * Checks whether a vector node has been tombstone-deleted.
   *
   * @param nodeKey the node key to check
   * @return true if the node is deleted, false otherwise
   */
  boolean isDeleted(long nodeKey);

  /**
   * Finds the HNSW-internal node key for a vector that was created for the given
   * document node key.
   *
   * @param documentNodeKey the document-level node key to look up
   * @return the HNSW-internal node key, or -1 if no vector exists for the document key
   */
  long findNodeKeyByDocumentKey(long documentNodeKey);
}
