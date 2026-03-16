package io.sirix.index.vector.hnsw;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * In-memory implementation of {@link VectorStore} for testing the HNSW algorithm in isolation.
 *
 * <p>Uses HashMaps for storage. Not intended for production use. Not thread-safe.
 */
public final class InMemoryVectorStore implements VectorStore {

  private final int dimension;
  private final Map<Long, float[]> vectors;
  private final Map<Long, long[][]> neighborLists;  // nodeKey -> layer -> neighbor keys
  private final Map<Long, int[]> neighborCounts;     // nodeKey -> layer -> count
  private final Map<Long, Integer> maxLayers;        // nodeKey -> max layer

  private final Set<Long> deletedKeys;
  private final Map<Long, Long> documentKeyToNodeKey; // documentNodeKey -> hnswNodeKey

  private long entryPointKey = -1;
  private int maxLevel = -1;
  private long nextNodeKey = 0;

  /**
   * Creates an in-memory vector store.
   *
   * @param dimension vector dimensionality, must be positive
   */
  public InMemoryVectorStore(final int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("Dimension must be positive: " + dimension);
    }
    this.dimension = dimension;
    this.vectors = new HashMap<>(1024);
    this.neighborLists = new HashMap<>(1024);
    this.neighborCounts = new HashMap<>(1024);
    this.maxLayers = new HashMap<>(1024);
    this.deletedKeys = new HashSet<>();
    this.documentKeyToNodeKey = new HashMap<>(1024);
  }

  @Override
  public float[] getVector(final long nodeKey) {
    final float[] vector = vectors.get(nodeKey);
    if (vector == null) {
      throw new IllegalArgumentException("Node key not found: " + nodeKey);
    }
    return vector;
  }

  @Override
  public long[] getNeighbors(final long nodeKey, final int layer) {
    final long[][] layers = neighborLists.get(nodeKey);
    if (layers == null || layer >= layers.length || layers[layer] == null) {
      return new long[0];
    }
    return layers[layer];
  }

  @Override
  public int getNeighborCount(final long nodeKey, final int layer) {
    final int[] counts = neighborCounts.get(nodeKey);
    if (counts == null || layer >= counts.length) {
      return 0;
    }
    return counts[layer];
  }

  @Override
  public int getMaxLayer(final long nodeKey) {
    final Integer ml = maxLayers.get(nodeKey);
    return ml != null ? ml : -1;
  }

  @Override
  public void setNeighbors(final long nodeKey, final int layer, final long[] neighbors, final int count) {
    long[][] layers = neighborLists.get(nodeKey);
    int[] counts = neighborCounts.get(nodeKey);

    final int requiredSize = layer + 1;

    if (layers == null || layers.length < requiredSize) {
      final long[][] newLayers = new long[requiredSize][];
      final int[] newCounts = new int[requiredSize];
      if (layers != null) {
        System.arraycopy(layers, 0, newLayers, 0, layers.length);
        System.arraycopy(counts, 0, newCounts, 0, counts.length);
      }
      layers = newLayers;
      counts = newCounts;
      neighborLists.put(nodeKey, layers);
      neighborCounts.put(nodeKey, counts);
    }

    layers[layer] = Arrays.copyOf(neighbors, neighbors.length);
    counts[layer] = count;
  }

  @Override
  public long createNode(final long documentNodeKey, final float[] vector, final int maxLayer) {
    if (vector == null || vector.length < dimension) {
      throw new IllegalArgumentException("Vector must have at least " + dimension + " dimensions");
    }
    final long nodeKey = nextNodeKey++;
    vectors.put(nodeKey, Arrays.copyOf(vector, dimension));
    maxLayers.put(nodeKey, maxLayer);
    documentKeyToNodeKey.put(documentNodeKey, nodeKey);

    // Pre-allocate neighbor arrays for all layers
    final int layerCount = maxLayer + 1;
    final long[][] layers = new long[layerCount][];
    final int[] counts = new int[layerCount];
    for (int l = 0; l < layerCount; l++) {
      layers[l] = new long[0];
      counts[l] = 0;
    }
    neighborLists.put(nodeKey, layers);
    neighborCounts.put(nodeKey, counts);

    return nodeKey;
  }

  @Override
  public void updateEntryPoint(final long entryPointKey, final int maxLevel) {
    this.entryPointKey = entryPointKey;
    this.maxLevel = maxLevel;
  }

  @Override
  public long getEntryPointKey() {
    return entryPointKey;
  }

  @Override
  public int getMaxLevel() {
    return maxLevel;
  }

  @Override
  public int getDimension() {
    return dimension;
  }

  @Override
  public void markDeleted(final long nodeKey) {
    if (!vectors.containsKey(nodeKey)) {
      throw new IllegalArgumentException("Node key not found: " + nodeKey);
    }
    deletedKeys.add(nodeKey);
  }

  @Override
  public boolean isDeleted(final long nodeKey) {
    return deletedKeys.contains(nodeKey);
  }

  @Override
  public long findNodeKeyByDocumentKey(final long documentNodeKey) {
    final Long nodeKey = documentKeyToNodeKey.get(documentNodeKey);
    return nodeKey != null ? nodeKey : -1L;
  }
}
