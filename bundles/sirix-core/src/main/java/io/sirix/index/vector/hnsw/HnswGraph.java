package io.sirix.index.vector.hnsw;

import io.sirix.index.vector.ops.DistanceFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Core HNSW (Hierarchical Navigable Small World) graph implementation.
 *
 * <p>Implements the algorithm described in "Efficient and robust approximate nearest neighbor
 * search using Hierarchical Navigable Small World graphs" (Malkov &amp; Yashunin, 2018).
 *
 * <p>This implementation focuses on:
 * <ul>
 *   <li>Zero-allocation hot paths — reuses heap objects via clear+refill pattern</li>
 *   <li>Primitive arrays instead of boxed collections</li>
 *   <li>Roaring64Bitmap for memory-efficient visited sets</li>
 *   <li>SIMD-accelerated distance computation via {@link DistanceFunction}</li>
 * </ul>
 *
 * <p>Not thread-safe. Callers must synchronize externally for concurrent insert/search.
 */
public final class HnswGraph {

  private final VectorStore store;
  private final HnswParams params;
  private final DistanceFunction distanceFunction;

  // Reusable heap objects — cleared before each operation
  private final PrimitiveLongFloatMinHeap candidateHeap;
  private final PrimitiveLongFloatMaxHeap resultHeap;
  private final Roaring64Bitmap visited;

  // Temporary buffer for neighbor selection
  private final long[] neighborBuffer;
  private final float[] neighborDistBuffer;

  /**
   * Creates an HNSW graph with the given store and parameters.
   *
   * @param store  the vector storage backend
   * @param params HNSW parameters
   */
  public HnswGraph(final VectorStore store, final HnswParams params) {
    if (store == null) {
      throw new IllegalArgumentException("store must not be null");
    }
    if (params == null) {
      throw new IllegalArgumentException("params must not be null");
    }
    this.store = store;
    this.params = params;
    this.distanceFunction = params.distanceType().getDistanceFunction(params.dimension());

    // Pre-allocate reusable structures with generous initial capacity
    final int heapCapacity = Math.max(params.efConstruction(), params.efSearch()) * 2;
    this.candidateHeap = new PrimitiveLongFloatMinHeap(heapCapacity);
    this.resultHeap = new PrimitiveLongFloatMaxHeap(heapCapacity);
    this.visited = new Roaring64Bitmap();

    // Buffer for neighbor selection heuristic
    final int maxNeighbors = Math.max(params.mMax0(), params.m()) + 1;
    this.neighborBuffer = new long[maxNeighbors * 2];
    this.neighborDistBuffer = new float[maxNeighbors * 2];
  }

  /**
   * Inserts a node into the HNSW graph. The node must already exist in the VectorStore
   * (created via {@link VectorStore#createNode}).
   *
   * @param nodeKey the key of the node to insert (must already exist in VectorStore)
   */
  public void insert(final long nodeKey) {
    final float[] vector = store.getVector(nodeKey);
    final int nodeMaxLayer = store.getMaxLayer(nodeKey);

    // First node in graph
    if (store.getEntryPointKey() == -1) {
      store.updateEntryPoint(nodeKey, nodeMaxLayer);
      return;
    }

    long epKey = store.getEntryPointKey();
    int currentMaxLevel = store.getMaxLevel();

    // Defensive check: if the entry point vector is missing, treat this node as the new entry point.
    final float[] epVector = store.getVector(epKey);
    if (epVector == null) {
      store.updateEntryPoint(nodeKey, nodeMaxLayer);
      return;
    }

    // Phase 1: Greedy descent from top level to (randomLevel + 1)
    float epDist = distanceFunction.distance(vector, epVector);

    for (int level = currentMaxLevel; level > nodeMaxLayer; level--) {
      boolean changed = true;
      while (changed) {
        changed = false;
        final long[] neighbors = store.getNeighbors(epKey, level);
        final int neighborCount = store.getNeighborCount(epKey, level);
        for (int i = 0; i < neighborCount; i++) {
          final long neighborKey = neighbors[i];
          final float dist = distanceFunction.distance(vector, store.getVector(neighborKey));
          if (dist < epDist) {
            epDist = dist;
            epKey = neighborKey;
            changed = true;
          }
        }
      }
    }

    // Phase 2: Insert at each layer from min(randomLevel, maxLevel) down to 0
    final int insertTopLayer = Math.min(nodeMaxLayer, currentMaxLevel);

    for (int level = insertTopLayer; level >= 0; level--) {
      // Search for nearest neighbors at this layer
      searchLayer(epKey, vector, params.efConstruction(), level);

      // Select neighbors using heuristic
      final int maxNeighbors = params.maxNeighbors(level);
      final int selectedCount = selectNeighborsHeuristic(nodeKey, vector, maxNeighbors);

      // Set bidirectional links
      final long[] selectedNeighbors = new long[selectedCount];
      System.arraycopy(neighborBuffer, 0, selectedNeighbors, 0, selectedCount);
      store.setNeighbors(nodeKey, level, selectedNeighbors, selectedCount);

      // Add reverse links and trim if needed
      for (int i = 0; i < selectedCount; i++) {
        final long neighborKey = selectedNeighbors[i];
        addReverseLink(neighborKey, nodeKey, vector, level, maxNeighbors);
      }

      // Update entry point for next layer down: use the closest selected neighbor.
      // (resultHeap is drained by selectNeighborsHeuristic, so use the buffers directly.)
      if (selectedCount > 0) {
        float bestDist = Float.MAX_VALUE;
        for (int i = 0; i < selectedCount; i++) {
          final float d = neighborDistBuffer[i];
          if (d < bestDist) {
            bestDist = d;
            epKey = selectedNeighbors[i];
          }
        }
        epDist = bestDist;
      }
    }

    // Update entry point if new node has a higher layer
    if (nodeMaxLayer > currentMaxLevel) {
      store.updateEntryPoint(nodeKey, nodeMaxLayer);
    }
  }

  /**
   * Finds the k nearest neighbors to the query vector.
   *
   * @param query the query vector
   * @param k     the number of nearest neighbors to return
   * @return array of node keys sorted by distance ascending (nearest first), length &lt;= k
   */
  public long[] searchKnn(final float[] query, final int k) {
    if (query == null || query.length < params.dimension()) {
      throw new IllegalArgumentException("Query vector must have at least " + params.dimension() + " dimensions");
    }
    for (int i = 0; i < query.length; i++) {
      if (Float.isNaN(query[i]) || Float.isInfinite(query[i])) {
        throw new IllegalArgumentException("Query vector contains NaN or Infinity at index " + i);
      }
    }
    if (k <= 0) {
      throw new IllegalArgumentException("k must be positive: " + k);
    }

    final long epKey = store.getEntryPointKey();
    if (epKey == -1) {
      return new long[0]; // Empty graph
    }

    long currentEp = epKey;
    int currentMaxLevel = store.getMaxLevel();

    // Phase 1: Greedy descent from top level to layer 1
    float epDist = distanceFunction.distance(query, store.getVector(currentEp));

    for (int level = currentMaxLevel; level >= 1; level--) {
      boolean changed = true;
      while (changed) {
        changed = false;
        final long[] neighbors = store.getNeighbors(currentEp, level);
        final int neighborCount = store.getNeighborCount(currentEp, level);
        for (int i = 0; i < neighborCount; i++) {
          final long neighborKey = neighbors[i];
          final float dist = distanceFunction.distance(query, store.getVector(neighborKey));
          if (dist < epDist) {
            epDist = dist;
            currentEp = neighborKey;
            changed = true;
          }
        }
      }
    }

    // Phase 2: Search layer 0 with efSearch
    final int ef = Math.max(params.efSearch(), k);
    searchLayer(currentEp, query, ef, 0);

    // Extract top-k from result heap
    // resultHeap is a max-heap; we want the k closest
    // First, trim the result heap to k elements
    while (resultHeap.size() > k) {
      resultHeap.poll();
    }

    return resultHeap.toSortedKeysAscending();
  }

  /**
   * Generates a random layer level for a new node using the exponential distribution.
   *
   * @return the random layer level (0-based)
   */
  public int generateRandomLevel() {
    final double uniform = ThreadLocalRandom.current().nextDouble();
    return (int) Math.floor(-Math.log(uniform) * params.mL());
  }

  /**
   * Searches a single layer of the HNSW graph, populating the internal resultHeap.
   *
   * <p>After this method returns, {@link #resultHeap} contains up to ef nearest neighbors
   * found at the specified layer (as a max-heap ordered by distance descending).
   *
   * @param entryPointKey the entry point to start searching from
   * @param query         the query vector
   * @param ef            the search width (number of candidates to track)
   * @param layer         the layer to search
   */
  private void searchLayer(final long entryPointKey, final float[] query, final int ef, final int layer) {
    visited.clear();
    candidateHeap.clear();
    resultHeap.clear();

    final float epDist = distanceFunction.distance(query, store.getVector(entryPointKey));

    visited.add(entryPointKey);
    candidateHeap.insert(entryPointKey, epDist);
    resultHeap.insert(entryPointKey, epDist);

    while (!candidateHeap.isEmpty()) {
      final float candidateDist = candidateHeap.peekDistance();
      final long candidateKey = candidateHeap.poll();

      // Stop if the closest candidate is farther than the farthest result
      final float farthestResult = resultHeap.peekDistance();
      if (candidateDist > farthestResult) {
        break;
      }

      // Explore neighbors
      final long[] neighbors = store.getNeighbors(candidateKey, layer);
      final int neighborCount = store.getNeighborCount(candidateKey, layer);

      for (int i = 0; i < neighborCount; i++) {
        final long neighborKey = neighbors[i];

        if (visited.contains(neighborKey)) {
          continue;
        }
        visited.add(neighborKey);

        final float dist = distanceFunction.distance(query, store.getVector(neighborKey));
        final float currentFarthest = resultHeap.peekDistance();

        if (dist < currentFarthest || resultHeap.size() < ef) {
          candidateHeap.insert(neighborKey, dist);
          resultHeap.insert(neighborKey, dist);

          if (resultHeap.size() > ef) {
            resultHeap.poll();
          }
        }
      }
    }
  }

  /**
   * Selects neighbors using the HNSW heuristic (Algorithm 4 from the paper).
   * Prefers diverse neighbors that are closer to the node than to each other.
   *
   * <p>Reads candidates from {@link #resultHeap} (which is populated by searchLayer).
   * Writes selected neighbors into {@link #neighborBuffer} and distances into
   * {@link #neighborDistBuffer}.
   *
   * @param nodeKey the node being inserted
   * @param vector  the node's vector
   * @param m       the maximum number of neighbors to select
   * @return the number of selected neighbors
   */
  private int selectNeighborsHeuristic(final long nodeKey, final float[] vector, final int m) {
    // Drain resultHeap into temporary arrays (sorted ascending by distance)
    final int candidateCount = resultHeap.size();
    if (candidateCount == 0) {
      return 0;
    }

    // Collect all candidates sorted ascending by distance
    final long[] candKeys = new long[candidateCount];
    final float[] candDists = new float[candidateCount];
    // resultHeap is max-heap, so drain and reverse
    for (int i = candidateCount - 1; i >= 0; i--) {
      candDists[i] = resultHeap.peekDistance();
      candKeys[i] = resultHeap.poll();
    }

    int selected = 0;

    for (int i = 0; i < candidateCount && selected < m; i++) {
      final long candKey = candKeys[i];
      final float candDist = candDists[i];

      if (candKey == nodeKey) {
        continue; // Skip self
      }

      // Heuristic: accept candidate if it's closer to the node than to any already-selected neighbor
      boolean good = true;
      final float[] candVector = store.getVector(candKey);

      for (int j = 0; j < selected; j++) {
        final float distToSelected = distanceFunction.distance(candVector, store.getVector(neighborBuffer[j]));
        if (distToSelected < candDist) {
          good = false;
          break;
        }
      }

      if (good) {
        neighborBuffer[selected] = candKey;
        neighborDistBuffer[selected] = candDist;
        selected++;
      }
    }

    // If heuristic was too aggressive and we have fewer than m neighbors,
    // fill remaining slots with the closest unused candidates
    if (selected < m) {
      for (int i = 0; i < candidateCount && selected < m; i++) {
        final long candKey = candKeys[i];
        if (candKey == nodeKey) {
          continue;
        }

        // Check if already selected
        boolean alreadySelected = false;
        for (int j = 0; j < selected; j++) {
          if (neighborBuffer[j] == candKey) {
            alreadySelected = true;
            break;
          }
        }

        if (!alreadySelected) {
          neighborBuffer[selected] = candKey;
          neighborDistBuffer[selected] = candDists[i];
          selected++;
        }
      }
    }

    return selected;
  }

  /**
   * Adds a reverse link from neighborKey back to nodeKey, trimming if needed.
   */
  private void addReverseLink(final long neighborKey, final long nodeKey, final float[] nodeVector,
                              final int layer, final int maxNeighbors) {
    final long[] existingNeighbors = store.getNeighbors(neighborKey, layer);
    final int existingCount = store.getNeighborCount(neighborKey, layer);

    if (existingCount < maxNeighbors) {
      // Room available — simply append
      final long[] newNeighbors;
      if (existingNeighbors == null) {
        newNeighbors = new long[1];
      } else if (existingNeighbors.length > existingCount) {
        newNeighbors = existingNeighbors;
      } else {
        newNeighbors = new long[existingCount + 1];
        System.arraycopy(existingNeighbors, 0, newNeighbors, 0, existingCount);
      }
      newNeighbors[existingCount] = nodeKey;
      store.setNeighbors(neighborKey, layer, newNeighbors, existingCount + 1);
    } else {
      // Over capacity — need to trim using heuristic
      // Build candidate set: existing neighbors + new node
      final float[] neighborVector = store.getVector(neighborKey);
      final int totalCandidates = existingCount + 1;

      // Use temporary arrays for trimming
      final long[] trimKeys = new long[totalCandidates];
      final float[] trimDists = new float[totalCandidates];

      for (int i = 0; i < existingCount; i++) {
        trimKeys[i] = existingNeighbors[i];
        trimDists[i] = distanceFunction.distance(neighborVector, store.getVector(existingNeighbors[i]));
      }
      trimKeys[existingCount] = nodeKey;
      trimDists[existingCount] = distanceFunction.distance(neighborVector, nodeVector);

      // Sort by distance ascending
      sortByDistance(trimKeys, trimDists, totalCandidates);

      // Apply heuristic selection
      final long[] selectedKeys = new long[maxNeighbors];
      int selected = 0;

      for (int i = 0; i < totalCandidates && selected < maxNeighbors; i++) {
        final long candKey = trimKeys[i];
        final float candDist = trimDists[i];

        boolean good = true;
        final float[] candVec = store.getVector(candKey);

        for (int j = 0; j < selected; j++) {
          final float distToSelected = distanceFunction.distance(candVec, store.getVector(selectedKeys[j]));
          if (distToSelected < candDist) {
            good = false;
            break;
          }
        }

        if (good) {
          selectedKeys[selected] = candKey;
          selected++;
        }
      }

      // If heuristic was too aggressive, fill with closest unused
      if (selected < maxNeighbors) {
        for (int i = 0; i < totalCandidates && selected < maxNeighbors; i++) {
          boolean alreadySelected = false;
          for (int j = 0; j < selected; j++) {
            if (selectedKeys[j] == trimKeys[i]) {
              alreadySelected = true;
              break;
            }
          }
          if (!alreadySelected) {
            selectedKeys[selected] = trimKeys[i];
            selected++;
          }
        }
      }

      store.setNeighbors(neighborKey, layer, selectedKeys, selected);
    }
  }

  /**
   * Simple insertion sort for small arrays — sorts both keys and dists by distance ascending.
   */
  private static void sortByDistance(final long[] keys, final float[] dists, final int count) {
    for (int i = 1; i < count; i++) {
      final long key = keys[i];
      final float dist = dists[i];
      int j = i - 1;
      while (j >= 0 && dists[j] > dist) {
        keys[j + 1] = keys[j];
        dists[j + 1] = dists[j];
        j--;
      }
      keys[j + 1] = key;
      dists[j + 1] = dist;
    }
  }
}
