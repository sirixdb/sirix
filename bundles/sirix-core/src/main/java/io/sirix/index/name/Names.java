package io.sirix.index.name;

import static java.util.Objects.requireNonNull;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.HashCountEntryNode;
import io.sirix.node.HashEntryNode;
import io.sirix.node.NodeKind;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.jspecify.annotations.Nullable;

/**
 * Names index structure.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class Names {

  /**
   * Map the hash of a name to the node key.
   */
  private final Int2LongMap countNodeMap;

  /**
   * Map the hash of a name to its name.
   */
  private final Int2ObjectMap<byte[]> nameMap;

  /**
   * Map which is used to count the occurences of a name mapping.
   */
  private final Int2IntMap countNameMapping;

  private long maxNodeKey;

  private final int indexNumber;

  /**
   * Constructor creating a new index structure.
   *
   * @param indexNumber the index number / offset of the names instance
   */
  private Names(final int indexNumber) {
    this.indexNumber = indexNumber;
    countNodeMap = new Int2LongOpenHashMap();
    nameMap = new Int2ObjectOpenHashMap<>();
    countNameMapping = new Int2IntOpenHashMap();
  }

  /**
   * Copy constructor.
   *
   * @param names the names to copy from
   */
  private Names(final Names names) {
    this.indexNumber = names.indexNumber;
    this.maxNodeKey = names.maxNodeKey;
    this.countNodeMap = new Int2LongOpenHashMap(names.countNodeMap);
    this.nameMap = new Int2ObjectOpenHashMap<>(names.nameMap);
    this.countNameMapping = new Int2IntOpenHashMap(names.countNameMapping);
  }

  /**
   * Constructor to build index from a persistent storage.
   *
   * @param storageEngineReader the storage engine reader
   * @param indexNumber the kind of name dictionary
   * @param maxNodeKey the maximum node key
   */
  private Names(final StorageEngineReader storageEngineReader, final int indexNumber, final long maxNodeKey) {
    this.indexNumber = indexNumber;
    this.maxNodeKey = maxNodeKey;
    // Size by the number of LIVE names (grown on demand), never by maxNodeKey. Under name churn
    // maxNodeKey climbs without bound — every remove-then-re-add of a name does maxNodeKey++ in
    // setName — so pre-sizing to maxNodeKey/0.75 made each dictionary O(historical slot count)
    // (tens of MB) even though only a handful of names are live; with the bounded names cache
    // retaining hundreds of these, the Java heap OOMs. fastutil maps rehash cheaply up to the live
    // count, which is what actually bounds the populated entries below.
    countNodeMap = new Int2LongOpenHashMap();
    nameMap = new Int2ObjectOpenHashMap<>();
    countNameMapping = new Int2IntOpenHashMap();

    // TODO: Next refactoring iteration: Move this to a factory, just assign stuff in constructors
    for (long i = 1; i < maxNodeKey; i += 2) {
      final var nameNode = storageEngineReader.getRecord(i, IndexType.NAME, indexNumber);

      if (nameNode != null && nameNode.getKind() != NodeKind.DELETE) {
        final HashEntryNode hashEntryNode = (HashEntryNode) nameNode;

        final int key = hashEntryNode.getKey();

        nameMap.put(key, hashEntryNode.getValue().getBytes(Constants.DEFAULT_ENCODING));

        final long nodeKeyOfCountNode = i + 1;
        final var countNode = storageEngineReader.getRecord(nodeKeyOfCountNode, IndexType.NAME, indexNumber);
        if (countNode == null) {
          throw new IllegalStateException("Node couldn't be fetched from persistent storage: " + nodeKeyOfCountNode);
        }

        final HashCountEntryNode hashKeyToNameCountEntryNode = (HashCountEntryNode) countNode;
        countNameMapping.put(key, hashKeyToNameCountEntryNode.getValue());
        countNodeMap.put(key, nodeKeyOfCountNode);
      }
    }
  }

  /**
   * Reconstruct from storage by iterating ONLY the live entry node-keys (O(live)), avoiding the
   * O(maxNodeKey) scan over the historical/tombstoned slot range that grows without bound under
   * name churn. {@code liveEntryNodeKeys} is the set of live {@link HashEntryNode} node-keys
   * persisted in the {@link io.sirix.page.NamePage}; each is read together with its paired count
   * node at {@code +1}. Produces an identical dictionary to the scan constructor (asserted by the
   * differential test).
   *
   * @param storageEngineReader the reader
   * @param indexNumber the dictionary offset
   * @param maxNodeKey the high-water mark (retained for subsequent allocations in {@link #setName})
   * @param liveEntryNodeKeys the live entry node-keys (not null)
   */
  private Names(final StorageEngineReader storageEngineReader, final int indexNumber, final long maxNodeKey,
      final Roaring64Bitmap liveEntryNodeKeys) {
    this.indexNumber = indexNumber;
    this.maxNodeKey = maxNodeKey;
    countNodeMap = new Int2LongOpenHashMap();
    nameMap = new Int2ObjectOpenHashMap<>();
    countNameMapping = new Int2IntOpenHashMap();

    final LongIterator it = liveEntryNodeKeys.getLongIterator();
    while (it.hasNext()) {
      final long entryNodeKey = it.next();
      final var nameNode = storageEngineReader.getRecord(entryNodeKey, IndexType.NAME, indexNumber);
      // Defensive: a live-set bit must resolve to a live HashEntryNode; skipping a stale bit keeps
      // reconstruction correct (the differential test asserts the bitmap equals the scan's live set).
      if (nameNode == null || nameNode.getKind() == NodeKind.DELETE) {
        continue;
      }
      final HashEntryNode hashEntryNode = (HashEntryNode) nameNode;
      final int key = hashEntryNode.getKey();
      nameMap.put(key, hashEntryNode.getValue().getBytes(Constants.DEFAULT_ENCODING));

      final long nodeKeyOfCountNode = entryNodeKey + 1;
      final var countNode = storageEngineReader.getRecord(nodeKeyOfCountNode, IndexType.NAME, indexNumber);
      if (countNode == null) {
        throw new IllegalStateException("Node couldn't be fetched from persistent storage: " + nodeKeyOfCountNode);
      }
      final HashCountEntryNode hashKeyToNameCountEntryNode = (HashCountEntryNode) countNode;
      countNameMapping.put(key, hashKeyToNameCountEntryNode.getValue());
      countNodeMap.put(key, nodeKeyOfCountNode);
    }
  }

  /**
   * The live {@link HashEntryNode} node-keys of this dictionary — one per live name, computed as
   * {@code countNodeKey - 1}. O(live). The {@link io.sirix.page.NamePage} persists this so a later
   * revision reconstructs in O(live) rather than scanning {@code 1..maxNodeKey}.
   *
   * @return a fresh bitmap of the live entry node-keys
   */
  public Roaring64Bitmap liveEntryNodeKeys() {
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    final var values = countNodeMap.values().iterator();
    while (values.hasNext()) {
      bitmap.add(values.nextLong() - 1L);
    }
    return bitmap;
  }

  /**
   * Content equality of the live dictionary — the {@code (key -> name)}, {@code (key -> count)} and
   * {@code (key -> countNodeKey)} mappings. Used by the reconstruction differential test to assert
   * the bitmap-driven reconstruction produces an identical dictionary to the {@code 1..maxNodeKey}
   * scan. ({@code byte[]} names are compared by value, not identity.)
   *
   * @param other the dictionary to compare against
   * @return true iff both hold identical live mappings
   */
  public boolean contentEquals(final Names other) {
    if (!countNameMapping.equals(other.countNameMapping) || !countNodeMap.equals(other.countNodeMap)
        || nameMap.size() != other.nameMap.size()) {
      return false;
    }
    for (final var entry : nameMap.int2ObjectEntrySet()) {
      if (!java.util.Arrays.equals(entry.getValue(), other.nameMap.get(entry.getIntKey()))) {
        return false;
      }
    }
    return true;
  }

  public Names setMaxNodeKey(final long maxNodeKey) {
    this.maxNodeKey = maxNodeKey;
    return this;
  }

  /**
   * Remove a name.
   *
   * @param key the key to remove
   */
  public void removeName(final int key, final StorageEngineWriter storageEngineWriter) {
    final int prevValue = countNameMapping.get(key);
    if (prevValue != 0) {
      final long countNodeKey = countNodeMap.get(key);

      if (prevValue - 1 == 0) {
        nameMap.remove(key);
        countNameMapping.remove(key);

        storageEngineWriter.removeRecord(countNodeKey - 1, IndexType.NAME, indexNumber);
        storageEngineWriter.removeRecord(countNodeKey, IndexType.NAME, indexNumber);
      } else {
        countNameMapping.put(key, prevValue - 1);

        final HashCountEntryNode hashCountEntryNode =
            storageEngineWriter.prepareRecordForModification(countNodeKey, IndexType.NAME, indexNumber);
        hashCountEntryNode.decrementValue();
      }
    }
  }

  /**
   * Get bytes representation of a string value in a map.
   *
   * @param name the string representation
   * @return byte representation of a string value in a map
   */
  private static byte[] getBytes(final String name) {
    return name.getBytes(Constants.DEFAULT_ENCODING);
  }

  /**
   * Create name key given a name.
   *
   * @param name name to create key for
   * @return generated key
   */
  public int setName(final String name, final StorageEngineWriter storageEngineWriter) {
    assert name != null;
    assert storageEngineWriter != null;

    final int key = name.hashCode();
    final byte[] previousByteValue = nameMap.get(key);

    final String previousStringValue;
    if (previousByteValue != null) {
      previousStringValue = new String(previousByteValue, Constants.DEFAULT_ENCODING);
    } else {
      previousStringValue = null;
    }

    if (previousStringValue == null || !previousStringValue.equals(name)) {
      final int newKey;

      if (nameMap.containsKey(key)) {
        newKey = getNewKey(key);
      } else {
        newKey = key;
      }

      maxNodeKey++;

      final HashEntryNode hashEntryNode = new HashEntryNode(maxNodeKey, newKey, name);
      final HashCountEntryNode hashCountEntryNode = new HashCountEntryNode(maxNodeKey + 1, 1);

      storageEngineWriter.createRecord(hashEntryNode, IndexType.NAME, indexNumber);
      maxNodeKey++;

      countNodeMap.put(newKey, maxNodeKey);
      storageEngineWriter.createRecord(hashCountEntryNode, IndexType.NAME, indexNumber);

      nameMap.put(newKey, requireNonNull(getBytes(name)));
      countNameMapping.put(newKey, 1);

      return newKey;
    } else {
      final int previousIntegerValue = countNameMapping.get(key);

      countNameMapping.put(key, previousIntegerValue + 1);

      final long nodeKey = countNodeMap.get(key);

      final HashCountEntryNode hashCountEntryNode =
          storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.NAME, indexNumber);
      hashCountEntryNode.incrementValue();

      return key;
    }
  }

  private int getNewKey(final int key) {
    int newKey = key;

    while (nameMap.containsKey(newKey))
      newKey++;

    if (newKey == Integer.MAX_VALUE) {
      newKey = 0;
      while (nameMap.containsKey(newKey) && newKey < key)
        newKey++;
    }

    if (newKey == key)
      throw new IllegalStateException("Key is not unique.");

    return newKey;
  }

  /**
   * Get the name for the key.
   *
   * @param key the key to look up
   * @return the string the key maps to, or {@code null} if no mapping exists
   */
  public String getName(final int key) {
    final byte[] name = nameMap.get(key);
    if (name == null) {
      return null;
    }
    return new String(name, Constants.DEFAULT_ENCODING);
  }

  /**
   * Get the number of nodes with the same name.
   *
   * @param key the key to lookup
   * @return number of nodes with the same name
   */
  public int getCount(final int key) {
    return countNameMapping.get(key);
  }

  /**
   * Get the name for the key.
   *
   * @param key the key to look up
   * @return the byte-array representing the string the key maps to
   */
  public byte[] getRawName(final int key) {
    return nameMap.get(key);
  }

  /**
   * Get a new instance.
   *
   * @return new instance of {@link Names}
   */
  public static Names getInstance(final int indexNumber) {
    return new Names(indexNumber);
  }

  /**
   * Clone an instance.
   *
   * @return cloned index
   */
  public static Names fromStorage(final StorageEngineReader readOnlyPageTrx, final int indexNumber,
      final long maxNodeKey) {
    return new Names(readOnlyPageTrx, indexNumber, maxNodeKey);
  }

  /**
   * Reconstruct from storage using the persisted set of live entry node-keys (O(live)). Falls back
   * to the full {@code 1..maxNodeKey} scan when {@code liveEntryNodeKeys} is null (no persisted set).
   *
   * @param readOnlyPageTrx the reader
   * @param indexNumber the dictionary offset
   * @param maxNodeKey the high-water mark
   * @param liveEntryNodeKeys live entry node-keys persisted in the NamePage, or null to scan
   * @return the reconstructed dictionary
   */
  public static Names fromStorage(final StorageEngineReader readOnlyPageTrx, final int indexNumber,
      final long maxNodeKey, final @Nullable Roaring64Bitmap liveEntryNodeKeys) {
    if (liveEntryNodeKeys == null) {
      return new Names(readOnlyPageTrx, indexNumber, maxNodeKey);
    }
    return new Names(readOnlyPageTrx, indexNumber, maxNodeKey, liveEntryNodeKeys);
  }

  /**
   * Copy method.
   *
   * @param names the names to copy
   * @return new instance with copied fields
   */
  public static Names copy(final Names names) {
    return new Names(names);
  }
}
