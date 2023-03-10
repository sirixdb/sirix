package org.sirix.index.name;

import static java.util.Objects.requireNonNull;

import it.unimi.dsi.fastutil.ints.*;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexType;
import org.sirix.node.HashCountEntryNode;
import org.sirix.node.HashEntryNode;
import org.sirix.node.NodeKind;
import org.sirix.settings.Constants;

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
   * @param pageReadTrx the page reading transaction
   * @param indexNumber the kind of name dictionary
   * @param maxNodeKey  the maximum node key
   */
  private Names(final PageReadOnlyTrx pageReadTrx, final int indexNumber, final long maxNodeKey) {
    this.indexNumber = indexNumber;
    this.maxNodeKey = maxNodeKey;
    // It's okay, we don't allow to store more than Integer.MAX key value pairs.
    int size = (int) Math.ceil(maxNodeKey / 0.75);
    countNodeMap = new Int2LongOpenHashMap(size);
    nameMap = new Int2ObjectOpenHashMap<>(size);
    countNameMapping = new Int2IntOpenHashMap(size);

    // TODO: Next refactoring iteration: Move this to a factory, just assign stuff in constructors
    for (long i = 1; i < maxNodeKey; i += 2) {
      final var nameNode = pageReadTrx.getRecord(i, IndexType.NAME, indexNumber);

      if (nameNode != null && nameNode.getKind() != NodeKind.DELETE) {
        final HashEntryNode hashEntryNode = (HashEntryNode) nameNode;

        final int key = hashEntryNode.getKey();

        nameMap.put(key, hashEntryNode.getValue().getBytes(Constants.DEFAULT_ENCODING));

        final long nodeKeyOfCountNode = i + 1;
        final var countNode = pageReadTrx.getRecord(nodeKeyOfCountNode, IndexType.NAME, indexNumber);
        if (countNode == null) {
          throw new IllegalStateException("Node couldn't be fetched from persistent storage: " + nodeKeyOfCountNode);
        }

        final HashCountEntryNode hashKeyToNameCountEntryNode = (HashCountEntryNode) countNode;
        countNameMapping.put(key, hashKeyToNameCountEntryNode.getValue());
        countNodeMap.put(key, nodeKeyOfCountNode);
      }
    }
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
  public void removeName(final int key, final PageTrx pageTrx) {
    final int prevValue = countNameMapping.get(key);
    if (prevValue != 0) {
      final long countNodeKey = countNodeMap.get(key);

      if (prevValue - 1 == 0) {
        nameMap.remove(key);
        countNameMapping.remove(key);

        pageTrx.removeRecord(countNodeKey - 1, IndexType.NAME, indexNumber);
        pageTrx.removeRecord(countNodeKey, IndexType.NAME, indexNumber);
      } else {
        countNameMapping.put(key, prevValue - 1);

        final HashCountEntryNode hashCountEntryNode =
            pageTrx.prepareRecordForModification(countNodeKey, IndexType.NAME, indexNumber);
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
  public int setName(final String name, final PageTrx pageTrx) {
    assert name != null;
    assert pageTrx != null;

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

      pageTrx.createRecord(hashEntryNode, IndexType.NAME, indexNumber);
      maxNodeKey++;

      countNodeMap.put(newKey, maxNodeKey);
      pageTrx.createRecord(hashCountEntryNode, IndexType.NAME, indexNumber);

      nameMap.put(newKey, requireNonNull(getBytes(name)));
      countNameMapping.put(newKey, 1);

      return newKey;
    } else {
      final int previousIntegerValue = countNameMapping.get(key);

      countNameMapping.put(key, previousIntegerValue + 1);

      final long nodeKey = countNodeMap.get(key);

      final HashCountEntryNode hashCountEntryNode =
          pageTrx.prepareRecordForModification(nodeKey, IndexType.NAME, indexNumber);
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
  public static Names fromStorage(final PageReadOnlyTrx readOnlyPageTrx, final int indexNumber, final long maxNodeKey) {
    return new Names(readOnlyPageTrx, indexNumber, maxNodeKey);
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
