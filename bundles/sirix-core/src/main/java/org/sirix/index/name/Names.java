package org.sirix.index.name;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.node.HashCountEntryNode;
import org.sirix.node.HashEntryNode;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageKind;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Constants;
import com.google.common.collect.HashBiMap;

/**
 * Names index structure.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class Names {

  /** Map the hash of a name the node key. */
  private final Map<Integer, Long> mCountNodeMap;

  /** Map the hash of a name to its name. */
  private final Map<Integer, byte[]> mNameMap;

  /** Map which is used to count the occurences of a name mapping. */
  private final Map<Integer, Integer> mCountNameMapping;

  private long mMaxNodeKey;

  private int mIndexNumber;

  /**
   * Constructor creating a new index structure.
   */
  private Names(final int indexNumber) {
    mIndexNumber = indexNumber;
    mCountNodeMap = new HashMap<>();
    mNameMap = new HashMap<>();
    mCountNameMapping = new HashMap<>();
  }

  /**
   * Constructor to build index from a persistent storage.
   *
   * @param pageReadTrx the page reading transaction
   * @param indexNumber the kind of name dictionary
   * @param maxNodeKey the maximum node key
   */
  private Names(final PageReadOnlyTrx pageReadTrx, final int indexNumber, final long maxNodeKey) {
    mIndexNumber = indexNumber;
    mMaxNodeKey = maxNodeKey;
    // It's okay, we don't allow to store more than Integer.MAX key value pairs.
    mCountNodeMap = new HashMap<>((int) maxNodeKey + 1);
    mNameMap = HashBiMap.create((int) maxNodeKey + 1);
    mCountNameMapping = new HashMap<>((int) maxNodeKey + 1);

    // TODO: Next refactoring iteration: Move this to a factory, just assign stuff in constructors
    for (long i = 1, l = maxNodeKey; i < l; i += 2) {
      final long nodeKeyOfNode = i;
      final Optional<? extends DataRecord> nameNode = pageReadTrx.getRecord(nodeKeyOfNode, PageKind.NAMEPAGE, indexNumber);

      if (nameNode.isPresent() && nameNode.get().getKind() != NodeKind.DELETE) {
        final HashEntryNode hashEntryNode = (HashEntryNode) nameNode.orElseThrow(
            () -> new IllegalStateException("Node couldn't be fetched from persistent storage: " + nodeKeyOfNode));

        final int key = hashEntryNode.getKey();

        mNameMap.put(key, hashEntryNode.getValue().getBytes(Constants.DEFAULT_ENCODING));

        final long nodeKeyOfCountNode = i + 1;

        final Optional<? extends DataRecord> countNode =
            pageReadTrx.getRecord(nodeKeyOfCountNode, PageKind.NAMEPAGE, indexNumber);

        final HashCountEntryNode hashKeyToNameCountEntryNode =
            (HashCountEntryNode) countNode.orElseThrow(() -> new IllegalStateException(
                "Node couldn't be fetched from persistent storage: " + nodeKeyOfCountNode));

        mCountNameMapping.put(key, hashKeyToNameCountEntryNode.getValue());
        mCountNodeMap.put(key, nodeKeyOfCountNode);
      }
    }
  }

  /**
   * Remove a name.
   *
   * @param key the key to remove
   */
  public void removeName(final int key, final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageTrx) {
    final Integer prevValue = mCountNameMapping.get(key);
    if (prevValue != null) {
      final long countNodeKey = mCountNodeMap.get(key);

      if (prevValue - 1 == 0) {
        mNameMap.remove(key);
        mCountNameMapping.remove(key);

        pageTrx.removeEntry(countNodeKey - 1, PageKind.NAMEPAGE, mIndexNumber);
        pageTrx.removeEntry(countNodeKey, PageKind.NAMEPAGE, mIndexNumber);
      } else {
        mCountNameMapping.put(key, prevValue - 1);

        final HashCountEntryNode hashCountEntryNode =
            (HashCountEntryNode) pageTrx.prepareEntryForModification(countNodeKey, PageKind.NAMEPAGE, mIndexNumber);
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
   *
   * @return generated key
   */
  public int setName(final String name, final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageTrx) {
    assert name != null;
    assert pageTrx != null;

    final int key = name.hashCode();
    final byte[] previousByteValue = mNameMap.get(key);

    final String previousStringValue;
    if (previousByteValue != null) {
      previousStringValue = new String(previousByteValue, Constants.DEFAULT_ENCODING);
    } else {
      previousStringValue = null;
    }

    if (previousStringValue == null || !previousStringValue.equals(name)) {
      final int newKey;

      if (mNameMap.containsKey(key)) {
        newKey = getNewKey(key);
      } else {
        newKey = key;
      }

      mMaxNodeKey++;

      final HashEntryNode hashEntryNode = new HashEntryNode(mMaxNodeKey, newKey, name);
      final HashCountEntryNode hashCountEntryNode = new HashCountEntryNode(mMaxNodeKey + 1, 1);

      pageTrx.createEntry(mMaxNodeKey++, hashEntryNode, PageKind.NAMEPAGE, mIndexNumber);

      mCountNodeMap.put(newKey, mMaxNodeKey);
      pageTrx.createEntry(mMaxNodeKey, hashCountEntryNode, PageKind.NAMEPAGE, mIndexNumber);

      mNameMap.put(newKey, checkNotNull(getBytes(name)));
      mCountNameMapping.put(newKey, 1);

      return newKey;
    } else {
      final int previousIntegerValue = mCountNameMapping.get(key);

      mCountNameMapping.put(key, previousIntegerValue + 1);

      final long nodeKey = mCountNodeMap.get(key);

      final HashCountEntryNode hashCountEntryNode =
          (HashCountEntryNode) pageTrx.prepareEntryForModification(nodeKey, PageKind.NAMEPAGE, mIndexNumber);
      hashCountEntryNode.incrementValue();

      return key;
    }
  }

  private int getNewKey(final int key) {
    int newKey = key;

    while (mNameMap.containsKey(newKey) && newKey <= Integer.MAX_VALUE)
      newKey++;

    if (newKey == Integer.MAX_VALUE) {
      newKey = 0;
      while (mNameMap.containsKey(newKey) && newKey < key)
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
    final byte[] name = mNameMap.get(key);
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
    Integer names = mCountNameMapping.get(key);
    if (names == null) {
      names = 0;
    }
    return names;
  }

  /**
   * Get the name for the key.
   *
   * @param key the key to look up
   * @return the byte-array representing the string the key maps to
   */
  public byte[] getRawName(final int key) {
    return mNameMap.get(key);
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
  public static Names clone(final PageReadOnlyTrx readOnlyPageTrx, final int indexNumber, final long maxNodeKey) {
    return new Names(readOnlyPageTrx, indexNumber, maxNodeKey);
  }
}
