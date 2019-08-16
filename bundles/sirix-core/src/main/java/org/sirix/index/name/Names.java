package org.sirix.index.name;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.sirix.settings.Constants;
import com.google.common.collect.HashBiMap;

/**
 * Names index structure.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class Names {

  /** Map the hash of a name to its name. */
  private final Map<Integer, byte[]> mNameMap;

  /** Map which is used to count the occurences of a name mapping. */
  private final Map<Integer, Integer> mCountNameMapping;

  /**
   * Constructor creating a new index structure.
   */
  private Names() {
    mNameMap = new HashMap<>();
    mCountNameMapping = new HashMap<>();
  }

  /**
   * Constructor to build index of from a persistent storage.
   *
   * @param in the persistent storage
   */
  private Names(final DataInput in) throws IOException {
    final int mapSize = in.readInt();
    mNameMap = HashBiMap.create(mapSize);
    mCountNameMapping = new HashMap<>(mapSize);
    for (int i = 0, l = mapSize; i < l; i++) {
      final int key = in.readInt();
      final int valSize = in.readInt();
      final byte[] bytes = new byte[valSize];
      for (int j = 0; j < bytes.length; j++) {
        bytes[j] = in.readByte();
      }
      mNameMap.put(key, bytes);
      mCountNameMapping.put(key, in.readInt());
    }
  }

  /**
   * Serialize name-index.
   *
   * @param out the persistent storage
   */
  public void serialize(final DataOutput out) throws IOException {
    out.writeInt(mNameMap.size());
    for (final Entry<Integer, byte[]> entry : mNameMap.entrySet()) {
      out.writeInt(entry.getKey());
      final byte[] bytes = entry.getValue();
      out.writeInt(bytes.length);
      for (final byte byteVal : bytes) {
        out.writeByte(byteVal);
      }
      out.writeInt(mCountNameMapping.get(entry.getKey()).intValue());
    }
  }

  /**
   * Remove a name.
   *
   * @param key the key to remove
   */
  public void removeName(final int key) {
    final Integer prevValue = mCountNameMapping.get(key);
    if (prevValue != null) {
      if (prevValue - 1 == 0) {
        mNameMap.remove(key);
        mCountNameMapping.remove(key);
      } else {
        mCountNameMapping.put(key, prevValue - 1);
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
   * @param key key for given name
   * @param name name to create key for
   *
   * @return generated key
   */
  public int setName(final String name) {
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

      mNameMap.put(newKey, checkNotNull(getBytes(name)));
      mCountNameMapping.put(newKey, 1);

      return newKey;
    } else {
      final int previousIntegerValue = mCountNameMapping.get(key);
      mCountNameMapping.put(key, previousIntegerValue + 1);

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
  public static Names getInstance() {
    return new Names();
  }

  /**
   * Clone an instance.
   *
   * @param in input source, the persistent storage
   * @return cloned index
   */
  public static Names clone(final DataInput in) throws IOException {
    return new Names(in);
  }
}
