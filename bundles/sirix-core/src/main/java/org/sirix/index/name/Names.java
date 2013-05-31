package org.sirix.index.name;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.sirix.settings.Constants;

import com.google.common.collect.HashBiMap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

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
	 * @param in
	 *          the persistent storage
	 */
	private Names(final ByteArrayDataInput in) {
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
	 * @param out
	 *          the persistent storage
	 */
	public void serialize(final ByteArrayDataOutput out) {
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
	 * @param key
	 *          the key to remove
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
	 * @param name
	 *          the string representation
	 * @return byte representation of a string value in a map
	 */
	private byte[] getBytes(final String name) {
		return name.getBytes(Constants.DEFAULT_ENCODING);
	}

	/**
	 * Create name key given a name.
	 * 
	 * @param pKey
	 *          key for given name
	 * @param name
	 *          name to create key for
	 */
	public void setName(final int pKey, final String name) {
		final Integer prevValue = mCountNameMapping.get(pKey);
		if (prevValue == null) {
			mNameMap.put(pKey, checkNotNull(getBytes(name)));
			mCountNameMapping.put(pKey, 1);
		} else {
			mCountNameMapping.put(pKey, prevValue + 1);
		}
	}

	/**
	 * Get the name for the key.
	 * 
	 * @param key
	 *          the key to look up
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
	 * @param key
	 *          the key to lookup
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
	 * @param key
	 *          the key to look up
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
	 * @param in
	 *          input source, the persistent storage
	 * @return cloned index
	 */
	public static Names clone(final ByteArrayDataInput in) {
		return new Names(in);
	}
}
