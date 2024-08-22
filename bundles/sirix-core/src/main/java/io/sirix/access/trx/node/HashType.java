package io.sirix.access.trx.node;

/**
 * How is the Hash for this storage computed?
 */
public enum HashType {
	/** Rolling hash, only nodes on ancestor axis are touched. */
	ROLLING,
	/**
	 * Postorder hash, all nodes on ancestor plus postorder are at least read.
	 */
	POSTORDER,
	/** No hash structure after all. */
	NONE;

	public static HashType fromString(String string) {
    for (final HashType hashType : values()) {
      if (hashType.name().equalsIgnoreCase(string)) {
        return hashType;
      }
    }
    throw new IllegalArgumentException(STR."No constant with name \{string} found");
  }
}
