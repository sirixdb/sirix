package org.sirix.index.art;

class InnerNodeUtils {

	static byte[] getValidPrefixKey(InnerNode innerNode) {
		int limit = Math.min(InnerNode.PESSIMISTIC_PATH_COMPRESSION_LIMIT, innerNode.prefixLen);
		byte[] valid = new byte[limit];
		System.arraycopy(innerNode.prefixKeys, 0, valid, 0, limit);
		return valid;
	}
}
