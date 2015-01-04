package org.sirix.node.interfaces.immutable;

import org.brackit.xquery.atomic.QNm;

/**
 * Immutable node with a name (element, attribute, namespace, PI...).
 * 
 * @author Johannes Lichtenberger
 *
 */
public interface ImmutableNameNode extends ImmutableNode {
	/**
	 * Gets local name key of qualified name.
	 * 
	 * @return local name key of qualified name
	 */
	int getLocalNameKey();

	/**
	 * Gets prefix key of qualified name.
	 * 
	 * @return prefix key of qualified name
	 */
	int getPrefixKey();

	/**
	 * Gets key of the URI.
	 * 
	 * @return URI key
	 */
	int getURIKey();

	/**
	 * Get a path node key.
	 * 
	 * @return path node key
	 */
	long getPathNodeKey();

	/**
	 * Get the {@link QNm} associated with the node.
	 * 
	 * @return the {@link QNm} of the node
	 */
	QNm getName();
}
