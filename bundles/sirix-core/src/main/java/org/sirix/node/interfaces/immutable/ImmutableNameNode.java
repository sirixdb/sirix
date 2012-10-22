package org.sirix.node.interfaces.immutable;

/**
 * Immutable node with a name (element, attribute, namespace, PI...).
 * 
 * @author Johannes Lichtenberger
 *
 */
public interface ImmutableNameNode extends ImmutableNode {
	/**
	 * Gets key of qualified name.
	 * 
	 * @return key of qualified name
	 */
	int getNameKey();

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
}
