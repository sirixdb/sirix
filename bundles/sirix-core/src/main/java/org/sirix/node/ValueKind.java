package org.sirix.node;

import javax.annotation.Nonnull;

import org.sirix.exception.SirixIOException;
import org.sirix.index.value.AVLTree;

/**
 * Determines the kind of value.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public enum ValueKind {
	/** Text value. */
	TEXT {
		@Override
		public Kind getKind() {
			return Kind.TEXT_VALUE;
		}

		@Override
		public <K extends Comparable<? super K>, V> void index(
				final @Nonnull AVLTree<K, V> tree, final @Nonnull K key,
				final @Nonnull V value) throws SirixIOException {
			tree.index(key, value);
		}
	},

	/** Attribute value. */
	ATTRIBUTE {
		@Override
		public Kind getKind() {
			return Kind.ATTRIBUTE_VALUE;
		}

		@Override
		public <K extends Comparable<? super K>, V> void index(
				final @Nonnull AVLTree<K, V> tree, final @Nonnull K key,
				final @Nonnull V value) throws SirixIOException {
			tree.index(key, value);
		}
	};

	/**
	 * Get the kind of value.
	 * 
	 * @return the kind of value
	 */
	public abstract Kind getKind();

	/**
	 * Index a value.
	 * 
	 * @param tree
	 *          the tree
	 * @param key
	 *          the key
	 * @param value
	 *          the value
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public abstract <K extends Comparable<? super K>, V> void index(
			final @Nonnull AVLTree<K, V> tree, final @Nonnull K key,
			final @Nonnull V value) throws SirixIOException;
}
