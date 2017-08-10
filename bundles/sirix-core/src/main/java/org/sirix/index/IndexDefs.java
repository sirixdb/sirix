package org.sirix.index;

import java.util.Set;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Type;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * {@link IndexDef} factory.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class IndexDefs {

	/**
	 * Private constructor.
	 */
	private IndexDefs() {
		throw new AssertionError("May never be instantiated!");
	}

	/**
	 * Create a CAS {@link IndexDef} instance.
	 * 
	 * @param unique determine if it's unique
	 * @param type an optional type
	 * @param paths the paths to index
	 * @return a new {@link IndexDef} instance
	 */
	public static IndexDef createCASIdxDef(final boolean unique, final Optional<Type> optType,
			final Set<Path<QNm>> paths, final int indexDefNo) {
		final Type type = optType.isPresent() ? optType.get() : Type.STR;
		return new IndexDef(type, paths, unique, indexDefNo);
	}

	/**
	 * Create a path {@link IndexDef}.
	 * 
	 * @param paths the paths to index
	 * @return a new path {@link IndexDef} instance
	 */
	public static IndexDef createPathIdxDef(final Set<Path<QNm>> paths, final int indexDefNo) {
		return new IndexDef(paths, indexDefNo);
	}

	public static IndexDef createNameIdxDef(final int indexDefNo) {
		return new IndexDef(ImmutableSet.<QNm>of(), ImmutableSet.<QNm>of(), indexDefNo);
	}

	public static IndexDef createFilteredNameIdxDef(final Set<QNm> excluded, final int indexDefNo) {
		return new IndexDef(ImmutableSet.<QNm>of(), excluded, indexDefNo);
	}

	public static IndexDef createSelectiveNameIdxDef(final Set<QNm> included, final int indexDefNo) {
		return new IndexDef(included, ImmutableSet.<QNm>of(), indexDefNo);
	}

	public static IndexDef createSelectiveFilteredNameIdxDef(final Set<QNm> included,
			final Set<QNm> excluded, final int indexDefNo) {
		return new IndexDef(included, excluded, indexDefNo);
	}
}
