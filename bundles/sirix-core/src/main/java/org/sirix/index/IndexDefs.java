package org.sirix.index;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Type;

import com.google.common.base.Optional;

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
	 * @param unique
	 *          determine if it's unique
	 * @param type
	 *          an optional type
	 * @param paths
	 *          the paths to index
	 * @return a new {@link IndexDef} instance
	 */
	public static IndexDef createCASIdxDef(final boolean unique,
			final @Nonnull Optional<Type> optType, final @Nonnull Set<Path<QNm>> paths) {
		final Type type = optType.isPresent() ? optType.get() : Type.STR;
		return new IndexDef(type, paths, unique);
	}

	/**
	 * Create a path {@link IndexDef}.
	 * @param paths
	 * 					the paths to index
	 * @return a new path {@link IndexDef} instance
	 */
	public static IndexDef createPathIdxDef(Set<Path<QNm>> paths) {
		return new IndexDef(paths);
	}

	public static IndexDef createNameIdxDef() {
		final Set<QNm> excluded = new HashSet<>();
		final Set<QNm> included = new HashSet<>();
		return new IndexDef(included, excluded);
	}

	public static IndexDef createFilteredNameIdxDef(
			final @Nonnull Set<QNm> excluded) {
		final Set<QNm> included = new HashSet<QNm>();
		return new IndexDef(included, excluded);
	}

	public static IndexDef createSelectiveNameIdxDef(
			final @Nonnull Set<QNm> included) {
		final HashSet<QNm> excluded = new HashSet<QNm>();
		return new IndexDef(included, excluded);
	}

}
