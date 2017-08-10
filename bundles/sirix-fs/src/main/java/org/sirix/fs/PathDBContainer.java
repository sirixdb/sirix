package org.sirix.fs;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.file.Path;

import javax.annotation.Nonnull;

import org.sirix.api.Database;
import org.sirix.api.ResourceManager;

import com.google.common.base.Objects;

/**
 * Container for {@code {@link Path}/{@link Database} combinations. Note that
 * it may be refined to {@code
 * 
 * @link IDatabase}/{@link ResourceManager}/{@link IResource} later on.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Nonnull
public final class PathDBContainer {

	/** {@link Path} to watch for modifications. */
	final Path mPath;

	/** sirix {@link Database}. */
	final Database mDatabase;

	/**
	 * Constructor.
	 * 
	 * @param pPath
	 *          {@link Path} reference
	 * @param pDatabase
	 *          sirix {@link Database} reference
	 */
	public PathDBContainer(final Path pPath, final Database pDatabase) {
		mPath = checkNotNull(pPath);
		mDatabase = checkNotNull(pDatabase);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mPath, mDatabase);
	}

	@Override
	public boolean equals(final Object pOther) {
		if (pOther == this) {
			return true;
		}
		if (!(pOther instanceof PathDBContainer)) {
			return false;
		}
		final PathDBContainer container = (PathDBContainer) pOther;
		return mPath.equals(container.mPath)
				&& mDatabase.equals(container.mDatabase);
	}
}
