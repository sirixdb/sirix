package org.sirix.fs;

import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Path;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;

/**
 * Container for {@code {@link Path}/{@link Database} combinations. Note that it may be refined to
 * {@code
 *
 * @link IDatabase}/{@link ResourceManager}/{@link IResource} later on.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
@NonNull
public final class PathDBContainer {
  /** Save the hashcode (lazy initialized). */
  private int mHashCode;

  /** {@link Path} to watch for modifications. */
  private final Path mPath;

  /** sirix {@link Database}. */
  private final Database mDatabase;

  /**
   * Constructor.
   *
   * @param pPath {@link Path} reference
   * @param pDatabase sirix {@link Database} reference
   */
  public PathDBContainer(final Path pPath, final Database pDatabase) {
    mPath = checkNotNull(pPath);
    mDatabase = checkNotNull(pDatabase);
  }

  @Override
  public int hashCode() {
    int result = mHashCode;
    if (result == 0) {
      result = mPath.hashCode();
      result = 31 * result + mDatabase.hashCode(); // 31 * i == (i << 5) - i optimized by modern VMs
      mHashCode = result;
    }

    return mHashCode;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof PathDBContainer)) {
      return false;
    }

    final PathDBContainer container = (PathDBContainer) other;
    return mPath.equals(container.mPath) && mDatabase.equals(container.mDatabase);
  }
}
