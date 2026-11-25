package io.sirix.query.function;

import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.sirix.api.Database;
import java.util.Objects;

/**
 * The rule {@code jn:store}, {@code jn:load}, {@code xn:store} and {@code xn:load} apply to a named
 * target in a database that already exists (#1384).
 *
 * <p>
 * A free name is added beside the resources already there. A name that is taken is refused, and
 * nothing is written: these functions never replace a resource in place and never drop a database.
 * Writing into an existing resource is {@code jn:import}/{@code xn:import}; replacing one is a
 * separate atomic operation that does not exist yet.
 * </p>
 *
 * <p>
 * The caller must hold {@code MutationGate.acquireResource} for the target across this call and the
 * write that follows, so no other caller can create the name in between.
 * </p>
 */
public final class FreeResourceName {
  private FreeResourceName() {
    throw new AssertionError("No instances");
  }

  /**
   * Require that {@code resourceName} is not taken in {@code database}.
   *
   * @param resourceExists the error raised when the name is already taken
   * @param databaseName the database name the query used
   * @param database the open database
   * @param resourceName the resource the caller is about to create
   * @throws QueryException with code {@code resourceExists} if the resource exists; nothing is
   *         changed then
   */
  public static void require(final QNm resourceExists, final String databaseName, final Database<?> database,
      final String resourceName) {
    Objects.requireNonNull(resourceExists, "resourceExists");
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(resourceName, "resourceName");
    if (database.existsResource(resourceName)) {
      throw new QueryException(resourceExists,
          "Resource '%s' already exists in database '%s'; drop it first, or use jn:import/xn:import to write into it",
          resourceName, databaseName);
    }
  }
}
