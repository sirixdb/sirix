package io.sirix.query.function;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.sirix.api.Database;
import io.sirix.api.ResourceSession;
import io.sirix.query.MutationAuthorization;
import io.sirix.query.compiler.optimizer.PlanCache;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * How {@code jn:store}, {@code jn:load}, {@code xn:store} and {@code xn:load} write one named
 * resource into a database that already exists (#1384).
 *
 * <p>
 * An absent name is simply added. An existing name is refused when {@code $create-new} is false,
 * and otherwise replaced alone: its history is discarded and the caller writes it anew, as a REST
 * PUT does (REST-210). Every other resource of the database is untouched; no store call drops a
 * database implicitly. Replacing discards history, so it needs write and delete permission besides
 * the create permission the functions check up front.
 * </p>
 *
 * <p>
 * The caller must hold {@code MutationGate.acquireResource} for the target across this call and the
 * write that follows, so no other caller can observe or create the name in between.
 * </p>
 */
public final class ExistingResourceTarget {
  private ExistingResourceTarget() {
    throw new AssertionError("No instances");
  }

  /**
   * Prepare {@code resourceName} of the existing {@code database} for a write.
   *
   * @param ctx the query context, for authorization
   * @param resourceExists the error raised when the name exists and {@code createNew} is false
   * @param databaseName the database name the query used
   * @param database the open database
   * @param remove the collection's {@code remove(documentID)}, which also clears what the collection
   *        caches for the resource
   * @param resourceName the resource to write
   * @param createNew whether an existing resource of that name is replaced
   * @throws QueryException with code {@code resourceExists} if the resource exists and
   *         {@code createNew} is false; nothing is changed then
   */
  public static void prepare(final QueryContext ctx, final QNm resourceExists, final String databaseName,
      final Database<?> database, final LongConsumer remove, final String resourceName, final boolean createNew) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(resourceExists, "resourceExists");
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(remove, "remove");
    Objects.requireNonNull(resourceName, "resourceName");
    if (!database.existsResource(resourceName)) {
      return;
    }
    if (!createNew) {
      throw new QueryException(resourceExists,
          "Resource '%s' already exists in database '%s'; store it with $create-new true() to replace it", resourceName,
          databaseName);
    }
    MutationAuthorization.requireWritable(ctx, databaseName, resourceName);
    MutationAuthorization.requireDelete(ctx, databaseName);
    // This handle keeps one session per resource, shared by the items earlier reads returned; close
    // it so the removal is not refused. A session held through another handle still refuses it.
    final long documentID;
    final ResourceSession<?, ?> session = database.beginResourceSession(resourceName);
    try {
      documentID = session.getResourceConfig().getID();
    } finally {
      session.close();
    }
    remove.accept(documentID);
    // The replacement starts a new history: statistics and plans of the old one must not serve it.
    StatisticsCatalog.getInstance().invalidateAll(databaseName, resourceName);
    PlanCache.signalIndexSchemaChange();
  }
}
