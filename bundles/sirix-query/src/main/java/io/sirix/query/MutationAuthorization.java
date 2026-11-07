package io.sirix.query;

import io.brackit.query.QueryContext;
import io.sirix.api.ResourceSession;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Resolves the {@link MutationAuthorizer} a query context was built with, and asks it to authorize
 * a mutation before that mutation takes effect.
 *
 * <p>
 * The authorizer is looked up on the context's store rather than on the item being mutated: a store
 * decorator carrying the caller's identity reaches a query through the context, while the
 * collection and item a decorator hands out delegate {@code getJsonDBStore()} straight to the
 * undecorated store, so an item cannot name the caller.
 * </p>
 *
 * <p>
 * Not a hot path: every caller is index DDL or an explicit commit/rollback, once per statement.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class MutationAuthorization {

  private MutationAuthorization() {
    throw new AssertionError("No instances");
  }

  /**
   * Authorize a mutation of the resource behind {@code session}, or do nothing when the context's
   * store does not authorize mutations (an embedded caller).
   *
   * <p>
   * Call this before acquiring, reverting or committing a write transaction, so a refused mutation
   * leaves no trace: an opened transaction, a reverted cursor and a published revision are all
   * observable effects.
   * </p>
   *
   * @param ctx the query context the function was handed
   * @param session the resource session the mutation targets
   * @throws RuntimeException if the caller may not mutate the resource
   */
  public static void requireWritable(final QueryContext ctx, final ResourceSession<?, ?> session) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(session, "session");
    final MutationAuthorizer authorizer = authorizerOf(ctx);
    if (authorizer == null) {
      return;
    }
    final Path resourcePath = session.getResourcePath();
    // <location>/<database>/resources/<resource>: LocalDatabase asserts this layout, so the
    // database directory is the grandparent and its name is what REST roles are scoped to.
    final Path resources = resourcePath.getParent();
    final Path databaseDirectory = resources == null
        ? null
        : resources.getParent();
    if (databaseDirectory == null) {
      throw new IllegalStateException("Cannot derive the database name from resource path " + resourcePath);
    }
    authorizer.authorizeWrite(databaseDirectory.getFileName().toString(), resourcePath.getFileName().toString());
  }

  /**
   * The JSON store is consulted first and the node store second; the REST API decorates both with the
   * same identity, and a query context carries only the store its items came from.
   */
  private static MutationAuthorizer authorizerOf(final QueryContext ctx) {
    if (ctx.getJsonItemStore() instanceof MutationAuthorizer jsonAuthorizer) {
      return jsonAuthorizer;
    }
    if (ctx.getNodeStore() instanceof MutationAuthorizer nodeAuthorizer) {
      return nodeAuthorizer;
    }
    return null;
  }
}
