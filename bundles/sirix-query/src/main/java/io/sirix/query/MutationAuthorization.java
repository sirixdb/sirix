package io.sirix.query;

import io.brackit.query.QueryContext;
import io.brackit.query.jdm.Item;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.node.XmlDBNode;
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
   * Authorize a mutation of the resource behind {@code document}, or do nothing when the context's
   * store does not authorize mutations (an embedded caller).
   *
   * <p>
   * Call this before acquiring, reverting or committing a write transaction, so a refused mutation
   * leaves no trace: an opened transaction, a reverted cursor and a published revision are all
   * observable effects.
   * </p>
   *
   * @param ctx the query context the function was handed
   * @param document the item the mutation targets
   * @throws RuntimeException if the caller may not mutate the resource
   */
  public static void requireWritable(final QueryContext ctx, final Item document) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(document, "document");
    final MutationAuthorizer authorizer = authorizerOf(ctx);
    if (authorizer == null) {
      return;
    }
    if (document instanceof JsonDBItem jsonItem) {
      authorizer.authorizeWrite(jsonItem.getCollection().getName(),
          jsonItem.getResourceSession().getResourceConfig().getName());
    } else if (document instanceof XmlDBNode xmlNode) {
      authorizer.authorizeWrite(xmlNode.getCollection().getName(),
          xmlNode.getTrx().getResourceSession().getResourceConfig().getName());
    } else {
      throw new IllegalArgumentException("Cannot authorize an item without a collection name");
    }
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
