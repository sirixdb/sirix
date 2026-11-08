package io.sirix.query;

import io.brackit.query.QueryContext;
import io.brackit.query.jdm.Item;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.node.XmlDBNode;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Resolves and propagates the caller's {@link MutationAuthorizer} to authorize a mutation before it
 * takes effect.
 *
 * <p>
 * Document mutation guards look up the authorizer on the context's store rather than on the item
 * being mutated: a store decorator carrying the caller's identity reaches a query through the
 * context, while the collection and item a decorator hands out delegate {@code getJsonDBStore()}
 * straight to the undecorated store, so an item cannot name the caller.
 * </p>
 *
 * <p>
 * REST store and collection decorators bind the same authorizer with {@link #where}, so shared
 * lifecycle code can check the actual creation, replacement or write target without a query
 * context.
 * </p>
 *
 * <p>
 * Called at statement-level mutation boundaries rather than per record.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class MutationAuthorization {
  private static final ScopedValue<MutationAuthorizer> AUTHORIZER = ScopedValue.newInstance();

  public static ScopedValue.Carrier where(final MutationAuthorizer authorizer) {
    return ScopedValue.where(AUTHORIZER, Objects.requireNonNull(authorizer));
  }

  public static void requireDelete(final String databaseName) {
    if (AUTHORIZER.isBound()) {
      AUTHORIZER.get().authorizeDelete(databaseName);
    }
  }

  public static void requireStore(final String databaseName, final String resourceName, final boolean exists) {
    if (AUTHORIZER.isBound()) {
      if (exists) {
        AUTHORIZER.get().authorizeWrite(databaseName, resourceName);
      } else {
        AUTHORIZER.get().authorizeCreate(databaseName, resourceName);
      }
    }
  }

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
      requireWritable(ctx, jsonItem.getCollection().getName(),
          jsonItem.getResourceSession().getResourceConfig().getName());
    } else if (document instanceof XmlDBNode xmlNode) {
      requireWritable(ctx, xmlNode.getCollection().getName(),
          xmlNode.getTrx().getResourceSession().getResourceConfig().getName());
    } else {
      throw new IllegalArgumentException("Cannot authorize an item without a collection name");
    }
  }

  public static void requireWritable(final QueryContext ctx, final String databaseName,
      final String resourceName) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(databaseName, "databaseName");
    Objects.requireNonNull(resourceName, "resourceName");
    final MutationAuthorizer authorizer = authorizerOf(ctx);
    if (authorizer != null) {
      authorizer.authorizeWrite(databaseName, resourceName);
    }
  }

  public static void requireCreate(final QueryContext ctx, final String databaseName,
      final @Nullable String resourceName) {
    Objects.requireNonNull(ctx, "ctx");
    Objects.requireNonNull(databaseName, "databaseName");
    final MutationAuthorizer authorizer = authorizerOf(ctx);
    if (authorizer != null) {
      authorizer.authorizeCreate(databaseName, resourceName);
    }
  }

  /**
   * If both stores implement MutationAuthorizer, they must carry the same caller identity.
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
