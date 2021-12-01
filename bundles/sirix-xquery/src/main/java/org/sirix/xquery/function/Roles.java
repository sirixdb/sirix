package org.sirix.xquery.function;

import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import org.brackit.xquery.QueryContext;
import org.sirix.rest.AuthRole;
import org.sirix.xquery.SirixQueryContext;

public final class Roles {
  private Roles() {
    throw new AssertionError();
  }

  public static void check(final QueryContext ctx, final String databaseName, final AuthRole authRole) {
    if (ctx instanceof SirixQueryContext sirixQueryContext) {
      final var properties = sirixQueryContext.getProperties();
      final var user = (User) properties.get("user");
      final var authz = (AuthorizationProvider) properties.get("authz");

      if (user != null && authz != null) {
        authz.getAuthorizations(user).result();
        if (!RoleBasedAuthorization.create(authRole.databaseRole(databaseName)).match(user)
            && !RoleBasedAuthorization.create(authRole.keycloakRole()).match(user)) {
          throw new IllegalStateException(
              "${HttpResponseStatus.UNAUTHORIZED.code()}: User is not allowed to modify the database");
        }
      }
    }
  }
}
