package io.sirix.rest.crud.json;

import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBCollection;
import kotlin.coroutines.Continuation;

final class JsonGetTestBinding {
  private JsonGetTestBinding() {}

  static Object collection(final JsonGet handler, final String name, final Database<JsonResourceSession> database,
      final Continuation<? super JsonDBCollection> continuation) {
    return handler.getDBCollection(name, database, continuation);
  }

  static void bind(final JsonGet handler, final JsonNodeReadOnlyTrx reader, final JsonDBCollection collection,
      final SirixQueryContext context, final JsonSessionDBStore store) {
    handler.handleQueryExtra(reader, collection, context, store);
  }
}
