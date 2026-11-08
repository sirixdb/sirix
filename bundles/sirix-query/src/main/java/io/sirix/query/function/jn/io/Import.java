package io.sirix.query.function.jn.io;

import com.google.gson.stream.JsonReader;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.Databases;
import io.sirix.access.MutationGate;
import io.sirix.diff.algorithm.fmse.json.JsonFMSE;
import io.sirix.diff.service.JsonFMSEImport;
import io.sirix.query.MutationAuthorization;
import io.sirix.query.function.ImportInput;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.SirixFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Imports inline JSON as one FMSE revision, preserving the identity of unchanged nodes. */
public final class Import extends AbstractFunction {
  public static final QNm IMPORT = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "import");

  public Import(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final String databaseName = ((Str) args[0]).stringValue();
    final String resourceName = ((Str) args[1]).stringValue();
    final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();
    final JsonDBCollection initial = store.lookup(databaseName);
    if (initial == null) {
      throw new QueryException(new QNm("Database does not exist"));
    }
    final Path databasePath = initial.getDatabase().getDatabaseConfig().getDatabaseFile();
    try (var gate = MutationGate.acquireResource(databasePath, resourceName)) {
      // Resolve again under the hold: a concurrent replacement must not change the authorized target.
      final JsonDBCollection collection = store.lookup(databaseName);
      if (collection == null) {
        throw new QueryException(new QNm("Database does not exist"));
      }
      if (!collection.getDatabase().existsResource(resourceName)) {
        throw new QueryException(new QNm("ResourceNotFound"), "Resource '%s' does not exist", resourceName);
      }
      final JsonDBItem document = collection.getDocument(resourceName);
      try {
        MutationAuthorization.requireWritable(ctx, document);
        final String content = ImportInput.json(args[2]);
        final Path temporary = Files.createTempDirectory("sirix-json-import-");
        try {
          try (final JsonReader reader = JsonShredder.createStringReader(content)) {
            new JsonFMSEImport().shredder(reader, temporary);
          }
          try (final var database = Databases.openJsonDatabase(temporary);
              final var session = database.beginResourceSession("shredded");
              final var reader = session.beginNodeReadOnlyTrx();
              final var writer = document.getResourceSession().beginNodeTrx();
              final var diff = JsonFMSE.createInstance()) {
            diff.diff(writer, reader);
          }
        } finally {
          SirixFiles.recursiveRemove(temporary);
        }
      } finally {
        document.getTrx().close();
      }
      return collection.getDocument(resourceName);
    } catch (final IOException failure) {
      throw new QueryException(failure, new QNm("JSON import failed"));
    }
  }
}
