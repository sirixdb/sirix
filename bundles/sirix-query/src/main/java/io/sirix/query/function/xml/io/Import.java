package io.sirix.query.function.xml.io;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.node.parser.DocumentParser;
import io.sirix.access.Databases;
import io.sirix.access.MutationGate;
import io.sirix.diff.algorithm.fmse.DefaultNodeComparisonFactory;
import io.sirix.diff.algorithm.fmse.FMSE;
import io.sirix.diff.service.FMSEImport;
import io.sirix.query.ContentOnlyStore;
import io.sirix.query.MutationAuthorization;
import io.sirix.query.function.ImportInput;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;
import io.sirix.query.node.XmlDBStore;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.utils.SirixFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;

/** Imports inline XML as one FMSE revision; embedded callers may also supply a local file path. */
public final class Import extends AbstractFunction {
  public static final QNm IMPORT = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "import");

  public Import(final QNm name, final Signature signature) {
    // Self-committing function: authorization uses the resolved resource, including CREATE
    // for a missing resource, rather than the query-level global MODIFY precheck.
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final String databaseName = ((Str) args[0]).stringValue();
    final String resourceName = ((Str) args[1]).stringValue();
    final String source = ((Str) args[2]).stringValue();
    final XmlDBStore store = (XmlDBStore) ctx.getNodeStore();
    final XmlDBCollection initial = store.lookup(databaseName);
    if (initial == null) {
      throw new QueryException(new QNm("Database does not exist"));
    }
    final Path databasePath = initial.getDatabase().getDatabaseConfig().getDatabaseFile();
    try (var gate = MutationGate.acquireResource(databasePath, resourceName)) {
      final XmlDBCollection collection = store.lookup(databaseName);
      if (collection == null) {
        throw new QueryException(new QNm("Database does not exist"));
      }
      if (!collection.getDatabase().existsResource(resourceName)) {
        MutationAuthorization.requireCreate(ctx, databaseName, resourceName);
        final String content = content(store, source);
        return collection.add(resourceName, new DocumentParser(content));
      }
      final XmlDBNode document = collection.getDocument(resourceName);
      try {
        MutationAuthorization.requireWritable(ctx, document);
        final boolean fileSource = !(store instanceof ContentOnlyStore) && !source.stripLeading().startsWith("<");
        final String content = fileSource ? null : content(store, source);
        final Path temporary = Files.createTempDirectory("sirix-xml-import-");
        try {
          if (fileSource) {
            new FMSEImport().shredder(Path.of(source), temporary);
          } else {
            final XMLEventReader input = XmlShredder.createStringReader(content);
            try {
              new FMSEImport().shredder(input, temporary);
            } finally {
              input.close();
            }
          }
          try (final var database = Databases.openXmlDatabase(temporary);
              final var session = database.beginResourceSession("shredded");
              final var reader = session.beginNodeReadOnlyTrx();
              final var writer = document.getTrx().getResourceSession().beginNodeTrx();
              final var diff = FMSE.createInstance(new DefaultNodeComparisonFactory())) {
            diff.diff(writer, reader);
          }
        } finally {
          SirixFiles.recursiveRemove(temporary);
        }
      } finally {
        document.getTrx().close();
      }
      return collection.getDocument(resourceName);
    } catch (final IOException | XMLStreamException failure) {
      throw new QueryException(failure, new QNm("XML import failed"));
    }
  }

  private static String content(final XmlDBStore store, final String source) throws IOException {
    if (store instanceof ContentOnlyStore || source.stripLeading().startsWith("<")) {
      ImportInput.validateXml(source);
      return source;
    }
    return Files.readString(Path.of(source));
  }
}
