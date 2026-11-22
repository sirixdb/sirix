package io.sirix.query.function.xml.io;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.node.TemporalNodeCollection;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.ElementType;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.brackit.query.util.io.URIHandler;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.ExistingResourceTarget;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBStore;
import io.sirix.query.ContentOnlyStore;
import io.sirix.query.MutationAuthorization;
import io.sirix.access.MutationGate;
import io.sirix.query.function.ImportInput;
import io.sirix.query.node.XmlDBCollection;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.nio.file.Path;

/**
 * Loads XML documents into a collection/database. See {@link XMLFun} for the registered overloads
 * and the repository README for content sources and collection import semantics.
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(
    description = "Load the given resources into a collection. "
        + "A missing collection is created. In an existing collection a new resource name is added "
        + "and every other resource is kept; an existing name is replaced alone, with a new history, "
        + "when $create-new is true (the default) and refused with SIRIXDBF0006 when it is false. "
        + "Several inputs go under new automatic names, or with $res all into that one resource "
        + "after the same check. No call drops a collection: "
        + "use drop-database first to replace one, and import to write into an existing resource.",
    parameters = {"$coll", "$res", "$fragments", "$create-new"})
public final class Load extends AbstractFunction {

  /**
   * Load function name.
   */
  public final static QNm LOAD = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "load");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param createNew determines if a new collection has to be created or not
   */
  public Load(final boolean createNew) {
    this(LOAD, createNew);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param createNew determines if a new collection has to be created or not
   */
  public Load(final QNm name, final boolean createNew) {
    super(name, createNew
        ? new Signature(new SequenceType(ElementType.ELEMENT, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))
        : new Signature(new SequenceType(ElementType.ELEMENT, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param signature the signature
   */
  public Load(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      @SuppressWarnings("NullAway") // FunUtil supports null defaults for absent optional arguments.
      final @Nullable String resName = FunUtil.getString(args, 1, "resName", null, null, false);
      final Sequence resources = args[2];
      if (resources == null)
        throw new QueryException(new QNm("No sequence of resources specified!"));
      final boolean createNew = args.length < 4 || args[3].booleanValue();

      final String commitMessage = args.length >= 5
          ? FunUtil.getString(args, 4, "commitMessage", null, null, false)
          : null;

      final DateTime dateTime = args.length == 6
          ? (DateTime) args[5]
          : null;
      final Instant commitTimesstamp = args.length == 6
          ? dateTimeToInstant.convert(dateTime)
          : null;

      final XmlDBStore store = (XmlDBStore) ctx.getNodeStore();
      MutationAuthorization.requireCreate(ctx, collName, resName);
      final boolean contentOnly = store instanceof ContentOnlyStore;
      final Sequence inputs = contentOnly
          ? ImportInput.strings(resources, true)
          : resources;
      // An existing database is never replaced (#1384); see xn:store.
      final Path databasePath = store.databaseDirectory(collName);
      XmlDBCollection coll = store.lookup(collName);
      if (coll == null) {
        try (var gate = databasePath == null
            ? null
            : MutationGate.acquireDatabase(databasePath)) {
          coll = store.lookup(collName);
          if (coll == null) {
            return create(store, collName, resName, inputs, commitMessage, commitTimesstamp, contentOnly);
          }
        }
      }
      if (resName != null) {
        // A named target is checked once, before any input is written: added, or replaced alone or
        // refused per $create-new. Several inputs are then all written into that resource.
        final var database = coll.getDatabase();
        try (var gate = MutationGate.acquireResource(database.getDatabaseConfig().getDatabaseFile(), resName)) {
          ExistingResourceTarget.prepare(ctx, XMLFun.ERR_RESOURCE_EXISTS, collName, database, coll::remove, resName,
              createNew);
          add(coll, resName, inputs, commitMessage, commitTimesstamp, contentOnly);
        }
      } else {
        add(coll, resName, inputs, commitMessage, commitTimesstamp, contentOnly);
      }
      return coll;
    } catch (final Exception e) {
      if (e instanceof QueryException refusal && XMLFun.ERR_RESOURCE_EXISTS.equals(refusal.getCode())) {
        throw refusal;
      }
      throw new QueryException(e, new QNm(e.getMessage()));
    }
  }

  @SuppressWarnings("NullAway") // XmlDBCollection's legacy signature omits nullable commit metadata.
  private static TemporalNodeCollection<?> add(final XmlDBCollection coll, final @Nullable String resName,
      final Sequence resources, final @Nullable String commitMessage, final @Nullable Instant commitTimestamp,
      final boolean contentOnly) {
    try (final ParserStream parsers = new ParserStream(resources, contentOnly)) {
      long resourceNumber = resName == null
          ? coll.getDocumentCount() + 1
          : 0;
      for (NodeSubtreeParser parser = parsers.next(); parser != null; parser = parsers.next()) {
        final String resourceName = resName == null
            ? "resource" + resourceNumber++
            : resName;
        coll.add(resourceName, parser, commitMessage, commitTimestamp).getTrx().close();
      }
    }
    return coll;
  }

  private static XmlDBCollection create(final XmlDBStore store, final String collName, final @Nullable String resName,
      final Sequence resources, final @Nullable String commitMessage, final @Nullable Instant commitTimestamp,
      final boolean contentOnly) {
    if (resources instanceof Atomic res) {
      return store.create(collName, resName, parser(res, contentOnly), commitMessage, commitTimestamp);
    } else {
      return store.create(collName, new ParserStream(resources, contentOnly));
    }
  }

  private static NodeSubtreeParser parser(final Atomic resource, final boolean contentOnly) {
    if (contentOnly) {
      return new DocumentParser(resource.stringValue());
    }
    final String uri = resource.stringValue();
    return handler -> {
      try (final InputStream input = URIHandler.getInputStream(uri)) {
        new DocumentParser(input).parse(handler);
      } catch (final IOException e) {
        throw new DocumentException(e);
      }
    };
  }

  private static class ParserStream implements Stream<NodeSubtreeParser> {
    private final Iter it;
    private final boolean contentOnly;

    public ParserStream(final Sequence locs, final boolean contentOnly) {
      it = locs.iterate();
      this.contentOnly = contentOnly;
    }

    @Override
    public NodeSubtreeParser next() {
      try {
        final Item i = it.next();
        if (i == null) {
          return null;
        }
        if (i instanceof Atomic resource) {
          return parser(resource, contentOnly);
        } else {
          throw new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE,
              "Cannot create subtree parser for item of type: %s", i.itemType());
        }
      } catch (final QueryException e) {
        throw new DocumentException(e);
      }
    }

    @Override
    public void close() {
      it.close();
    }
  }
}
