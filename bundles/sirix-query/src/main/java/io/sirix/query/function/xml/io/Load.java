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
import io.sirix.query.function.FreeResourceName;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Loads XML documents into a collection/database. See {@link XMLFun} for the registered overloads
 * and the repository README for content sources and collection import semantics.
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(
    description = "Load the given resources into a collection. "
        + "A missing collection is created. In an existing collection a new resource name is added "
        + "and every other resource is kept; an existing name is refused with SIRIXDBF0006 "
        + "without writes, whatever $create-new says. "
        + "Several inputs go under new automatic names. No call drops a collection: "
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
   * @param createNew whether this overload omits the explicit {@code $create-new} argument
   */
  public Load(final boolean createNew) {
    this(LOAD, createNew);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param createNew whether this overload omits the explicit {@code $create-new} argument
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
      @SuppressWarnings("NullAway")
      final @Nullable String resName = FunUtil.getString(args, 1, "resName", null, null, false);
      final Sequence resources = args[2];
      if (resources == null) {
        throw new QueryException(new QNm("No sequence of resources specified!"));
      }
      final String commitMessage = args.length >= 5
          ? FunUtil.getString(args, 4, "commitMessage", null, null, false)
          : null;
      final Instant commitTimestamp = args.length == 6
          ? dateTimeToInstant.convert((DateTime) args[5])
          : null;
      final XmlDBStore store = (XmlDBStore) ctx.getNodeStore();
      MutationAuthorization.requireCreate(ctx, collName, resName);
      final boolean contentOnly = store instanceof ContentOnlyStore;
      final Sequence inputs = contentOnly
          ? ImportInput.strings(resources, true)
          : resources;
      try (final ParserStream parsers = new ParserStream(inputs, contentOnly)) {
        final boolean singleton = parsers.size() == 1;
        final Path databasePath = store.databaseDirectory(collName);
        XmlDBCollection coll = store.lookup(collName);
        if (coll == null) {
          try (var gate = databasePath == null
              ? null
              : MutationGate.acquireDatabase(databasePath)) {
            coll = store.lookup(collName);
            if (coll == null) {
              return singleton
                  ? store.create(collName, resName, parsers.next(), commitMessage, commitTimestamp)
                  : store.create(collName, parsers);
            }
          }
        }
        if (singleton && resName != null) {
          final var database = coll.getDatabase();
          try (var gate = MutationGate.acquireResource(database.getDatabaseConfig().getDatabaseFile(), resName)) {
            FreeResourceName.require(XMLFun.ERR_RESOURCE_EXISTS, collName, database, resName);
            add(coll, resName, parsers, commitMessage, commitTimestamp);
          }
        } else {
          add(coll, null, parsers, commitMessage, commitTimestamp);
        }
        return coll;
      }
    } catch (final Exception e) {
      if (e instanceof QueryException refusal && XMLFun.ERR_RESOURCE_EXISTS.equals(refusal.getCode())) {
        throw refusal;
      }
      throw new QueryException(e, new QNm(e.getMessage()));
    }
  }

  @SuppressWarnings("NullAway")
  private static void add(final XmlDBCollection coll, final @Nullable String resName, final ParserStream parsers,
      final @Nullable String commitMessage, final @Nullable Instant commitTimestamp) {
    long next = coll.getDocumentCount() + 1;
    for (NodeSubtreeParser parser = parsers.next(); parser != null; parser = parsers.next()) {
      if (resName != null) {
        coll.importResource(resName, parser, commitMessage, commitTimestamp);
        continue;
      }
      final Path databaseFile = coll.getDatabase().getDatabaseConfig().getDatabaseFile();
      boolean written = false;
      while (!written) {
        final String candidate = "resource" + next++;
        try (var gate = MutationGate.acquireResource(databaseFile, candidate)) {
          if (!coll.getDatabase().existsResource(candidate)) {
            coll.importResource(candidate, parser, commitMessage, commitTimestamp);
            written = true;
          }
        }
      }
    }
  }

  private static final class ParserStream implements Stream<NodeSubtreeParser> {
    private final List<NodeSubtreeParser> parsers = new ArrayList<>();
    private final List<InputStream> sources = new ArrayList<>();
    private int next;

    private ParserStream(final Sequence resources, final boolean contentOnly) throws IOException {
      try (final Iter iterator = resources.iterate()) {
        for (Item item; (item = iterator.next()) != null;) {
          if (!(item instanceof Atomic resource)) {
            throw new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE,
                "Cannot create subtree parser for item of type: %s", item.itemType());
          }
          if (contentOnly) {
            parsers.add(new DocumentParser(resource.stringValue()));
          } else {
            final InputStream source = URIHandler.getInputStream(resource.stringValue());
            sources.add(source);
            parsers.add(new DocumentParser(source));
          }
        }
      } catch (final IOException | RuntimeException failure) {
        try {
          close();
        } catch (final RuntimeException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    private int size() {
      return parsers.size();
    }

    @Override
    public @Nullable NodeSubtreeParser next() {
      return next < parsers.size()
          ? parsers.get(next++)
          : null;
    }

    @Override
    public void close() {
      IOException failure = null;
      for (final InputStream source : sources) {
        try {
          source.close();
        } catch (final IOException cleanup) {
          if (failure == null) {
            failure = cleanup;
          } else {
            failure.addSuppressed(cleanup);
          }
        }
      }
      if (failure != null) {
        throw new DocumentException(failure);
      }
    }
  }
}
