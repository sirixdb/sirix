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
import io.brackit.query.jdm.Kind;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.jdm.type.AnyNodeType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.ElementType;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.node.parser.NodeStreamSubtreeParser;
import io.brackit.query.node.parser.NodeSubtreeHandler;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.access.MutationGate;
import io.sirix.query.MutationAuthorization;
import io.sirix.query.function.ExistingResourceTarget;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBStore;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * <p>
 * Function for storing a document in a collection/database.
 * </p>
 * <p>
 * Registered signatures are defined in {@link XMLFun}.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(
    description = "Store the given fragments in a collection. "
        + "A missing collection is created. In an existing collection a new resource name is added "
        + "and every other resource is kept; an existing name is replaced alone, with a new history, "
        + "when $create-new is true (the default) and refused with SIRIXDBF0006 when it is false. "
        + "Several fragments are added under new automatic names. No call drops a collection: "
        + "use drop-database first to replace one, and import to write into an existing resource.",
    parameters = {"$coll", "$res", "$fragments", "$create-new"})
public final class Store extends AbstractFunction {

  /**
   * Store function name.
   */
  public final static QNm STORE = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "store");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param createNew determines if a new collection has to be created or not
   */
  public Store(final boolean createNew) {
    this(STORE, createNew);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param createNew determines if a new collection has to be created or not
   */
  public Store(final QNm name, final boolean createNew) {
    super(name, createNew
        ? new Signature(new SequenceType(ElementType.ELEMENT, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AnyNodeType.ANY_NODE, Cardinality.ZeroOrMany))
        : new Signature(new SequenceType(ElementType.ELEMENT, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AnyNodeType.ANY_NODE, Cardinality.ZeroOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param signature the signature
   */
  public Store(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      final String resName = FunUtil.getString(args, 1, "resName", null, null, false);
      final Sequence nodes = args[2];
      if (nodes == null) {
        throw new QueryException(new QNm("No sequence of nodes specified!"));
      }
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

      // Store is an ordinary function call, so authorize CREATE before touching the store, as
      // xn:load and jn:store do; the query-level updating-expression check does not cover it.
      MutationAuthorization.requireCreate(ctx, collName, resName);

      // An existing database is never replaced (#1384). Only a missing one is created, under the
      // exclusive database hold so a caller racing to create it is seen.
      final XmlDBStore store = (XmlDBStore) ctx.getNodeStore();
      final Path databaseDirectory = store.databaseDirectory(collName);
      try (final Iter iterator = nodes.iterate()) {
        final ParserStream parsers = new ParserStream(iterator);
        final boolean singleton = parsers.readFirstParsers();
        XmlDBCollection coll = store.lookup(collName);
        if (coll == null) {
          try (var gate = databaseDirectory == null
              ? null
              : MutationGate.acquireDatabase(databaseDirectory)) {
            coll = store.lookup(collName);
            if (coll == null) {
              if (singleton) {
                store.create(collName, resName, parsers.next(), commitMessage, commitTimesstamp);
              } else {
                store.create(collName, parsers);
              }
              return null;
            }
          }
        }
        if (singleton) {
          // Cardinality, including sorted and lazy singletons, determines the named target.
          final String target = resName == null
              ? "resource"
              : resName;
          final var database = coll.getDatabase();
          try (var gate = MutationGate.acquireResource(database.getDatabaseConfig().getDatabaseFile(), target)) {
            ExistingResourceTarget.prepare(ctx, XMLFun.ERR_RESOURCE_EXISTS, collName, database, coll::remove, target,
                createNew);
            coll.add(target, parsers.next(), commitMessage, commitTimesstamp).getTrx().close();
          }
        } else {
          final List<NodeSubtreeParser> inputs = new ArrayList<>();
          for (NodeSubtreeParser parser = parsers.next(); parser != null; parser = parsers.next()) {
            inputs.add(parser);
          }
          coll.addAll(inputs);
        }
      }

      return null;
    } catch (final Exception e) {
      if (e instanceof QueryException refusal && XMLFun.ERR_RESOURCE_EXISTS.equals(refusal.getCode())) {
        throw refusal;
      }
      throw new QueryException(e, new QNm(e.getMessage()));
    }
  }

  private static class StoreParser implements NodeSubtreeParser {

    private final NodeStreamSubtreeParser parser;
    private final boolean intercept;

    public StoreParser(final Node<?> node) throws DocumentException {
      parser = new NodeStreamSubtreeParser(node.getSubtree());
      intercept = (node.getKind() != Kind.DOCUMENT);
    }

    @Override
    public void parse(NodeSubtreeHandler handler) throws DocumentException {
      if (intercept) {
        handler = new InterceptorHandler(handler);
      }
      parser.parse(handler);
    }
  }

  private record InterceptorHandler(NodeSubtreeHandler handler) implements NodeSubtreeHandler {

    @Override
    public void beginFragment() throws DocumentException {
      handler.beginFragment();
      handler.startDocument();
    }

    @Override
    public void endFragment() throws DocumentException {
      handler.endDocument();
      handler.endFragment();
    }

    @Override
    public void startDocument() throws DocumentException {
      handler.startDocument();
    }

    @Override
    public void endDocument() throws DocumentException {
      handler.endDocument();
    }

    @Override
    public void text(final Atomic content) throws DocumentException {
      handler.text(content);
    }

    @Override
    public void comment(final Atomic content) throws DocumentException {
      handler.comment(content);
    }

    @Override
    public void processingInstruction(final QNm target, final Atomic content) throws DocumentException {
      handler.processingInstruction(target, content);
    }

    @Override
    public void startMapping(final String prefix, final String uri) throws DocumentException {
      handler.startMapping(prefix, uri);
    }

    @Override
    public void endMapping(final String prefix) throws DocumentException {
      handler.endMapping(prefix);
    }

    @Override
    public void startElement(final QNm name) throws DocumentException {
      handler.startElement(name);
    }

    @Override
    public void endElement(final QNm name) throws DocumentException {
      handler.endElement(name);
    }

    @Override
    public void attribute(final QNm name, final Atomic value) throws DocumentException {
      handler.attribute(name, value);
    }

    @Override
    public void begin() throws DocumentException {
      handler.begin();
    }

    @Override
    public void end() throws DocumentException {
      handler.end();
    }

    @Override
    public void fail() throws DocumentException {
      handler.fail();
    }
  }

  private static class ParserStream implements Stream<NodeSubtreeParser> {
    private final Iter it;
    private @Nullable NodeSubtreeParser first;
    private @Nullable NodeSubtreeParser second;

    public ParserStream(final Iter iterator) {
      it = iterator;
    }

    private boolean readFirstParsers() {
      first = readNext();
      second = readNext();
      return first != null && second == null;
    }

    @Override
    public NodeSubtreeParser next() throws DocumentException {
      if (first != null) {
        final NodeSubtreeParser parser = first;
        first = second;
        second = null;
        return parser;
      }
      return readNext();
    }

    private NodeSubtreeParser readNext() throws DocumentException {
      try {
        final Item i = it.next();
        if (i == null) {
          return null;
        }
        if (i instanceof final Node<?> n) {
          return new StoreParser(n);
        } else {
          throw new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE,
              "Cannot create subtree parser for item of type: %s", i.itemType());
        }
      } catch (final QueryException e) {
        throw new DocumentException(e);
      }
    }

    @Override
    public void close() {}
  }
}
