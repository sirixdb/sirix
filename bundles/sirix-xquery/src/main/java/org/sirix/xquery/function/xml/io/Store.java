package org.sirix.xquery.function.xml.io;

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.node.parser.StreamSubtreeParser;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.*;
import org.brackit.xquery.xdm.node.Node;
import org.brackit.xquery.xdm.type.*;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBStore;

/**
 * <p>
 * Function for storing a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>xml:store($coll as xs:string, $res as xs:string, $fragment as xs:node, $create-new as xs:boolean?) as ()</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionAnnotation(
    description = "Store the given fragments in a collection. "
        + "If explicitly required or if the collection does not exist, " + "a new collection will be created. ",
    parameters = {"$coll", "$res", "$fragments", "$create-new"})
public final class Store extends AbstractFunction {

  /** Store function name. */
  public final static QNm STORE = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "store");

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

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      final Sequence nodes = args[2];
      if (nodes == null)
        throw new QueryException(new QNm("No sequence of nodes specified!"));
      final boolean createNew = args.length != 4 || args[3].booleanValue();
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, !createNew);

      final XmlDBStore store = (XmlDBStore) ctx.getNodeStore();
      if (createNew) {
        create(store, collName, resName, nodes);
      } else {
        try {
          final XmlDBCollection coll = store.lookup(collName);
          add(coll, resName, nodes);
        } catch (final DocumentException e) {
          // collection does not exist
          create(store, collName, resName, nodes);
        }
      }

      return null;
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }
  }

  private static void add(final XmlDBCollection coll, final String resName, final Sequence nodes) throws DocumentException {
    if (nodes instanceof final Node<?> n) {
      coll.add(resName, new StoreParser(n));
    } else {
      try (ParserStream parsers = new ParserStream(nodes)) {
        for (SubtreeParser parser = parsers.next(); parser != null; parser = parsers.next()) {
          coll.add(parser);
        }
      }
    }
  }

  private static void create(final XmlDBStore store, final String collName, final String resName, final Sequence nodes)
      throws DocumentException {
    if (nodes instanceof final Node<?> n) {
      store.create(collName, resName, new StoreParser(n));
    } else {
      store.create(collName, new ParserStream(nodes));
    }
  }

  private static class StoreParser extends StreamSubtreeParser {
    private final boolean intercept;

    public StoreParser(final Node<?> node) throws DocumentException {
      super(node.getSubtree());
      intercept = (node.getKind() != Kind.DOCUMENT);
    }

    @Override
    public void parse(SubtreeHandler handler) throws DocumentException {
      if (intercept) {
        handler = new InterceptorHandler(handler);
      }
      super.parse(handler);
    }
  }

  private record InterceptorHandler(SubtreeHandler handler) implements SubtreeHandler {

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

  private static class ParserStream implements Stream<SubtreeParser> {
    private final Iter it;

    public ParserStream(final Sequence locs) {
      it = locs.iterate();
    }

    @Override
    public SubtreeParser next() throws DocumentException {
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
    public void close() {
      it.close();
    }
  }
}
