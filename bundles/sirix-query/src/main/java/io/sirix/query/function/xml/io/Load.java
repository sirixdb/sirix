package io.sirix.query.function.xml.io;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.*;
import io.brackit.query.jdm.node.Node;
import io.brackit.query.jdm.node.TemporalNodeCollection;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.ElementType;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.node.parser.DocumentParser;
import io.brackit.query.node.parser.NodeStreamSubtreeParser;
import io.brackit.query.node.parser.NodeSubtreeHandler;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.brackit.query.util.io.URIHandler;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;

import java.io.IOException;
import java.time.Instant;

/**
 * <p>
 * Function for loading a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>xml:load($coll as xs:string, $res as xs:string, $fragment as xs:string, $create-new as xs:boolean?) as node()?</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Store the given fragments in a collection. "
    + "If explicitly required or if the collection does not exist, " + "a new collection will be created. ",
    parameters = { "$coll", "$res", "$fragments", "$create-new" })
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
   * @param name      the function name
   * @param createNew determines if a new collection has to be created or not
   */
  public Load(final QNm name, final boolean createNew) {
    super(name,
          createNew
              ? new Signature(new SequenceType(ElementType.ELEMENT, Cardinality.ZeroOrOne),
                              new SequenceType(AtomicType.STR, Cardinality.One),
                              new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                              new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))
              : new Signature(new SequenceType(ElementType.ELEMENT, Cardinality.ZeroOrOne),
                              new SequenceType(AtomicType.STR, Cardinality.One),
                              new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                              new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                              new SequenceType(AtomicType.BOOL, Cardinality.One)),
          true);
  }

  /**
   * Constructor.
   *
   * @param name      the function name
   * @param signature the signature
   */
  public Load(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, false);
      final Sequence resources = args[2];
      if (resources == null)
        throw new QueryException(new QNm("No sequence of resources specified!"));
      final boolean createNew = args.length < 4 || args[3].booleanValue();

      final String commitMessage =
          args.length >= 5 ? FunUtil.getString(args, 4, "commitMessage", null, null, false) : null;

      final DateTime dateTime = args.length == 6 ? (DateTime) args[5] : null;
      final Instant commitTimesstamp = args.length == 6 ? dateTimeToInstant.convert(dateTime) : null;

      final BasicXmlDBStore store = (BasicXmlDBStore) ctx.getNodeStore();
      XmlDBCollection coll;
      if (createNew) {
        coll = create(store, collName, resName, resources, commitMessage, commitTimesstamp);
      } else {
        try {
          coll = store.lookup(collName);
          add(coll, resName, resources, commitMessage, commitTimesstamp);
        } catch (final DocumentException e) {
          // collection does not exist
          coll = create(store, collName, resName, resources, commitMessage, commitTimesstamp);
        }
      }

      return coll;
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }
  }

  private static TemporalNodeCollection<?> add(final XmlDBCollection coll, final String resName,
      final Sequence resources, final String commitMessage, final Instant commitTimestamp) throws IOException {
    if (resources instanceof final Atomic res) {
      coll.add(resName, new DocumentParser(URIHandler.getInputStream(res.stringValue())), commitMessage, commitTimestamp);
    } else {
      try (ParserStream parsers = new ParserStream(resources)) {
        for (NodeSubtreeParser parser = parsers.next(); parser != null; parser = parsers.next()) {
          coll.add(resName, parser);
        }
      }
    }
    return coll;
  }

  private static XmlDBCollection create(final BasicXmlDBStore store, final String collName, final String resName,
      final Sequence resources, final String commitMessage, final Instant commitTimestamp) throws IOException {
    if (resources instanceof Atomic res) {
      return store.create(collName,
                          resName,
                          new DocumentParser(URIHandler.getInputStream(res.stringValue())),
                          commitMessage,
                          commitTimestamp);
    } else {
      return store.create(collName, new ParserStream(resources));
    }
  }

  private static class StoreParser implements NodeSubtreeParser{

    private final NodeStreamSubtreeParser parser;
    private final boolean intercept;

    public StoreParser(final Node<?> node) {
      parser = new NodeStreamSubtreeParser(node.getSubtree());
      intercept = (node.getKind() != Kind.DOCUMENT);
    }

    @Override
    public void parse(NodeSubtreeHandler handler) {
      if (intercept) {
        handler = new InterceptorHandler(handler);
      }
      parser.parse(handler);
    }
  }

  private static class InterceptorHandler implements NodeSubtreeHandler {
    private final NodeSubtreeHandler handler;

    public InterceptorHandler(final NodeSubtreeHandler handler) {
      this.handler = handler;
    }

    @Override
    public void beginFragment() {
      handler.beginFragment();
      handler.startDocument();
    }

    @Override
    public void endFragment() {
      handler.endDocument();
      handler.endFragment();
    }

    @Override
    public void startDocument() {
      handler.startDocument();
    }

    @Override
    public void endDocument() {
      handler.endDocument();
    }

    @Override
    public void text(final Atomic content) {
      handler.text(content);
    }

    @Override
    public void comment(final Atomic content) {
      handler.comment(content);
    }

    @Override
    public void processingInstruction(final QNm target, final Atomic content) {
      handler.processingInstruction(target, content);
    }

    @Override
    public void startMapping(final String prefix, final String uri) {
      handler.startMapping(prefix, uri);
    }

    @Override
    public void endMapping(final String prefix) {
      handler.endMapping(prefix);
    }

    @Override
    public void startElement(final QNm name) {
      handler.startElement(name);
    }

    @Override
    public void endElement(final QNm name) {
      handler.endElement(name);
    }

    @Override
    public void attribute(final QNm name, final Atomic value) {
      handler.attribute(name, value);
    }

    @Override
    public void begin() {
      handler.begin();
    }

    @Override
    public void end() {
      handler.end();
    }

    @Override
    public void fail() {
      handler.fail();
    }
  }

  private static class ParserStream implements Stream<NodeSubtreeParser> {
    private final Iter it;

    public ParserStream(final Sequence locs) {
      it = locs.iterate();
    }

    @Override
    public NodeSubtreeParser next() {
      try {
        final Item i = it.next();
        if (i == null) {
          return null;
        }
        if (i instanceof Node<?> n) {
          return new StoreParser(n);
        } else {
          throw new QueryException(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE,
                                   "Cannot create subtree parser for item of type: %s",
                                   i.itemType());
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
