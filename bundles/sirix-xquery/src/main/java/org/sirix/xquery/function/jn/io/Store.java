package org.sirix.xquery.function.jn.io;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.type.AnyJsonItemType;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBStore;

/**
 * <p>
 * Function for storing a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>sdb:store($coll as xs:string, $res as xs:string, $fragment as xs:node, $create-new as xs:boolean?) as ()</code>
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
  public final static QNm STORE = new QNm(JNFun.JN_NSURI, JNFun.JN_PREFIX, "store");

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
        ? new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
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
      final boolean createNew = args.length == 4
          ? args[3].booleanValue()
          : true;
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, createNew
          ? false
          : true);

      final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();
      if (createNew) {
        create(store, collName, resName, nodes);
      } else {
        try {
          final JsonDBCollection coll = store.lookup(collName);
          add(store, coll, resName, nodes);
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

  private static void add(final JsonDBStore store, final JsonDBCollection coll, final String resName,
      final Sequence nodes) {
    // if
    // coll.add
    // final ParserStream parsers = new ParserStream(nodes);
    // try {
    // for (SubtreeParser parser = parsers.next(); parser != null; parser = parsers.next()) {
    // coll.add(resName, parser);
    // }
    // } finally {
    // parsers.close();
    // }
    // }

  }

  private static void create(final JsonDBStore store, final String collName, final String resName,
      final Sequence nodes) {
    if (nodes instanceof Str) {
      store.create(collName, resName, ((Str) nodes).stringValue());
    }
  }
}
