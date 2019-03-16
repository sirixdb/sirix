package org.sirix.xquery.function.jn.io;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
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
 * Function for loading a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>sdb:load($coll as xs:string, $res as xs:string, $fragment as xs:string, $create-new as xs:boolean?) as json-item()?</code>
 * </pre>
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionAnnotation(
    description = "Store the given fragments in a collection. "
        + "If explicitly required or if the collection does not exist, " + "a new collection will be created. ",
    parameters = {"$coll", "$res", "$fragments", "$create-new"})
public final class Load extends AbstractFunction {

  /** Load function name. */
  public final static QNm LOAD = new QNm(JNFun.JN_NSURI, JNFun.JN_PREFIX, "load");

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
        ? new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.OneOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.OneOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      final Sequence resources = args[2];
      if (resources == null)
        throw new QueryException(new QNm("No sequence of resources specified!"));
      final boolean createNew = args.length == 4
          ? args[3].booleanValue()
          : true;
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, createNew
          ? false
          : true);

      final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();
      JsonDBCollection coll;
      if (createNew) {
        coll = create(store, collName, resName, resources);
      } else {
        try {
          coll = store.lookup(collName);
          add(store, coll, resName, resources);
        } catch (final DocumentException e) {
          // collection does not exist
          coll = create(store, collName, resName, resources);
        }
      }

      return coll;
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }
  }

  private static JsonDBCollection add(final JsonDBStore store, final JsonDBCollection coll, final String resName,
      final Sequence resources) {
    return null;
    // if (resources instanceof Atomic) {
    // final Atomic res = (Atomic) resources;
    // coll.add(resName, new DocumentParser(URIHandler.getInputStream(res.stringValue())));
    // return coll;
    // } else {
    // final ParserStream parsers = new ParserStream(resources);
    // try {
    // for (SubtreeParser parser = parsers.next(); parser != null; parser = parsers.next()) {
    // coll.add(resName, parser);
    // }
    // } finally {
    // parsers.close();
    // }
    // return coll;
    // }
  }

  private static JsonDBCollection create(final JsonDBStore store, final String collName, final String resName,
      final Sequence resources) {
    return null;
    // if (resources instanceof Atomic) {
    // final Atomic res = (Atomic) resources;
    // return store.create(collName, resName, new
    // DocumentParser(URIHandler.getInputStream(res.stringValue())));
    // } else {
    // return store.create(collName, new ParserStream(resources));
    // }
  }
}
