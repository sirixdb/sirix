package org.sirix.xquery.function.jn.io;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.sequence.FunctionConversionSequence;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.xdm.*;
import org.brackit.xquery.xdm.type.AnyJsonItemType;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.rest.AuthRole;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.xquery.function.FunUtil;
import org.sirix.xquery.function.Roles;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBStore;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Function for storing a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>jn:store($coll as xs:string, $res as xs:string, $fragment as xs:string, $create-new as xs:boolean?) as json-item()</code>
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
  public final static QNm STORE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "store");

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
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collName = FunUtil.getString(args, 0, "collName", "collection", null, true);
      final Sequence nodes = args[2];
      if (nodes == null) {
        throw new QueryException(new QNm("No sequence of nodes specified!"));
      }
      final boolean createNew = args.length != 4 || args[3].booleanValue();
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, false);

      Roles.check(ctx, collName, AuthRole.CREATE);

      final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();
      if (createNew) {
        create(store, collName, resName, nodes);
      } else {
        try {
          final JsonDBCollection coll = store.lookup(collName);
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

  private static void add(final JsonDBCollection coll, final String resName,
      final Sequence nodes) {
    if (nodes instanceof Str) {
      try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
        coll.add(resName, reader);
      } catch (final Exception e) {
        throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    } else if (nodes instanceof FunctionConversionSequence) {
      final FunctionConversionSequence seq = (FunctionConversionSequence) nodes;
      try (final Iter iter = seq.iterate()) {
        int size = coll.getDatabase().listResources().size();
        for (Item item; (item = iter.next()) != null; ) {
          try (final JsonReader reader = JsonShredder.createStringReader(((Str) item).stringValue())) {
            coll.add("resource" + size++, reader);
          } catch (final Exception e) {
            throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
          }
        }
      }
    }
  }

  private static void create(final JsonDBStore store, final String collName, final String resName,
      final Sequence nodes) {
    if (nodes instanceof Str) {
      final var string = (Str) nodes;
      if (string.stringValue().isEmpty()) {
        store.create(collName, resName, (String) null);
      } else {
        try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
          store.create(collName, resName, reader);
        } catch (final Exception e) {
          throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
        }
      }
    } else if (nodes instanceof FunctionConversionSequence) {
      final FunctionConversionSequence seq = (FunctionConversionSequence) nodes;
      try (final Iter iter = seq.iterate()) {
        final List<Str> list = new ArrayList<>();

        for (Item item; (item = iter.next()) != null; ) {
          list.add((Str) item);
        }

        store.createFromJsonStrings(collName, new ArrayStream<>(list.toArray(new Str[0])));
      }
    }
  }
}
