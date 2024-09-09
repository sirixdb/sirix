package io.sirix.query.function.jn.io;

import com.google.gson.stream.JsonReader;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.*;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jdm.type.AnyJsonItemType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.module.StaticContext;
import io.brackit.query.node.stream.ArrayStream;
import io.brackit.query.sequence.FunctionConversionSequence;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;

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
   * @param createIfNotExists determines if a new collection has to be created or not
   */
  public Store(final boolean createIfNotExists) {
    this(STORE, createIfNotExists);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param createIfNotExists determines if a new collection has to be created or not
   */
  public Store(final QNm name, final boolean createIfNotExists) {
    super(name, createIfNotExists
        ? new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public Store(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {

      final String collectionName = FunUtil.getString(args, 0, "collectionName", "collection", null, true);
      final String resourceName = FunUtil.getString(args, 1, "resourceName", "resource", null, false);
      final Sequence nodes = args[2];
      final boolean createIfNotExists = args.length < 4 || args[3].booleanValue();
      final Object options;
      final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();

      if (nodes == null) {
        throw new QueryException(new QNm("No sequence of nodes specified!"));
      }

      if (args.length >= 5) {
        options = (Object) args[4];
      } else {
        options = new ArrayObject(new QNm[0], new Sequence[0]);
      }

      if (createIfNotExists) {
        create(store, collectionName, resourceName, nodes, options);
      } else {
        try {
          final JsonDBCollection collection = store.lookup(collectionName);
          add(collection, resourceName, nodes, options);
        } catch (final DocumentException e) {
          // collection does not exist
          create(store, collectionName, resourceName, nodes, options);
        }
      }

      return null;
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }
  }

  private static void add(final JsonDBCollection collection, final String resourceName,
      final Sequence nodes, final Object options) {
    if (nodes instanceof Str) {
      try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
        collection.add(resourceName, reader, options);
      } catch (final Exception e) {
        throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    } else if (nodes instanceof final FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        int size = collection.getDatabase().listResources().size();
        for (Item item; (item = iter.next()) != null; ) {
          // TODO: use item shredder
          try (final JsonReader reader = JsonShredder.createStringReader(((Str) item).stringValue())) {
            collection.add("resource" + size++, reader, options);
          } catch (final Exception e) {
            throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
          }
        }
      }
    }
  }

  private static void create(final JsonDBStore store, final String collectionName, final String resourceName,
      final Sequence nodes, Object options) {
    if (nodes instanceof Str string) {
      if (string.stringValue().isEmpty()) {
        store.create(collectionName, resourceName, (String) null, options);
      } else {
        try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
          store.create(collectionName, resourceName, reader);
        } catch (final Exception e) {
          throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
        }
      }
    } else if (nodes instanceof final FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        final List<Str> list = new ArrayList<>();

        for (Item item; (item = iter.next()) != null; ) {
          list.add((Str) item);
        }

        store.createFromJsonStrings(collectionName, new ArrayStream<>(list.toArray(new Str[0])));
      }
    }
  }
}
