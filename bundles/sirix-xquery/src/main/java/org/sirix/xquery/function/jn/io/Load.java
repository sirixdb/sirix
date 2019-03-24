package org.sirix.xquery.function.jn.io;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.FunctionConversionSequence;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.brackit.xquery.util.io.URIHandler;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
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
import com.google.gson.stream.JsonReader;

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
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.OneOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
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
      final String resName = FunUtil.getString(args, 1, "resName", "resource", null, false);

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

  private static void add(final JsonDBStore store, final JsonDBCollection coll, final String resName,
      final Sequence resources) {
    if (resources instanceof Atomic) {
      final Atomic res = (Atomic) resources;
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(res.stringValue())))) {
        coll.add(resName, reader);
      } catch (final Exception e) {
        throw new QueryException(new QNm(e.getMessage()), e);
      }
    } else if (resources instanceof FunctionConversionSequence) {
      final FunctionConversionSequence seq = (FunctionConversionSequence) resources;
      final Iter iter = seq.iterate();
      int size = coll.getDatabase().listResources().size();
      try {
        for (Item item = null; (item = iter.next()) != null;) {
          try (final JsonReader reader =
              new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) item).stringValue())))) {
            coll.add("resource" + size++, reader);
          } catch (final Exception e) {
            throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
          }
        }
      } finally {
        iter.close();
      }
    }
  }

  private JsonDBCollection create(final JsonDBStore store, final String collName, final String resName,
      final Sequence resources) throws IOException {
    if (resources instanceof Str) {
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) resources).stringValue())))) {
        return store.create(collName, resName, reader);
      } catch (final Exception e) {
        throw new QueryException(new QNm("Failed to inser subtree: " + e.getMessage()));
      }
    } else if (resources instanceof FunctionConversionSequence) {
      final FunctionConversionSequence seq = (FunctionConversionSequence) resources;
      final Iter iter = seq.iterate();
      try {
        final Set<JsonReader> jsonReaders = new HashSet<>();

        for (Item item = null; (item = iter.next()) != null;) {
          jsonReaders.add(new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) item).stringValue()))));
        }

        final JsonDBCollection collection = store.create(collName, jsonReaders);

        jsonReaders.forEach(reader -> closeReader(reader));

        return collection;
      } finally {
        iter.close();
      }
    }

    return null;
  }

  private void closeReader(JsonReader reader) {
    try {
      reader.close();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
