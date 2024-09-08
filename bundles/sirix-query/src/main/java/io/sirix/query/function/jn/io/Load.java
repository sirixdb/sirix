package io.sirix.query.function.jn.io;

import com.google.gson.stream.JsonReader;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.DateTime;
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
import io.brackit.query.sequence.FunctionConversionSequence;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.brackit.query.util.io.URIHandler;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * Function for loading a document in a collection/database. The Supported signature is:
 * </p>
 *
 * <pre>
 * <code>jn:load($coll as xs:string, $res as xs:string, $fragment as xs:string, $create-new as xs:boolean?) as json-item()?</code>
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
  public final static QNm LOAD = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "load");

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Constructor.
   *
   * @param createIfNotExists determines if a new collection has to be created or not
   */
  public Load(final boolean createIfNotExists) {
    this(LOAD, createIfNotExists);
  }

  /**
   * Constructor.
   *
   * @param name the function name
   * @param createIfNotExists determines if a new collection has to be created or not
   */
  public Load(final QNm name, final boolean createIfNotExists) {
    super(name, createIfNotExists
        ? new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.OneOrMany))
        : new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.OneOrMany),
            new SequenceType(AtomicType.BOOL, Cardinality.One)),
        true);
  }

  /**
   * Constructor.
   *
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public Load(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext staticContext, final QueryContext queryContext, final Sequence[] args) {
    try {
      final String collectionName = FunUtil.getString(args, 0, "collectionName", "collection", null, true);
      final String resourceName = FunUtil.getString(args, 1, "resourceName", "resource", null, false);
      final Sequence resources = args[2];
      final boolean createIfNotExists = args.length < 4 || args[3].booleanValue();
      final JsonDBStore store = (JsonDBStore) queryContext.getJsonItemStore();
      final Object options;

      if (resources == null) {
        throw new QueryException(new QNm("No sequence of resources specified!"));
      }

      if (args.length >= 5) {
        options = (Object) args[4];
      } else {
        options = new ArrayObject(new QNm[0], new Sequence[0]);
      }

      JsonDBCollection collection;

      if (createIfNotExists) {
        collection = create(store, collectionName, resourceName, resources, options);
      } else {
        try {
          collection = store.lookup(collectionName);
          add(collection, resourceName, resources, options);
        } catch (final DocumentException e) {
          // collection does not exist
          collection = create(store, collectionName, resourceName, resources, options);
        }
      }

      return collection;
    } catch (final Exception e) {
      throw new QueryException(new QNm(e.getMessage()), e);
    }
  }

  private static void add(final JsonDBCollection collection, final String resourceName,
      final Sequence resources, final Object options) {
    if (resources instanceof Atomic res) {
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(res.stringValue())))) {
        collection.add(resourceName, reader, options);
      } catch (final Exception e) {
        throw new QueryException(new QNm(e.getMessage()), e);
      }
    } else if (resources instanceof FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        int size = collection.getDatabase().listResources().size();
        for (Item item; (item = iter.next()) != null; ) {
          try (final JsonReader reader = new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) item).stringValue())))) {
            collection.add("resource" + size++, reader, options);
          } catch (final Exception e) {
            throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
          }
        }
      }
    }
  }

  private JsonDBCollection create(final JsonDBStore store, final String collectionName, final String resourceName,
      final Sequence resources, final Object options) throws IOException {
    if (resources instanceof Atomic atomic) {
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(atomic.stringValue())))) {
        return store.create(collectionName, resourceName, reader, options);
      } catch (final Exception e) {
        throw new QueryException(new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    } else if (resources instanceof FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        final Set<JsonReader> jsonReaders = new HashSet<>();

        for (Item item; (item = iter.next()) != null; ) {
          jsonReaders.add(new JsonReader(new InputStreamReader(URIHandler.getInputStream(((Str) item).stringValue()))));
        }

        final JsonDBCollection collection = store.create(collectionName, jsonReaders, options);

        jsonReaders.forEach(this::closeReader);

        return collection;
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
