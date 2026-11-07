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
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
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
import io.sirix.access.MutationGate;
import io.sirix.query.MutationAuthorization;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.FunUtil;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;

import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Load(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    try {
      final String collectionName = FunUtil.getString(args, 0, "collectionName", "collection", null, true);
      final String resourceName = FunUtil.getString(args, 1, "resourceName", "resource", null, false);
      final Sequence resources = args[2];
      final boolean createIfNotExists = args.length < 4 || args[3].booleanValue();
      final JsonDBStore store = (JsonDBStore) ctx.getJsonItemStore();
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

      // Before the store is touched and before any input is read: storing and loading mutate but
      // are not updating expressions, so this is the only check standing between a caller
      // without write permission and the write.
      MutationAuthorization.requireCreate(ctx, collectionName, resourceName);

      // Only the create path can replace the whole database, so only it takes the exclusive
      // database hold. Appending into an existing collection takes nothing here: each target's
      // check and write are ordered on that resource's own hold, inside the collection, so two
      // callers writing different resources of one database never wait on each other. The gate
      // orders operations; it does not take ownership of sessions or sources, so the streaming
      // lifecycle and its cleanup are untouched.
      final Path databaseDirectory = store.databaseDirectory(collectionName);
      final boolean mayReplaceDatabase = createIfNotExists || store.lookup(collectionName) == null;
      try (var gate = databaseDirectory == null || !mayReplaceDatabase
          ? null
          : MutationGate.acquireDatabase(databaseDirectory)) {
        if (createIfNotExists) {
          collection = create(store, collectionName, resourceName, resources, options);
        } else {
          collection = store.lookup(collectionName);
          if (collection == null) {
            collection = create(store, collectionName, resourceName, resources, options);
          } else {
            add(collection, resourceName, resources, options);
          }
        }

      }

      return collection;
    } catch (final Exception e) {
      // (Throwable, QNm) is the cause-attaching overload. The (QNm, Object) one this used to bind to
      // renders the throwable into the message and drops it, so an authorization refusal reached the
      // caller with no HttpException in its chain and the REST failure handler could not map it to a
      // status code. Store.java carries the same fix.
      throw new QueryException(e, new QNm(e.getMessage()));
    }
  }

  private static void add(final JsonDBCollection collection, final String resourceName, final Sequence resources,
      final Object options) {
    if (resources instanceof Atomic res) {
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(res.stringValue())))) {
        collection.add(resourceName, reader, options);
      } catch (final Exception e) {
        throw new QueryException(e, new QNm(e.getMessage()));
      }
    } else if (resources instanceof FunctionConversionSequence seq) {
      final List<String> uris = new ArrayList<>();
      try (final Iter iter = seq.iterate()) {
        for (Item item; (item = iter.next()) != null;) {
          uris.add(((Str) item).stringValue());
        }
        collection.addAllFromUris(uris, options);
      } catch (final Exception e) {
        throw new QueryException(e, new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    }
  }

  private JsonDBCollection create(final JsonDBStore store, final String collectionName, final String resourceName,
      final Sequence resources, final Object options) {
    if (resources instanceof Atomic atomic) {
      try (final JsonReader reader =
          new JsonReader(new InputStreamReader(URIHandler.getInputStream(atomic.stringValue())))) {
        return store.create(collectionName, resourceName, reader, options);
      } catch (final Exception e) {
        throw new QueryException(e, new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    } else if (resources instanceof FunctionConversionSequence seq) {
      try (final Iter iter = seq.iterate()) {
        final List<String> uris = new ArrayList<>();
        for (Item item; (item = iter.next()) != null;) {
          uris.add(((Str) item).stringValue());
        }
        return store.createFromUris(collectionName, uris, options);
      }
    }

    return null;
  }
}
