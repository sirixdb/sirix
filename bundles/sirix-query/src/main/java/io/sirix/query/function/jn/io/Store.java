package io.sirix.query.function.jn.io;

import com.google.gson.stream.JsonReader;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
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
import io.brackit.query.node.stream.ArrayStream;
import io.brackit.query.sequence.FunctionConversionSequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.access.MutationGate;
import io.sirix.query.MutationAuthorization;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.function.ExistingResourceTarget;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
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
        + "A missing collection is created. In an existing collection a new resource name is added "
        + "and every other resource is kept; an existing name is replaced alone, with a new history, "
        + "when $create-new is true (the default) and refused with SIRIXDBF0006 when it is false. "
        + "Several fragments are added under new automatic names. No call drops a collection: "
        + "use drop-database first to replace one, and import to write into an existing resource.",
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
   * @param name the name of the function
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

      // Store/load are ordinary function calls, so require CREATE before touching the store or
      // reading input; the query-level updating-expression check does not cover them.
      MutationAuthorization.requireCreate(ctx, collectionName, resourceName);

      // An existing database is never replaced (#1384). Only a missing one is created, under the
      // exclusive database hold so a caller racing to create it is seen; writing into an existing
      // database takes only the target resource's hold.
      final Path databaseDirectory = store.databaseDirectory(collectionName);
      JsonDBCollection collection = store.lookup(collectionName);
      if (collection == null) {
        try (var gate = databaseDirectory == null
            ? null
            : MutationGate.acquireDatabase(databaseDirectory)) {
          collection = store.lookup(collectionName);
          if (collection == null) {
            create(store, collectionName, resourceName, nodes, options);
            return null;
          }
        }
      }
      write(ctx, collection, collectionName, resourceName, nodes, createIfNotExists, options);

      return null;
    } catch (final Exception e) {
      if (e instanceof QueryException refusal && JNFun.ERR_RESOURCE_EXISTS.equals(refusal.getCode())) {
        throw refusal;
      }
      // (Throwable, QNm) is the cause-attaching overload. The (QNm, Object) one this used to bind
      // to renders the throwable into the message and drops it, so a failing store — an async-flush
      // SirixIOException, say — arrived with no `Caused by:` chain and was undiagnosable.
      throw new QueryException(e, new QNm(e.getMessage()));
    }
  }

  /**
   * Write into the existing database: one item goes to {@code resourceName}, which is added, or
   * replaced alone or refused per {@code createNew}; several items are added under new names.
   */
  static void write(final QueryContext ctx, final JsonDBCollection collection, final String collectionName,
      final String resourceName, final Sequence nodes, final boolean createNew, final Object options) {
    if (nodes instanceof Str) {
      final var database = collection.getDatabase();
      try (var gate = MutationGate.acquireResource(database.getDatabaseConfig().getDatabaseFile(), resourceName)) {
        ExistingResourceTarget.prepare(ctx, JNFun.ERR_RESOURCE_EXISTS, collectionName, database, collection::remove,
            resourceName, createNew);
        add(collection, resourceName, nodes, options);
      }
    } else {
      add(collection, resourceName, nodes, options);
    }
  }

  private static void add(final JsonDBCollection collection, final String resourceName, final Sequence nodes,
      final Object options) {
    if (nodes instanceof Str) {
      try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
        collection.add(resourceName, reader, options);
      } catch (final Exception e) {
        throw new QueryException(e, new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    } else if (nodes instanceof FunctionConversionSequence || nodes instanceof ItemSequence) {
      final List<JsonReader> readers = new ArrayList<>();
      try (final Closeable inputs = () -> closeReaders(readers); final Iter iter = nodes.iterate()) {
        for (Item item; (item = iter.next()) != null;) {
          readers.add(JsonShredder.createStringReader(((Str) item).stringValue()));
        }
        collection.addAll(readers, options);
      } catch (final Exception e) {
        throw new QueryException(e, new QNm("Failed to insert subtree: " + e.getMessage()));
      }
    }
  }

  private static void closeReaders(final List<JsonReader> readers) throws IOException {
    IOException failure = null;
    for (final JsonReader reader : readers) {
      try {
        reader.close();
      } catch (final IOException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void create(final JsonDBStore store, final String collectionName, final String resourceName,
      final Sequence nodes, Object options) {
    if (nodes instanceof Str string) {
      if (string.stringValue().isEmpty()) {
        store.create(collectionName, resourceName, (String) null, options);
      } else {
        try (final JsonReader reader = JsonShredder.createStringReader(((Str) nodes).stringValue())) {
          store.create(collectionName, resourceName, reader, options);
        } catch (final Exception e) {
          throw new QueryException(e, new QNm("Failed to insert subtree: " + e.getMessage()));
        }
      }
    } else if (nodes instanceof FunctionConversionSequence || nodes instanceof ItemSequence) {
      try (final Iter iter = nodes.iterate()) {
        final List<Str> list = new ArrayList<>();

        for (Item item; (item = iter.next()) != null;) {
          list.add((Str) item);
        }

        store.createFromJsonStrings(collectionName, new ArrayStream<>(list.toArray(new Str[0])), options);
      }
    }
  }
}
